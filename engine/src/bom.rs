//! Australian Bureau of Meteorology radar GridFamily adapters: one source
//! per radar, read from the Bureau's anonymous FTP (`ftp.rs`). Live fetch
//! only; no vendored imagery.
//!
//! The Bureau publishes no open measured grid, only 512×512 PNGs drawn in
//! a fixed fifteen-colour rain-rate scale. Each colour is one published
//! intensity level, so a pixel maps back to its level exactly and the frame
//! is classified values, not a pasted picture. The images are terms-of-use
//! restricted to personal use, which is why this adapter lives in a fork
//! (`docs/grid-adapters.md`, Attribution and license).
//!
//! A frame nests the 128 km product (`IDRnn3`, 500 m pixels) inside the
//! 256 km product (`IDRnn2`, 1 km pixels) of the same scan: both are
//! azimuthal about the radar, which sits at the image centre.

use crate::{
    ftp::Ftp,
    protocol::{
        AdapterTarget, Coverage, Crs, Ellipsoid, Family, FrameStatus, GeoPoint, Kind, MosaicFrame,
        ProductClass,
    },
    source::{GridEvent, MosaicMeta, SourceMetadataBorrowed},
    sweep,
};
use chrono::{NaiveDateTime, Utc};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashSet},
    io::Cursor,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::mpsc::Sender,
    task::{JoinHandle, spawn_blocking},
    time::{sleep, timeout},
};

pub const ATTRIBUTION: &str = "Bureau of Meteorology";
/// National radars rank above a continental fallback (OPERA is 10).
pub const SELECTION_PRIORITY: i32 = 20;

pub const HOST: &str = "ftp.bom.gov.au";
pub const DIR: &str = "/anon/gen/radar";

/// The outer product's range: the declared coverage circle.
pub const RANGE_KM: f64 = 256.0;
/// Both products are this many pixels square.
const SOURCE_SIZE: usize = 512;
/// The nested frame: the outer product doubled to the inner's 500 m pixels.
const SIZE: usize = SOURCE_SIZE * 2;
const PIXEL_M: f64 = 500.0;
/// Where the inner product's corner lands in the frame.
const INNER_OFFSET: usize = SOURCE_SIZE / 2;

/// The FTP keeps about a hundred minutes per product; a radar scans every
/// five to ten.
pub const HISTORY_MAX: usize = 18;
const BODY_MAX: usize = 1 << 20;
const LISTING_MAX: usize = 64 << 10;
const START_TIMEOUT: Duration = Duration::from_secs(45);
const CYCLE_TIMEOUT: Duration = Duration::from_secs(120);
const IDLE: Duration = Duration::from_secs(60);
const BACKFILL_DELAY: Duration = Duration::from_secs(3);

/// The Bureau's rain-rate colours, lightest first; level `n` is index
/// `n - 1`.
const LEVELS: [[u8; 3]; 15] = [
    [245, 245, 255],
    [180, 180, 255],
    [120, 120, 255],
    [20, 20, 255],
    [0, 216, 195],
    [0, 150, 144],
    [0, 102, 102],
    [255, 255, 0],
    [255, 200, 0],
    [255, 150, 0],
    [255, 100, 0],
    [255, 0, 0],
    [200, 0, 0],
    [120, 0, 0],
    [40, 0, 0],
];
/// Approximate rain rate at each level's lower edge, mm/h, as the Bureau's
/// legend states them; the last is the top of the scale.
pub const BOUNDS: [f64; 16] = [
    0.2, 0.5, 1.5, 2.5, 4.0, 6.0, 10.0, 15.0, 20.0, 35.0, 50.0, 80.0, 120.0, 200.0, 300.0, 360.0,
];

/// A decoded source pixel: `1..=15` is a level.
const UNDETECT: u8 = 0;
/// Not a measurement: the caption strips drawn over the image, or no image.
const MISSING: u8 = 255;

#[derive(Deserialize)]
struct SiteTable {
    sites: Vec<Site>,
}

#[derive(Deserialize)]
struct Site {
    id: String,
    name: String,
    lat: f64,
    lon: f64,
}

/// One radar: the product code `IDRnn` is the source id.
pub struct BomRadar {
    pub id: String,
    name: String,
    lat: f64,
    lon: f64,
    coverage: Coverage,
    palette: Arc<Vec<String>>,
}

/// Every radar in `data/bom_sites.json` (`scripts/refresh-bom-sites.py`).
pub fn radars() -> Vec<BomRadar> {
    let table: SiteTable = serde_json::from_str(include_str!("../data/bom_sites.json")).unwrap();
    let template: crate::protocol::Frame =
        serde_json::from_str(include_str!("../data/product.json")).unwrap();
    let palette = Arc::new(resample_palette(&template.palette, LEVELS.len()));
    table
        .sites
        .into_iter()
        .map(|site| BomRadar {
            coverage: Coverage::Circle {
                lat: Some(site.lat),
                lon: Some(site.lon),
                radius_km: RANGE_KM,
            },
            id: site.id,
            name: site.name,
            lat: site.lat,
            lon: site.lon,
            palette: Arc::clone(&palette),
        })
        .collect()
}

/// The product ramp stretched to `classes` evenly spaced colours, so the
/// Bureau's fifteen levels draw in the same ramp as every other source.
pub fn resample_palette(palette: &[String], classes: usize) -> Vec<String> {
    let rgb: Vec<[f64; 3]> = palette
        .iter()
        .filter_map(|c| {
            let hex = c.strip_prefix('#')?;
            let channel = |i: usize| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok();
            Some([channel(0)? as f64, channel(2)? as f64, channel(4)? as f64])
        })
        .collect();
    if rgb.len() != palette.len() || rgb.len() < 2 || classes < 2 {
        return palette.to_vec();
    }
    (0..classes)
        .map(|i| {
            let t = i as f64 * (rgb.len() - 1) as f64 / (classes - 1) as f64;
            let lo = (t.floor() as usize).min(rgb.len() - 2);
            let f = t - lo as f64;
            let mix = |k: usize| (rgb[lo][k] + (rgb[lo + 1][k] - rgb[lo][k]) * f).round() as u8;
            format!("#{:02x}{:02x}{:02x}", mix(0), mix(1), mix(2))
        })
        .collect()
}

impl BomRadar {
    pub fn metadata(&self) -> SourceMetadataBorrowed<'_> {
        SourceMetadataBorrowed {
            id: &self.id,
            family: Family::Grid,
            kind: Kind::Mosaic,
            default_product_class: ProductClass::PrecipitationRate,
            name: &self.name,
            attribution: ATTRIBUTION,
            mosaic: Some(MosaicMeta {
                coverage: &self.coverage,
                selection_priority: SELECTION_PRIORITY,
                covering: true,
            }),
        }
    }

    pub fn location(&self) -> GeoPoint {
        GeoPoint {
            lat: self.lat,
            lon: self.lon,
        }
    }

    fn plan(&self) -> Plan {
        Plan {
            id: self.id.clone(),
            lat: self.lat,
            lon: self.lon,
            palette: Arc::clone(&self.palette),
        }
    }

    pub fn loading_placeholder(&self) -> Option<(MosaicFrame, Vec<u8>)> {
        let mut frame = self.plan().frame(None);
        frame.width = 1;
        frame.height = 1;
        frame.geotransform = [0.0, 1.0, 0.0, 0.0, 0.0, -1.0];
        Some((frame, sweep::png(1, 1, &[0, 0, 0, 0]).ok()?))
    }

    pub fn poll(
        &self,
        target: &AdapterTarget,
        events: Sender<GridEvent>,
        known: HashSet<String>,
    ) -> Option<JoinHandle<()>> {
        match target {
            AdapterTarget::Mosaic => Some(tokio::spawn(poll_loop(self.plan(), events, known))),
            AdapterTarget::Site { .. } => None,
        }
    }
}

/// What a poller needs from its radar, owned so the task is `'static`.
#[derive(Clone)]
struct Plan {
    id: String,
    lat: f64,
    lon: f64,
    palette: Arc<Vec<String>>,
}

impl Plan {
    /// Azimuthal about the radar. The Bureau does not name its projection;
    /// equal-area and equidistant differ by under 30 m at 256 km.
    fn crs(&self) -> Crs {
        Crs::LambertAzimuthalEqualArea {
            ellipsoid: Ellipsoid::WGS84,
            lat0_deg: self.lat,
            lon0_deg: self.lon,
            false_easting_m: 0.0,
            false_northing_m: 0.0,
            datum_transform: None,
        }
    }

    /// The frame for `stamp`, or the loading frame (empty `scan_time` draws
    /// no radar) while the first one is in flight.
    fn frame(&self, stamp: Option<NaiveDateTime>) -> MosaicFrame {
        let half = SIZE as f64 / 2.0 * PIXEL_M;
        MosaicFrame {
            id: match stamp {
                Some(stamp) => format!("{}-{}", self.id, stamp.format("%Y%m%dT%H%M%SZ")),
                None => format!("{}-loading", self.id),
            },
            product: "RATE".into(),
            product_name: "Rain rate (est.)".into(),
            units: "mm/h".into(),
            scan_time: stamp
                .map(|s| s.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_default(),
            sweep_end: None,
            status: if stamp.is_some() {
                FrameStatus::Complete
            } else {
                FrameStatus::Partial
            },
            texture: String::new(),
            width: SIZE as u32,
            height: SIZE as u32,
            crs: self.crs(),
            geotransform: [-half, PIXEL_M, 0.0, half, 0.0, -PIXEL_M],
            palette: self.palette.to_vec(),
            bounds: BOUNDS.to_vec(),
        }
    }
}

/// One scan time and which range products the listing has for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scan {
    pub stamp: NaiveDateTime,
    pub outer: bool,
    pub inner: bool,
}

impl Scan {
    fn key(&self) -> String {
        self.stamp.format("%Y%m%d%H%M").to_string()
    }
}

/// `IDR713.T.202610030539.png` → (`'3'`, 2026-10-03 05:39).
pub fn parse_name(code: &str, name: &str) -> Option<(char, NaiveDateTime)> {
    let rest = name.rsplit('/').next()?.strip_prefix(code)?;
    let mut chars = rest.chars();
    let tier = chars.next().filter(|t| matches!(t, '2' | '3'))?;
    let stamp = chars.as_str().strip_prefix(".T.")?.strip_suffix(".png")?;
    Some((
        tier,
        NaiveDateTime::parse_from_str(stamp, "%Y%m%d%H%M").ok()?,
    ))
}

/// Scans a listing can show, oldest first. The newest waits until every
/// product the radar publishes has arrived for it, so a frame is not built
/// from half a scan; an older scan missing a product draws what exists.
pub fn ready_scans(code: &str, names: &[String]) -> Vec<Scan> {
    let mut scans: BTreeMap<NaiveDateTime, (bool, bool)> = BTreeMap::new();
    for (tier, stamp) in names.iter().filter_map(|n| parse_name(code, n)) {
        let entry = scans.entry(stamp).or_default();
        match tier {
            '2' => entry.0 = true,
            _ => entry.1 = true,
        }
    }
    let publishes_outer = scans.values().any(|s| s.0);
    let publishes_inner = scans.values().any(|s| s.1);
    let newest = scans.keys().next_back().copied();
    scans
        .into_iter()
        .filter(|&(stamp, (outer, inner))| {
            Some(stamp) != newest || (outer == publishes_outer && inner == publishes_inner)
        })
        .map(|(stamp, (outer, inner))| Scan {
            stamp,
            outer,
            inner,
        })
        .collect()
}

/// Decode one product PNG to a level per pixel. Transparent is no echo;
/// any colour outside the scale (the caption strips) is missing.
pub fn decode_layer(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = png::Decoder::new(Cursor::new(bytes));
    decoder.set_transformations(png::Transformations::normalize_to_color8());
    let mut reader = decoder.read_info().map_err(|e| format!("png: {e}"))?;
    let (width, height) = reader.info().size();
    if (width as usize, height as usize) != (SOURCE_SIZE, SOURCE_SIZE) {
        return Err(format!("unexpected image size {width}x{height}"));
    }
    let size = reader.output_buffer_size().ok_or("png: image too large")?;
    let mut pixels = vec![0; size];
    let info = reader
        .next_frame(&mut pixels)
        .map_err(|e| format!("png: {e}"))?;
    let channels = match info.color_type {
        png::ColorType::Rgba => 4,
        png::ColorType::Rgb => 3,
        other => return Err(format!("unexpected colour type {other:?}")),
    };
    Ok(pixels[..info.buffer_size()]
        .chunks_exact(channels)
        .map(|p| {
            if channels == 4 && p[3] == 0 {
                return UNDETECT;
            }
            match LEVELS.iter().position(|level| level[..] == p[..3]) {
                Some(index) => index as u8 + 1,
                None => MISSING,
            }
        })
        .collect())
}

/// Nest the inner product in the doubled outer one and encode the grid
/// texture: R is class + 1, G bit 0 missing, G bit 1 undetect.
pub fn compose(outer: Option<&[u8]>, inner: Option<&[u8]>) -> Vec<u8> {
    let mut pixels = Vec::with_capacity(SIZE * SIZE * 4);
    let nested = INNER_OFFSET..INNER_OFFSET + SOURCE_SIZE;
    for y in 0..SIZE {
        for x in 0..SIZE {
            let mut level = outer.map_or(MISSING, |o| o[(y / 2) * SOURCE_SIZE + x / 2]);
            if let Some(inner) = inner
                && nested.contains(&x)
                && nested.contains(&y)
            {
                let near = inner[(y - INNER_OFFSET) * SOURCE_SIZE + (x - INNER_OFFSET)];
                if near != MISSING {
                    level = near;
                }
            }
            let (r, g) = match level {
                MISSING => (0, 1),
                UNDETECT => (0, 2),
                level => (level, 0),
            };
            pixels.extend_from_slice(&[r, g, 0, 255]);
        }
    }
    pixels
}

struct Loaded {
    scan: Scan,
    frame: MosaicFrame,
    texture: Vec<u8>,
}

fn build(
    plan: &Plan,
    scan: Scan,
    outer: Option<Vec<u8>>,
    inner: Option<Vec<u8>>,
) -> Result<Loaded, String> {
    let outer = outer.as_deref().map(decode_layer).transpose()?;
    let inner = inner.as_deref().map(decode_layer).transpose()?;
    let pixels = compose(outer.as_deref(), inner.as_deref());
    let texture = sweep::png(SIZE as u32, SIZE as u32, &pixels)
        .map_err(|e| format!("encoding {} texture: {e}", plan.id))?;
    Ok(Loaded {
        scan,
        frame: plan.frame(Some(scan.stamp)),
        texture,
    })
}

async fn load(ftp: &mut Ftp, plan: &Plan, scan: Scan) -> Result<Loaded, String> {
    let name = |tier: char| format!("{}{tier}.T.{}.png", plan.id, scan.key());
    let outer = match scan.outer {
        true => Some(ftp.retr(&name('2'), BODY_MAX).await?),
        false => None,
    };
    let inner = match scan.inner {
        true => Some(ftp.retr(&name('3'), BODY_MAX).await?),
        false => None,
    };
    let plan = plan.clone();
    let key = scan.key();
    spawn_blocking(move || build(&plan, scan, outer, inner))
        .await
        .unwrap_or_else(|e| Err(format!("decode {key}: {e}")))
}

/// One FTP session: list the radar's products and load up to `limit` of the
/// newest ready scans not in `known`, oldest first. `Ok(None)` when the
/// Bureau lists nothing for the radar.
async fn fetch(
    plan: &Plan,
    known: &HashSet<String>,
    limit: usize,
) -> Result<Option<Vec<Loaded>>, String> {
    let mut ftp = Ftp::connect(HOST).await?;
    ftp.cwd(DIR).await?;
    let names = ftp
        .nlst(&format!("{}[23].T.*.png", plan.id), LISTING_MAX)
        .await?;
    let scans = ready_scans(&plan.id, &names);
    if scans.is_empty() {
        ftp.quit().await;
        return Ok(None);
    }
    let start = scans.len().saturating_sub(limit);
    let mut loaded = Vec::new();
    for scan in scans[start..].iter().filter(|s| !known.contains(&s.key())) {
        match load(&mut ftp, plan, *scan).await {
            Ok(frame) => loaded.push(frame),
            // The control channel is out of step after a failed transfer.
            Err(reason) if loaded.is_empty() => return Err(reason),
            Err(reason) => {
                log(format_args!("{} {}: {reason}", plan.id, scan.key()));
                break;
            }
        }
    }
    ftp.quit().await;
    Ok(Some(loaded))
}

fn log(message: impl std::fmt::Display) {
    eprintln!("{} BOM {message}", Utc::now().to_rfc3339());
}

fn event(plan: &Plan, loaded: Loaded, backfill: bool) -> GridEvent {
    let source_id = plan.id.clone();
    let frame = Box::new(loaded.frame);
    let start_ms = loaded.scan.stamp.and_utc().timestamp_millis();
    match backfill {
        true => GridEvent::Backfill {
            source_id,
            frame,
            texture: loaded.texture,
            start_ms,
        },
        false => GridEvent::Frame {
            source_id,
            frame,
            texture: loaded.texture,
            start_ms,
        },
    }
}

async fn poll_loop(plan: Plan, events: Sender<GridEvent>, mut known: HashSet<String>) {
    let offline = |reason: String| GridEvent::Offline {
        source_id: plan.id.clone(),
        reason,
    };
    // Newest first, so a picture is up before the loop fills in behind it.
    let mut newest = None;
    match timeout(START_TIMEOUT, fetch(&plan, &known, 1)).await {
        Ok(Ok(Some(loaded))) => {
            for frame in loaded {
                known.insert(frame.scan.key());
                newest = Some(frame.scan.stamp);
                let _ = events.send(event(&plan, frame, false)).await;
            }
        }
        Ok(Ok(None)) => {
            let _ = events
                .send(GridEvent::Silent {
                    source_id: plan.id.clone(),
                    reason: format!("the Bureau lists no recent images for {}", plan.id),
                })
                .await;
        }
        Ok(Err(reason)) => {
            let _ = events.send(offline(reason)).await;
        }
        Err(_) => {
            let _ = events
                .send(offline(format!("{} fetch timed out", plan.id)))
                .await;
        }
    }
    sleep(BACKFILL_DELAY).await;
    match timeout(CYCLE_TIMEOUT, fetch(&plan, &known, HISTORY_MAX)).await {
        Ok(Ok(Some(loaded))) => {
            for frame in loaded {
                known.insert(frame.scan.key());
                let live = newest.is_none_or(|n| frame.scan.stamp > n);
                if live {
                    newest = Some(frame.scan.stamp);
                }
                if events.send(event(&plan, frame, !live)).await.is_err() {
                    return;
                }
            }
        }
        Ok(Ok(None)) => {}
        Ok(Err(reason)) => log(format_args!("{} backfill: {reason}", plan.id)),
        Err(_) => log(format_args!("{} backfill timed out", plan.id)),
    }
    loop {
        sleep(IDLE).await;
        match timeout(CYCLE_TIMEOUT, fetch(&plan, &known, HISTORY_MAX)).await {
            Ok(Ok(Some(loaded))) => {
                for frame in loaded {
                    known.insert(frame.scan.key());
                    // A scan older than the live edge that was skipped
                    // earlier does not jump the timeline backwards.
                    if newest.is_some_and(|n| frame.scan.stamp <= n) {
                        continue;
                    }
                    newest = Some(frame.scan.stamp);
                    if events.send(event(&plan, frame, false)).await.is_err() {
                        return;
                    }
                }
            }
            Ok(Ok(None)) => {}
            Ok(Err(reason)) => {
                if events.send(offline(reason)).await.is_err() {
                    return;
                }
            }
            Err(_) => {
                if events
                    .send(offline(format!("{} fetch timed out", plan.id)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        if known.len() > HISTORY_MAX * 8 {
            known = newest
                .map(|n| HashSet::from([n.format("%Y%m%d%H%M").to_string()]))
                .unwrap_or_default();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::inverse_affine;

    /// A 512×512 indexed PNG shaped like the Bureau's: index 0 transparent,
    /// then the fifteen levels, then caption silver and black.
    fn layer_png(paint: impl Fn(usize, usize) -> u8) -> Vec<u8> {
        let mut palette = vec![0, 0, 0];
        palette.extend(LEVELS.iter().flatten());
        palette.extend([192, 192, 192, 0, 0, 0]);
        let mut out = Vec::new();
        let mut encoder = png::Encoder::new(&mut out, SOURCE_SIZE as u32, SOURCE_SIZE as u32);
        encoder.set_color(png::ColorType::Indexed);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_palette(palette);
        encoder.set_trns(vec![0u8]);
        let mut data = Vec::with_capacity(SOURCE_SIZE * SOURCE_SIZE);
        for y in 0..SOURCE_SIZE {
            for x in 0..SOURCE_SIZE {
                data.push(paint(x, y));
            }
        }
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&data).unwrap();
        writer.finish().unwrap();
        out
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn at(texture: &[u8], x: usize, y: usize) -> [u8; 2] {
        let i = (y * SIZE + x) * 4;
        [texture[i], texture[i + 1]]
    }

    #[test]
    fn every_site_is_a_distinct_grid_source_over_australia() {
        let radars = radars();
        assert!(radars.len() >= 60, "{} radars", radars.len());
        let ids: HashSet<&str> = radars.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids.len(), radars.len());
        for radar in &radars {
            assert!(radar.id.starts_with("IDR"), "{}", radar.id);
            assert!(
                crate::envelope::AUSTRALIA.contains(radar.lon, radar.lat),
                "{} outside the envelope",
                radar.id
            );
            assert_eq!(radar.palette.len(), LEVELS.len());
        }
        assert_eq!(BOUNDS.len(), LEVELS.len() + 1);
        assert!(BOUNDS.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn the_ramp_resamples_to_fifteen_keeping_its_ends() {
        let ramp = names(&["#000000", "#808080", "#ffffff"]);
        let out = resample_palette(&ramp, 5);
        assert_eq!(out, ["#000000", "#404040", "#808080", "#c0c0c0", "#ffffff"]);
        assert_eq!(resample_palette(&names(&["oops", "#ffffff"]), 5).len(), 2);
    }

    #[test]
    fn product_names_parse_and_neighbours_do_not() {
        let stamp = NaiveDateTime::parse_from_str("202610030539", "%Y%m%d%H%M").unwrap();
        assert_eq!(
            parse_name("IDR71", "IDR713.T.202610030539.png"),
            Some(('3', stamp))
        );
        assert_eq!(
            parse_name("IDR71", "/anon/gen/radar/IDR712.T.202610030539.png"),
            Some(('2', stamp))
        );
        // Other ranges, Doppler products, and a three-digit radar sharing
        // the prefix are not this radar's 256/128 km images.
        for other in [
            "IDR711.T.202610030539.png",
            "IDR71I.T.202610030539.png",
            "IDR1062.T.202610030539.png",
            "IDR713.T.202610030539.png.tmp",
            "IDR713.gif",
        ] {
            assert_eq!(parse_name("IDR71", other), None, "{other}");
            assert_eq!(parse_name("IDR10", other), None, "{other}");
        }
    }

    #[test]
    fn the_newest_scan_waits_for_both_products() {
        let listing = names(&[
            "IDR712.T.202610030529.png",
            "IDR713.T.202610030529.png",
            "IDR713.T.202610030534.png",
            "IDR712.T.202610030539.png",
        ]);
        let scans = ready_scans("IDR71", &listing);
        // 05:39 has only the outer product so far; 05:34 never got its
        // outer and draws the inner alone.
        assert_eq!(
            scans
                .iter()
                .map(|s| (s.key(), s.outer, s.inner))
                .collect::<Vec<_>>(),
            [
                ("202610030529".to_owned(), true, true),
                ("202610030534".to_owned(), false, true),
            ]
        );
        // A radar that publishes one product is ready as soon as it lists.
        let single = names(&["IDR252.T.202610030530.png"]);
        assert_eq!(ready_scans("IDR25", &single).len(), 1);
        assert!(ready_scans("IDR71", &[]).is_empty());
    }

    #[test]
    fn colours_decode_to_levels_and_captions_are_missing() {
        let png = layer_png(|x, y| match (x, y) {
            (_, 0..16) => 16, // silver caption strip
            (0, 500) => 17,   // black caption text
            (10, 100) => 1,   // lightest level
            (11, 100) => 15,  // heaviest level
            _ => 0,
        });
        let layer = decode_layer(&png).unwrap();
        assert_eq!(layer[0], MISSING);
        assert_eq!(layer[500 * SOURCE_SIZE], MISSING);
        assert_eq!(layer[100 * SOURCE_SIZE + 10], 1);
        assert_eq!(layer[100 * SOURCE_SIZE + 11], 15);
        assert_eq!(layer[200 * SOURCE_SIZE + 200], UNDETECT);
        assert!(decode_layer(b"not a png").is_err());
    }

    #[test]
    fn the_inner_product_nests_in_the_doubled_outer_one() {
        let outer = decode_layer(&layer_png(|x, y| match (x, y) {
            (_, 0..16) => 16,
            (10, 100) => 4,
            (200, 200) => 6,
            _ => 0,
        }))
        .unwrap();
        let inner = decode_layer(&layer_png(|x, y| match (x, y) {
            (_, 0..16) => 16,
            (300, 300) => 9,
            _ => 0,
        }))
        .unwrap();
        let texture = compose(Some(&outer), Some(&inner));
        assert_eq!(texture.len(), SIZE * SIZE * 4);
        // Outside the inner square the outer pixel doubles.
        for (x, y) in [(20, 200), (21, 200), (20, 201), (21, 201)] {
            assert_eq!(at(&texture, x, y), [4, 0]);
        }
        assert_eq!(at(&texture, 22, 200), [0, 2]);
        // The outer caption strip is missing, not a colour class.
        assert_eq!(at(&texture, 600, 5), [0, 1]);
        // Inside it the inner product wins, echo or not.
        assert_eq!(at(&texture, 556, 556), [9, 0]);
        assert_eq!(
            at(&texture, 400, 400),
            [0, 2],
            "inner clear over outer echo"
        );
        // Under the inner caption strip the outer product shows through.
        assert_eq!(at(&texture, 400, 260), [0, 2]);
        let covered = compose(Some(&vec![6; SOURCE_SIZE * SOURCE_SIZE]), Some(&inner));
        assert_eq!(at(&covered, 400, 260), [6, 0]);
        // One product alone still draws.
        let alone = compose(None, Some(&inner));
        assert_eq!(at(&alone, 556, 556), [9, 0]);
        assert_eq!(at(&alone, 10, 10), [0, 1]);
    }

    /// `OMASTORM_BOM_OUT=dir cargo test live_bom -- --ignored` writes the
    /// newest textures there.
    #[test]
    #[ignore = "reads the live Bureau of Meteorology FTP"]
    fn live_bom_fetch_loads_recent_frames() {
        let radar = radars().into_iter().find(|r| r.id == "IDR04").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let loaded = runtime
            .block_on(fetch(&radar.plan(), &HashSet::new(), 3))
            .unwrap()
            .expect("the Bureau lists Newcastle");
        assert!(!loaded.is_empty());
        assert!(loaded.windows(2).all(|w| w[0].scan.stamp < w[1].scan.stamp));
        if let Ok(dir) = std::env::var("OMASTORM_BOM_OUT") {
            for frame in &loaded {
                let path = format!("{dir}/{}.png", frame.frame.id);
                std::fs::write(path, &frame.texture).unwrap();
            }
        }
    }

    #[test]
    fn the_radar_sits_at_the_centre_of_a_256_km_frame() {
        let radar = radars().into_iter().find(|r| r.id == "IDR71").unwrap();
        let stamp = NaiveDateTime::parse_from_str("202610030539", "%Y%m%d%H%M").unwrap();
        let frame = radar.plan().frame(Some(stamp));
        assert_eq!(frame.id, "IDR71-20261003T053900Z");
        assert_eq!(frame.scan_time, "2026-10-03T05:39:00Z");
        assert_eq!((frame.width, frame.height), (1024, 1024));
        assert_eq!(
            inverse_affine(frame.geotransform, 0.0, 0.0),
            Some((512.0, 512.0))
        );
        assert_eq!(
            inverse_affine(frame.geotransform, -256_000.0, 256_000.0),
            Some((0.0, 0.0))
        );
        assert_eq!(frame.palette.len() + 1, frame.bounds.len());
        let (loading, _) = radar.loading_placeholder().unwrap();
        assert_eq!(loading.id, "IDR71-loading");
        assert!(loading.scan_time.is_empty());
    }
}
