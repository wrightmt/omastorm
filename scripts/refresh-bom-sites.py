#!/usr/bin/env python3
"""Regenerate engine/data/bom_sites.json from the Bureau of Meteorology's
radar site table (IDR00007.dbf on the anonymous FTP). Keeps public radars
that publish both the 256 km (IDRnn2) and 128 km (IDRnn3) products."""
import ftplib
import io
import json
import pathlib
import struct

HOST = "ftp.bom.gov.au"
OUT = pathlib.Path(__file__).resolve().parent.parent / "engine/data/bom_sites.json"


def fetch(ftp, path):
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {path}", buf.write)
    return buf.getvalue()


def dbf_rows(data):
    count, header_len, record_len = struct.unpack("<IHH", data[4:12])
    fields, pos = [], 32
    while data[pos] != 0x0D:
        fields.append((data[pos:pos + 11].split(b"\0")[0].decode(), data[pos + 16]))
        pos += 32
    for i in range(count):
        record = data[header_len + i * record_len + 1:header_len + (i + 1) * record_len]
        row, offset = {}, 0
        for name, length in fields:
            row[name] = record[offset:offset + length].decode("latin1").strip()
            offset += length
        yield row


def main():
    ftp = ftplib.FTP(HOST, timeout=60)
    ftp.login()
    layers = set(ftp.nlst("/anon/gen/radar_transparencies/IDR*.background.png"))
    sites = []
    for row in dbf_rows(fetch(ftp, "/anon/home/adfd/spatial/IDR00007.dbf")):
        number = int(row["Radar_id"])
        code = f"IDR{number:02d}"
        published = all(
            f"/anon/gen/radar_transparencies/{code}{tier}.background.png" in layers
            for tier in "23"
        )
        if row["Status"] != "Public" or not published:
            continue
        sites.append({
            "id": code,
            "name": row["Full_Name"],
            "state": row["State"],
            "lat": round(float(row["Latitude"]), 5),
            "lon": round(float(row["Longitude"]), 5),
        })
    ftp.quit()
    sites.sort(key=lambda s: s["id"])
    OUT.write_text(json.dumps({"sites": sites}, indent=2) + "\n")
    print(f"{len(sites)} radars -> {OUT}")


if __name__ == "__main__":
    main()
