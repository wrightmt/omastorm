//! Minimal anonymous passive-mode FTP for the BOM adapter (`bom.rs`). The
//! Bureau publishes its radar images on anonymous FTP only; its HTTPS site
//! refuses non-browser clients. One control connection per poll cycle,
//! `EPSV` data connections, binary transfers, and a hard byte cap on every
//! body. Callers bound each cycle with a timeout.

use std::net::SocketAddr;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

/// Longest control-channel line kept; the BOM banner lines are under 100.
const LINE_MAX: usize = 4096;

pub struct Ftp {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    peer: SocketAddr,
}

/// One complete reply: its three-digit code and final line.
#[derive(Debug, PartialEq, Eq)]
pub struct Reply {
    pub code: u16,
    pub text: String,
}

/// `Some(code)` when `line` ends a reply that opened with `code`
/// (`"226 Done"`), `None` for a `"226-…"` or free-text continuation.
pub fn final_line(line: &str, open: Option<u16>) -> Option<u16> {
    let bytes = line.as_bytes();
    if bytes.len() < 4 || bytes[3] != b' ' {
        return None;
    }
    let code: u16 = line.get(..3)?.parse().ok()?;
    match open {
        Some(expected) if expected != code => None,
        _ => Some(code),
    }
}

/// The port in `229 Entering Extended Passive Mode (|||50123|)`.
pub fn parse_epsv(text: &str) -> Option<u16> {
    let inner = text.rsplit_once('(')?.1.split_once(')')?.0;
    let delimiter = inner.chars().next()?;
    inner.split(delimiter).nth(3)?.parse().ok()
}

impl Ftp {
    /// Connect and log in anonymously to `host:21`.
    pub async fn connect(host: &str) -> Result<Self, String> {
        let stream = TcpStream::connect((host, 21))
            .await
            .map_err(|e| format!("connecting to {host}: {e}"))?;
        let peer = stream
            .peer_addr()
            .map_err(|e| format!("connecting to {host}: {e}"))?;
        let (read, writer) = stream.into_split();
        let mut ftp = Self {
            reader: BufReader::new(read),
            writer,
            peer,
        };
        ftp.expect(220).await?;
        let user = ftp.command("USER anonymous").await?;
        if user.code == 331 {
            ftp.command("PASS omastorm@").await?.require(230)?;
        } else {
            user.require(230)?;
        }
        ftp.command("TYPE I").await?.require(200)?;
        Ok(ftp)
    }

    pub async fn cwd(&mut self, dir: &str) -> Result<(), String> {
        self.command(&format!("CWD {dir}")).await?.require(250)
    }

    /// Names matching `pattern` in the working directory. A server that
    /// answers a no-match glob with `550` lists nothing.
    pub async fn nlst(&mut self, pattern: &str, max: usize) -> Result<Vec<String>, String> {
        match self.transfer(&format!("NLST {pattern}"), max).await? {
            Some(body) => Ok(String::from_utf8_lossy(&body)
                .lines()
                .map(|line| line.trim().to_owned())
                .filter(|line| !line.is_empty())
                .collect()),
            None => Ok(Vec::new()),
        }
    }

    pub async fn retr(&mut self, name: &str, max: usize) -> Result<Vec<u8>, String> {
        self.transfer(&format!("RETR {name}"), max)
            .await?
            .ok_or_else(|| format!("{name}: no such file"))
    }

    pub async fn quit(mut self) {
        let _ = self.writer.write_all(b"QUIT\r\n").await;
    }

    /// Run one data-connection command. `None` when the server refuses it
    /// with `550` before opening the transfer.
    async fn transfer(&mut self, command: &str, max: usize) -> Result<Option<Vec<u8>>, String> {
        let epsv = self.command("EPSV").await?;
        epsv.require(229)?;
        let port = parse_epsv(&epsv.text)
            .ok_or_else(|| format!("unreadable EPSV reply: {}", epsv.text))?;
        let mut data = TcpStream::connect((self.peer.ip(), port))
            .await
            .map_err(|e| format!("opening data connection: {e}"))?;
        let opened = self.command(command).await?;
        if opened.code == 550 {
            return Ok(None);
        }
        if opened.code != 150 && opened.code != 125 {
            return Err(format!("{command}: {} {}", opened.code, opened.text));
        }
        let mut body = Vec::new();
        (&mut data)
            .take(max as u64 + 1)
            .read_to_end(&mut body)
            .await
            .map_err(|e| format!("{command}: {e}"))?;
        drop(data);
        if body.len() > max {
            return Err(format!("{command}: body exceeds {max} bytes"));
        }
        self.expect(226).await?;
        Ok(Some(body))
    }

    async fn command(&mut self, line: &str) -> Result<Reply, String> {
        self.writer
            .write_all(format!("{line}\r\n").as_bytes())
            .await
            .map_err(|e| format!("{line}: {e}"))?;
        self.reply().await
    }

    async fn expect(&mut self, code: u16) -> Result<(), String> {
        self.reply().await?.require(code)
    }

    async fn reply(&mut self) -> Result<Reply, String> {
        let mut open = None;
        loop {
            let mut raw = Vec::new();
            let read = (&mut self.reader)
                .take(LINE_MAX as u64)
                .read_until(b'\n', &mut raw)
                .await
                .map_err(|e| format!("reading FTP reply: {e}"))?;
            if read == 0 {
                return Err("FTP server closed the connection".into());
            }
            if raw.last() != Some(&b'\n') {
                return Err("FTP reply line too long".into());
            }
            let line = String::from_utf8_lossy(&raw);
            // Only the line ending: a bare `"220 "` ends the BOM banner.
            let line = line.trim_end_matches(['\r', '\n']);
            if let Some(code) = final_line(line, open) {
                return Ok(Reply {
                    code,
                    text: line[4..].to_owned(),
                });
            }
            if open.is_none() {
                open = line.get(..3).and_then(|c| c.parse().ok());
            }
        }
    }
}

impl Reply {
    fn require(&self, code: u16) -> Result<(), String> {
        if self.code == code {
            Ok(())
        } else {
            Err(format!("FTP {} {}", self.code, self.text))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn final_lines_end_a_reply_and_continuations_do_not() {
        assert_eq!(final_line("220 Ready", None), Some(220));
        assert_eq!(final_line("220-Welcome", None), None);
        assert_eq!(final_line("220-", Some(220)), None);
        assert_eq!(final_line(". is general information", Some(220)), None);
        assert_eq!(final_line("220 ", Some(220)), Some(220));
        // A numbered line inside another reply's text is not its end.
        assert_eq!(final_line("211 lines follow", Some(220)), None);
        assert_eq!(final_line("", Some(220)), None);
    }

    #[test]
    fn epsv_port_is_read_from_the_reply() {
        assert_eq!(
            parse_epsv("Entering Extended Passive Mode (|||50123|)"),
            Some(50123)
        );
        assert_eq!(parse_epsv("Entering Extended Passive Mode"), None);
        assert_eq!(parse_epsv("(|||notaport|)"), None);
    }

    /// A scripted server: multi-line banner, login, a glob listing, one
    /// file, and an oversized file.
    async fn serve(listener: TcpListener) {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        write
            .write_all(b"220-Welcome\r\n220-\r\n. free text\r\n220 \r\n")
            .await
            .unwrap();
        let mut data: Option<TcpListener> = None;
        while let Ok(Some(line)) = lines.next_line().await {
            let reply: String = match line.as_str() {
                "USER anonymous" => "331 Password please.".into(),
                "PASS omastorm@" => "230 Login successful.".into(),
                "TYPE I" => "200 Binary.".into(),
                "CWD /radar" => "250 Changed.".into(),
                "EPSV" => {
                    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let port = l.local_addr().unwrap().port();
                    data = Some(l);
                    format!("229 Entering Extended Passive Mode (|||{port}|)")
                }
                "NLST none*" => {
                    data = None;
                    "550 No files found.".into()
                }
                command => {
                    let body: &[u8] = match command {
                        "NLST IDR*" => b"IDR712.T.1.png\r\nIDR713.T.1.png\r\n",
                        "RETR small" => b"abc",
                        _ => &[7u8; 64],
                    };
                    write.write_all(b"150 Here it comes.\r\n").await.unwrap();
                    let (mut conn, _) = data.take().unwrap().accept().await.unwrap();
                    conn.write_all(body).await.unwrap();
                    drop(conn);
                    "226 Done.".into()
                }
            };
            write
                .write_all(format!("{reply}\r\n").as_bytes())
                .await
                .unwrap();
        }
    }

    #[test]
    fn lists_and_fetches_against_a_scripted_server() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(serve(listener));
            let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let peer = stream.peer_addr().unwrap();
            let (read, writer) = stream.into_split();
            let mut ftp = Ftp {
                reader: BufReader::new(read),
                writer,
                peer,
            };
            ftp.expect(220).await.unwrap();
            ftp.command("USER anonymous").await.unwrap();
            ftp.command("PASS omastorm@").await.unwrap();
            ftp.command("TYPE I").await.unwrap();
            ftp.cwd("/radar").await.unwrap();
            assert_eq!(
                ftp.nlst("IDR*", 1024).await.unwrap(),
                vec!["IDR712.T.1.png", "IDR713.T.1.png"]
            );
            assert_eq!(ftp.nlst("none*", 1024).await.unwrap(), Vec::<String>::new());
            assert_eq!(ftp.retr("small", 16).await.unwrap(), b"abc");
            let err = ftp.retr("big", 16).await.unwrap_err();
            assert!(err.contains("exceeds 16 bytes"), "{err}");
        });
    }
}
