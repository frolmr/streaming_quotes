use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::ExitCode;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::RngExt;

use stock_streamer::protocol::{StreamResponse, PING_MESSAGE, STOP_MESSAGE};
use stock_streamer::quote::StockQuote;

const PING_INTERVAL: Duration = Duration::from_secs(2);
const FIRST_QUOTE_TIMEOUT: Duration = Duration::from_secs(10);
const QUOTE_GAP_TIMEOUT: Duration = Duration::from_secs(6);

const RANDOM_TICKERS_MIN: usize = 2;
const RANDOM_TICKERS_MAX: usize = 3;

const RECV_BUF_SIZE: usize = 2048;

#[derive(Debug, Parser)]
#[command(name = "client", version, about = "Stock quote subscriber client")]
struct Cli {
    /// Address of the server's TCP control channel, e.g. `127.0.0.1:34254`
    /// or `server:34254` (hostnames are resolved via DNS).
    #[arg(long, value_name = "HOST:PORT")]
    server_addr: String,

    /// UDP port this client will bind for receiving quotes and sending PINGs.
    #[arg(long, value_name = "PORT")]
    udp_port: u16,

    /// File containing ticker symbols (comma- or whitespace-separated).
    #[arg(long, value_name = "PATH")]
    tickers_file: String,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let server_tcp_addr: SocketAddr = match cli.server_addr.to_socket_addrs() {
        Ok(mut it) => match it.next() {
            Some(a) => a,
            None => {
                eprintln!("[client] '{}' resolved to no addresses", cli.server_addr);
                return ExitCode::from(1);
            }
        },
        Err(e) => {
            eprintln!("[client] failed to resolve '{}': {e}", cli.server_addr);
            return ExitCode::from(1);
        }
    };

    let all_tickers = match read_tickers_file(&cli.tickers_file) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[client] failed to read '{}': {e}", cli.tickers_file);
            return ExitCode::from(1);
        }
    };
    if all_tickers.is_empty() {
        eprintln!(
            "[client] ticker file '{}' contains no tickers",
            cli.tickers_file
        );
        return ExitCode::from(1);
    }
    let picked = pick_random_tickers(&all_tickers, RANDOM_TICKERS_MIN, RANDOM_TICKERS_MAX);
    println!("[client] picked tickers: {:?}", picked);

    let udp_bind = format!("0.0.0.0:{}", cli.udp_port);
    let udp = match UdpSocket::bind(&udp_bind) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[client] failed to bind UDP on {udp_bind}: {e}");
            return ExitCode::from(1);
        }
    };
    let client_udp_addr = match udp.local_addr() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[client] failed to get local UDP addr: {e}");
            return ExitCode::from(1);
        }
    };
    println!("[client] UDP socket bound at {client_udp_addr}");

    let stream_cmd = format!("STREAM {} {}\n", client_udp_addr, picked.join(", "));
    let response = match send_stream_and_read_response(server_tcp_addr, &stream_cmd) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[client] STREAM exchange with {server_tcp_addr} failed: {e}");
            return ExitCode::from(1);
        }
    };
    match response {
        StreamResponse::Ok => println!("[client] server accepted subscription"),
        StreamResponse::Err(reason) => {
            eprintln!("[client] server rejected subscription: {reason}");
            return ExitCode::from(1);
        }
    }

    let (server_addr_tx, server_addr_rx) = mpsc::channel::<SocketAddr>();
    let (quote_heartbeat_tx, quote_heartbeat_rx) = mpsc::channel::<()>();

    {
        let udp = udp.try_clone().expect("dup UDP socket");
        thread::spawn(move || {
            let mut buf = [0u8; RECV_BUF_SIZE];
            let mut server_addr_reported = false;
            loop {
                match udp.recv_from(&mut buf) {
                    Ok((n, src)) => {
                        if !server_addr_reported {
                            let _ = server_addr_tx.send(src);
                            println!("[client] server UDP source addr learned: {src}");
                            server_addr_reported = true;
                        }

                        let data = &buf[..n];

                        if data == STOP_MESSAGE {
                            println!("[client] server sent STOP, shutting down");
                            let _ = std::io::stdout().flush();
                            std::process::exit(0);
                        }

                        let _ = quote_heartbeat_tx.send(());

                        let text = String::from_utf8_lossy(data);
                        let line = text.trim_end_matches(['\r', '\n']);
                        match StockQuote::from_wire_line(line) {
                            Ok(q) => println!("[quote] {}", q.to_wire_line()),
                            Err(e) => eprintln!("[client] bad quote line {:?}: {e}", line),
                        }
                        let _ = std::io::stdout().flush();
                    }
                    Err(e) => {
                        eprintln!("[client] UDP recv error: {e}");
                        std::process::exit(1);
                    }
                }
            }
        });
    }

    {
        let udp = udp.try_clone().expect("dup UDP socket");
        thread::spawn(move || {
            let server_udp_addr = match server_addr_rx.recv_timeout(FIRST_QUOTE_TIMEOUT) {
                Ok(a) => a,
                Err(_) => {
                    eprintln!(
                        "[client] no quote received in {:?}, exiting",
                        FIRST_QUOTE_TIMEOUT
                    );
                    std::process::exit(1);
                }
            };
            println!(
                "[client] pinging {server_udp_addr} every {:?}",
                PING_INTERVAL
            );
            loop {
                if let Err(e) = udp.send_to(PING_MESSAGE, server_udp_addr) {
                    eprintln!("[client] ping send error: {e}");
                }
                thread::sleep(PING_INTERVAL);
            }
        });
    }

    thread::spawn(move || {
        let mut last = Instant::now();
        loop {
            match quote_heartbeat_rx.recv_timeout(QUOTE_GAP_TIMEOUT) {
                Ok(()) => last = Instant::now(),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    eprintln!(
                        "[client] no quote for {:?} (last was {:?} ago), exiting",
                        QUOTE_GAP_TIMEOUT,
                        last.elapsed()
                    );
                    std::process::exit(1);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    eprintln!("[client] quote receiver thread exited, exiting");
                    std::process::exit(1);
                }
            }
        }
    });

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn send_stream_and_read_response(
    server: SocketAddr,
    line: &str,
) -> std::io::Result<StreamResponse> {
    let stream = TcpStream::connect(server)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let mut writer = &stream;
    writer.write_all(line.as_bytes())?;
    writer.flush()?;

    let mut reader = BufReader::new(&stream);
    let mut response_line = String::new();
    reader.read_line(&mut response_line)?;
    if response_line.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "server closed without responding",
        ));
    }
    StreamResponse::parse_line(&response_line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn read_tickers_file(path: &str) -> std::io::Result<Vec<String>> {
    let text = std::fs::read_to_string(path)?;
    Ok(text
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect())
}

fn pick_random_tickers(all: &[String], min: usize, max: usize) -> Vec<String> {
    let mut rng = rand::rng();
    let target = rng.random_range(min..=max).min(all.len());
    let mut picked: Vec<String> = Vec::with_capacity(target);
    for (i, item) in all.iter().enumerate() {
        if i < target {
            picked.push(item.clone());
        } else {
            let j = rng.random_range(0..=i);
            if j < target {
                picked[j] = item.clone();
            }
        }
    }
    picked
}
