# Stock Streamer

Rust project that streams simulated stock quotes from a TCP/UDP server to
multiple subscribers over the network. Built as a study project — the "market
data" is just random numbers, but the networking, concurrency, and lifecycle
management are real.

The workspace produces one library and two binaries:

- **server** — accepts subscriptions over TCP, generates quotes, and streams
  them to each subscriber over UDP. Drops subscribers that stop pinging.
- **client** — subscribes to a random subset of tickers, receives quotes,
  prints them to stdout, and pings the server to stay alive.

## Contents

- [How it works](#how-it-works)
  - [Wire protocol](#wire-protocol)
  - [Server](#server)
  - [Client](#client)
- [Project layout](#project-layout)
- [Running locally](#running-locally)
  - [Prerequisites](#prerequisites)
  - [1. Start the server](#1-start-the-server)
  - [2. Start one or more clients](#2-start-one-or-more-clients)
  - [3. Clean shutdown](#3-clean-shutdown)
- [Running with Docker](#running-with-docker)
  - [1. Build and start](#1-build-and-start)
  - [2. View logs](#2-view-logs)
  - [3. Stop](#3-stop)
- [Configuration](#configuration)
  - [Server CLI](#server-cli)
  - [Client CLI](#client-cli)
  - [Logging](#logging)
- [Ticker allow-list](#ticker-allow-list)
- [Design notes](#design-notes)
- [Limitations](#limitations)

## How it works

### Wire protocol

Two transports, each with a clear job:

**TCP — control plane.** The client sends exactly one STREAM line and the
server replies with exactly one line, then the connection closes. After that,
all traffic is UDP.

```
client → server:  STREAM <udp_addr> <TICKER1>, <TICKER2>, ...\n
server → client:  OK\n
               or ERR <reason>\n
```

Example:

```
STREAM 127.0.0.1:9000 AAPL, TSLA, MSFT
```

\<udp_addr\> is the address the client is listening on for quotes — **not** the
source address of the TCP connection. The server normalizes 0.0.0.0 to the
TCP peer's IP, so a client that binds 0.0.0.0:\<port> (the usual choice) is
reachable regardless of which interface the TCP connection arrived on.

Possible ERR reasons:
- invalid command,
- invalid udp address,
- empty ticker list,
- unknown ticker: \<TICKER>,
- udp address already registered.

**UDP — data + liveness.** Three kinds of datagrams share one socket:

- **Quotes** (server → client), one per datagram, in the format:

  ```
  <ticker>|<price>|<volume>|<timestamp_ms>\n
  ```

  e.g. AAPL|150.25|4200|1710000000123.

- **PING** (client → server), literal bytes PING\n, sent every 2 seconds from
  **the same UDP socket** the client receives quotes on. The server uses the
  datagram's source address to look up the subscriber, so pinging from a
  different socket would leave the client un-pinged despite its best efforts.

- **STOP** (server → client), literal bytes STOP\n, broadcast to all
  subscribers on graceful shutdown. Clients exit cleanly instead of waiting
  for the quote-gap timeout.

### Server

Three background threads plus a non-blocking TCP accept loop:

1. **Quote generator** — every 100–500 ms, picks a fresh random quote for each
   ticker that has at least one subscriber, and sends it to every subscriber
   interested in that ticker via that subscriber's channel.
2. **Ping receiver** — recv_from on the shared UDP socket; on a PING\n
   datagram, refreshes the sender's last_ping timestamp.
3. **Evictor** — every second, drops subscribers whose last_ping is older
   than 5 seconds and joins their sender threads.
4. **Per-subscriber sender threads** — one thread per subscriber, draining that
   subscriber's quote channel and calling send_to on the shared UDP socket
   for each quote. One quote = one datagram.

Subscribers are stored in a Registry: three maps (ticker → subscribers,
subscriber ID → record, UDP address → subscriber ID) behind a single Mutex.
Read paths clone what they return, so the lock is never held across socket I/O.
Write paths detach subscribers under the lock and join their sender threads
outside it.

On SIGINT (Ctrl-C) the server broadcasts STOP\n to every subscriber, then
removes them all — which joins every sender thread — and exits.

### Client

1. Parses the server address (hostnames are resolved via to_socket_addrs).
2. Reads the tickers file, picks 2–3 at random.
3. Binds a UDP socket on 0.0.0.0:\<udp_port>.
4. Sends STREAM over TCP and reads the one-line response.
   On ERR, prints the reason and exits with code 1.
5. Spawns three threads:
   - **Quote receiver** — recv_from on the UDP socket; on the first
     datagram, records the server's UDP source address for the ping thread;
     on STOP\n, exits with code 0; otherwise parses the quote and prints it
     to stdout as [quote] \<ticker>|\<price>|\<volume>|\<timestamp_ms>.
   - **Ping sender** — waits for the server address, then sends PING\n
     every 2 seconds.
   - **Watchdog** — exits with code 1 if no quote has arrived for 6 seconds
     (covers "server vanished without sending STOP").

## Project layout

```
.
├── assets
│   └── tickers.txt          # allow-list of tickers (embedded in the server)
├── Cargo.toml
├── Dockerfile               # multi-stage build for both binaries
├── docker-compose.yml       # server + 3 clients on a private bridge network
├── src
│   ├── bin
│   │   ├── client.rs        # client binary entry point
│   │   └── server.rs        # server binary entry point
│   ├── lib.rs               # public API re-exports
│   ├── protocol.rs          # STREAM / OK / ERR / PING / STOP, parse + serialize
│   ├── quote.rs             # StockQuote type + wire format
│   ├── registry.rs          # subscriber registry + sender-thread lifecycle
│   └── tickers.rs           # compile-time allow-list
```

## Running locally

### Prerequisites

- Rust 1.85 or newer (the project uses edition 2024).
- Optionally, Docker + Docker Compose for the containerized flow.

```
cargo build --release
```

### 1. Start the server

In one terminal:

```
RUST_LOG=info cargo run --bin server
```

Default bind is 127.0.0.1:34254 for both TCP and UDP (same numeric port — TCP
and UDP are separate namespaces, so this is legal and lets clients remember a
single number). Override with --bind-addr:

```
RUST_LOG=info cargo run --bin server -- --bind-addr 0.0.0.0:40000
```

Expected output:

```
[INFO  server] starting
[INFO  server] allow-list (8 tickers): ["AAPL", "AMZN", "META", "MSFT", "MU", "NFLX", "NVDA", "TSLA"]
[INFO  server] UDP socket bound at 127.0.0.1:34254
[INFO  server] TCP listening on 127.0.0.1:34254
```

### 2. Start one or more clients

In another terminal:

```
cargo run --bin client -- \
    --server-addr 127.0.0.1:34254 \
    --udp-port 9000 \
    --tickers-file assets/tickers.txt
```

In a third terminal, with a different UDP port (each client needs its own):

```
cargo run --bin client -- \
    --server-addr 127.0.0.1:34254 \
    --udp-port 9001 \
    --tickers-file assets/tickers.txt
```

Client output:

```
[client] picked tickers: ["MU", "NFLX"]
[client] UDP socket bound at 0.0.0.0:9000
[client] server accepted subscription
[client] server UDP source addr learned: 127.0.0.1:34254
[client] pinging 127.0.0.1:34254 every 2s
[quote] MU|312.4567|421|1710000000123
[quote] NFLX|87.1234|823|1710000000456
[quote] MU|198.7654|900|1710000000999
...
```

Server-side, each client shows up as a registered subscriber:

```
[INFO  server] registered SubscriberId(0) udp=127.0.0.1:9000 tickers=["MU", "NFLX"]
[INFO  server] registered SubscriberId(1) udp=127.0.0.1:9001 tickers=["AAPL", "TSLA"]
```

### 3. Clean shutdown

Press **Ctrl-C** in the server terminal. It broadcasts STOP to every
subscriber, joins their sender threads, and exits. Clients print
[client] server sent STOP, shutting down and exit with code 0.

If you kill the server with kill -9 instead (no handler runs, no STOP sent),
clients detect the silence after 6 seconds and exit with code 1 — that's the
fallback path, and it's the correct behavior.

## Running with Docker

The provided docker-compose.yml starts the server and three clients on a
private bridge network, so nothing outside the compose project needs to know
about ports or addresses.

### 1. Build and start

```
docker compose up -d --build
```

### 2. View logs

All services:

```
docker compose logs -f
```

Just the server:

```
docker compose logs -f server
```

Just one client:

```
docker compose logs -f client1
```

You should see each client's [quote] lines and the server's registered and
evicted log lines.

### 3. Stop

```
docker compose down
```

Pressing Ctrl-C in the server container sends SIGINT, which the server handles
gracefully (same as local): clients get STOP, print the message, exit cleanly.

To exercise the graceful-shutdown path manually:

```
docker compose kill -s SIGINT server
docker compose logs --tail 5 server client1 client2 client3
```

## Configuration

### Server CLI

```
Usage: server [OPTIONS]

Options:
      --bind-addr <IP:PORT>  Address to bind for both TCP (control) and UDP
                             (quotes + ping) [default: 127.0.0.1:34254]
  -h, --help                 Print help
  -V, --version              Print version
```

### Client CLI

```
Usage: client --server-addr <HOST:PORT> --udp-port <PORT> --tickers-file <PATH>

Options:
      --server-addr <HOST:PORT>  Address of the server's TCP control channel,
                                 e.g. 127.0.0.1:34254 or server:34254
                                 (hostnames are resolved via DNS)
      --udp-port <PORT>          UDP port this client will bind for receiving
                                 quotes and sending PINGs
      --tickers-file <PATH>      File containing ticker symbols (comma- or
                                 whitespace-separated)
  -h, --help                     Print help
  -V, --version                  Print version
```

### Logging

The server uses env_logger and respects RUST_LOG. Default (when unset) is info:

```
RUST_LOG=debug cargo run --bin server     # per-request detail, sender-thread start/stop
RUST_LOG=warn  cargo run --bin server     # warnings and errors only
```

The client prints diagnostics to stderr and **product output** (quotes) to
stdout, so piping cargo run --bin client ... | tee quotes.txt captures only
the quotes.

## Ticker allow-list

The server's allow-list lives in assets/tickers.txt and is **embedded into the
binary at compile time** via include_str!. Changing the file requires a
rebuild — it's a static property of the deployment, not a runtime setting. The
file is parsed leniently: commas and whitespace both separate entries, empty
entries are ignored, so both of these work:

```
NVDA, AAPL, MSFT, META, AMZN, TSLA, NFLX, MU
```

```
NVDA
AAPL
MSFT
META
AMZN
TSLA
NFLX
MU
```

The client reads its tickers file at runtime via --tickers-file, so the two
sides don't have to agree on the same file — but any ticker the client requests
that isn't in the server's allow-list will be rejected with
ERR unknown ticker: \<TICKER>.

## Design notes

A few decisions worth knowing about if you're reading the code:

- **One shared UdpSocket on the server**, Arc-cloned into every sender
  thread. Saves file descriptors and — more importantly — gives every client
  the same server UDP source (ip, port), which the client learns from its
  first received datagram and uses as its PING destination.
- **One sender thread per subscriber.** The task's wording ("one subscriber =
  one thread") drove this. The generator fans quotes out over per-subscriber
  mpsc channels rather than calling send_to itself, so a slow sender can't
  stall the generator.
- **Unbounded channels.** The sender thread is much faster than the generator,
  so the channels never back up in practice. If a sender thread ever blocked on
  a full kernel buffer, memory would grow unboundedly — not an issue for a
  course project, but the natural next step would be sync_channel with a
  small capacity and drop-on-full semantics.
- **PING-only liveness.** The registry knows nothing about TCP connections
  after the one-line response. A subscriber is evicted solely because it stopped
  pinging. That makes the model simple and matches the spec ("if a client does
  not send ping for 5 seconds, it must be excluded").
- **Duplicate STREAM from the same address is an error.** We deliberately do
  not replace the previous subscriber — a second client claiming the same UDP
  address would otherwise silently evict the first. The cost is that a client
  restarting within the 5-second ping-timeout window gets
  ERR udp address already registered and has to wait for eviction.
- **include_str! for the allow-list.** No runtime file I/O, no path
  dependencies, and cargo build --release produces a single self-contained
  server binary.

## Limitations

This is a study project, not production software. Known simplifications:

- No TLS, no authentication, no rate limiting.
- Quotes are random numbers; there is no market data feed behind them.
- UDP is best-effort — a quote or a STOP can be lost, and the receiving side
  has no way to ask for it again. The 6-second client watchdog covers the
  "no quotes at all" case, not individual dropped datagrams.
- Re-registering the same UDP address is rejected, not replaced.
- No backpressure between the generator and sender threads (see "Design notes").
