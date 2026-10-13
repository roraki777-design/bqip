//! TLS/HTTP upgrade uses reqwest-rustls; tungstenite owns framing and control protocol.
//! The adapter changes only incoming TEXT opcodes internally to preserve invalid UTF-8.
use crate::{
    capture::{Ingress, Lane},
    config::{backoff, delay, Config},
    http,
    raw_writer::RawSink,
    realtime::{self, Item},
    telemetry::Meter,
    Error, Kind, Result,
};
use bqip_contracts::proto::PayloadKind;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, watch},
};
use tokio_tungstenite::{
    tungstenite::{
        protocol::{frame::coding::CloseCode, CloseFrame, Role, WebSocketConfig},
        Message,
    },
    WebSocketStream,
};
use uuid::Uuid;

pub struct RawTextTransport<S> {
    inner: S,
    header: [u8; 14],
    filled: usize,
    needed: usize,
    sent: usize,
    remaining: u64,
    reading_header: bool,
    kind: Option<PayloadKind>,
}
impl<S> RawTextTransport<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            header: [0; 14],
            filled: 0,
            needed: 2,
            sent: 0,
            remaining: 0,
            reading_header: true,
            kind: None,
        }
    }
    pub fn take_kind(&mut self) -> Option<PayloadKind> {
        self.kind.take()
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for RawTextTransport<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.reading_header {
            loop {
                if this.filled < this.needed {
                    let mut temp = [0u8; 14];
                    let mut read = ReadBuf::new(&mut temp[..this.needed - this.filled]);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut read) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Ready(Ok(())) => {}
                    }
                    let n = read.filled().len();
                    if n == 0 {
                        return if this.filled == 0 {
                            Poll::Ready(Ok(()))
                        } else {
                            Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "partial WS header",
                            )))
                        };
                    }
                    this.header[this.filled..this.filled + n].copy_from_slice(read.filled());
                    this.filled += n;
                    if this.filled < 2 {
                        continue;
                    }
                    let extended = match this.header[1] & 127 {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };
                    this.needed = 2 + extended + if this.header[1] & 128 != 0 { 4 } else { 0 };
                    if this.filled < this.needed {
                        continue;
                    }
                    this.remaining = match this.header[1] & 127 {
                        126 => u64::from(u16::from_be_bytes([this.header[2], this.header[3]])),
                        127 => u64::from_be_bytes(this.header[2..10].try_into().map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "WS length")
                        })?),
                        n => u64::from(n),
                    };
                    match this.header[0] & 15 {
                        1 => {
                            this.kind = Some(PayloadKind::Text);
                            this.header[0] = (this.header[0] & 240) | 2;
                        }
                        2 => this.kind = Some(PayloadKind::Binary),
                        _ => {}
                    }
                }
                let n = (this.needed - this.sent).min(out.remaining());
                out.put_slice(&this.header[this.sent..this.sent + n]);
                this.sent += n;
                if this.sent == this.needed {
                    this.reading_header = false;
                }
                if this.remaining == 0 && !this.reading_header {
                    this.reading_header = true;
                    this.filled = 0;
                    this.sent = 0;
                    this.needed = 2;
                }
                return Poll::Ready(Ok(()));
            }
        }
        let available = out
            .remaining()
            .min(usize::try_from(this.remaining).unwrap_or(usize::MAX));
        let mut limited = ReadBuf::new(&mut out.initialize_unfilled()[..available]);
        match Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                let n = limited.filled().len();
                out.advance(n);
                this.remaining -= n as u64;
                if this.remaining == 0 {
                    this.reading_header = true;
                    this.filled = 0;
                    this.sent = 0;
                    this.needed = 2;
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for RawTextTransport<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
pub type Socket = WebSocketStream<RawTextTransport<reqwest::Upgraded>>;
pub async fn connect(client: &reqwest::Client, url: &str, config: &Config) -> Result<Socket> {
    let target = if let Some(s) = url.strip_prefix("wss://") {
        format!("https://{s}")
    } else if let Some(s) = url.strip_prefix("ws://") {
        format!("http://{s}")
    } else {
        return Err(Error::new(Kind::Configuration, "WS scheme"));
    };
    let key = tokio_tungstenite::tungstenite::handshake::client::generate_key();
    let response = client
        .get(target)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", &key)
        .timeout(delay(config.connect_timeout_ms))
        .send()
        .await
        .map_err(crate::error::websocket_error)?;
    let h = response.headers();
    let has = |name: &str, token: &str| {
        h.get(name).and_then(|v| v.to_str().ok()).is_some_and(|v| {
            v.split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(token))
        })
    };
    let expected = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    if response.status().as_u16() != 101
        || !has("connection", "upgrade")
        || !has("upgrade", "websocket")
        || h.get("sec-websocket-accept").and_then(|v| v.to_str().ok()) != Some(expected.as_str())
        || h.contains_key("sec-websocket-extensions")
        || h.contains_key("sec-websocket-protocol")
    {
        return Err(Error::new(
            Kind::Handshake,
            "invalid upgrade status/headers or unsolicited extension",
        ));
    }
    let stream = response
        .upgrade()
        .await
        .map_err(|e| Error::new(Kind::Handshake, e))?;
    let settings = WebSocketConfig::default()
        .max_message_size(Some(config.ws_max_bytes))
        .max_frame_size(Some(config.ws_max_bytes));
    Ok(WebSocketStream::from_raw_socket(
        RawTextTransport::new(stream),
        Role::Client,
        Some(settings),
    )
    .await)
}
pub struct Source {
    pub lane: Lane,
    pub url: String,
    pub config: Config,
    pub ingress: Arc<Ingress>,
    pub raw: RawSink,
    pub realtime: mpsc::Sender<Item>,
    pub meter: Arc<Meter>,
}
impl Source {
    pub async fn run(self, mut stop: watch::Receiver<bool>) -> Result<()> {
        let client = http::client(&self.config)?;
        let mut attempt = 0u32;
        let mut connections = 0u64;
        while !*stop.borrow() && !self.meter.health(self.lane).raw_failed {
            let connection = Uuid::new_v4();
            self.meter.event(
                self.lane,
                "CONNECT_ATTEMPT",
                "POSSIBLE",
                json!({"connection_id":connection.to_string(),"endpoint":self.lane.endpoint()}),
            );
            let result = tokio::select! {r=connect(&client,&self.url,&self.config)=>r,_=stop.changed()=>{break;}};
            match result {
                Ok(mut socket) => {
                    let session = Uuid::new_v4();
                    self.meter.open(self.lane, connection, session);
                    self.meter.count(self.lane, "websocket_connects_total", 1);
                    if connections > 0 {
                        self.meter.count(self.lane, "reconnects_total", 1);
                    }
                    connections += 1;
                    self.meter.event(self.lane, "CONNECTED", "NONE", json!({}));
                    let start = tokio::time::Instant::now();
                    let result = self
                        .receive(&mut socket, connection, session, stop.clone())
                        .await;
                    self.meter
                        .count(self.lane, "websocket_disconnects_total", 1);
                    self.meter
                        .disconnect(self.lane, if *stop.borrow() { "NONE" } else { "POSSIBLE" });
                    if let Err(e) = result {
                        self.meter.event(
                            self.lane,
                            "TRANSPORT_ERROR",
                            "POSSIBLE",
                            json!({"kind":e.kind,"error":e.detail}),
                        );
                    }
                    self.raw.boundary().await?;
                    if start.elapsed() >= delay(30000) {
                        attempt = 0;
                    }
                }
                Err(e) => self.meter.event(
                    self.lane,
                    "CONNECT_FAILURE",
                    "POSSIBLE",
                    json!({"kind":e.kind,"error":e.detail}),
                ),
            }
            if *stop.borrow() {
                break;
            }
            let wait = backoff(
                self.config.reconnect_min_ms,
                self.config.reconnect_max_ms,
                attempt,
            );
            attempt = attempt.saturating_add(1);
            tokio::select! {_=tokio::time::sleep(wait)=>{},_=stop.changed()=>{}}
        }
        Ok(())
    }
    pub async fn receive(
        &self,
        socket: &mut Socket,
        connection: Uuid,
        session: Uuid,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut health_tick = tokio::time::interval(delay(100));
        loop {
            if *stop.borrow() || self.meter.health(self.lane).raw_failed {
                let _ =
                    tokio::time::timeout(delay(self.config.close_timeout_ms), socket.close(None))
                        .await;
                return Ok(());
            }
            let message = tokio::select! {v=socket.next()=>v,_=stop.changed()=>continue,_=health_tick.tick()=>continue};
            let Some(message) = message else {
                return Ok(());
            };
            let message = message.map_err(|e| Error::new(Kind::Handshake, e))?;
            match message {
                Message::Binary(bytes) => {
                    let stamp = self.ingress.stamp()?;
                    let epoch = self
                        .meter
                        .health(self.lane)
                        .epoch
                        .ok_or_else(|| Error::new(Kind::RawWrite, "missing session"))?;
                    self.meter.count(self.lane, "events_received_total", 1);
                    let kind = socket.get_mut().take_kind().ok_or_else(|| {
                        Error::new(Kind::Handshake, "missing original payload kind")
                    })?;
                    let record = Arc::new(self.ingress.record(
                        self.lane,
                        connection,
                        session,
                        kind,
                        bytes.to_vec(),
                        stamp,
                    )?);
                    self.raw.append(record.clone()).await?;
                    if kind == PayloadKind::Text && std::str::from_utf8(&record.payload).is_err() {
                        self.meter.count(self.lane, "parse_failures_total", 1);
                        self.meter.invalidate(self.lane, "PARSE_FAILURE", "NONE");
                        let frame = CloseFrame {
                            code: CloseCode::Invalid,
                            reason: "invalid UTF-8 preserved".into(),
                        };
                        let _ = tokio::time::timeout(
                            delay(self.config.close_timeout_ms),
                            socket.close(Some(frame)),
                        )
                        .await;
                        return Ok(());
                    }
                    if !*stop.borrow() {
                        realtime::handoff(&self.realtime, record, epoch, &self.meter);
                    }
                }
                Message::Ping(_) => {
                    self.meter.count(self.lane, "ping_received_total", 1);
                    socket
                        .flush()
                        .await
                        .map_err(|e| Error::new(Kind::Handshake, e))?;
                    self.meter
                        .event(self.lane, "PONG_SENT_AUTOMATIC", "NONE", json!({}));
                }
                Message::Pong(_) => self
                    .meter
                    .event(self.lane, "PONG_RECEIVED", "NONE", json!({})),
                Message::Close(frame) => {
                    self.meter.event(self.lane,"CLOSE_FRAME","POSSIBLE",json!({"code":frame.as_ref().map(|f|u16::from(f.code)),"reason":frame.as_ref().map(|f|f.reason.to_string())}));
                    let _ =
                        tokio::time::timeout(delay(self.config.close_timeout_ms), socket.flush())
                            .await;
                    return Ok(());
                }
                _ => return Err(Error::new(Kind::Handshake, "unexpected message type")),
            }
        }
    }
}
