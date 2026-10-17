mod support;
use bqip_binance_collector::{
    capture::Lane,
    http::{self, Rest},
    raw_writer::{Faults, RawWriter},
    ws::{self, Source},
};
use bqip_contracts::{binance, proto::PayloadKind};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use support::*;
use tokio::{
    io::AsyncWriteExt,
    net::TcpListener,
    sync::{mpsc, watch},
    time::{sleep, timeout},
};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn actual_ws_preservation_attack_ping_pong_and_writer_failure() {
    for fail in [false, true] {
        let env = Env::new();
        let (c, s, _) = env.open(Lane::Depth);
        let writer = RawWriter::start(
            env.config.clone(),
            Lane::Depth,
            env.meter.clone(),
            Faults {
                append_at: if fail { Some(1) } else { None },
                ..Faults::default()
            },
        );
        let (tx, rx) = mpsc::channel(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/public", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::Ping(b"probe".to_vec().into()))
                .await
                .unwrap();
            assert_eq!(
                timeout(Duration::from_secs(2), ws.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                Message::Pong(b"probe".to_vec().into())
            );
            for _ in 0..32 {
                ws.send(Message::Text(std::str::from_utf8(DEPTH).unwrap().into()))
                    .await
                    .unwrap();
            }
            ws.send(Message::Binary(vec![255, 0, 254].into()))
                .await
                .unwrap();
            if !fail {
                ws.send(Message::Close(None)).await.unwrap();
                assert!(matches!(
                    timeout(Duration::from_secs(3), ws.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    Message::Close(_)
                ));
            }
        });
        let source = Source {
            lane: Lane::Depth,
            url: url.clone(),
            config: env.config.clone(),
            ingress: env.ingress.clone(),
            raw: writer.sink.clone(),
            realtime: tx,
            meter: env.meter.clone(),
        };
        let mut socket = ws::connect(&http::client(&env.config).unwrap(), &url, &env.config)
            .await
            .unwrap();
        let (_stop, stop) = watch::channel(false);
        let result = timeout(
            Duration::from_secs(5),
            source.receive(&mut socket, c, s, stop),
        )
        .await
        .unwrap();
        assert_eq!(result.is_err(), fail);
        server.await.unwrap();
        assert_eq!(rx.len(), 1);
        if fail {
            assert!(env.meter.health(Lane::Depth).raw_failed);
            assert_eq!(env.meter.value(Lane::Depth, "raw_append_accepted_total"), 1);
            assert!(writer.shutdown().await.is_err());
        } else {
            writer.shutdown().await.unwrap();
            let records = env.records(Lane::Depth);
            assert_eq!(records.len(), 33);
            for r in &records[..32] {
                assert_eq!(r.payload, DEPTH);
                assert_eq!(
                    r.decode_event().unwrap().payload_kind,
                    Some(PayloadKind::Text as i32)
                );
            }
            assert_eq!(records[32].payload, [255, 0, 254]);
            assert_eq!(
                env.meter.value(Lane::Depth, "local_capacity_drops_total"),
                32
            );
        }
        assert_eq!(env.meter.value(Lane::Depth, "ping_received_total"), 1);
        env.finish();
    }
}
#[tokio::test]
async fn fragmented_invalid_utf8_is_raw_before_protocol_close() {
    let env = Env::new();
    let (c, s, _) = env.open(Lane::Depth);
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    let (tx, rx) = mpsc::channel(1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/x", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        upgraded(&mut tcp).await;
        for b in frame(1, false, &[255, 1])
            .into_iter()
            .chain(frame(9, true, b"p"))
            .chain(frame(0, true, &[0, 254]))
        {
            tcp.write_all(&[b]).await.unwrap();
        }
        let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            tcp,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        assert_eq!(
            timeout(Duration::from_secs(3), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Pong(b"p".to_vec().into())
        );
        match timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Close(Some(f)) => assert_eq!(u16::from(f.code), 1007),
            other => panic!("{other:?}"),
        }
    });
    let source = Source {
        lane: Lane::Depth,
        url: url.clone(),
        config: env.config.clone(),
        ingress: env.ingress.clone(),
        raw: writer.sink.clone(),
        realtime: tx,
        meter: env.meter.clone(),
    };
    let mut socket = ws::connect(&http::client(&env.config).unwrap(), &url, &env.config)
        .await
        .unwrap();
    let (_stop, stop) = watch::channel(false);
    source.receive(&mut socket, c, s, stop).await.unwrap();
    server.await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(rx.len(), 0);
    let records = env.records(Lane::Depth);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, [255, 1, 0, 254]);
    assert_eq!(
        records[0].decode_event().unwrap().payload_kind,
        Some(PayloadKind::Text as i32)
    );
    env.finish();
}
#[tokio::test]
async fn frame_lengths_and_coalesced_messages_are_byte_exact() {
    let env = Env::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/x", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        upgraded(&mut tcp).await;
        let mut wire = Vec::new();
        for n in [0, 125, 126, 65535, 65536] {
            wire.extend(frame(1, true, &vec![b'a'; n]));
        }
        tcp.write_all(&wire).await.unwrap();
        sleep(Duration::from_millis(100)).await;
    });
    let mut socket = ws::connect(&http::client(&env.config).unwrap(), &url, &env.config)
        .await
        .unwrap();
    for n in [0, 125, 126, 65535, 65536] {
        assert_eq!(
            timeout(Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Binary(vec![b'a'; n].into())
        );
        assert_eq!(socket.get_mut().take_kind(), Some(PayloadKind::Text));
    }
    server.await.unwrap();
    env.finish();
}
#[tokio::test]
async fn reconnect_changes_depth_identity_but_not_aggregate_trade_identity() {
    let mut env = Env::new();
    env.config.reconnect_min_ms = 5;
    env.config.reconnect_max_ms = 10;
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    let (tx, _rx) = mpsc::channel(8);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/x", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::Text(std::str::from_utf8(DEPTH).unwrap().into()))
                .await
                .unwrap();
            sleep(Duration::from_millis(20)).await;
        }
    });
    let source = Source {
        lane: Lane::Depth,
        url,
        config: env.config.clone(),
        ingress: env.ingress.clone(),
        raw: writer.sink.clone(),
        realtime: tx,
        meter: env.meter.clone(),
    };
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(source.run(rx));
    timeout(Duration::from_secs(5), async {
        while env.meter.value(Lane::Depth, "raw_append_accepted_total") < 2 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    server.await.unwrap();
    writer.shutdown().await.unwrap();
    let records = env.records(Lane::Depth);
    assert_eq!(records.len(), 2);
    assert_eq!(env.paths(Lane::Depth).len(), 2);
    let a = records[0].decode_event().unwrap();
    let b = records[1].decode_event().unwrap();
    assert_ne!(a.connection_id, b.connection_id);
    assert_ne!(a.session_id, b.session_id);
    assert_ne!(
        binance::parse_depth(DEPTH, a.stream_source.as_ref().unwrap())
            .unwrap()
            .0
            .logical_event_id,
        binance::parse_depth(DEPTH, b.stream_source.as_ref().unwrap())
            .unwrap()
            .0
            .logical_event_id
    );
    let (c, s, _) = env.open(Lane::Aggtrade);
    let r1 = env.record(Lane::Aggtrade, c, s, TRADE);
    let (c, s, _) = env.open(Lane::Aggtrade);
    let r2 = env.record(Lane::Aggtrade, c, s, TRADE);
    assert_eq!(
        binance::parse_trade(TRADE, r1.event.stream_source.as_ref().unwrap())
            .unwrap()
            .0
            .logical_event_id,
        binance::parse_trade(TRADE, r2.event.stream_source.as_ref().unwrap())
            .unwrap()
            .0
            .logical_event_id
    );
    env.finish();
}
#[tokio::test]
async fn http_status_encoding_and_timeout_preserve_complete_bodies() {
    let mut env = Env::new();
    env.config.http_timeout_ms = 100;
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Rest,
        env.meter.clone(),
        Faults::default(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for (status, body, encoding) in [
            (200, SNAPSHOT, ""),
            (500, b"error".as_slice(), ""),
            (200, b"encoded".as_slice(), "Content-Encoding: gzip\r\n"),
        ] {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let q = String::from_utf8(request(&mut tcp).await).unwrap();
            assert!(q.starts_with("GET /fapi/v1/depth?limit=1000&symbol=BTCUSDT "));
            assert!(q.to_ascii_lowercase().contains("accept-encoding: identity"));
            tcp.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\n{encoding}Connection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            tcp.write_all(body).await.unwrap();
        }
        let (mut tcp, _) = listener.accept().await.unwrap();
        request(&mut tcp).await;
        sleep(Duration::from_millis(200)).await;
    });
    let rest = Rest {
        client: http::client(&env.config).unwrap(),
        base,
        config: env.config.clone(),
        ingress: env.ingress.clone(),
        raw: writer.sink.clone(),
        meter: env.meter.clone(),
    };
    let a = rest.capture(true).await.unwrap();
    assert!(Rest::successful(&a).is_ok());
    let b = rest.capture(true).await.unwrap();
    assert!(Rest::successful(&b).is_err());
    let c = rest.capture(true).await.unwrap();
    assert!(Rest::successful(&c).is_err());
    assert_eq!(
        c.event.http_content_encoding.as_deref(),
        Some(b"gzip".as_slice())
    );
    assert!(rest.capture(true).await.is_err());
    server.await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(
        env.records(Lane::Rest)
            .iter()
            .map(|f| f.payload.as_slice())
            .collect::<Vec<_>>(),
        [SNAPSHOT, b"error", b"encoded"]
    );
    env.finish();
}
#[tokio::test]
async fn bad_upgrade_or_unsolicited_extensions_are_rejected() {
    let env = Env::new();
    for extra in [
        "Sec-WebSocket-Accept: wrong\r\n",
        "Sec-WebSocket-Extensions: permessage-deflate\r\n",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/x", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            request(&mut tcp).await;
            tcp.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n{extra}\r\n").as_bytes()).await.unwrap();
        });
        assert!(
            ws::connect(&http::client(&env.config).unwrap(), &url, &env.config)
                .await
                .is_err()
        );
        task.await.unwrap();
    }
    env.finish();
}
