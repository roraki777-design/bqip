use super::*;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
type ReplyQueue = Arc<Mutex<VecDeque<(u16, Vec<u8>)>>>;
#[derive(Clone, Debug)]
struct Wire {
    replies: ReplyQueue,
    methods: Arc<Mutex<Vec<String>>>,
}
impl HttpConnector for Wire {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}
impl HttpService for Wire {
    // Explicit boxed ABI of the external trait avoids a new async-trait dependency.
    fn call<'life0, 'async_trait>(
        &'life0 self,
        req: HttpRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<HttpResponse, HttpError>> + Send + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            assert_eq!(req.uri().scheme_str(), Some("https"));
            assert!(matches!(req.method().as_str(), "GET" | "PUT"));
            if req.method() == "PUT" {
                assert_eq!(req.headers().get("if-none-match").unwrap(), "*");
            }
            self.methods.lock().unwrap().push(req.method().to_string());
            let (status, body) = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("bounded expected requests");
            let len = body.len();
            let mut r = HttpResponse::new(HttpResponseBody::from(body));
            *r.status_mut() = reqwest::StatusCode::from_u16(status).unwrap();
            r.headers_mut()
                .insert("content-length", len.to_string().parse().unwrap());
            r.headers_mut()
                .insert("etag", "\"informational\"".parse().unwrap());
            r.headers_mut().insert(
                "last-modified",
                "Wed, 01 Jan 2025 00:00:00 GMT".parse().unwrap(),
            );
            Ok(r)
        })
    }
}
fn credentials() -> Credentials {
    Credentials {
        access: "test-only".into(),
        secret: "not-a-real-credential".into(),
        token: None,
    }
}
fn wire(replies: Vec<(u16, Vec<u8>)>) -> (S3Store, Wire) {
    let w = Wire {
        replies: Arc::new(Mutex::new(replies.into())),
        methods: Arc::new(Mutex::new(Vec::new())),
    };
    let c = Config::default().validate().unwrap();
    let s = S3Store {
        inner: builder(&c, credentials())
            .with_http_connector(w.clone())
            .build()
            .unwrap(),
    };
    (s, w)
}
#[tokio::test]
async fn production_adapter_conditional_create_and_full_get() {
    let data = b"exact\r\n\0bytes";
    let (s, w) = wire(vec![
        (200, vec![]),
        (412, vec![]),
        (200, data.to_vec()),
        (404, vec![]),
    ]);
    assert_eq!(
        s.create_if_absent("safe/key", data).await.unwrap(),
        Created::New
    );
    assert_eq!(
        s.create_if_absent("safe/key", data).await.unwrap(),
        Created::AlreadyExists
    );
    assert_eq!(
        s.get_exact("safe/key", 100).await.unwrap().unwrap().bytes,
        data
    );
    assert!(s.get_exact("missing/key", 100).await.unwrap().is_none());
    assert_eq!(*w.methods.lock().unwrap(), ["PUT", "PUT", "GET", "GET"]);
}
#[tokio::test]
async fn production_errors_are_typed_and_redacted() {
    for (status, code, kind) in [
        (403, "AccessDenied", Kind::ArchiveAuthFailure),
        (501, "NotImplemented", Kind::ArchiveCapabilityUnsupported),
        (503, "SlowDown", Kind::UploadRetryable),
        (429, "SlowDown", Kind::UploadRetryable),
    ] {
        let body =
            format!("<Error><Code>{code}</Code><Message>SENSITIVE_SENTINEL</Message></Error>")
                .into_bytes();
        let (s, w) = wire(vec![(status, body)]);
        let e = s.create_if_absent("safe/key", b"data").await.unwrap_err();
        assert_eq!(e.kind, kind);
        assert_eq!(e.provider_status, Some(status));
        assert_eq!(e.provider_code, Some(code));
        assert!(!format!("{e:?}").contains("SENSITIVE_SENTINEL"));
        assert_eq!(w.methods.lock().unwrap().len(), 1);
    }
    assert_eq!(format!("{:?}", credentials()), "Credentials([REDACTED])");
    S3Store::new(&Config::default(), credentials()).unwrap();
}
#[tokio::test]
async fn oversized_remote_is_rejected_before_buffering() {
    let (s, _) = wire(vec![(200, vec![0; 100])]);
    assert!(matches!(
        s.get_exact("safe/key", 10).await,
        Err(Error {
            kind: Kind::RemoteLengthMismatch,
            ..
        })
    ));
}
