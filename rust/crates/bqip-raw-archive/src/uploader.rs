use crate::{
    config::Config,
    key,
    local::{self, Descriptor, Segment},
    now_ns, receipt, sha256,
    store::{ArchiveStore, Created, RemoteMetadata, RemoteObject},
    telemetry::{Backlog, Meter, State},
    Error, Kind, Result,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Instant,
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{sleep, timeout, Instant as Deadline},
};
use uuid::Uuid;
pub struct Archive<S> {
    config: Config,
    store: Arc<S>,
    pub meter: Arc<Meter>,
    commit: String,
    permits: Semaphore,
}
struct Active(Arc<Meter>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active(false);
    }
}
fn retry() -> Error {
    Error::new(
        Kind::UploadRetryable,
        "remote state unavailable or request deadline elapsed",
    )
}
fn divergence() -> Error {
    Error::new(
        Kind::RemoteArchiveDivergence,
        "committed archive differs or is unreadable; no repair",
    )
}
impl<S: ArchiveStore> Archive<S> {
    pub fn new(c: Config, store: Arc<S>, meter: Arc<Meter>, commit: &str) -> Result<Self> {
        if !receipt::commit_valid(commit) {
            return Err(Error::new(
                Kind::Configuration,
                "full uploader commit required",
            ));
        }
        let config = c.validate()?;
        Ok(Self {
            permits: Semaphore::new(config.max_inflight_segments),
            config,
            store,
            meter,
            commit: commit.into(),
        })
    }
    pub fn config(&self) -> &Config {
        &self.config
    }
    async fn get(&self, key: &str, expected: u64) -> Result<Option<RemoteObject>> {
        self.meter.count("remote_verify_attempts_total", 1);
        let r = timeout(self.config.timeout(), self.store.get_exact(key, expected))
            .await
            .unwrap_or_else(|_| Err(retry()));
        if let Ok(Some(o)) = &r {
            self.meter
                .count("bytes_verified_download_total", o.bytes.len() as u64);
        }
        if r.is_err() {
            self.meter.count("remote_verify_failures_total", 1);
        }
        r
    }
    async fn get_retry(&self, key: &str, expected: u64) -> Result<Option<RemoteObject>> {
        for n in 1..=3 {
            match self.get(key, expected).await {
                Err(e) if e.retryable() && n < 3 => {
                    self.meter.count("upload_retries_total", 1);
                    sleep(self.config.backoff(n)).await;
                }
                r => return r,
            }
        }
        Err(retry())
    }
    fn compare(&self, s: &Segment, i: usize, o: &RemoteObject) -> Result<()> {
        let error = if o.bytes.len() != s.objects[i].len() {
            Some(Error::new(
                Kind::RemoteLengthMismatch,
                "full GET length mismatch",
            ))
        } else if sha256(&o.bytes) != s.hashes[i] || o.bytes != s.objects[i] {
            Some(Error::new(
                Kind::RemoteHashMismatch,
                "full GET byte/hash mismatch",
            ))
        } else {
            None
        };
        if let Some(e) = error {
            self.meter.count("remote_verify_failures_total", 1);
            Err(e)
        } else {
            self.meter.count("remote_verify_successes_total", 1);
            Ok(())
        }
    }
    async fn resolve_existing(&self, s: &Segment, k: &[String; 3], i: usize) -> Result<()> {
        let conflict = || {
            Error::new(
                Kind::RemoteObjectConflict,
                "existing object differs; overwrite forbidden",
            )
        };
        let o = match self.get_retry(&k[i], s.objects[i].len() as u64).await {
            Err(e)
                if matches!(
                    e.kind,
                    Kind::RemoteLengthMismatch | Kind::RemoteHashMismatch
                ) =>
            {
                return Err(conflict())
            }
            r => r?,
        }
        .ok_or_else(retry)?;
        self.compare(s, i, &o).map_err(|_| conflict())
    }
    async fn create(&self, s: &Segment, k: &[String; 3], i: usize) -> Result<()> {
        self.meter.count("upload_attempts_total", 1);
        self.meter
            .count("bytes_uploaded_total", s.objects[i].len() as u64);
        let start = Instant::now();
        let r = timeout(
            self.config.timeout(),
            self.store.create_if_absent(&k[i], &s.objects[i]),
        )
        .await
        .unwrap_or_else(|_| Err(retry()));
        self.meter.latency(true, start.elapsed());
        match r {
            Ok(Created::New) => (),
            Ok(Created::AlreadyExists) => self.resolve_existing(s, k, i).await?,
            Err(e) if e.retryable() => self.resolve_existing(s, k, i).await?,
            Err(e) => return Err(e),
        }
        self.meter.count("upload_successes_total", 1);
        Ok(())
    }
    async fn verify_set(
        &self,
        s: &Segment,
        k: &[String; 3],
        committed: bool,
    ) -> Result<[RemoteMetadata; 3]> {
        let mut metadata: [RemoteMetadata; 3] = Default::default();
        let mut controls: [Vec<u8>; 2] = Default::default();
        for i in [2, 0, 1] {
            let check = async {
                let o = self
                    .get_retry(&k[i], s.objects[i].len() as u64)
                    .await?
                    .ok_or_else(retry)?;
                self.compare(s, i, &o)?;
                Ok::<_, Error>(o)
            }
            .await;
            let o = match check {
                Err(_) if committed => return Err(divergence()),
                r => r?,
            };
            metadata[i] = o.metadata;
            if i > 0 {
                controls[i - 1] = o.bytes;
            }
        }
        // Remote RAW was fully read and compared; reuse the equal snapshot with
        // the remote-returned controls in the accepted semantic validator.
        bqip_binance_collector::recovery::verify_manifest_bytes(
            &s.objects[0],
            &controls[0],
            &controls[1],
            s.descriptor.lane,
            s.descriptor.id,
        )
        .map_err(|_| {
            Error::new(
                Kind::RemoteManifestMismatch,
                "remote control lineage invalid",
            )
        })?;
        Ok(metadata)
    }
    pub async fn archive(&self, d: Descriptor) -> Result<()> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| Error::new(Kind::UploadFailed, "archive permit closed"))?;
        self.meter.active(true);
        let _active = Active(self.meter.clone());
        let id = d.id;
        let r = async {
            self.meter.state(id, State::Discovered);
            self.meter.state(id, State::LocalVerifying);
            let c = self.config.clone();
            let s = tokio::task::spawn_blocking(move || local::load(&c, &d))
                .await
                .map_err(|_| Error::new(Kind::LocalSourceInvalid, "verification task failed"))??;
            self.meter.state(id, State::LocalVerified);
            receipt::invalidate(&self.config, id)?;
            self.meter.state(id, State::UploadPending);
            let k = key::keys(&self.config.prefix, s.descriptor.lane, id)?;
            let committed = match self.get_retry(&k[2], s.objects[2].len() as u64).await {
                Ok(Some(o)) => {
                    self.compare(&s, 2, &o).map_err(|_| divergence())?;
                    true
                }
                Ok(None) => false,
                Err(e)
                    if matches!(
                        e.kind,
                        Kind::RemoteLengthMismatch | Kind::RemoteHashMismatch
                    ) =>
                {
                    return Err(divergence())
                }
                Err(e) => return Err(e),
            };
            if !committed {
                for (i, state) in [
                    State::UploadingCapture,
                    State::UploadingSeal,
                    State::UploadingManifest,
                ]
                .into_iter()
                .enumerate()
                {
                    self.meter.state(id, state);
                    self.create(&s, &k, i).await?;
                }
            }
            self.meter.state(id, State::RemoteVerifying);
            let start = Instant::now();
            let verified = self.verify_set(&s, &k, committed).await;
            self.meter.latency(false, start.elapsed());
            let metadata = verified?;
            let (c, d, hashes) = (self.config.clone(), s.descriptor.clone(), s.hashes.clone());
            let unchanged = tokio::task::spawn_blocking(move || {
                local::load(&c, &d).map(|v| v.hashes == hashes)
            })
            .await
            .map_err(|_| {
                Error::new(Kind::LocalSourceInvalid, "final verification task failed")
            })??;
            if !unchanged {
                return Err(Error::new(
                    Kind::LocalSourceInvalid,
                    "source changed during archival",
                ));
            }
            receipt::publish(
                &self.config,
                &s,
                &receipt::build(&self.config, &s, &metadata, &self.commit)?,
            )?;
            self.meter.state(id, State::RemoteDurable);
            Ok(())
        }
        .await;
        if let Err(e) = &r {
            self.meter.failure(id, e);
        }
        r
    }
    pub fn scan(&self, reverify: bool) -> Result<Vec<Descriptor>> {
        let mut ids = HashSet::new();
        let mut pending = Vec::new();
        let mut b = Backlog::default();
        let now = now_ns()?;
        for d in local::discover(&self.config)? {
            if !ids.insert(d.id) {
                return Err(Error::new(
                    Kind::LocalSourceInvalid,
                    "segment UUID repeated across lanes",
                ));
            }
            match local::load(&self.config, &d) {
                Ok(s) if !reverify && receipt::valid(&self.config, &s) => {
                    b.archive_remote_durable_segments += 1;
                    b.archive_remote_durable_bytes += s.bytes();
                }
                Ok(s) => {
                    if receipt::path(&self.config, d.id)?.exists()
                        && !receipt::valid(&self.config, &s)
                    {
                        self.meter.count("invalid_receipts_total", 1);
                    }
                    b.archive_pending_segments += 1;
                    b.archive_pending_bytes += s.bytes();
                    b.oldest_unuploaded_age_seconds = b
                        .oldest_unuploaded_age_seconds
                        .max(now.saturating_sub(s.first_receive_ns) as f64 / 1e9);
                    pending.push(d);
                }
                Err(e) => {
                    b.archive_pending_segments += 1;
                    b.invalid_source_segments += 1;
                    *b.source_errors.entry(e.kind).or_default() += 1;
                    for n in local::NAMES {
                        b.archive_pending_bytes = b.archive_pending_bytes.saturating_add(
                            std::fs::symlink_metadata(d.directory.join(n))
                                .map(|m| m.len())
                                .unwrap_or(0),
                        );
                    }
                }
            }
        }
        self.meter.backlog(b);
        Ok(pending)
    }
    pub async fn once(self: &Arc<Self>, reverify: bool) -> Result<()> {
        self.once_until(reverify, std::future::pending()).await
    }
    pub async fn once_until(
        self: &Arc<Self>,
        reverify: bool,
        stop: impl Future<Output = ()>,
    ) -> Result<()> {
        let a = self.clone();
        let pending = tokio::task::spawn_blocking(move || a.scan(reverify))
            .await
            .map_err(|_| Error::new(Kind::LocalSourceInvalid, "scan task failed"))??;
        let mut pending = pending.into_iter();
        let mut tasks = FuturesUnordered::new();
        tokio::pin!(stop);
        let mut stopping = false;
        let mut error = None;
        loop {
            while !stopping && tasks.len() < self.config.max_inflight_segments {
                let Some(d) = pending.next() else { break };
                tasks.push(self.archive(d));
            }
            if tasks.is_empty() {
                break;
            }
            tokio::select! {
                _ = &mut stop, if !stopping => stopping = true,
                r = tasks.next() => { if let Some(Err(e)) = r { error = Some(e); } }
            }
        }
        let a = self.clone();
        tokio::task::spawn_blocking(move || a.scan(false))
            .await
            .map_err(|_| Error::new(Kind::LocalSourceInvalid, "scan task failed"))??;
        if let Some(e) = error {
            return Err(e);
        }
        if stopping {
            return Ok(());
        }
        let summary = self.meter.summary();
        if summary["backlog"]["source_errors"]["ARCHIVE_OBJECT_TOO_LARGE"]
            .as_u64()
            .is_some_and(|n| n > 0)
        {
            return Err(Error::new(
                Kind::ArchiveObjectTooLarge,
                "oversized source remains pending",
            ));
        }
        if summary["backlog"]["archive_pending_segments"].as_u64() != Some(0) {
            return Err(Error::new(
                Kind::LocalSourceInvalid,
                "pending sources require operator attention",
            ));
        }
        Ok(())
    }
}
struct Failure {
    attempts: u32,
    next: Deadline,
    permanent: bool,
}
pub struct Runner<S> {
    pub archive: Arc<Archive<S>>,
    jobs: JoinSet<(Uuid, Result<()>)>,
    active: HashSet<Uuid>,
    failures: HashMap<Uuid, Failure>,
}
impl<S: ArchiveStore> Runner<S> {
    pub fn new(archive: Arc<Archive<S>>) -> Self {
        Self {
            archive,
            jobs: JoinSet::new(),
            active: HashSet::new(),
            failures: HashMap::new(),
        }
    }
    fn finished(&mut self, id: Uuid, r: Result<()>) {
        self.active.remove(&id);
        if let Err(e) = r {
            let n = self
                .failures
                .get(&id)
                .map_or(1, |f| f.attempts.saturating_add(1));
            self.failures.insert(
                id,
                Failure {
                    attempts: n,
                    next: Deadline::now() + self.archive.config.backoff(n),
                    permanent: !e.retryable(),
                },
            );
        } else {
            self.failures.remove(&id);
        }
    }
    pub async fn tick(&mut self) -> Result<()> {
        while let Some(r) = self.jobs.try_join_next() {
            let (id, r) = r.map_err(|_| Error::new(Kind::UploadFailed, "archive worker failed"))?;
            self.finished(id, r);
        }
        let a = self.archive.clone();
        let pending = tokio::task::spawn_blocking(move || a.scan(false))
            .await
            .map_err(|_| Error::new(Kind::LocalSourceInvalid, "scan task failed"))??;
        let ids: HashSet<_> = pending
            .iter()
            .map(|d| d.id)
            .chain(self.active.iter().copied())
            .collect();
        self.failures.retain(|id, _| ids.contains(id));
        for d in pending {
            if self.jobs.len() >= self.archive.config.max_inflight_segments {
                break;
            }
            if self.active.contains(&d.id) {
                continue;
            }
            if let Some(f) = self.failures.get(&d.id) {
                if f.permanent || Deadline::now() < f.next {
                    continue;
                }
                self.archive.meter.count("upload_retries_total", 1);
            }
            self.active.insert(d.id);
            let a = self.archive.clone();
            self.jobs.spawn(async move {
                let id = d.id;
                (id, a.archive(d).await)
            });
        }
        Ok(())
    }
    pub async fn drain(&mut self) -> Result<()> {
        while let Some(r) = self.jobs.join_next().await {
            let (id, r) = r.map_err(|_| Error::new(Kind::UploadFailed, "archive worker failed"))?;
            self.finished(id, r);
        }
        let a = self.archive.clone();
        tokio::task::spawn_blocking(move || a.scan(false))
            .await
            .map_err(|_| Error::new(Kind::LocalSourceInvalid, "scan task failed"))??;
        Ok(())
    }
}
