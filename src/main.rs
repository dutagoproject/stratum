use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use duta_core::{address, dutahash, netparams::Network, types::H32};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    fs::OpenOptions,
    io::Write,
    net::IpAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use time::{format_description, OffsetDateTime, UtcOffset};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "DUTA stratum bridge for dutad /work + /submit_work",
    after_help = "Examples:\n  duta-stratumd --bind 127.0.0.1:11001 --daemon http://127.0.0.1:19085\n  duta-stratumd --bind 0.0.0.0:11001 --daemon http://127.0.0.1:19085 --network mainnet\n  duta-stratumd --pool-api-url http://127.0.0.1:8080/share --pool-api-key secret-key"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:11001")]
    bind: String,

    #[arg(long, default_value = "http://127.0.0.1:19085")]
    daemon: String,

    #[arg(long, default_value_t = 24)]
    share_bits: u64,

    #[arg(long, default_value_t = 15)]
    job_refresh_secs: u64,

    #[arg(long, default_value_t = 30)]
    job_ttl_secs: u64,

    #[arg(long)]
    pool_api_url: Option<String>,

    #[arg(long)]
    pool_api_key: Option<String>,

    #[arg(long)]
    network: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RpcMsg {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Clone)]
struct WorkJob {
    job_id: String,
    session_id: String,
    worker_name: String,
    worker_id: String,
    wallet: String,
    work_id: String,
    blob: String,
    height: u64,
    bits: u64,
    share_bits: u64,
    anchor_hash32: String,
    target: String,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct WorkerState {
    session_id: String,
    wallet: String,
    worker_name: String,
}

#[derive(Debug, Clone)]
struct ShareSample {
    at: Instant,
    hashes: f64,
}

#[derive(Debug, Clone)]
struct WorkerStats {
    session_id: String,
    wallet: String,
    worker_name: String,
    peer_label: String,
    connected: bool,
    last_job_at: Instant,
    last_share_at: Option<Instant>,
    accepted_shares: u64,
    rejected_shares: u64,
    candidate_blocks: u64,
    best_hash_bits: u32,
    last_error: Option<String>,
    share_samples: VecDeque<ShareSample>,
}

#[derive(Debug, Clone)]
struct ConsumedJob {
    session_id: String,
    worker_id: String,
    consumed_at: Instant,
}

#[derive(Debug, Deserialize)]
struct WorkReply {
    work_id: String,
    height: u64,
    bits: u64,
    anchor_hash32: String,
    header80: String,
}

#[derive(Debug, Serialize)]
struct SubmitReq {
    work_id: String,
    nonce: u64,
}

#[derive(Debug, Serialize)]
struct PoolShareEvent<'a> {
    #[serde(rename = "minerAddress")]
    miner_address: &'a str,
    #[serde(rename = "workerName")]
    worker_name: &'a str,
    #[serde(rename = "jobId")]
    job_id: &'a str,
    #[serde(rename = "workId")]
    work_id: &'a str,
    network: &'a str,
    #[serde(rename = "isValid")]
    is_valid: bool,
    #[serde(rename = "isCandidate")]
    is_candidate: bool,
    #[serde(rename = "shareBits")]
    share_bits: String,
    #[serde(rename = "rejectReason", skip_serializing_if = "Option::is_none")]
    reject_reason: Option<String>,
    #[serde(rename = "heightHint")]
    height_hint: u64,
    #[serde(rename = "remoteIp", skip_serializing_if = "Option::is_none")]
    remote_ip: Option<String>,
    #[serde(rename = "blockHash", skip_serializing_if = "Option::is_none")]
    block_hash: Option<String>,
    #[serde(rename = "extraJson", skip_serializing_if = "Option::is_none")]
    extra_json: Option<Value>,
}

#[derive(Clone)]
struct App {
    args: Args,
    http: reqwest::Client,
    seq: Arc<AtomicU64>,
    jobs: Arc<Mutex<HashMap<String, WorkJob>>>,
    consumed_jobs: Arc<Mutex<HashMap<String, ConsumedJob>>>,
    workers: Arc<Mutex<HashMap<String, WorkerState>>>,
    stats: Arc<Mutex<HashMap<String, WorkerStats>>>,
    conn_limit: Arc<Semaphore>,
    peer_limits: Arc<Mutex<HashMap<IpAddr, PeerState>>>,
    subnet_limits: Arc<Mutex<HashMap<String, usize>>>,
}

#[derive(Debug, Clone)]
struct PeerState {
    open_connections: usize,
    window_started_at: Instant,
    total_requests: u32,
    login_requests: u32,
    getjob_requests: u32,
    submit_requests: u32,
    last_seen_at: Instant,
    ban_until: Option<Instant>,
}

impl PeerState {
    fn new(now: Instant) -> Self {
        Self {
            open_connections: 0,
            window_started_at: now,
            total_requests: 0,
            login_requests: 0,
            getjob_requests: 0,
            submit_requests: 0,
            last_seen_at: now,
            ban_until: None,
        }
    }
}

#[derive(Debug)]
struct PeerConnectionGuard {
    peer_limits: Arc<Mutex<HashMap<IpAddr, PeerState>>>,
    subnet_limits: Arc<Mutex<HashMap<String, usize>>>,
    ip: IpAddr,
    subnet_key: Option<String>,
}

impl Drop for PeerConnectionGuard {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut peer_limits = lock_or_recover(&self.peer_limits, "peer_limits");
        if let Some(state) = peer_limits.get_mut(&self.ip) {
            state.open_connections = state.open_connections.saturating_sub(1);
            state.last_seen_at = now;
        }
        cleanup_peer_limits(&mut peer_limits, now);
        if let Some(subnet_key) = self.subnet_key.as_ref() {
            let mut subnet_limits = lock_or_recover(&self.subnet_limits, "subnet_limits");
            if let Some(open) = subnet_limits.get_mut(subnet_key) {
                *open = open.saturating_sub(1);
                if *open == 0 {
                    subnet_limits.remove(subnet_key);
                }
            }
        }
    }
}

static DATASET_CACHE: Lazy<Mutex<HashMap<String, Arc<Vec<u8>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static STRATUM_LOG: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_NET: &str = "\x1b[46;30m";
const ANSI_CPU: &str = "\x1b[43;30m";
const ANSI_JOB: &str = "\x1b[45;97m";
const ANSI_ERR: &str = "\x1b[41;97m";
const ANSI_SYS: &str = "\x1b[44;97m";
const HASHRATE_WINDOW_SECS: u64 = 120;
const OPERATOR_SNAPSHOT_SECS: u64 = 30;
const MAX_LOGIN_BYTES: usize = 192;
const MAX_WORKER_NAME_BYTES: usize = 64;
const CONSUMED_JOB_TTL_SECS: u64 = 30;
const MAX_STRATUM_CONNECTIONS: usize = 256;
const MAX_STRATUM_LINE_BYTES: usize = 16 * 1024;
const CLIENT_IDLE_TIMEOUT_SECS: u64 = 10;
const PRELOGIN_IDLE_TIMEOUT_SECS: u64 = 5;
const MAX_CONNECTIONS_PER_IP: usize = 16;
const MAX_CONNECTIONS_PER_PUBLIC_SUBNET: usize = 32;
const PEER_REQUEST_WINDOW_SECS: u64 = 10;
const MAX_REQUESTS_PER_WINDOW: u32 = 1024;
const MAX_LOGIN_REQUESTS_PER_WINDOW: u32 = 64;
const MAX_GETJOB_REQUESTS_PER_WINDOW: u32 = 512;
const MAX_SUBMIT_REQUESTS_PER_WINDOW: u32 = 768;
const TEMP_BAN_SECS: u64 = 30;
const PEER_STATE_TTL_SECS: u64 = 900;

fn validate_http_url(raw: &str, field: &str) -> Result<()> {
    let url = reqwest::Url::parse(raw).with_context(|| format!("invalid_{}_url", field))?;
    if !url.username().is_empty() || url.password().is_some() {
        bail!("invalid_{}_url_credentials_not_allowed", field);
    }
    match url.scheme() {
        "http" | "https" => Ok(()),
        _ => bail!("invalid_{}_url", field),
    }
}

fn url_host_is_local(url: &reqwest::Url) -> bool {
    match url.host_str() {
        Some(host) if host.eq_ignore_ascii_case("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .map(|ip| match ip {
                IpAddr::V4(v4) => v4.is_loopback() || v4.is_private(),
                IpAddr::V6(v6) => v6.is_loopback() || v6.is_unique_local(),
            })
            .unwrap_or(false),
        None => false,
    }
}

async fn read_line_limited<R>(reader: &mut R, max_bytes: usize) -> Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line_buf = Vec::new();
    let mut limited = reader.take((max_bytes + 1) as u64);
    let read = limited.read_until(b'\n', &mut line_buf).await?;
    if read == 0 {
        return Ok(None);
    }
    if line_buf.len() > max_bytes {
        bail!("request_too_large");
    }
    Ok(Some(line_buf))
}

fn cleanup_peer_limits(peer_limits: &mut HashMap<IpAddr, PeerState>, now: Instant) {
    let ttl = Duration::from_secs(PEER_STATE_TTL_SECS);
    peer_limits.retain(|_, state| {
        let banned = state.ban_until.map(|until| until > now).unwrap_or(false);
        state.open_connections > 0 || banned || now.duration_since(state.last_seen_at) < ttl
    });
}

fn public_subnet24_key(ip: IpAddr) -> Option<String> {
    match ip {
        IpAddr::V4(v4) => {
            if v4.is_private() || v4.is_loopback() || v4.is_link_local() {
                None
            } else {
                let oct = v4.octets();
                Some(format!("{}.{}.{}", oct[0], oct[1], oct[2]))
            }
        }
        IpAddr::V6(_) => None,
    }
}

fn apply_temporary_peer_ban(state: &mut PeerState, now: Instant) {
    state.ban_until = Some(now + Duration::from_secs(TEMP_BAN_SECS));
    state.window_started_at = now;
    state.total_requests = 0;
    state.login_requests = 0;
    state.getjob_requests = 0;
    state.submit_requests = 0;
}

fn ban_peer_temporarily(app: &App, ip: IpAddr) {
    let now = Instant::now();
    let mut peer_limits = lock_or_recover(&app.peer_limits, "peer_limits");
    cleanup_peer_limits(&mut peer_limits, now);
    let state = peer_limits.entry(ip).or_insert_with(|| PeerState::new(now));
    state.last_seen_at = now;
    apply_temporary_peer_ban(state, now);
}

fn try_open_peer_connection(app: &App, ip: IpAddr) -> Result<PeerConnectionGuard> {
    let now = Instant::now();
    let mut peer_limits = lock_or_recover(&app.peer_limits, "peer_limits");
    cleanup_peer_limits(&mut peer_limits, now);
    let state = peer_limits.entry(ip).or_insert_with(|| PeerState::new(now));
    state.last_seen_at = now;
    if state.ban_until.map(|until| until > now).unwrap_or(false) {
        bail!("peer_temporarily_banned");
    }
    if state.open_connections >= MAX_CONNECTIONS_PER_IP {
        apply_temporary_peer_ban(state, now);
        bail!("too_many_connections_per_ip");
    }
    let subnet_key = public_subnet24_key(ip);
    if let Some(subnet_key) = subnet_key.as_ref() {
        let mut subnet_limits = lock_or_recover(&app.subnet_limits, "subnet_limits");
        let open = subnet_limits.entry(subnet_key.clone()).or_insert(0);
        if *open >= MAX_CONNECTIONS_PER_PUBLIC_SUBNET {
            apply_temporary_peer_ban(state, now);
            bail!("too_many_connections_per_public_subnet");
        }
        *open += 1;
    }
    state.open_connections += 1;
    Ok(PeerConnectionGuard {
        peer_limits: Arc::clone(&app.peer_limits),
        subnet_limits: Arc::clone(&app.subnet_limits),
        ip,
        subnet_key,
    })
}

fn enforce_peer_rate_limit(app: &App, ip: IpAddr, method: &str) -> Result<()> {
    let now = Instant::now();
    let mut peer_limits = lock_or_recover(&app.peer_limits, "peer_limits");
    let state = peer_limits.entry(ip).or_insert_with(|| PeerState::new(now));
    state.last_seen_at = now;
    if state.ban_until.map(|until| until > now).unwrap_or(false) {
        bail!("peer_temporarily_banned");
    }
    if now.duration_since(state.window_started_at) >= Duration::from_secs(PEER_REQUEST_WINDOW_SECS) {
        state.window_started_at = now;
        state.total_requests = 0;
        state.login_requests = 0;
        state.getjob_requests = 0;
        state.submit_requests = 0;
    }
    state.total_requests = state.total_requests.saturating_add(1);
    match method {
        "login" => state.login_requests = state.login_requests.saturating_add(1),
        "getjob" => state.getjob_requests = state.getjob_requests.saturating_add(1),
        "submit" => state.submit_requests = state.submit_requests.saturating_add(1),
        _ => {}
    }
    let exceeded = state.total_requests > MAX_REQUESTS_PER_WINDOW
        || state.login_requests > MAX_LOGIN_REQUESTS_PER_WINDOW
        || state.getjob_requests > MAX_GETJOB_REQUESTS_PER_WINDOW
        || state.submit_requests > MAX_SUBMIT_REQUESTS_PER_WINDOW;
    if exceeded {
        apply_temporary_peer_ban(state, now);
        bail!("peer_rate_limited");
    }
    Ok(())
}

fn timestamp_now() -> String {
    let format =
        format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").ok();
    let offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    match format {
        Some(fmt) => OffsetDateTime::now_utc()
            .to_offset(offset)
            .format(&fmt)
            .unwrap_or_else(|_| {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                format!("{}.{:03}", now.as_secs(), now.subsec_millis())
            }),
        None => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            format!("{}.{:03}", now.as_secs(), now.subsec_millis())
        }
    }
}

fn log_file_path() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("stratum.log")
}

fn init_logging() {
    let path = log_file_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(file) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = STRATUM_LOG.set(Mutex::new(file));
    }
}

fn log_line(tag: &str, color: &str, msg: impl AsRef<str>) {
    let ts = timestamp_now();
    let plain = format!("[{}] {:<8} {}", ts, tag, msg.as_ref());
    if let Some(lock) = STRATUM_LOG.get() {
        if let Ok(mut file) = lock.lock() {
            let _ = writeln!(file, "{}", plain);
            let _ = file.flush();
        }
    }
    eprintln!("[{}]  {} {:<8} {} {}", ts, color, tag, ANSI_RESET, msg.as_ref());
}

fn log_net(msg: impl AsRef<str>) {
    log_line("net", ANSI_NET, msg);
}
fn log_cpu(msg: impl AsRef<str>) {
    log_line("cpu", ANSI_CPU, msg);
}
fn log_job(msg: impl AsRef<str>) {
    log_line("job", ANSI_JOB, msg);
}
fn log_err(msg: impl AsRef<str>) {
    log_line("error", ANSI_ERR, msg);
}
fn log_sys(msg: impl AsRef<str>) {
    log_line("sys", ANSI_SYS, msg);
}

fn worker_tag(worker_name: &str) -> &str {
    let trimmed = worker_name.trim();
    if trimmed.is_empty() {
        "-"
    } else {
        trimmed
    }
}

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> std::sync::MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log_err(format!("mutex_poison_recovered name={}", name));
            poisoned.into_inner()
        }
    }
}

fn short_id(id: &str) -> &str {
    if id.len() <= 8 {
        id
    } else {
        &id[..8]
    }
}

fn split_login(s: &str) -> (String, String) {
    let trimmed = s.trim();
    match trimmed.split_once('.') {
        Some((wallet, worker)) if !wallet.is_empty() => (wallet.to_string(), worker.to_string()),
        _ => (trimmed.to_string(), String::new()),
    }
}

fn network_from_name(name: &str) -> Network {
    match name.trim().to_ascii_lowercase().as_str() {
        "testnet" => Network::Testnet,
        "stagenet" => Network::Stagenet,
        _ => Network::Mainnet,
    }
}

fn normalize_worker_name(raw: &str) -> Result<String> {
    let worker = raw.trim();
    if worker.len() > MAX_WORKER_NAME_BYTES {
        bail!("worker_name_too_long");
    }
    if worker.is_empty() {
        return Ok(String::new());
    }
    if !worker
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        bail!("invalid_worker_name");
    }
    Ok(worker.to_string())
}

fn validate_login_identity(app: &App, login: &str) -> Result<(String, String)> {
    let login = login.trim();
    if login.is_empty() {
        bail!("missing_login");
    }
    if login.len() > MAX_LOGIN_BYTES {
        bail!("login_too_long");
    }
    if login.chars().any(char::is_whitespace) {
        bail!("login_has_whitespace");
    }
    let (wallet, worker_name) = split_login(login);
    if wallet.is_empty() {
        bail!("missing_wallet");
    }
    let worker_name = normalize_worker_name(&worker_name)?;
    let net = network_from_name(&infer_network(&app.args));
    if address::parse_address_for_network(net, &wallet).is_none() {
        bail!("invalid_wallet");
    }
    Ok((wallet, worker_name))
}

fn parse_h32(hex64: &str) -> Result<H32> {
    if hex64.len() != 64 {
        bail!("expected 64 hex chars, got {}", hex64.len());
    }
    let bytes = hex::decode(hex64).context("decode h32")?;
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(H32(out))
}

fn dataset_for(height: u64, anchor: H32) -> Arc<Vec<u8>> {
    let epoch = dutahash::epoch_number(height);
    let mem_mb = dutahash::stage_mem_mb(height);
    let key = format!("{}:{}:{}", epoch, mem_mb, hex::encode(anchor.0));

    if let Some(ds) = lock_or_recover(&DATASET_CACHE, "dataset_cache").get(&key) {
        return Arc::clone(ds);
    }

    log_job(format!(
        "dataset build epoch={} height={} mem_mb={} anchor={}",
        epoch,
        height,
        mem_mb,
        hex::encode(anchor.0)
    ));
    let ds = Arc::new(dutahash::build_dataset_for_epoch(epoch, anchor, mem_mb));
    lock_or_recover(&DATASET_CACHE, "dataset_cache").insert(key, Arc::clone(&ds));
    ds
}

fn leading_zero_bits(bytes: &[u8]) -> u32 {
    let mut n = 0u32;
    for &b in bytes {
        if b == 0 {
            n += 8;
            continue;
        }
        n += (b as u32).leading_zeros() - 24;
        break;
    }
    n
}

fn nonce_from_hex8(s: &str) -> Result<u64> {
    let b = hex::decode(s).context("decode nonce")?;
    if b.len() != 4 {
        bail!("expected 4-byte nonce, got {}", b.len());
    }
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as u64)
}

fn job_target_hex(share_bits: u64) -> String {
    let target = if share_bits >= 32 {
        0u32
    } else {
        u32::MAX >> (share_bits as u32)
    };
    format!("{:08x}", target)
}

fn hashrate_window() -> Duration {
    Duration::from_secs(HASHRATE_WINDOW_SECS)
}

fn hashes_for_share_bits(bits: u64) -> f64 {
    2f64.powf(bits.min(63) as f64)
}

fn rig_key(peer_label: &str, worker_name: &str) -> String {
    let worker = worker_name.trim();
    if worker.is_empty() {
        peer_label.trim().to_string()
    } else {
        format!("{}::{}", peer_label.trim(), worker)
    }
}

fn format_hashrate(rate: f64) -> String {
    if rate >= 1_000_000_000.0 {
        format!("{:.2} GH/s", rate / 1_000_000_000.0)
    } else if rate >= 1_000_000.0 {
        format!("{:.2} MH/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.2} KH/s", rate / 1_000.0)
    } else {
        format!("{:.2} H/s", rate)
    }
}

fn format_age(last: Option<Instant>, now: Instant) -> String {
    match last {
        Some(ts) => format!("{}s", now.duration_since(ts).as_secs()),
        None => "-".to_string(),
    }
}

fn short_wallet(wallet: &str) -> String {
    if wallet.len() <= 18 {
        wallet.to_string()
    } else {
        format!("{}..{}", &wallet[..10], &wallet[wallet.len() - 6..])
    }
}

fn short_peer_label(peer_label: &str) -> &str {
    peer_label
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(peer_label)
}

fn prune_share_samples(samples: &mut VecDeque<ShareSample>, now: Instant) {
    let window = hashrate_window();
    while let Some(front) = samples.front() {
        if now.duration_since(front.at) > window {
            samples.pop_front();
        } else {
            break;
        }
    }
}

fn sample_hashrate(samples: &VecDeque<ShareSample>, now: Instant) -> f64 {
    let window = hashrate_window();
    let mut hashes = 0.0f64;
    for sample in samples.iter().rev() {
        if now.duration_since(sample.at) > window {
            break;
        }
        hashes += sample.hashes;
    }
    hashes / window.as_secs_f64()
}

fn record_worker_login(
    app: &App,
    worker_id: &str,
    session_id: &str,
    wallet: &str,
    worker_name: &str,
    peer_label: &str,
) {
    let now = Instant::now();
    let mut stats = lock_or_recover(&app.stats, "stats");
    let prev_samples = stats
        .get(worker_id)
        .map(|s| s.share_samples.clone())
        .unwrap_or_default();
    stats.insert(
        worker_id.to_string(),
        WorkerStats {
            session_id: session_id.to_string(),
            wallet: wallet.to_string(),
            worker_name: worker_name.to_string(),
            peer_label: peer_label.to_string(),
            connected: true,
            last_job_at: now,
            last_share_at: None,
            accepted_shares: 0,
            rejected_shares: 0,
            candidate_blocks: 0,
            best_hash_bits: 0,
            last_error: None,
            share_samples: prev_samples,
        },
    );
}

fn mark_worker_job(app: &App, worker_id: &str, wallet: &str, worker_name: &str, peer_label: &str) {
    let now = Instant::now();
    let mut stats = lock_or_recover(&app.stats, "stats");
    let entry = stats
        .entry(worker_id.to_string())
        .or_insert_with(|| WorkerStats {
            session_id: String::new(),
            wallet: wallet.to_string(),
            worker_name: worker_name.to_string(),
            peer_label: peer_label.to_string(),
            connected: true,
            last_job_at: now,
            last_share_at: None,
            accepted_shares: 0,
            rejected_shares: 0,
            candidate_blocks: 0,
            best_hash_bits: 0,
            last_error: None,
            share_samples: VecDeque::new(),
        });
    entry.wallet = wallet.to_string();
    entry.worker_name = worker_name.to_string();
    if !peer_label.is_empty() {
        entry.peer_label = peer_label.to_string();
    }
    entry.connected = true;
    entry.last_job_at = now;
    prune_share_samples(&mut entry.share_samples, now);
}

fn record_share_result(
    app: &App,
    worker_id: &str,
    wallet: &str,
    worker_name: &str,
    peer_label: &str,
    share_bits: u64,
    hash_bits: Option<u32>,
    accepted: bool,
    candidate: bool,
    error: Option<String>,
) {
    let now = Instant::now();
    let mut stats = lock_or_recover(&app.stats, "stats");
    let entry = stats
        .entry(worker_id.to_string())
        .or_insert_with(|| WorkerStats {
            session_id: String::new(),
            wallet: wallet.to_string(),
            worker_name: worker_name.to_string(),
            peer_label: peer_label.to_string(),
            connected: true,
            last_job_at: now,
            last_share_at: None,
            accepted_shares: 0,
            rejected_shares: 0,
            candidate_blocks: 0,
            best_hash_bits: 0,
            last_error: None,
            share_samples: VecDeque::new(),
        });
    entry.wallet = wallet.to_string();
    entry.worker_name = worker_name.to_string();
    if !peer_label.is_empty() {
        entry.peer_label = peer_label.to_string();
    }
    prune_share_samples(&mut entry.share_samples, now);
    if accepted {
        entry.accepted_shares = entry.accepted_shares.saturating_add(1);
        entry.last_share_at = Some(now);
        entry.share_samples.push_back(ShareSample {
            at: now,
            hashes: hashes_for_share_bits(share_bits),
        });
        if candidate {
            entry.candidate_blocks = entry.candidate_blocks.saturating_add(1);
        }
    } else {
        entry.rejected_shares = entry.rejected_shares.saturating_add(1);
    }
    if let Some(bits) = hash_bits {
        entry.best_hash_bits = entry.best_hash_bits.max(bits);
    }
    entry.last_error = error;
}

fn estimated_wallet_hashrate(app: &App, wallet: &str) -> f64 {
    let now = Instant::now();
    let mut stats = lock_or_recover(&app.stats, "stats");
    let mut total = 0.0f64;
    for stat in stats.values_mut() {
        prune_share_samples(&mut stat.share_samples, now);
        if stat.wallet == wallet {
            total += sample_hashrate(&stat.share_samples, now);
        }
    }
    total
}

fn estimated_rig_hashrate(app: &App, peer_label: &str, worker_name: &str) -> f64 {
    let now = Instant::now();
    let mut stats = lock_or_recover(&app.stats, "stats");
    let wanted = rig_key(peer_label, worker_name);
    let mut total = 0.0f64;
    for stat in stats.values_mut() {
        prune_share_samples(&mut stat.share_samples, now);
        if rig_key(&stat.peer_label, &stat.worker_name) == wanted {
            total += sample_hashrate(&stat.share_samples, now);
        }
    }
    total
}

fn canonical_stratum_error(err_text: &str) -> &'static str {
    if err_text.starts_with("unknown_method:") {
        "unknown_method"
    } else if err_text == "missing_method" {
        "missing_method"
    } else if err_text.starts_with("launch_guard") {
        "launch_guard"
    } else if matches!(
        err_text,
        "unknown_job"
            | "wrong_session_job"
            | "wrong_session_worker"
            | "wrong_worker_id"
            | "stale"
            | "stale_work"
    ) {
        "stale_job"
    } else if matches!(
        err_text,
        "work_mismatch" | "bad_prevhash" | "out_of_order" | "stale_or_out_of_order_block"
    ) {
        "work_mismatch"
    } else if matches!(err_text, "low_difficulty" | "pow_invalid") {
        "low_difficulty"
    } else if matches!(err_text, "bad_result" | "invalid_nonce" | "bad_submit") {
        "invalid_share"
    } else if err_text == "busy" {
        "busy"
    } else if err_text == "syncing" {
        "syncing"
    } else {
        "request_failed"
    }
}

fn operator_worker_line(stat: &WorkerStats, rate: f64, now: Instant) -> String {
    let last_error = stat.last_error.as_deref().unwrap_or("-");
    format!(
        "{}@{}={} ok={} bad={} blocks={} best={} last_share={} last_err={}",
        worker_tag(&stat.worker_name),
        stat.peer_label,
        format_hashrate(rate),
        stat.accepted_shares,
        stat.rejected_shares,
        stat.candidate_blocks,
        stat.best_hash_bits,
        format_age(stat.last_share_at, now),
        last_error
    )
}

fn canonical_submit_reject_reason(reply: &Value) -> String {
    reply
        .get("reject_reason")
        .and_then(|v| v.as_str())
        .or_else(|| reply.get("reason").and_then(|v| v.as_str()))
        .or_else(|| reply.get("error").and_then(|v| v.as_str()))
        .map(|s| canonical_stratum_error(s).to_string())
        .unwrap_or_else(|| "daemon_reject".to_string())
}

fn canonical_work_fetch_reject_reason(status: reqwest::StatusCode, body: &str) -> &'static str {
    if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        if body.contains("launch_guard_not_ready") || body.contains("launch_guard_") {
            return "launch_guard";
        }
        if body.contains("\"syncing\"") || body.contains("syncing") {
            return "syncing";
        }
        if body.contains("\"busy\"") || body.contains("busy") {
            return "busy";
        }
    }
    if body.contains("too_many_outstanding_work") {
        return "busy";
    }
    "work_fetch_failed"
}

fn should_ban_for_client_error(err_text: &str) -> bool {
    matches!(
        err_text,
        "client_idle_timeout" | "invalid_utf8" | "request_too_large" | "peer_rate_limited"
    )
}

fn is_stale_error_text(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("stale")
        || message.contains("stale_job")
        || message.contains("stale_work")
        || message.contains("stale_or_out_of_order_block")
}

fn is_stale_reject_reason(reason: &str) -> bool {
    matches!(
        reason,
        "stale" | "stale_job" | "stale_work" | "wrong_session_worker"
    )
}

fn unregister_session(app: &App, session_id: &str) -> usize {
    let mut removed = 0usize;
    {
        let mut workers = lock_or_recover(&app.workers, "workers");
        workers.retain(|_, worker| {
            let keep = worker.session_id != session_id;
            if !keep {
                removed += 1;
            }
            keep
        });
    }
    {
        let mut stats = lock_or_recover(&app.stats, "stats");
        stats.retain(|_, stat| stat.session_id != session_id);
    }
    removed
}

fn log_operator_snapshot(app: &App) {
    #[derive(Debug)]
    struct WalletSnapshot {
        wallet: String,
        rate: f64,
        connected: usize,
        workers: usize,
        accepted: u64,
        rejected: u64,
        blocks: u64,
        best_hash_bits: u32,
        last_share_at: Option<Instant>,
        worker_lines: Vec<String>,
    }

    let now = Instant::now();
    let mut stats = lock_or_recover(&app.stats, "stats");
    let mut wallets: HashMap<String, WalletSnapshot> = HashMap::new();

    for stat in stats.values_mut() {
        prune_share_samples(&mut stat.share_samples, now);
        let rate = sample_hashrate(&stat.share_samples, now);
        let entry = wallets
            .entry(stat.wallet.clone())
            .or_insert_with(|| WalletSnapshot {
                wallet: stat.wallet.clone(),
                rate: 0.0,
                connected: 0,
                workers: 0,
                accepted: 0,
                rejected: 0,
                blocks: 0,
                best_hash_bits: 0,
                last_share_at: None,
                worker_lines: Vec::new(),
            });
        entry.rate += rate;
        entry.workers += 1;
        if stat.connected {
            entry.connected += 1;
        }
        entry.accepted = entry.accepted.saturating_add(stat.accepted_shares);
        entry.rejected = entry.rejected.saturating_add(stat.rejected_shares);
        entry.blocks = entry.blocks.saturating_add(stat.candidate_blocks);
        entry.best_hash_bits = entry.best_hash_bits.max(stat.best_hash_bits);
        if stat
            .last_share_at
            .map(|ts| entry.last_share_at.map(|prev| ts > prev).unwrap_or(true))
            .unwrap_or(false)
        {
            entry.last_share_at = stat.last_share_at;
        }
        entry
            .worker_lines
            .push(operator_worker_line(stat, rate, now));
    }

    let mut snapshots: Vec<WalletSnapshot> = wallets.into_values().collect();
    snapshots.sort_by(|a, b| {
        b.rate
            .partial_cmp(&a.rate)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.connected.cmp(&a.connected))
            .then_with(|| b.accepted.cmp(&a.accepted))
    });

    for snapshot in snapshots.into_iter().take(8) {
        log_sys(format!(
            "ops wallet={} total_hs={} connected={}/{} shares_ok={} shares_bad={} blocks={} best_bits={} last_share={}",
            short_wallet(&snapshot.wallet),
            format_hashrate(snapshot.rate),
            snapshot.connected,
            snapshot.workers,
            snapshot.accepted,
            snapshot.rejected,
            snapshot.blocks,
            snapshot.best_hash_bits,
            format_age(snapshot.last_share_at, now)
        ));
        if snapshot.worker_lines.len() > 1 {
            let mut worker_lines = snapshot.worker_lines;
            worker_lines.sort();
            log_sys(format!(
                "ops wallet={} workers={}",
                short_wallet(&snapshot.wallet),
                worker_lines.join(" | ")
            ));
        }
    }
}

fn prune_jobs(jobs: &mut HashMap<String, WorkJob>, ttl: Duration) -> usize {
    let now = Instant::now();
    let before = jobs.len();
    jobs.retain(|_, job| now.duration_since(job.created_at) <= ttl);
    before.saturating_sub(jobs.len())
}

fn prune_consumed_jobs(jobs: &mut HashMap<String, ConsumedJob>, ttl: Duration) -> usize {
    let now = Instant::now();
    let before = jobs.len();
    jobs.retain(|_, job| now.duration_since(job.consumed_at) <= ttl);
    before.saturating_sub(jobs.len())
}

fn upsert_worker(app: &App, worker_id: &str, session_id: &str, wallet: &str, worker_name: &str) {
    let mut workers = lock_or_recover(&app.workers, "workers");
    workers.insert(
        worker_id.to_string(),
        WorkerState {
            session_id: session_id.to_string(),
            wallet: wallet.to_string(),
            worker_name: worker_name.to_string(),
        },
    );
}

fn worker_lookup(app: &App, session_id: &str, worker_id: &str) -> Result<WorkerState> {
    let workers = lock_or_recover(&app.workers, "workers");
    let Some(worker) = workers.get(worker_id) else {
        bail!("unknown_worker_id");
    };
    if worker.session_id != session_id {
        bail!("wrong_session_worker");
    }
    Ok(worker.clone())
}

fn insert_recent_job(app: &App, job: WorkJob, ttl: Duration) {
    let mut jobs = lock_or_recover(&app.jobs, "jobs");
    let _ = prune_jobs(&mut jobs, ttl);
    jobs.insert(job.job_id.clone(), job.clone());

    let mut consumed = lock_or_recover(&app.consumed_jobs, "consumed_jobs");
    let _ = prune_consumed_jobs(&mut consumed, ttl);
    consumed.remove(&job.job_id);

    let mut job_ids: Vec<(String, Instant)> = jobs
        .iter()
        .filter(|(_, j)| j.session_id == job.session_id && j.worker_id == job.worker_id)
        .map(|(id, j)| (id.clone(), j.created_at))
        .collect();
    job_ids.sort_by_key(|(_, created_at)| *created_at);
    while job_ids.len() > 1 {
        let (old_id, _) = job_ids.remove(0);
        if jobs.remove(&old_id).is_some() {
            consumed.insert(
                old_id,
                ConsumedJob {
                    session_id: job.session_id.clone(),
                    worker_id: job.worker_id.clone(),
                    consumed_at: Instant::now(),
                },
            );
        }
    }
}

fn consume_job(app: &App, session_id: &str, worker_id: &str, job_id: &str) {
    let mut jobs = lock_or_recover(&app.jobs, "jobs");
    let removed = jobs.remove(job_id);
    drop(jobs);
    if let Some(job) = removed {
        if job.session_id == session_id && job.worker_id == worker_id {
            let mut consumed = lock_or_recover(&app.consumed_jobs, "consumed_jobs");
            let ttl = Duration::from_secs(CONSUMED_JOB_TTL_SECS.max(app.args.job_ttl_secs));
            let _ = prune_consumed_jobs(&mut consumed, ttl);
            consumed.insert(
                job_id.to_string(),
                ConsumedJob {
                    session_id: session_id.to_string(),
                    worker_id: worker_id.to_string(),
                    consumed_at: Instant::now(),
                },
            );
        }
    }
}

fn infer_network(args: &Args) -> String {
    if let Some(n) = args.network.as_deref() {
        return n.trim().to_string();
    }
    if args.daemon.contains(":18085") || args.daemon.contains("/testnet") {
        "testnet".to_string()
    } else if args.daemon.contains(":19085") {
        "mainnet".to_string()
    } else {
        "mainnet".to_string()
    }
}

fn parse_network_arg(network: &str) -> Result<Network> {
    match network.trim().to_ascii_lowercase().as_str() {
        "mainnet" => Ok(Network::Mainnet),
        "testnet" => Ok(Network::Testnet),
        "stagenet" => Ok(Network::Stagenet),
        _ => bail!("invalid_network"),
    }
}

fn validate_runtime_config(args: &Args) -> Result<()> {
    let bind_addr: std::net::SocketAddr = args
        .bind
        .parse()
        .with_context(|| format!("invalid_bind: {}", args.bind))?;
    validate_http_url(&args.daemon, "daemon")?;
    if args.share_bits == 0 || args.share_bits > 31 {
        bail!("invalid_share_bits");
    }
    if args.job_refresh_secs == 0 {
        bail!("invalid_job_refresh_secs");
    }
    if args.job_ttl_secs < args.job_refresh_secs {
        bail!("invalid_job_ttl_secs");
    }
    match (&args.pool_api_url, &args.pool_api_key) {
        (Some(_), None) | (None, Some(_)) => bail!("pool_api_requires_url_and_key"),
        _ => {}
    }
    if let Some(url) = args.pool_api_url.as_deref() {
        validate_http_url(url, "pool_api")?;
        let parsed =
            reqwest::Url::parse(url).with_context(|| "invalid_pool_api_url".to_string())?;
        if parsed.scheme() == "http" && !url_host_is_local(&parsed) {
            bail!("pool_api_requires_https_for_non_local_host");
        }
    }
    if let Some(network) = args.network.as_deref() {
        parse_network_arg(network)?;
    }
    if bind_addr.ip().is_unspecified() {
        log_sys("public bind enabled explicitly by operator");
    }
    Ok(())
}

async fn verify_daemon_reachable(
    http: &reqwest::Client,
    base_url: &str,
    network: Network,
) -> Result<()> {
    let base = base_url.trim_end_matches('/');
    let health_url = format!("{}/health", base);
    if let Ok(reply) = http.get(&health_url).send().await {
        if reply.status().is_success() {
            return Ok(());
        }
        if reply.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            let body = reply.text().await.unwrap_or_default();
            bail!(
                "daemon_preflight_syncing: status={} base_url={} health_body={}",
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                base,
                body
            );
        }
    }
    let tip_url = format!("{}/tip", base);
    let tip_reply = http
        .get(&tip_url)
        .send()
        .await
        .with_context(|| format!("daemon_preflight_failed: GET {}", tip_url))?;
    if tip_reply.status().is_success() {
        return Ok(());
    }
    let probe_addr = match network {
        Network::Mainnet => "dut1111111111111111111111111111111111111111",
        Network::Testnet => "test1111111111111111111111111111111111111111",
        Network::Stagenet => "stg1111111111111111111111111111111111111111",
    };
    let work_url = format!("{}/work?address={}", base, probe_addr);
    let work_reply = http
        .get(&work_url)
        .send()
        .await
        .with_context(|| format!("daemon_preflight_failed: GET {}", work_url))?;
    let work_status = work_reply.status();
    if work_status.is_success() {
        return Ok(());
    }
    if work_status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        let body = work_reply.text().await.unwrap_or_default();
        if body.contains("\"syncing\"") || body.contains("syncing") {
            bail!(
                "daemon_preflight_syncing: status={} base_url={} work_body={}",
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                base,
                body
            );
        }
    }
    bail!(
        "daemon_preflight_failed: tip_status={} work_status={} base_url={}",
        tip_reply.status(),
        work_status,
        base
    );
}

async fn write_json_line(w: &mut tokio::net::tcp::OwnedWriteHalf, v: &Value) -> Result<()> {
    w.write_all(v.to_string().as_bytes()).await?;
    w.write_all(b"\n").await?;
    Ok(())
}

async fn fetch_work(
    app: &App,
    wallet: &str,
    worker_name: &str,
    worker_id: &str,
    session_id: &str,
    peer_label: &str,
) -> Result<WorkJob> {
    let wallet_hs = estimated_wallet_hashrate(app, wallet);
    let rig_hs = estimated_rig_hashrate(app, peer_label, worker_name);
    let effective_hs = wallet_hs.max(rig_hs);
    let url = if effective_hs > 0.0 {
        format!(
            "{}/work?address={}&hs={:.3}",
            app.args.daemon.trim_end_matches('/'),
            wallet,
            effective_hs
        )
    } else {
        format!(
            "{}/work?address={}",
            app.args.daemon.trim_end_matches('/'),
            wallet
        )
    };
    let reply = app
        .http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {}", url))?;

    if !reply.status().is_success() {
        let status = reply.status();
        let body = reply.text().await.unwrap_or_default();
        let reason = canonical_work_fetch_reject_reason(status, &body);
        log_err(format!(
            "work fetch failed wallet={} worker={} peer={} status={} reason={} body={}",
            short_wallet(wallet),
            worker_tag(worker_name),
            short_peer_label(peer_label),
            status,
            reason,
            body
        ));
        bail!("{}", reason);
    }

    let work: WorkReply = reply.json().await.context("decode /work reply")?;
    let seq = app.seq.fetch_add(1, Ordering::Relaxed);
    let job_id = format!("{:016x}", seq);
    mark_worker_job(app, worker_id, wallet, worker_name, peer_label);
    let job = WorkJob {
        job_id,
        session_id: session_id.to_string(),
        worker_name: worker_name.to_string(),
        worker_id: worker_id.to_string(),
        wallet: wallet.to_string(),
        work_id: work.work_id,
        blob: work.header80,
        height: work.height,
        bits: work.bits,
        share_bits: app.args.share_bits,
        anchor_hash32: work.anchor_hash32,
        target: job_target_hex(app.args.share_bits),
        created_at: Instant::now(),
    };
    log_net(format!(
        "new work wallet={} worker={} height={} block_bits={} share_bits={} wallet_hs={} rig_hs={} active_hs={}",
        short_wallet(wallet),
        worker_tag(&job.worker_name),
        job.height,
        job.bits,
        job.share_bits,
        format_hashrate(wallet_hs),
        format_hashrate(rig_hs),
        format_hashrate(effective_hs),
    ));
    Ok(job)
}

fn job_json(job: &WorkJob) -> Value {
    json!({
        "job_id": job.job_id,
        "blob": job.blob,
        "target": job.target,
        "height": job.height,
        "anchor_hash32": job.anchor_hash32,
        "bits": job.bits,
        "share_bits": job.share_bits,
    })
}

fn session_job_lookup(app: &App, session_id: &str, worker_id: &str, job_id: &str) -> Result<WorkJob> {
    let jobs = lock_or_recover(&app.jobs, "jobs");
    let Some(job) = jobs.get(job_id) else {
        drop(jobs);
        let mut consumed = lock_or_recover(&app.consumed_jobs, "consumed_jobs");
        let ttl = Duration::from_secs(CONSUMED_JOB_TTL_SECS.max(app.args.job_ttl_secs));
        let _ = prune_consumed_jobs(&mut consumed, ttl);
        if let Some(job) = consumed.get(job_id) {
            if job.session_id == session_id && job.worker_id == worker_id {
                bail!("stale_job");
            }
        }
        bail!("unknown_job");
    };
    if job.session_id != session_id {
        bail!("wrong_session_job");
    }
    Ok(job.clone())
}

fn verify_share(job: &WorkJob, nonce: u64, result_hex: &str) -> Result<(bool, u32)> {
    let anchor = parse_h32(&job.anchor_hash32)?;
    let blob_bytes = hex::decode(&job.blob).context("decode blob")?;
    if blob_bytes.len() < 80 {
        bail!("blob_too_short");
    }
    let mut header80 = [0u8; 80];
    header80.copy_from_slice(&blob_bytes[..80]);

    let dataset = dataset_for(job.height, anchor);
    let digest = dutahash::pow_digest(&header80, nonce, job.height, anchor, dataset.as_slice());
    let digest_hex = hex::encode(digest.0);
    if !digest_hex.eq_ignore_ascii_case(result_hex) {
        bail!("bad_result");
    }
    let hash_bits = leading_zero_bits(&digest.0);
    Ok((hash_bits >= job.bits as u32, hash_bits))
}

async fn send_pool_event(
    app: &App,
    remote_ip: Option<String>,
    job: &WorkJob,
    is_valid: bool,
    is_candidate: bool,
    reject_reason: Option<String>,
    block_hash: Option<String>,
    extra_json: Option<Value>,
) {
    let Some(url) = app.args.pool_api_url.as_deref() else {
        return;
    };
    let Some(key) = app.args.pool_api_key.as_deref() else {
        return;
    };

    let payload = PoolShareEvent {
        miner_address: &job.wallet,
        worker_name: &job.worker_name,
        job_id: &job.job_id,
        work_id: &job.work_id,
        network: &infer_network(&app.args),
        is_valid,
        is_candidate,
        share_bits: format!("{:08x}", job.share_bits),
        reject_reason,
        height_hint: job.height,
        remote_ip,
        block_hash,
        extra_json,
    };

    match app
        .http
        .post(url)
        .header("x-internal-key", key)
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) => {
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                log_err(format!(
                    "pool event failed wallet={} worker={} job={} status={} body={}",
                    job.wallet,
                    worker_tag(&job.worker_name),
                    job.job_id,
                    status,
                    body
                ));
            }
        }
        Err(e) => {
            log_err(format!(
                "pool event failed wallet={} worker={} job={} err={}",
                job.wallet,
                worker_tag(&job.worker_name),
                job.job_id,
                e
            ));
        }
    }
}

async fn submit_candidate(
    app: &App,
    _session_id: &str,
    worker_name: &str,
    job_id: &str,
    work_id: &str,
    nonce: u64,
) -> Result<Value> {
    log_cpu(format!(
        "block attempt worker={} job={} work={} nonce={}",
        worker_tag(worker_name),
        job_id,
        short_id(work_id),
        nonce
    ));
    let url = format!("{}/submit_work", app.args.daemon.trim_end_matches('/'));
    let started = Instant::now();
    let reply = app
        .http
        .post(&url)
        .json(&SubmitReq {
            work_id: work_id.to_string(),
            nonce,
        })
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;

    let status = reply.status();
    let body = reply.text().await.unwrap_or_default();
    let parsed: Value =
        serde_json::from_str(&body).unwrap_or_else(|_| json!({"status":"rejected","detail":body}));
    let reject_reason = canonical_submit_reject_reason(&parsed);
    if status.is_success() {
        log_cpu(format!(
            "block submit accepted worker={} job={} work={} status={} elapsed_ms={}",
            worker_tag(worker_name),
            job_id,
            short_id(work_id),
            status,
            started.elapsed().as_millis()
        ));
    } else if !is_stale_reject_reason(&reject_reason) {
        log_cpu(format!(
            "block submit rejected worker={} job={} work={} status={} elapsed_ms={} reason={}",
            worker_tag(worker_name),
            job_id,
            short_id(work_id),
            status,
            started.elapsed().as_millis(),
            reject_reason
        ));
    }
    if !status.is_success() {
        bail!(parsed.to_string());
    }
    Ok(parsed)
}

async fn handle_login(
    app: &App,
    session_id: &str,
    peer_label: &str,
    params: &Value,
) -> Result<Value> {
    let login = params
        .get("login")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim();
    let (wallet, worker_name) = match validate_login_identity(app, login) {
        Ok(v) => v,
        Err(e) => {
            log_err(format!(
                "login rejected peer={} reason={}",
                short_peer_label(peer_label),
                e
            ));
            return Err(e);
        }
    };
    let worker_id = format!(
        "{}-{:016x}",
        session_id,
        app.seq.fetch_add(1, Ordering::Relaxed)
    );
    upsert_worker(app, &worker_id, session_id, &wallet, &worker_name);
    record_worker_login(
        app,
        &worker_id,
        session_id,
        &wallet,
        &worker_name,
        peer_label,
    );
    let job = match fetch_work(
        app,
        &wallet,
        &worker_name,
        &worker_id,
        session_id,
        peer_label,
    )
    .await
    {
        Ok(job) => job,
        Err(e) => {
            unregister_worker(app, &worker_id);
            log_err(format!(
                "login rejected peer={} wallet={} worker={} reason=work_fetch_failed err={}",
                short_peer_label(peer_label),
                short_wallet(&wallet),
                worker_tag(&worker_name),
                e
            ));
            return Err(e);
        }
    };
    insert_recent_job(app, job.clone(), Duration::from_secs(app.args.job_ttl_secs));
    log_net(format!(
        "miner connected peer={} wallet={} worker={} height={} bits={} share_bits={} job={} work={}",
        short_peer_label(peer_label),
        short_wallet(&wallet),
        worker_tag(&worker_name),
        job.height,
        job.bits,
        job.share_bits,
        job.job_id,
        short_id(&job.work_id)
    ));

    Ok(json!({
        "id": worker_id,
        "status": "OK",
        "job": job_json(&job)
    }))
}

async fn handle_getjob(app: &App, session_id: &str, params: &Value) -> Result<Value> {
    let worker_id = params.get("id").and_then(|x| x.as_str()).unwrap_or("");
    if worker_id.is_empty() {
        log_err("job request rejected reason=missing_worker_id");
        bail!("missing_worker_id");
    }

    let worker = match worker_lookup(app, session_id, worker_id) {
        Ok(worker) => worker,
        Err(e) => {
            log_err(format!(
                "job request rejected worker={} reason={}",
                worker_tag(worker_id),
                e
            ));
            return Err(e);
        }
    };
    let wallet = worker.wallet;
    let worker_name = worker.worker_name;
    let peer_label = {
        let stats = lock_or_recover(&app.stats, "stats");
        stats
            .get(worker_id)
            .map(|s| s.peer_label.clone())
            .unwrap_or_else(|| "-".to_string())
    };

    let job = fetch_work(
        app,
        &wallet,
        &worker_name,
        worker_id,
        session_id,
        &peer_label,
    )
    .await?;
    insert_recent_job(app, job.clone(), Duration::from_secs(app.args.job_ttl_secs));
    log_job(format!(
        "assigned work peer={} wallet={} worker={} height={} block_bits={} share_bits={} job={} work={}",
        short_peer_label(&peer_label),
        short_wallet(&wallet),
        worker_tag(&worker_name),
        job.height,
        job.bits,
        job.share_bits,
        job.job_id,
        short_id(&job.work_id)
    ));

    Ok(json!({"status": "OK", "job": job_json(&job)}))
}

async fn handle_submit(
    app: &App,
    session_id: &str,
    peer_ip: Option<String>,
    params: &Value,
) -> Result<Value> {
    let worker_id = params.get("id").and_then(|x| x.as_str()).unwrap_or("");
    let job_id = params.get("job_id").and_then(|x| x.as_str()).unwrap_or("");
    let nonce_hex = params.get("nonce").and_then(|x| x.as_str()).unwrap_or("");
    let result_hex = params.get("result").and_then(|x| x.as_str()).unwrap_or("");
    if worker_id.is_empty() || job_id.is_empty() || nonce_hex.is_empty() || result_hex.is_empty() {
        log_err(format!(
            "share rejected worker={} job={} reason=bad_submit has_nonce={} has_result={}",
            worker_tag(worker_id),
            job_id,
            !nonce_hex.is_empty(),
            !result_hex.is_empty()
        ));
        bail!("bad_submit");
    }

    let job = match session_job_lookup(app, session_id, worker_id, job_id) {
        Ok(job) => job,
        Err(e) => {
            log_err(format!(
                "share rejected worker={} job={} reason={}",
                worker_tag(worker_id),
                job_id,
                e
            ));
            return Err(e);
        }
    };
    if job.worker_id != worker_id {
        log_err(format!(
            "share rejected worker={} job={} work={} reason=wrong_worker_id expected_worker={}",
            worker_tag(&job.worker_name),
            job_id,
            short_id(&job.work_id),
            worker_tag(&job.worker_name)
        ));
        bail!("wrong_worker_id");
    }

    let nonce = match nonce_from_hex8(nonce_hex) {
        Ok(nonce) => nonce,
        Err(e) => {
            let reject_reason = canonical_stratum_error(&e.to_string()).to_string();
            record_share_result(
                app,
                &job.worker_id,
                &job.wallet,
                &job.worker_name,
                peer_ip.as_deref().unwrap_or("-"),
                job.share_bits,
                None,
                false,
                false,
                Some(reject_reason),
            );
            log_err(format!(
                "share rejected worker={} job={} work={} nonce={} reason=invalid_nonce err={}",
                worker_tag(&job.worker_name),
                job_id,
                short_id(&job.work_id),
                nonce_hex,
                e
            ));
            return Err(e);
        }
    };
    let (is_candidate, hash_bits) = match verify_share(&job, nonce, result_hex) {
        Ok(v) => v,
        Err(e) => {
            let reject_reason = canonical_stratum_error(&e.to_string()).to_string();
            record_share_result(
                app,
                &job.worker_id,
                &job.wallet,
                &job.worker_name,
                peer_ip.as_deref().unwrap_or("-"),
                job.share_bits,
                None,
                false,
                false,
                Some(reject_reason.clone()),
            );
            log_err(format!(
                "share rejected worker={} job={} work={} nonce={} reason=verify_failed err={}",
                worker_tag(&job.worker_name),
                job_id,
                short_id(&job.work_id),
                nonce,
                e
            ));
            send_pool_event(
                app,
                peer_ip,
                &job,
                false,
                false,
                Some(reject_reason),
                None,
                Some(json!({"nonce": nonce, "result": result_hex, "error": e.to_string()})),
            )
            .await;
            return Err(e);
        }
    };
    log_cpu(format!(
        "share check wallet={} worker={} height={} nonce={} hash_bits={} block_bits={} share_bits={} block_found={}",
        short_wallet(&job.wallet),
        worker_tag(&job.worker_name),
        job.height,
        nonce,
        hash_bits,
        job.bits,
        job.share_bits,
        is_candidate
    ));

    if is_candidate {
        let accepted = match submit_candidate(
            app,
            session_id,
            &job.worker_name,
            job_id,
            &job.work_id,
            nonce,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                let reject_reason = e
                    .to_string()
                    .parse::<Value>()
                    .ok()
                    .map(|reply| canonical_submit_reject_reason(&reply))
                    .unwrap_or_else(|| canonical_stratum_error(&e.to_string()).to_string());
                if !is_stale_reject_reason(&reject_reason) {
                    log_err(format!(
                        "block submit failed worker={} job={} work={} nonce={} reason=daemon_reject err={}",
                        worker_tag(&job.worker_name),
                        job_id,
                        short_id(&job.work_id),
                        nonce,
                        e
                    ));
                }
                send_pool_event(
                    app,
                    peer_ip,
                    &job,
                    false,
                    true,
                    Some(reject_reason),
                    None,
                    Some(json!({"nonce": nonce, "result": result_hex, "error": e.to_string()})),
                )
                .await;
                return Err(e);
            }
        };
        let block_hash = accepted
            .get("hash32")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        record_share_result(
            app,
            &job.worker_id,
            &job.wallet,
            &job.worker_name,
            peer_ip.as_deref().unwrap_or("-"),
            job.share_bits,
            Some(hash_bits),
            true,
            true,
            None,
        );
        log_cpu(format!(
            "block found worker={} height={} block_bits={} algo=dutahash hash={}",
            worker_tag(&job.worker_name),
            job.height,
            job.bits,
            block_hash.as_deref().unwrap_or("-")
        ));
        send_pool_event(
            app,
            peer_ip,
            &job,
            true,
            true,
            None,
            block_hash,
            Some(json!({"nonce": nonce, "result": result_hex, "accepted": accepted})),
        )
        .await;
        consume_job(app, session_id, worker_id, job_id);
        return Ok(json!({
            "status": "OK",
            "candidate": true,
            "accepted": accepted,
        }));
    }

    if hash_bits < job.share_bits as u32 {
        record_share_result(
            app,
            &job.worker_id,
            &job.wallet,
            &job.worker_name,
            peer_ip.as_deref().unwrap_or("-"),
            job.share_bits,
            Some(hash_bits),
            false,
            false,
            Some("low_difficulty".to_string()),
        );
        log_err(format!(
            "submit reject worker={} height={} nonce={} reason=low_difficulty hash_bits={} share_bits={}",
            worker_tag(&job.worker_name),
            job.height,
            nonce,
            hash_bits,
            job.share_bits
        ));
        send_pool_event(
            app,
            peer_ip,
            &job,
            false,
            false,
            Some("low_difficulty".to_string()),
            None,
            Some(json!({"nonce": nonce, "result": result_hex, "hash_bits": hash_bits})),
        )
        .await;
        bail!("low_difficulty");
    }

    record_share_result(
        app,
        &job.worker_id,
        &job.wallet,
        &job.worker_name,
        peer_ip.as_deref().unwrap_or("-"),
        job.share_bits,
        Some(hash_bits),
        true,
        is_candidate,
        None,
    );

    log_cpu(format!(
        "share accepted worker={} height={} job={} work={} nonce={} hash_bits={} share_bits={}",
        worker_tag(&job.worker_name),
        job.height,
        job_id,
        short_id(&job.work_id),
        nonce,
        hash_bits,
        job.share_bits
    ));
    send_pool_event(
        app,
        peer_ip,
        &job,
        true,
        false,
        None,
        None,
        Some(json!({"nonce": nonce, "result": result_hex, "hash_bits": hash_bits})),
    )
    .await;
    Ok(json!({
        "status": "OK",
        "candidate": false,
        "hash_bits": hash_bits,
    }))
}

async fn handle_client(
    app: App,
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    _peer_guard: PeerConnectionGuard,
) -> Result<()> {
    let peer = stream.peer_addr().ok();
    let peer_ip = peer.map(|p| p.ip().to_string());
    let peer_label = peer
        .map(|p| p.to_string())
        .unwrap_or_else(|| "-".to_string());
    let session_id = format!("s{:016x}", app.seq.fetch_add(1, Ordering::Relaxed));
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut authenticated = false;

    loop {
        let idle_timeout_secs = if authenticated {
            CLIENT_IDLE_TIMEOUT_SECS
        } else {
            PRELOGIN_IDLE_TIMEOUT_SECS
        };
        let read = match tokio::time::timeout(
            Duration::from_secs(idle_timeout_secs),
            read_line_limited(&mut reader, MAX_STRATUM_LINE_BYTES),
        )
        .await
        {
            Ok(Ok(read)) => read,
            Ok(Err(e)) => {
                let err_text = e.to_string();
                if should_ban_for_client_error(&err_text) {
                    if let Some(ip) = peer.and_then(|p| Some(p.ip())) {
                        ban_peer_temporarily(&app, ip);
                    }
                }
                log_err(format!(
                    "disconnect peer={} reason={}",
                    short_peer_label(&peer_label),
                    err_text
                ));
                return Err(e);
            }
            Err(_) => {
                if let Some(ip) = peer.and_then(|p| Some(p.ip())) {
                    ban_peer_temporarily(&app, ip);
                }
                log_err(format!(
                    "disconnect peer={} reason=client_idle_timeout",
                    short_peer_label(&peer_label)
                ));
                bail!("client_idle_timeout");
            }
        };
        let Some(line_buf) = read else { break; };
        let line = match std::str::from_utf8(&line_buf) {
            Ok(line) => line.trim(),
            Err(_) => {
                if let Some(ip) = peer.and_then(|p| Some(p.ip())) {
                    ban_peer_temporarily(&app, ip);
                }
                log_err(format!(
                    "disconnect peer={} reason=invalid_utf8",
                    short_peer_label(&peer_label)
                ));
                bail!("invalid_utf8");
            }
        };
        if line.is_empty() {
            continue;
        }

        let msg: RpcMsg = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                if let Some(ip) = peer.and_then(|p| Some(p.ip())) {
                    enforce_peer_rate_limit(&app, ip, "invalid_json")?;
                }
                log_err(format!(
                    "request invalid_json peer={} err={}",
                    short_peer_label(&peer_label),
                    e
                ));
                let out = json!({"id": Value::Null, "error": {"message": format!("invalid_json: {}", e)}});
                write_json_line(&mut w, &out).await?;
                continue;
            }
        };

        let method_name = msg.method.as_deref().unwrap_or("<missing>");
        if let Some(ip) = peer.and_then(|p| Some(p.ip())) {
            if let Err(e) = enforce_peer_rate_limit(&app, ip, method_name) {
                log_err(format!(
                    "disconnect peer={} method={} reason={}",
                    short_peer_label(&peer_label),
                    method_name,
                    e
                ));
                return Err(e);
            }
        }
        let response = match msg.method.as_deref() {
            Some("login") => handle_login(&app, &session_id, &peer_label, &msg.params).await,
            Some("getjob") => handle_getjob(&app, &session_id, &msg.params).await,
            Some("submit") => handle_submit(&app, &session_id, peer_ip.clone(), &msg.params).await,
            Some(other) => Err(anyhow!("unknown_method:{}", other)),
            None => Err(anyhow!("missing_method")),
        };

        let out = match response {
            Ok(result) => {
                if msg.method.as_deref() == Some("login") {
                    authenticated = true;
                }
                json!({"id": msg.id, "result": result, "error": Value::Null})
            }
            Err(e) => {
                let err_text = e.to_string();
                let reason = canonical_stratum_error(&err_text);
                if !is_stale_reject_reason(reason) && !is_stale_error_text(&err_text) {
                    log_err(format!(
                        "request failed peer={} method={} reason={} err={}",
                        short_peer_label(&peer_label),
                        method_name,
                        reason,
                        err_text
                    ));
                }
                json!({"id": msg.id, "result": Value::Null, "error": {"message": format!("{}", e)}})
            }
        };
        write_json_line(&mut w, &out).await?;
    }

    let removed = unregister_session(&app, &session_id);
    log_net(format!(
        "miner disconnected peer={} workers_removed={}",
        short_peer_label(&peer_label),
        removed
    ));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_runtime_config(&args)?;
    init_logging();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.job_refresh_secs.max(5)))
        .build()
        .context("build http client")?;
    verify_daemon_reachable(&http, &args.daemon, parse_network_arg(&infer_network(&args))?).await?;
    let listener = TcpListener::bind(&args.bind)
        .await
        .with_context(|| format!("bind {}", args.bind))?;

    let app = App {
        args: args.clone(),
        http,
        seq: Arc::new(AtomicU64::new(1)),
        jobs: Arc::new(Mutex::new(HashMap::new())),
        consumed_jobs: Arc::new(Mutex::new(HashMap::new())),
        workers: Arc::new(Mutex::new(HashMap::new())),
        stats: Arc::new(Mutex::new(HashMap::new())),
        conn_limit: Arc::new(Semaphore::new(MAX_STRATUM_CONNECTIONS)),
        peer_limits: Arc::new(Mutex::new(HashMap::new())),
        subnet_limits: Arc::new(Mutex::new(HashMap::new())),
    };

    let mut operator_tick = tokio::time::interval(Duration::from_secs(OPERATOR_SNAPSHOT_SECS));

    log_sys(format!(
        "start bind={} daemon={} network={} share_bits={} refresh={}s ttl={}s pool_api_url={}",
        args.bind,
        args.daemon,
        infer_network(&args),
        args.share_bits,
        args.job_refresh_secs,
        args.job_ttl_secs,
        args.pool_api_url.as_deref().unwrap_or("-")
    ));

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, peer) = accept?;
                let permit = match app.conn_limit.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        log_err(format!("reject peer={} reason=too_many_connections", peer));
                        drop(stream);
                        continue;
                    }
                };
                let peer_guard = match try_open_peer_connection(&app, peer.ip()) {
                    Ok(guard) => guard,
                    Err(e) => {
                        log_err(format!("reject peer={} reason={}", peer, e));
                        drop(permit);
                        drop(stream);
                        continue;
                    }
                };
                log_net(format!("connect peer={}", peer));
                let app_cloned = app.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(app_cloned, stream, permit, peer_guard).await {
                        log_err(format!("client err peer={} err={}", peer, e));
                    }
                });
            }
            _ = operator_tick.tick() => {
                log_operator_snapshot(&app);
            }
        }
    }
}
fn unregister_worker(app: &App, worker_id: &str) {
    {
        let mut workers = lock_or_recover(&app.workers, "workers");
        workers.remove(worker_id);
    }
    {
        let mut stats = lock_or_recover(&app.stats, "stats");
        stats.remove(worker_id);
    }
    {
        let mut jobs = lock_or_recover(&app.jobs, "jobs");
        jobs.retain(|_, job| job.worker_id != worker_id);
    }
    {
        let mut consumed = lock_or_recover(&app.consumed_jobs, "consumed_jobs");
        consumed.retain(|_, job| job.worker_id != worker_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app() -> App {
        App {
            args: Args {
                bind: "127.0.0.1:11001".to_string(),
                daemon: "http://127.0.0.1:19085".to_string(),
                share_bits: 24,
                job_refresh_secs: 15,
                job_ttl_secs: 30,
                pool_api_url: None,
                pool_api_key: None,
                network: Some("mainnet".to_string()),
            },
            http: reqwest::Client::new(),
            seq: Arc::new(AtomicU64::new(1)),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            consumed_jobs: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(HashMap::new())),
            conn_limit: Arc::new(Semaphore::new(MAX_STRATUM_CONNECTIONS)),
            peer_limits: Arc::new(Mutex::new(HashMap::new())),
            subnet_limits: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[test]
    fn split_login_keeps_wallet_and_worker() {
        assert_eq!(
            split_login("dut123.worker-a"),
            ("dut123".to_string(), "worker-a".to_string())
        );
        assert_eq!(split_login("dut123"), ("dut123".to_string(), String::new()));
    }

    #[test]
    fn estimated_wallet_hashrate_sums_recent_samples_only() {
        let app = test_app();
        let now = Instant::now();
        let mut stats = lock_or_recover(&app.stats, "stats");
        stats.insert(
            "worker-1".to_string(),
            WorkerStats {
                session_id: "s1".to_string(),
                wallet: "dut1".to_string(),
                worker_name: "rig1".to_string(),
                peer_label: "127.0.0.1".to_string(),
                connected: true,
                last_job_at: now,
                last_share_at: Some(now),
                accepted_shares: 1,
                rejected_shares: 0,
                candidate_blocks: 0,
                best_hash_bits: 30,
                last_error: None,
                share_samples: VecDeque::from([
                    ShareSample {
                        at: now - Duration::from_secs(HASHRATE_WINDOW_SECS + 5),
                        hashes: 4096.0,
                    },
                    ShareSample {
                        at: now - Duration::from_secs(5),
                        hashes: 1024.0,
                    },
                ]),
            },
        );
        drop(stats);

        let rate = estimated_wallet_hashrate(&app, "dut1");
        assert!(rate > 0.0);
        assert!(rate < 20.0);
    }

    #[test]
    fn canonical_error_maps_daemon_rejects_to_public_stratum_reasons() {
        assert_eq!(canonical_stratum_error("stale_work"), "stale_job");
        assert_eq!(canonical_stratum_error("work_mismatch"), "work_mismatch");
        assert_eq!(canonical_stratum_error("pow_invalid"), "low_difficulty");
        assert_eq!(canonical_stratum_error("syncing"), "syncing");
        assert_eq!(canonical_stratum_error("busy"), "busy");
        assert_eq!(
            canonical_stratum_error("launch_guard_not_ready"),
            "launch_guard"
        );
        assert_eq!(canonical_stratum_error("blob_too_short"), "request_failed");
    }

    #[test]
    fn submit_reject_reason_prefers_structured_reason_fields() {
        assert_eq!(
            canonical_submit_reject_reason(&json!({"reject_reason":"low_difficulty"})),
            "low_difficulty"
        );
        assert_eq!(
            canonical_submit_reject_reason(&json!({"reason":"stale"})),
            "stale_job"
        );
        assert_eq!(
            canonical_submit_reject_reason(&json!({"error":"work_mismatch"})),
            "work_mismatch"
        );
        assert_eq!(
            canonical_submit_reject_reason(&json!({"detail":"opaque"})),
            "daemon_reject"
        );
    }

    #[test]
    fn work_fetch_reject_reason_prefers_launch_guard_and_syncing() {
        assert_eq!(
            canonical_work_fetch_reject_reason(
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                r#"{"error":"launch_guard_not_ready","detail":"launch_guard_syncing tip_height=10 best_seen_height=12"}"#
            ),
            "launch_guard"
        );
        assert_eq!(
            canonical_work_fetch_reject_reason(
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                r#"{"error":"syncing","detail":"syncing tip_height=10 best_seen_height=12"}"#
            ),
            "syncing"
        );
        assert_eq!(
            canonical_work_fetch_reject_reason(
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                r#"{"error":"busy"}"#
            ),
            "busy"
        );
        assert_eq!(
            canonical_work_fetch_reject_reason(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                r#"{"error":"too_many_outstanding_work"}"#
            ),
            "busy"
        );
    }

    #[test]
    fn canonical_error_maps_session_worker_to_stale_job() {
        assert_eq!(canonical_stratum_error("wrong_session_worker"), "stale_job");
    }

    #[test]
    fn runtime_config_rejects_invalid_share_bits_and_ttl() {
        let mut args = test_app().args;
        args.share_bits = 0;
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_share_bits"
        );

        args.share_bits = 24;
        args.job_refresh_secs = 15;
        args.job_ttl_secs = 10;
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_job_ttl_secs"
        );
    }

    #[test]
    fn runtime_config_requires_complete_pool_api_settings() {
        let mut args = test_app().args;
        args.pool_api_url = Some("http://127.0.0.1:8080/share".to_string());
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "pool_api_requires_url_and_key"
        );

        args.pool_api_key = Some("secret".to_string());
        assert!(validate_runtime_config(&args).is_ok());

        args.pool_api_url = Some("http://pool.example.org/share".to_string());
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "pool_api_requires_https_for_non_local_host"
        );

        args.pool_api_url = Some("https://pool.example.org/share".to_string());
        assert!(validate_runtime_config(&args).is_ok());
    }

    #[test]
    fn runtime_config_rejects_unknown_network_name() {
        let mut args = test_app().args;
        args.network = Some("beta".to_string());
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_network"
        );
    }

    #[test]
    fn runtime_config_rejects_invalid_daemon_and_pool_urls() {
        let mut args = test_app().args;
        args.daemon = "not-a-url".to_string();
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_daemon_url"
        );

        args.daemon = "http://127.0.0.1:19085".to_string();
        args.pool_api_url = Some("ftp://pool.example.org/share".to_string());
        args.pool_api_key = Some("secret".to_string());
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_pool_api_url"
        );

        args.pool_api_url = Some("https://user:pass@pool.example.org/share".to_string());
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_pool_api_url_credentials_not_allowed"
        );

        args.pool_api_url = Some("https://pool.example.org/share".to_string());
        args.daemon = "http://user:pass@127.0.0.1:19085".to_string();
        assert_eq!(
            validate_runtime_config(&args).unwrap_err().to_string(),
            "invalid_daemon_url_credentials_not_allowed"
        );
    }

    #[test]
    fn default_bind_stays_loopback_for_release_safety() {
        let args = Args::parse_from(["stratum"]);
        assert_eq!(args.bind, "127.0.0.1:11001");
    }

    #[tokio::test]
    async fn read_line_limited_rejects_oversized_line_without_unbounded_read() {
        let payload = vec![b'a'; MAX_STRATUM_LINE_BYTES + 32];
        let mut reader = BufReader::new(std::io::Cursor::new(payload));
        let err = read_line_limited(&mut reader, MAX_STRATUM_LINE_BYTES)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "request_too_large");
    }

    #[test]
    fn peer_rate_limit_triggers_temporary_ban() {
        let app = test_app();
        let ip: IpAddr = "127.0.0.1".parse().expect("ip");
        for _ in 0..MAX_LOGIN_REQUESTS_PER_WINDOW {
            enforce_peer_rate_limit(&app, ip, "login").expect("within limit");
        }
        let err = enforce_peer_rate_limit(&app, ip, "login").unwrap_err();
        assert_eq!(err.to_string(), "peer_rate_limited");

        let err = enforce_peer_rate_limit(&app, ip, "getjob").unwrap_err();
        assert_eq!(err.to_string(), "peer_temporarily_banned");
    }

    #[test]
    fn peer_rate_limit_counts_unknown_methods_toward_global_window() {
        let app = test_app();
        let ip: IpAddr = "127.0.0.1".parse().expect("ip");
        for _ in 0..MAX_REQUESTS_PER_WINDOW {
            enforce_peer_rate_limit(&app, ip, "invalid_json").expect("within global limit");
        }
        let err = enforce_peer_rate_limit(&app, ip, "invalid_json").unwrap_err();
        assert_eq!(err.to_string(), "peer_rate_limited");
    }

    #[test]
    fn abuse_disconnect_errors_are_ban_eligible() {
        assert!(should_ban_for_client_error("client_idle_timeout"));
        assert!(should_ban_for_client_error("invalid_utf8"));
        assert!(should_ban_for_client_error("request_too_large"));
        assert!(should_ban_for_client_error("peer_rate_limited"));
        assert!(!should_ban_for_client_error("missing_method"));
    }

    #[test]
    fn peer_connection_limit_is_enforced_per_ip() {
        let app = test_app();
        let ip: IpAddr = "127.0.0.1".parse().expect("ip");
        let mut guards = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_IP {
            guards.push(try_open_peer_connection(&app, ip).expect("within limit"));
        }
        let err = try_open_peer_connection(&app, ip).unwrap_err();
        assert_eq!(err.to_string(), "too_many_connections_per_ip");
        drop(guards);
    }

    #[test]
    fn temporary_ban_blocks_new_connections_for_peer() {
        let app = test_app();
        let ip: IpAddr = "127.0.0.1".parse().expect("ip");
        ban_peer_temporarily(&app, ip);
        let err = try_open_peer_connection(&app, ip).unwrap_err();
        assert_eq!(err.to_string(), "peer_temporarily_banned");
    }

    #[test]
    fn public_subnet_connection_limit_is_enforced() {
        let app = test_app();
        let mut guards = Vec::new();
        for idx in 1..=MAX_CONNECTIONS_PER_PUBLIC_SUBNET {
            let ip: IpAddr = format!("8.8.8.{}", idx).parse().expect("ip");
            guards.push(try_open_peer_connection(&app, ip).expect("within subnet limit"));
        }
        let err = try_open_peer_connection(&app, "8.8.8.250".parse().unwrap()).unwrap_err();
        assert_eq!(err.to_string(), "too_many_connections_per_public_subnet");
        drop(guards);
    }

    #[test]
    fn newer_job_makes_previous_job_stale_for_same_worker() {
        let app = test_app();
        let ttl = Duration::from_secs(app.args.job_ttl_secs);
        let base_job = WorkJob {
            job_id: "job-1".to_string(),
            session_id: "s1".to_string(),
            worker_name: "rig1".to_string(),
            worker_id: "w1".to_string(),
            wallet: "dut1".to_string(),
            work_id: "work-1".to_string(),
            blob: "00".repeat(80),
            height: 10,
            bits: 24,
            share_bits: 24,
            anchor_hash32: "00".repeat(32),
            target: "ff".repeat(32),
            created_at: Instant::now(),
        };
        let mut newer_job = base_job.clone();
        newer_job.job_id = "job-2".to_string();
        newer_job.work_id = "work-2".to_string();
        newer_job.created_at = Instant::now() + Duration::from_millis(1);

        insert_recent_job(&app, base_job, ttl);
        insert_recent_job(&app, newer_job, ttl);

        let err = session_job_lookup(&app, "s1", "w1", "job-1").unwrap_err();
        assert_eq!(err.to_string(), "stale_job");
        assert!(session_job_lookup(&app, "s1", "w1", "job-2").is_ok());
    }

    #[test]
    fn consumed_job_is_reported_as_stale_for_same_worker_session() {
        let app = test_app();
        lock_or_recover(&app.consumed_jobs, "consumed_jobs").insert(
            "job-1".to_string(),
            ConsumedJob {
                session_id: "s1".to_string(),
                worker_id: "w1".to_string(),
                consumed_at: Instant::now(),
            },
        );

        let err = session_job_lookup(&app, "s1", "w1", "job-1").unwrap_err();
        assert_eq!(err.to_string(), "stale_job");
    }

    #[test]
    fn operator_worker_line_includes_reject_and_block_counters() {
        let now = Instant::now();
        let stat = WorkerStats {
            session_id: "s1".to_string(),
            wallet: "dut1".to_string(),
            worker_name: "rig1".to_string(),
            peer_label: "127.0.0.1:1000".to_string(),
            connected: true,
            last_job_at: now,
            last_share_at: Some(now),
            accepted_shares: 4,
            rejected_shares: 2,
            candidate_blocks: 1,
            best_hash_bits: 31,
            last_error: Some("stale_job".to_string()),
            share_samples: VecDeque::new(),
        };
        let line = operator_worker_line(&stat, 128.0, now);
        assert!(line.contains("ok=4"));
        assert!(line.contains("bad=2"));
        assert!(line.contains("blocks=1"));
        assert!(line.contains("best=31"));
        assert!(line.contains("last_err=stale_job"));
    }
}
