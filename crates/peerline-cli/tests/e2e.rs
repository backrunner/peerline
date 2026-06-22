use peerline_core::{HumanCode, HumanName, NameCode};
use std::{
    io::{BufRead, BufReader},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{Mutex, MutexGuard, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

static NETWORK_E2E_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const TEST_NAME: &str = "river-mango-42";
const TEST_CODE: &str = "rose-lime-iris-jade-1234";
const PKARR_WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const RENDEZVOUS_BLACKHOLE_URL: &str = "http://127.0.0.1:9";

struct LocalPkarrTestnet {
    _testnet: pkarr::mainline::Testnet,
    bootstrap: String,
}

fn network_e2e_guard() -> MutexGuard<'static, ()> {
    NETWORK_E2E_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl LocalPkarrTestnet {
    fn new() -> Self {
        let testnet = pkarr::mainline::Testnet::builder(3).build().unwrap();
        let bootstrap = testnet.bootstrap.join(",");
        Self {
            _testnet: testnet,
            bootstrap,
        }
    }

    fn bootstrap(&self) -> &str {
        &self.bootstrap
    }
}

fn bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin!("peerline").into()
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_recv(cwd: &Path, port: u16, overwrite: bool, extra_env: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(bin());
    cmd.current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("RUST_LOG")
        .env("PEERLINE_DISABLE_TOR", "1")
        .env("PEERLINE_DISABLE_PUBLIC_TUNNELS", "1")
        .env("PEERLINE_DISABLE_I2P", "1")
        .arg("recv")
        .arg("river-mango-42")
        .arg("rose-lime-iris-jade-1234")
        .arg("--no-tui")
        .arg("--port")
        .arg(port.to_string())
        .arg("--idle-timeout-minutes")
        .arg("0.1");
    if overwrite {
        cmd.arg("--overwrite");
    }
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    cmd.spawn().unwrap()
}

fn spawn_send(cwd: &Path, args: &[&str], extra_env: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.current_dir(cwd)
        .env_remove("RUST_LOG")
        .env("PEERLINE_DISABLE_TOR", "1")
        .env("PEERLINE_DISABLE_PUBLIC_TUNNELS", "1")
        .env("PEERLINE_DISABLE_I2P", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    for arg in args {
        cmd.arg(arg);
    }
    cmd.output().unwrap()
}

fn wait_for_port(port: u16, timeout: Duration) {
    let start = Instant::now();
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for 127.0.0.1:{port}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_direct_recv_ready(child: &mut Child, timeout: Duration) -> u16 {
    let stdout = child.stdout.take().expect("recv stdout must be piped");
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if let Some(endpoint) = line.strip_prefix("direct: ")
                && let Ok(addr) = endpoint.parse::<std::net::SocketAddr>()
            {
                let _ = ready_tx.send(addr.port());
            }
        }
    });

    if let Ok(port) = ready_rx.recv_timeout(timeout) {
        return port;
    }

    match child.try_wait() {
        Ok(Some(status)) => panic!("recv exited before advertising direct listener: {status}"),
        Ok(None) => panic!("timed out waiting for recv to advertise direct listener"),
        Err(error) => panic!("could not inspect recv process: {error}"),
    }
}

fn configure_pkarr_only_env(cmd: &mut Command, bootstrap: &str) {
    cmd.env("PEERLINE_BOOTSTRAP", "")
        .env("PEERLINE_DISABLE_MDNS", "1")
        .env("PEERLINE_DISABLE_TOR", "1")
        .env("PEERLINE_DISABLE_PUBLIC_TUNNELS", "1")
        .env("PEERLINE_DISABLE_I2P", "1")
        .env("PEERLINE_PKARR_BOOTSTRAP", bootstrap)
        .env("PEERLINE_RENDEZVOUS_URLS", RENDEZVOUS_BLACKHOLE_URL);
}

fn describe_output(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

fn pkarr_public_key(name: &str, code: &str) -> pkarr::PublicKey {
    let lookup_key = NameCode::new(
        HumanName::parse(name).unwrap(),
        HumanCode::parse(code).unwrap(),
    )
    .lookup_key();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"peerline:pkarr:v1");
    hasher.update(&lookup_key.bytes());
    pkarr::Keypair::from_secret_key(hasher.finalize().as_bytes()).public_key()
}

fn wait_for_pkarr_publish(name: &str, code: &str, bootstrap: &str, timeout: Duration) {
    let bootstrap = bootstrap
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let mut builder = pkarr::Client::builder();
    builder
        .no_default_network()
        .bootstrap(&bootstrap)
        .no_relays()
        .request_timeout(Duration::from_secs(1));
    if !bootstrap.is_empty()
        && bootstrap
            .iter()
            .all(|value| value.starts_with("127.0.0.1:") || value.starts_with("localhost:"))
    {
        builder.dht(|dht| dht.bind_address(std::net::Ipv4Addr::LOCALHOST));
    }
    let client = builder.build().unwrap();
    let public_key = pkarr_public_key(name, code);

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let start = Instant::now();
            loop {
                if client.resolve_most_recent(&public_key).await.is_some() {
                    return;
                }
                assert!(
                    start.elapsed() <= timeout,
                    "timed out waiting for pkarr record for {name}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
}

fn spawn_named_pkarr_recv(
    cwd: &Path,
    port: u16,
    bootstrap: &str,
    identity_args: &[&str],
    config_home: Option<&Path>,
    debug: bool,
) -> Child {
    let mut cmd = Command::new(bin());
    cmd.current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("RUST_LOG");
    if let Some(config_home) = config_home {
        cmd.env("XDG_CONFIG_HOME", config_home);
    }
    configure_pkarr_only_env(&mut cmd, bootstrap);
    if debug {
        cmd.arg("--debug");
    }
    cmd.arg("recv");
    for arg in identity_args {
        cmd.arg(arg);
    }
    cmd.arg("--no-tui")
        .arg("--port")
        .arg(port.to_string())
        .arg("--idle-timeout-minutes")
        .arg("1")
        .arg("--no-quic")
        .arg("--no-dcutr")
        .arg("--no-turn")
        .arg("--no-relay-fallback");
    cmd.spawn().unwrap()
}

fn spawn_named_pkarr_send(
    cwd: &Path,
    bootstrap: &str,
    identity_args: &[&str],
    path: &Path,
    debug: bool,
) -> Output {
    let mut cmd = Command::new(bin());
    cmd.current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("RUST_LOG");
    configure_pkarr_only_env(&mut cmd, bootstrap);
    if debug {
        cmd.arg("--debug");
    }
    cmd.arg("send")
        .arg("--retry-attempts")
        .arg("1")
        .arg("--no-quic")
        .arg("--no-dcutr")
        .arg("--no-turn")
        .arg("--no-relay-fallback");
    for arg in identity_args {
        cmd.arg(arg);
    }
    cmd.arg(path);
    cmd.output().unwrap()
}

fn wait_for_named_pkarr_receiver(port: u16, bootstrap: &str) {
    wait_for_port(port, Duration::from_secs(5));
    wait_for_pkarr_publish(TEST_NAME, TEST_CODE, bootstrap, PKARR_WAIT_TIMEOUT);
}

fn kill_and_collect(mut child: Child) -> Output {
    let _ = child.kill();
    child.wait_with_output().unwrap()
}

fn assert_named_send_succeeded(send: &Output, recv: &mut Option<Child>) {
    if send.status.success() {
        return;
    }

    let recv_output = kill_and_collect(recv.take().unwrap());
    panic!(
        "send failed\nsend:\n{}\nrecv:\n{}",
        describe_output(send),
        describe_output(&recv_output),
    );
}

fn assert_recv_completed(recv_output: &Output) {
    assert!(
        String::from_utf8_lossy(&recv_output.stdout).contains("received 1 file(s), "),
        "recv output did not show a completed transfer\n{}",
        describe_output(recv_output)
    );
}

#[test]
fn direct_tcp_roundtrip_uses_destination_cwd() {
    let _guard = network_e2e_guard();
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("hello.txt"), "hello direct cli").unwrap();

    let port = 0;
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    let recv_port = wait_for_direct_recv_ready(&mut recv, Duration::from_secs(20));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{recv_port}"),
            src.join("hello.txt").to_str().unwrap(),
            "--code",
            "rose-lime-iris-jade-1234",
        ],
        &[],
    );

    assert!(
        send.status.success(),
        "send stderr: {}",
        String::from_utf8_lossy(&send.stderr)
    );
    assert!(recv.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(dst.join("hello.txt")).unwrap(),
        "hello direct cli"
    );
}

#[test]
fn direct_tcp_roundtrip_multiple_files_in_one_send() {
    let _guard = network_e2e_guard();
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("one.txt"), "first").unwrap();
    std::fs::write(src.join("two.txt"), "second").unwrap();

    let port = 0;
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    let recv_port = wait_for_direct_recv_ready(&mut recv, Duration::from_secs(20));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{recv_port}"),
            src.join("one.txt").to_str().unwrap(),
            src.join("two.txt").to_str().unwrap(),
            "--code",
            "rose-lime-iris-jade-1234",
        ],
        &[],
    );

    assert!(
        send.status.success(),
        "send stderr: {}",
        String::from_utf8_lossy(&send.stderr)
    );
    assert!(recv.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(dst.join("one.txt")).unwrap(),
        "first"
    );
    assert_eq!(
        std::fs::read_to_string(dst.join("two.txt")).unwrap(),
        "second"
    );
}

#[test]
fn direct_tcp_respects_overwrite_flag() {
    let _guard = network_e2e_guard();
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("hello.txt"), "fresh").unwrap();
    std::fs::write(dst.join("hello.txt"), "stale").unwrap();

    let port = 0;
    let mut recv = spawn_recv(
        &dst,
        port,
        true,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    let recv_port = wait_for_direct_recv_ready(&mut recv, Duration::from_secs(20));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{recv_port}"),
            src.join("hello.txt").to_str().unwrap(),
            "--code",
            "rose-lime-iris-jade-1234",
        ],
        &[],
    );

    assert!(
        send.status.success(),
        "send stderr: {}",
        String::from_utf8_lossy(&send.stderr)
    );
    assert!(recv.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(dst.join("hello.txt")).unwrap(),
        "fresh"
    );
    assert!(!dst.join("hello (1).txt").exists());
}

#[test]
fn direct_tcp_roundtrip_directory() {
    let _guard = network_e2e_guard();
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("nested/hello.txt"), "hello folder cli").unwrap();

    let port = 0;
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    let recv_port = wait_for_direct_recv_ready(&mut recv, Duration::from_secs(20));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{recv_port}"),
            src.to_str().unwrap(),
            "--code",
            "rose-lime-iris-jade-1234",
        ],
        &[],
    );

    assert!(
        send.status.success(),
        "send stderr: {}",
        String::from_utf8_lossy(&send.stderr)
    );
    assert!(recv.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(dst.join("src/nested/hello.txt")).unwrap(),
        "hello folder cli"
    );
}

#[test]
fn direct_tcp_receiver_accepts_multiple_sends_before_idle_exit() {
    let _guard = network_e2e_guard();
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("one.txt"), "first").unwrap();
    std::fs::write(src.join("two.txt"), "second").unwrap();

    let port = 0;
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    let recv_port = wait_for_direct_recv_ready(&mut recv, Duration::from_secs(20));

    for file in ["one.txt", "two.txt"] {
        let send = spawn_send(
            temp.path(),
            &[
                "send",
                &format!("127.0.0.1:{recv_port}"),
                src.join(file).to_str().unwrap(),
                "--code",
                "rose-lime-iris-jade-1234",
            ],
            &[],
        );
        assert!(
            send.status.success(),
            "send stderr: {}",
            String::from_utf8_lossy(&send.stderr)
        );
    }

    assert!(recv.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(dst.join("one.txt")).unwrap(),
        "first"
    );
    assert_eq!(
        std::fs::read_to_string(dst.join("two.txt")).unwrap(),
        "second"
    );
}

#[test]
fn named_send_uses_saved_name_and_can_route_locally() {
    let _guard = network_e2e_guard();
    let testnet = LocalPkarrTestnet::new();
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    let config_home = temp.path().join("config");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("hello.txt"), "hello named cli").unwrap();

    let port = free_port();
    let mut set_name = Command::new(bin());
    set_name
        .env("XDG_CONFIG_HOME", &config_home)
        .arg("set")
        .arg("name")
        .arg(TEST_NAME);
    assert!(set_name.output().unwrap().status.success());

    let mut recv = Some(spawn_named_pkarr_recv(
        &dst,
        port,
        testnet.bootstrap(),
        &[TEST_CODE],
        Some(&config_home),
        true,
    ));
    wait_for_named_pkarr_receiver(port, testnet.bootstrap());

    let send = spawn_named_pkarr_send(
        temp.path(),
        testnet.bootstrap(),
        &[TEST_NAME, TEST_CODE],
        &src.join("hello.txt"),
        true,
    );

    assert_named_send_succeeded(&send, &mut recv);
    let recv_output = kill_and_collect(recv.take().unwrap());
    assert_recv_completed(&recv_output);
    assert_eq!(
        std::fs::read_to_string(dst.join("hello.txt")).unwrap(),
        "hello named cli"
    );
}

#[test]
fn named_send_discovers_receiver_through_pkarr_mainline() {
    let _guard = network_e2e_guard();
    let testnet = LocalPkarrTestnet::new();

    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("hello.txt"), "hello pkarr cli").unwrap();

    let port = free_port();
    let mut recv = Some(spawn_named_pkarr_recv(
        &dst,
        port,
        testnet.bootstrap(),
        &[TEST_NAME, TEST_CODE],
        None,
        false,
    ));
    wait_for_named_pkarr_receiver(port, testnet.bootstrap());

    let send = spawn_named_pkarr_send(
        temp.path(),
        testnet.bootstrap(),
        &[TEST_NAME, TEST_CODE],
        &src.join("hello.txt"),
        true,
    );

    assert_named_send_succeeded(&send, &mut recv);
    let recv_output = kill_and_collect(recv.take().unwrap());

    let send_stderr = String::from_utf8_lossy(&send.stderr);
    assert!(
        send_stderr.contains("initial pkarr probe returned descriptor")
            || send_stderr.contains("pkarr discovery returned descriptor"),
        "send output did not show pkarr discovery\n{}",
        describe_output(&send)
    );
    assert_eq!(
        std::fs::read_to_string(dst.join("hello.txt")).unwrap(),
        "hello pkarr cli"
    );
    assert_recv_completed(&recv_output);
}
