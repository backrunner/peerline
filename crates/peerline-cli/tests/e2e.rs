use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

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
        .arg("recv")
        .arg("river-mango-42")
        .arg("rose-lime-iris-jade-1234")
        .arg("--no-tui")
        .arg("--port")
        .arg(port.to_string())
        .arg("--idle-timeout-minutes")
        .arg("0.02");
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

#[test]
fn direct_tcp_roundtrip_uses_destination_cwd() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("hello.txt"), "hello direct cli").unwrap();

    let port = free_port();
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    wait_for_port(port, Duration::from_secs(5));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{port}"),
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
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("one.txt"), "first").unwrap();
    std::fs::write(src.join("two.txt"), "second").unwrap();

    let port = free_port();
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    wait_for_port(port, Duration::from_secs(5));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{port}"),
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
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("hello.txt"), "fresh").unwrap();
    std::fs::write(dst.join("hello.txt"), "stale").unwrap();

    let port = free_port();
    let mut recv = spawn_recv(
        &dst,
        port,
        true,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    wait_for_port(port, Duration::from_secs(5));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{port}"),
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
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("nested/hello.txt"), "hello folder cli").unwrap();

    let port = free_port();
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    wait_for_port(port, Duration::from_secs(5));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            &format!("127.0.0.1:{port}"),
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
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("one.txt"), "first").unwrap();
    std::fs::write(src.join("two.txt"), "second").unwrap();

    let port = free_port();
    let mut recv = spawn_recv(
        &dst,
        port,
        false,
        &[("PEERLINE_BOOTSTRAP", ""), ("PEERLINE_DISABLE_MDNS", "1")],
    );
    wait_for_port(port, Duration::from_secs(5));

    for file in ["one.txt", "two.txt"] {
        let send = spawn_send(
            temp.path(),
            &[
                "send",
                &format!("127.0.0.1:{port}"),
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
        .arg("river-mango-42");
    assert!(set_name.output().unwrap().status.success());

    let mut recv = Command::new(bin());
    recv.current_dir(&dst)
        .env("XDG_CONFIG_HOME", &config_home)
        .env("PEERLINE_ALLOW_LOOPBACK_DISCOVERY", "1")
        .env("PEERLINE_BOOTSTRAP", "")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("recv")
        .arg("rose-lime-iris-jade-1234")
        .arg("--no-tui")
        .arg("--port")
        .arg(port.to_string())
        .arg("--idle-timeout-minutes")
        .arg("0.05");
    let mut recv = recv.spawn().unwrap();
    wait_for_port(port, Duration::from_secs(5));

    let send = spawn_send(
        temp.path(),
        &[
            "send",
            "river-mango-42",
            "rose-lime-iris-jade-1234",
            src.join("hello.txt").to_str().unwrap(),
        ],
        &[
            ("PEERLINE_ALLOW_LOOPBACK_DISCOVERY", "1"),
            ("PEERLINE_BOOTSTRAP", ""),
        ],
    );

    assert!(
        send.status.success(),
        "send stderr: {}",
        String::from_utf8_lossy(&send.stderr)
    );
    assert!(recv.wait().unwrap().success());
    assert_eq!(
        std::fs::read_to_string(dst.join("hello.txt")).unwrap(),
        "hello named cli"
    );
}
