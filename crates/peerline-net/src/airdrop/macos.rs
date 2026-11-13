use super::*;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    net::{SocketAddr, SocketAddrV6},
    os::fd::AsRawFd,
    process::Stdio,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::Command,
    sync::Semaphore,
    task::JoinSet,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{self, pki_types::PrivatePkcs8KeyDer},
};

pub(super) async fn receive(options: Options, events: mpsc::Sender<Event>) -> anyhow::Result<()> {
    crate::ble::ensure_hardware().await?;
    let (ip, index) = if_addrs::get_if_addrs()?
        .into_iter()
        .filter(|interface| interface.name == "awdl0")
        .find_map(|interface| match (interface.ip(), interface.index) {
            (IpAddr::V6(ip), Some(index)) if ip.is_unicast_link_local() => Some((ip, index)),
            _ => None,
        })
        .context(
            "No AWDL IPv6 interface; enable Wi-Fi on a supported Mac. AirDrop was not started",
        )?;
    let socket = socket2::Socket::new(
        socket2::Domain::IPV6,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_only_v6(true)?;
    // Darwin requires SO_RECV_ANYIF to receive traffic on AWDL. Bind exclusively
    // to the scoped AWDL address, not public/LAN interfaces.
    let enabled: libc::c_int = 1;
    // SAFETY: valid socket, pointer and length for a synchronous integer option.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            0x1104,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    socket.bind(&SocketAddr::V6(SocketAddrV6::new(ip, 0, 0, index)).into())?;
    socket.listen(8)?;
    socket.set_nonblocking(true)?;
    let listener = TcpListener::from_std(socket.into())?;
    let port = listener.local_addr()?.port();

    let acceptor = tls_acceptor()?;

    // System DNS-SD provides native AWDL publication; child lifetime is scoped
    // to this receiver. No persistent system AirDrop setting is changed.
    let mut advertisement = Command::new("/usr/bin/dns-sd")
        .args([
            "-includeAWDL",
            "-i",
            "awdl0",
            "-R",
            &format!("{:012x}", rand::random::<u64>() & 0xffffffffffff),
            "_airdrop._tcp",
            "local.",
            &port.to_string(),
            "flags=136",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(
        advertisement
            .stdout
            .take()
            .context("DNS-SD output unavailable")?,
    )
    .lines();
    time::timeout(Duration::from_secs(8), async {
        while let Some(line) = lines.next_line().await? {
            if line.contains("registered and active") && line.contains("_airdrop._tcp") {
                return Ok::<_, anyhow::Error>(());
            }
        }
        anyhow::bail!("AirDrop service registration failed")
    })
    .await
    .context("AirDrop service registration timed out")??;
    let state = Arc::new(State {
        options,
        events: events.clone(),
        grant: Mutex::new(None),
        upload: Mutex::new(()),
    });
    events.send(Event::Ready).await?;
    let permits = Arc::new(Semaphore::new(8));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            result = advertisement.wait() => anyhow::bail!("AirDrop service advertisement stopped: {:?}", result?),
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                if !matches!(peer.ip(), IpAddr::V6(ip) if ip.is_unicast_link_local()) { continue; }
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                let acceptor = acceptor.clone(); let state = state.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let Ok(Ok(tls)) = time::timeout(Duration::from_secs(10), acceptor.accept(stream)).await else { return; };
                    let service = service_fn(move |request| handle(request, peer.ip(), state.clone()));
                    let _ = time::timeout(Duration::from_secs(600), http1::Builder::new()
                        .timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(10)).max_buf_size(64 * 1024)
                        .serve_connection(TokioIo::new(tls), service)).await;
                });
            }
        }
    }
}

fn tls_acceptor() -> anyhow::Result<TlsAcceptor> {
    let identity = rcgen::generate_simple_self_signed(vec!["peerline.local".into()])?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.der().clone()],
            PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
        )?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn https_discover_approve_and_chunked_upload() {
        let root = tempfile::tempdir().unwrap();
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(State {
            options: Options {
                name: "Peerline test".into(),
                destination: root.path().into(),
                max_bytes: 1024,
            },
            events,
            grant: Mutex::new(None),
            upload: Mutex::new(()),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("https://{}", listener.local_addr().unwrap());
        let acceptor = tls_acceptor().unwrap();
        let server = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                let (stream, peer) = listener.accept().await.unwrap();
                let acceptor = acceptor.clone();
                let state = state.clone();
                tasks.spawn(async move {
                    let stream = acceptor.accept(stream).await.unwrap();
                    http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request| handle(request, peer.ip(), state.clone())),
                        )
                        .await
                        .unwrap();
                });
            }
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .tls_danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let discover = client
            .post(format!("{endpoint}/Discover"))
            .body(super::super::tests::offer_bytes())
            .send()
            .await
            .unwrap();
        assert_eq!(discover.status(), StatusCode::OK);
        let identity =
            Value::from_reader(std::io::Cursor::new(discover.bytes().await.unwrap())).unwrap();
        assert_eq!(
            identity.as_dictionary().unwrap()["ReceiverComputerName"].as_string(),
            Some("Peerline test")
        );
        let ask = client
            .post(format!("{endpoint}/Ask"))
            .body(super::super::tests::offer_bytes())
            .send();
        let approve = async {
            let Event::Offer(offer) = rx.recv().await.unwrap() else {
                panic!("expected offer");
            };
            offer.accept.send(true).unwrap();
        };
        let (ask, ()) = tokio::join!(ask, approve);
        assert_eq!(ask.unwrap().status(), StatusCode::OK);
        let response = client
            .post(format!("{endpoint}/Upload"))
            .header("Content-Type", "application/x-cpio")
            .header("Transfer-Encoding", "chunked")
            .header("Expect", "100-continue")
            .body(include_bytes!("../../tests/fixtures/airdrop-newc.cpio.gz").as_slice())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(matches!(rx.recv().await.unwrap(), Event::Receiving));
        let Event::Received(path) = rx.recv().await.unwrap() else {
            panic!("expected completed upload");
        };
        assert_eq!(
            std::fs::read(path.join("photo.jpg")).unwrap(),
            b"AirDrop fixture\n"
        );
        server.abort();
        let _ = server.await;
    }
}
