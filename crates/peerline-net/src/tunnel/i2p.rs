use super::{
    I2P_ROUTE_LABEL, PublicTunnelSession, bind_public_tunnel_listener, open_websocket_session,
};
use peerline_core::TransferId;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    time,
};
use tokio_tungstenite::client_async;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct I2pSession {
    pub destination: String,
    pub b32: String,
    id: String,
}

#[derive(Debug)]
pub struct I2pForward {
    session_control: tokio::net::TcpStream,
    forward_control: tokio::net::TcpStream,
}

impl I2pForward {
    pub async fn close(&mut self) {
        let _ = self.forward_control.shutdown().await;
        let _ = self.session_control.shutdown().await;
    }
}

pub fn normalize_i2p_url(raw: &str) -> anyhow::Result<String> {
    let raw = if raw.contains("://") {
        raw.to_string()
    } else {
        format!("ws://{raw}")
    };
    let mut url = reqwest::Url::parse(&raw)?;
    match url.scheme() {
        "http" => {
            url.set_scheme("ws")
                .map_err(|_| anyhow::anyhow!("could not convert I2P URL to ws"))?;
        }
        "ws" => {}
        "https" | "wss" => {
            anyhow::bail!("I2P transport currently supports ws/http, not wss/https")
        }
        other => anyhow::bail!("unsupported I2P URL scheme: {other}"),
    }

    let Some(host) = url.host_str() else {
        anyhow::bail!("I2P URL is missing a host");
    };
    let host = host.to_ascii_lowercase();
    if !host.ends_with(".i2p") {
        anyhow::bail!("I2P URL host must end with .i2p");
    }
    if !host.ends_with(".b32.i2p") {
        anyhow::bail!("I2P endpoint must be a .b32.i2p address");
    }
    if !is_i2p_b32_host(&host) {
        anyhow::bail!("I2P .b32.i2p address must use a 52-character base32 destination");
    }

    Ok(url.to_string())
}

pub async fn bind_i2p_listener() -> anyhow::Result<(TcpListener, SocketAddr)> {
    bind_public_tunnel_listener().await
}

pub async fn i2p_sam_available(sam_addr: SocketAddr) -> bool {
    matches!(
        time::timeout(Duration::from_secs(2), SamControl::connect(sam_addr)).await,
        Ok(Ok(_))
    )
}

pub async fn create_i2p_stream_session(sam_addr: SocketAddr) -> anyhow::Result<I2pSession> {
    let mut control = SamControl::connect(sam_addr).await?;
    let generated = control.dest_generate().await?;
    let id = unique_sam_session_id("peerline-inbound");
    Ok(I2pSession {
        b32: i2p_b32_address(&generated.public_destination)?,
        destination: generated.private_destination,
        id,
    })
}

pub async fn forward_i2p_to_listener(
    sam_addr: SocketAddr,
    session: &I2pSession,
    local_addr: SocketAddr,
) -> anyhow::Result<I2pForward> {
    let mut session_control = SamControl::connect(sam_addr).await?;
    session_control
        .session_create(
            &session.id,
            &session.destination,
            &[
                ("SIGNATURE_TYPE", "7"),
                ("inbound.quantity", "2"),
                ("outbound.quantity", "2"),
            ],
        )
        .await?;
    let mut forward_control = SamControl::connect(sam_addr).await?;
    forward_control
        .stream_forward(&session.id, local_addr.port())
        .await?;
    Ok(I2pForward {
        session_control: session_control.into_stream(),
        forward_control: forward_control.into_stream(),
    })
}

pub(super) async fn open_i2p_session(
    endpoint: String,
    sam_addr: SocketAddr,
    name: Option<&peerline_core::HumanName>,
    code: &peerline_core::HumanCode,
    descriptor: peerline_core::TransferDescriptor,
) -> anyhow::Result<PublicTunnelSession<tokio::net::TcpStream>> {
    let url = reqwest::Url::parse(&endpoint)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("I2P URL is missing a host"))?
        .to_string();
    let port = url.port_or_known_default().unwrap_or(80);
    let mut session_control = SamControl::connect(sam_addr).await?;
    let id = unique_sam_session_id("peerline-outbound");
    session_control
        .session_create(
            &id,
            "TRANSIENT",
            &[
                ("SIGNATURE_TYPE", "7"),
                ("inbound.quantity", "1"),
                ("outbound.quantity", "2"),
            ],
        )
        .await?;
    let session_keepalive = session_control.into_stream();
    let mut connect_control = SamControl::connect(sam_addr).await?;
    let stream = connect_control.stream_connect(&id, &host, port).await?;
    let (stream, _) = client_async(&endpoint, stream).await?;
    let mut session =
        open_websocket_session(stream, endpoint, name, code, descriptor, I2P_ROUTE_LABEL).await?;
    session.transport_control = Some(session_keepalive);
    Ok(session)
}

struct SamControl {
    stream: Option<BufReader<tokio::net::TcpStream>>,
}

struct SamDestination {
    public_destination: String,
    private_destination: String,
}

impl SamControl {
    async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let mut control = Self {
            stream: Some(BufReader::new(stream)),
        };
        control.hello().await?;
        Ok(control)
    }

    async fn hello(&mut self) -> anyhow::Result<()> {
        self.command_ok(
            "HELLO VERSION MIN=3.1 MAX=3.1\n",
            "HELLO REPLY",
            "SAM hello failed",
        )
        .await
    }

    async fn dest_generate(&mut self) -> anyhow::Result<SamDestination> {
        self.write_all("DEST GENERATE SIGNATURE_TYPE=7\n").await?;
        let line = self.read_line().await?;
        let fields = parse_sam_fields(&line);
        if sam_field(&fields, "RESULT") != Some("OK") {
            anyhow::bail!(
                "SAM destination generation failed: {}",
                sam_message(&fields)
            );
        }
        let public_destination = sam_field(&fields, "PUB")
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("SAM destination generation did not return PUB"))?;
        let private_destination = sam_field(&fields, "PRIV")
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("SAM destination generation did not return PRIV"))?;
        Ok(SamDestination {
            public_destination,
            private_destination,
        })
    }

    async fn session_create(
        &mut self,
        id: &str,
        destination: &str,
        options: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        validate_sam_id(id)?;
        validate_sam_destination(destination)?;
        let mut command = format!(
            "SESSION CREATE STYLE=STREAM ID={} DESTINATION={}",
            id, destination
        );
        for (key, value) in options {
            command.push(' ');
            command.push_str(key);
            command.push('=');
            command.push_str(value);
        }
        command.push('\n');
        self.command_ok(&command, "SESSION STATUS", "SAM session create failed")
            .await
    }

    async fn stream_forward(&mut self, id: &str, port: u16) -> anyhow::Result<()> {
        self.command_ok(
            &format!("STREAM FORWARD ID={id} HOST=127.0.0.1 PORT={port} SILENT=true\n"),
            "STREAM STATUS",
            "SAM stream forward failed",
        )
        .await
    }

    async fn stream_connect(
        &mut self,
        id: &str,
        destination: &str,
        port: u16,
    ) -> anyhow::Result<tokio::net::TcpStream> {
        validate_sam_id(id)?;
        validate_sam_destination(destination)?;
        let command = format!(
            "STREAM CONNECT ID={id} DESTINATION={destination} TO_PORT={port} SILENT=false\n"
        );
        self.write_all(&command).await?;
        let line = self.read_line().await?;
        let fields = parse_sam_fields(&line);
        if sam_field(&fields, "RESULT") != Some("OK") {
            anyhow::bail!("SAM stream connect failed: {}", sam_message(&fields));
        }
        Ok(self.take_stream())
    }

    async fn command_ok(
        &mut self,
        command: &str,
        expected_prefix: &str,
        context: &str,
    ) -> anyhow::Result<()> {
        self.write_all(command).await?;
        let line = self.read_line().await?;
        if !line.starts_with(expected_prefix) {
            anyhow::bail!("{context}: unexpected SAM response `{line}`");
        }
        let fields = parse_sam_fields(&line);
        if sam_field(&fields, "RESULT") != Some("OK") {
            anyhow::bail!("{context}: {}", sam_message(&fields));
        }
        Ok(())
    }

    async fn write_all(&mut self, command: &str) -> anyhow::Result<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("SAM stream already taken"))?;
        stream.get_mut().write_all(command.as_bytes()).await?;
        stream.get_mut().flush().await?;
        Ok(())
    }

    async fn read_line(&mut self) -> anyhow::Result<String> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("SAM stream already taken"))?;
        let mut line = String::new();
        let read = stream.read_line(&mut line).await?;
        if read == 0 {
            anyhow::bail!("SAM bridge closed the connection");
        }
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }

    fn into_stream(mut self) -> tokio::net::TcpStream {
        self.stream
            .take()
            .expect("SAM stream should still be present")
            .into_inner()
    }

    fn take_stream(&mut self) -> tokio::net::TcpStream {
        self.stream
            .take()
            .expect("SAM stream should still be present")
            .into_inner()
    }
}

fn is_i2p_b32_host(host: &str) -> bool {
    let Some(label) = host.strip_suffix(".b32.i2p") else {
        return false;
    };
    label.len() == 52
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || matches!(byte, b'2'..=b'7'))
}

fn parse_sam_fields(line: &str) -> Vec<(&str, &str)> {
    line.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect()
}

fn sam_field<'a>(fields: &'a [(&str, &str)], key: &str) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|(field_key, value)| (*field_key == key).then_some(*value))
}

fn sam_message(fields: &[(&str, &str)]) -> String {
    sam_field(fields, "MESSAGE")
        .unwrap_or_else(|| sam_field(fields, "RESULT").unwrap_or("unknown error"))
        .to_string()
}

fn validate_sam_id(id: &str) -> anyhow::Result<()> {
    if !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        anyhow::bail!("invalid SAM session id")
    }
}

fn validate_sam_destination(destination: &str) -> anyhow::Result<()> {
    if !destination.is_empty() && destination.bytes().all(|byte| byte.is_ascii_graphic()) {
        Ok(())
    } else {
        anyhow::bail!("invalid SAM destination")
    }
}

fn i2p_b32_address(destination: &str) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};

    let decoded = i2p_base64_decode(destination)?;
    let digest = Sha256::digest(&decoded);
    Ok(format!("{}.b32.i2p", base32_i2p_no_padding(&digest)))
}

fn unique_sam_session_id(prefix: &str) -> String {
    let suffix = TransferId::random()
        .bytes()
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}-{suffix}")
}

fn i2p_base64_decode(value: &str) -> anyhow::Result<Vec<u8>> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    let mut standard = value.replace('-', "+").replace('~', "/");
    while !standard.len().is_multiple_of(4) {
        standard.push('=');
    }
    STANDARD
        .decode(standard)
        .map_err(|error| anyhow::anyhow!("invalid I2P destination base64: {error}"))
}

fn base32_i2p_no_padding(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut output = String::new();
    let mut buffer = 0u16;
    let mut bits = 0u8;
    for byte in bytes {
        buffer = (buffer << 8) | u16::from(*byte);
        bits += 8;
        while bits >= 5 {
            let shift = bits - 5;
            let index = ((buffer >> shift) & 0x1f) as usize;
            output.push(ALPHABET[index] as char);
            bits -= 5;
            buffer &= (1 << bits) - 1;
        }
    }
    if bits > 0 {
        let index = ((buffer << (5 - bits)) & 0x1f) as usize;
        output.push(ALPHABET[index] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::normalize_i2p_url;

    #[test]
    fn normalizes_i2p_b32_urls_to_websocket_schemes() {
        assert_eq!(
            normalize_i2p_url("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz.b32.i2p")
                .unwrap(),
            "ws://abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz.b32.i2p/"
        );
        assert_eq!(
            normalize_i2p_url(
                "http://abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz.b32.i2p/x"
            )
            .unwrap(),
            "ws://abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz.b32.i2p/x"
        );
    }

    #[test]
    fn rejects_invalid_i2p_b32_urls() {
        assert!(normalize_i2p_url("https://example.com").is_err());
        assert!(normalize_i2p_url("ws://example.b32.i2p").is_err());
        assert!(
            normalize_i2p_url("ws://abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxy0.b32.i2p")
                .is_err()
        );
        assert!(
            normalize_i2p_url("wss://abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz.b32.i2p")
                .is_err()
        );
    }
}
