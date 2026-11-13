//! Opt-in, unauthenticated (Everyone-mode) AirDrop protocol receiver.
//! File transport is HTTPS over AWDL, separate from Peerline's PAKE protocol.
#![cfg_attr(not(target_os = "macos"), allow(dead_code))]
mod archive;
#[cfg(target_os = "macos")]
mod macos;

use anyhow::{Context, ensure};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request, Response, StatusCode, body::Body};
use plist::Value;
use std::{
    convert::Infallible,
    net::IpAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, mpsc, oneshot},
    time::{self, Instant},
};

pub struct Options {
    pub name: String,
    pub destination: PathBuf,
    pub max_bytes: u64,
}

pub struct Offer {
    pub sender: String,
    pub files: Vec<String>,
    pub accept: oneshot::Sender<bool>,
}

pub enum Event {
    Ready,
    Offer(Offer),
    Receiving,
    Received(PathBuf),
    Failed(String),
}

pub async fn receive(options: Options, events: mpsc::Sender<Event>) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        macos::receive(options, events).await
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (options, events);
        anyhow::bail!(
            "AirDrop receive requires macOS with an available AWDL interface; it was not started"
        )
    }
}

// Protocol parsing/auth tests are compiled on other platforms as well.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
struct State {
    options: Options,
    events: mpsc::Sender<Event>,
    grant: Mutex<Option<Grant>>,
    upload: Mutex<()>,
}
struct Grant {
    id: u64,
    peer: IpAddr,
    files: Vec<String>,
    expires: Instant,
    approved: bool,
}
const PLIST_LIMIT: usize = 1024 * 1024;
type Reply = Response<Full<Bytes>>;

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
async fn handle<B>(
    request: Request<B>,
    peer: IpAddr,
    state: Arc<State>,
) -> Result<Reply, Infallible>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let reply = match handle_inner(request, peer, state).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "AirDrop request rejected");
            reply(StatusCode::BAD_REQUEST, vec![])
        }
    };
    Ok(reply)
}

async fn handle_inner<B>(
    request: Request<B>,
    peer: IpAddr,
    state: Arc<State>,
) -> anyhow::Result<Reply>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if request.method() != hyper::Method::POST {
        return Ok(reply(StatusCode::METHOD_NOT_ALLOWED, vec![]));
    }
    match request.uri().path() {
        "/Discover" => {
            let _ = read_plist(request.into_body()).await?;
            identity_reply(&state.options.name)
        }
        "/Ask" => {
            let value = read_plist(request.into_body()).await?;
            let (sender, files) = parse_offer(&value)?;
            let id = rand::random();
            {
                let mut grant = state.grant.lock().await;
                if state.upload.try_lock().is_err()
                    || grant.as_ref().is_some_and(|g| g.expires > Instant::now())
                {
                    return Ok(reply(StatusCode::CONFLICT, vec![]));
                }
                *grant = Some(Grant {
                    id,
                    peer,
                    files: files.clone(),
                    expires: Instant::now() + Duration::from_secs(60),
                    approved: false,
                });
            }
            let (accept, answer) = oneshot::channel();
            let offered = state
                .events
                .try_send(Event::Offer(Offer {
                    sender,
                    files,
                    accept,
                }))
                .is_ok();
            let approved = offered
                && matches!(
                    time::timeout(Duration::from_secs(45), answer).await,
                    Ok(Ok(true))
                );
            let mut grant = state.grant.lock().await;
            if grant.as_ref().is_none_or(|g| g.id != id) {
                return Ok(reply(StatusCode::CONFLICT, vec![]));
            }
            if !approved {
                *grant = None;
                return Ok(reply(StatusCode::FORBIDDEN, vec![]));
            }
            let grant = grant.as_mut().expect("checked above");
            grant.approved = true;
            grant.expires = Instant::now() + Duration::from_secs(60);
            identity_reply(&state.options.name)
        }
        "/Upload" => {
            let Ok(_upload) = state.upload.try_lock() else {
                return Ok(reply(StatusCode::CONFLICT, vec![]));
            };
            let files = {
                let mut guard = state.grant.lock().await;
                match guard.as_ref() {
                    Some(grant)
                        if grant.approved
                            && grant.peer == peer
                            && grant.expires > Instant::now() => {}
                    _ => return Ok(reply(StatusCode::FORBIDDEN, vec![])),
                }
                guard.take().expect("checked above").files
            };
            ensure!(
                request
                    .headers()
                    .get(hyper::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    == Some("application/x-cpio"),
                "Expected CPIO upload"
            );
            state.events.send(Event::Receiving).await?;
            let mut notice = UploadNotice {
                events: state.events.clone(),
                finished: false,
            };
            let result = upload(request.into_body(), files, &state.options).await;
            match result {
                Ok(path) => {
                    let _ = state.events.send(Event::Received(path)).await;
                    notice.finished = true;
                    Ok(reply(StatusCode::OK, vec![]))
                }
                Err(error) => {
                    let _ = state.events.try_send(Event::Failed(error.to_string()));
                    notice.finished = true;
                    Err(error)
                }
            }
        }
        _ => Ok(reply(StatusCode::NOT_FOUND, vec![])),
    }
}

fn parse_offer(value: &Value) -> anyhow::Result<(String, Vec<String>)> {
    let dictionary = value.as_dictionary().context("Expected offer dictionary")?;
    ensure!(
        !dictionary.contains_key("Items"),
        "URL offers are not supported"
    );
    let sender = dictionary
        .get("SenderComputerName")
        .and_then(Value::as_string)
        .unwrap_or("Unknown device");
    ensure!(sender.len() <= 256, "Sender name too long");
    let items = dictionary
        .get("Files")
        .and_then(Value::as_array)
        .context("Missing file list")?;
    ensure!(
        !items.is_empty() && items.len() <= 256,
        "Invalid file count"
    );
    let mut files = Vec::new();
    for item in items {
        let name = item
            .as_dictionary()
            .and_then(|d| d.get("FileName"))
            .and_then(Value::as_string)
            .context("Missing file name")?;
        ensure!(
            name.len() <= 255
                && !name.contains(['/', '\\', ':'])
                && !name.chars().any(char::is_control),
            "Unsafe offer filename"
        );
        peerline_core::path::validate_relative_path(std::path::Path::new(name))?;
        ensure!(!files.iter().any(|f| f == name), "Duplicate offer filename");
        files.push(name.to_owned());
    }
    Ok((sender.to_owned(), files))
}

async fn read_plist<B>(body: B) -> anyhow::Result<Value>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let bytes = time::timeout(
        Duration::from_secs(10),
        Limited::new(body, PLIST_LIMIT).collect(),
    )
    .await?
    .map_err(|e| anyhow::anyhow!(e.to_string()))?
    .to_bytes();
    Ok(Value::from_reader(std::io::Cursor::new(bytes))?)
}
fn identity_reply(name: &str) -> anyhow::Result<Reply> {
    let mut dictionary = plist::Dictionary::new();
    dictionary.insert("ReceiverComputerName".into(), name.into());
    dictionary.insert("ReceiverModelName".into(), "Peerline".into());
    dictionary.insert(
        "ReceiverMediaCapabilities".into(),
        Value::Data(br#"{"Version":1}"#.to_vec()),
    );
    let mut bytes = Vec::new();
    Value::Dictionary(dictionary).to_writer_binary(&mut bytes)?;
    Ok(reply(StatusCode::OK, bytes))
}
fn reply(status: StatusCode, bytes: Vec<u8>) -> Reply {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/octet-stream")
        .body(Full::new(Bytes::from(bytes)))
        .expect("static response headers")
}

struct CancelExtraction(Arc<AtomicBool>);
impl Drop for CancelExtraction {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct UploadNotice {
    events: mpsc::Sender<Event>,
    finished: bool,
}
impl Drop for UploadNotice {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self
                .events
                .try_send(Event::Failed("AirDrop upload interrupted".into()));
        }
    }
}

async fn upload<B>(mut body: B, approved: Vec<String>, options: &Options) -> anyhow::Result<PathBuf>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let compressed = tempfile::NamedTempFile::new_in(&options.destination)?;
    let mut file = tokio::fs::File::from_std(compressed.reopen()?);
    let mut total = 0u64;
    time::timeout(Duration::from_secs(300), async {
        while let Some(frame) = time::timeout(Duration::from_secs(15), body.frame()).await? {
            if let Ok(bytes) = frame?.into_data() {
                total = total
                    .checked_add(bytes.len() as u64)
                    .context("Upload size overflow")?;
                ensure!(total <= options.max_bytes, "Upload size limit exceeded");
                file.write_all(&bytes).await?;
            }
        }
        file.flush().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    drop(file);
    let destination = options.destination.clone();
    let limit = options.max_bytes;
    let cancelled = Arc::new(AtomicBool::new(false));
    let _guard = CancelExtraction(cancelled.clone());
    let staging = tokio::task::spawn_blocking(move || {
        let staging = tempfile::Builder::new()
            .prefix(".peerline-airdrop-")
            .tempdir_in(&destination)?;
        archive::extract(
            compressed.reopen()?,
            staging.path(),
            &approved,
            limit,
            &cancelled,
        )?;
        ensure!(
            !cancelled.load(Ordering::Relaxed),
            "AirDrop transfer cancelled"
        );
        Ok::<_, anyhow::Error>(staging)
    })
    .await??;
    // Publish only after the entire archive is verified; reserve our own fresh
    // directory, never overwrite an existing user file or follow their symlinks.
    let output = tempfile::Builder::new()
        .prefix("AirDrop-")
        .tempdir_in(&options.destination)?;
    std::fs::rename(staging.path(), output.path())?;
    Ok(output.keep())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(path: &str, bytes: Vec<u8>) -> Request<Full<Bytes>> {
        Request::post(path)
            .header("Content-Type", "application/x-cpio")
            .body(Full::new(Bytes::from(bytes)))
            .unwrap()
    }
    pub(super) fn offer_bytes() -> Vec<u8> {
        let mut file = plist::Dictionary::new();
        file.insert("FileName".into(), "photo.jpg".into());
        let mut value = plist::Dictionary::new();
        value.insert("SenderComputerName".into(), "Phone".into());
        value.insert("Files".into(), Value::Array(vec![Value::Dictionary(file)]));
        let mut bytes = Vec::new();
        Value::Dictionary(value)
            .to_writer_binary(&mut bytes)
            .unwrap();
        bytes
    }
    #[tokio::test]
    #[cfg(target_os = "macos")]
    async fn upload_requires_approval_and_is_scoped_to_peer_and_single_use() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("photo.jpg"), b"existing file").unwrap();
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(State {
            options: Options {
                name: "Peerline".into(),
                destination: root.path().into(),
                max_bytes: 1024,
            },
            events,
            grant: Mutex::new(None),
            upload: Mutex::new(()),
        });
        let peer: IpAddr = "fe80::1".parse().unwrap();
        let other = "fe80::2".parse().unwrap();
        let archive = archive::cpio("./photo.jpg", 0o100600, b"contents");
        assert_eq!(
            handle(request("/Upload", archive.clone()), peer, state.clone())
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let ask = handle(request("/Ask", offer_bytes()), peer, state.clone());
        let confirm = async {
            let Event::Offer(offer) = rx.recv().await.unwrap() else {
                panic!()
            };
            assert_eq!(offer.files, ["photo.jpg"]);
            offer.accept.send(true).unwrap();
        };
        let (response, ()) = tokio::join!(ask, confirm);
        assert_eq!(response.unwrap().status(), StatusCode::OK);
        assert_eq!(
            handle(request("/Upload", archive.clone()), other, state.clone())
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            handle(request("/Upload", archive.clone()), peer, state.clone())
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert!(matches!(rx.recv().await.unwrap(), Event::Receiving));
        let Event::Received(path) = rx.recv().await.unwrap() else {
            panic!()
        };
        assert_eq!(std::fs::read(path.join("photo.jpg")).unwrap(), b"contents");
        assert_eq!(
            std::fs::read(root.path().join("photo.jpg")).unwrap(),
            b"existing file"
        );
        assert_eq!(
            handle(request("/Upload", archive), peer, state)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn invalid_upload_removes_staging_and_keeps_existing_files() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("photo.jpg"), b"existing file").unwrap();
        let options = Options {
            name: "Peerline".into(),
            destination: root.path().into(),
            max_bytes: 1024,
        };
        let mut archive = archive::cpio("photo.jpg", 0o100600, b"partial new file");
        archive.pop();
        assert!(
            upload(
                Full::new(Bytes::from(archive)),
                vec!["photo.jpg".into()],
                &options
            )
            .await
            .is_err()
        );
        assert_eq!(
            std::fs::read(root.path().join("photo.jpg")).unwrap(),
            b"existing file"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[cfg(not(target_os = "macos"))]
    #[tokio::test]
    async fn native_receiver_is_explicitly_unsupported() {
        let root = tempfile::tempdir().unwrap();
        let (events, _) = mpsc::channel(1);
        let error = receive(
            Options {
                name: "Peerline".into(),
                destination: root.path().into(),
                max_bytes: 1024,
            },
            events,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("requires macOS"));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn declined_or_expired_offers_cannot_upload() {
        let root = tempfile::tempdir().unwrap();
        let (events, mut rx) = mpsc::channel(8);
        let state = Arc::new(State {
            options: Options {
                name: "Peerline".into(),
                destination: root.path().into(),
                max_bytes: 1024,
            },
            events,
            grant: Mutex::new(None),
            upload: Mutex::new(()),
        });
        let peer = "fe80::1".parse().unwrap();
        let ask = handle(request("/Ask", offer_bytes()), peer, state.clone());
        let decline = async {
            let Event::Offer(offer) = rx.recv().await.unwrap() else {
                panic!()
            };
            offer.accept.send(false).unwrap();
        };
        let (response, ()) = tokio::join!(ask, decline);
        assert_eq!(response.unwrap().status(), StatusCode::FORBIDDEN);
        *state.grant.lock().await = Some(Grant {
            id: 1,
            peer,
            files: vec!["photo.jpg".into()],
            approved: true,
            expires: Instant::now() - Duration::from_secs(1),
        });
        assert_eq!(
            handle(
                request("/Upload", archive::cpio("photo.jpg", 0o100600, b"x")),
                peer,
                state
            )
            .await
            .unwrap()
            .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
