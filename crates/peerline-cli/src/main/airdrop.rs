use super::*;
use peerline_net::airdrop::{Event, Offer, Options};

pub(super) async fn recv(args: RecvArgs) -> anyhow::Result<()> {
    anyhow::ensure!(
        std::io::stdin().is_terminal(),
        "AirDrop receive needs an interactive terminal to approve each transfer"
    );
    let name = ConfigStore::user_default()?
        .load()?
        .name
        .unwrap_or_else(HumanName::generate);
    let (events, mut messages) = mpsc::channel(8);
    let options = Options {
        name: format!("Peerline {name}"),
        destination: std::env::current_dir()?,
        max_bytes: args.airdrop_max_mib * 1024 * 1024,
    };
    let receiver = peerline_net::airdrop::receive(options, events);
    tokio::pin!(receiver);
    let idle = recv_idle_timeout(args.idle_timeout_minutes);
    let mut activity = tokio::time::Instant::now();
    let mut receiving = false;
    loop {
        tokio::select! {
            result = &mut receiver => return result,
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = tokio::time::sleep_until(activity + idle.unwrap_or(Duration::from_secs(86400))), if idle.is_some() && !receiving => {
                println!("AirDrop receiver idle timeout"); return Ok(());
            }
            message = messages.recv() => match message {
                Some(Event::Ready) => {
                    println!("AirDrop receiver: Peerline {name}");
                    println!("Experimental compatibility mode. Sender identity is unverified; approve each batch below.");
                }
                Some(Event::Offer(offer)) => {
                    // Continue polling the server while the user decides.
                    let confirmation = confirm(offer); tokio::pin!(confirmation);
                    tokio::select! {
                        result = &mut receiver => return result,
                        _ = tokio::signal::ctrl_c() => return Ok(()),
                        result = &mut confirmation => result?,
                    }
                    activity = tokio::time::Instant::now();
                }
                Some(Event::Receiving) => { receiving = true; println!("Receiving AirDrop files..."); }
                Some(Event::Received(path)) => { receiving = false; activity = tokio::time::Instant::now(); println!("AirDrop files saved in {}", path.display()); }
                Some(Event::Failed(error)) => { receiving = false; activity = tokio::time::Instant::now(); eprintln!("AirDrop transfer failed: {error}"); }
                None => return Ok(()),
            }
        }
    }
}

async fn confirm(offer: Offer) -> anyhow::Result<()> {
    struct RawMode;
    impl Drop for RawMode {
        fn drop(&mut self) {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
    crossterm::terminal::enable_raw_mode()?;
    let guard = RawMode;
    // Do not interpret keys typed before this offer as consent.
    while crossterm::event::poll(Duration::ZERO)? {
        let _ = crossterm::event::read()?;
    }
    // Raw mode does not translate LF to CRLF.
    print!("AirDrop offer from {:?} (unverified):\r\n", offer.sender);
    for file in &offer.files {
        print!("  {file:?}\r\n");
    }
    print!("Accept these files? [y/N] (40 seconds) ");
    std::io::stdout().flush()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    let accepted = loop {
        if offer.accept.is_closed() || tokio::time::Instant::now() >= deadline {
            break false;
        }
        if crossterm::event::poll(Duration::ZERO)?
            && let crossterm::event::Event::Key(key) = crossterm::event::read()?
            && key.kind != crossterm::event::KeyEventKind::Release
        {
            use crossterm::event::{KeyCode, KeyModifiers};
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                drop(guard);
                println!("cancelled");
                anyhow::bail!("AirDrop receive cancelled");
            }
            break matches!(key.code, KeyCode::Char('y' | 'Y'))
                && key.modifiers.difference(KeyModifiers::SHIFT).is_empty();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    drop(guard);
    let delivered = offer.accept.send(accepted).is_ok();
    println!(
        "{}",
        if !delivered {
            "offer expired"
        } else if accepted {
            "accepted"
        } else {
            "declined"
        }
    );
    Ok(())
}
