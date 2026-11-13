//! Explicit, short-lived code copies. A separate worker survives receiver exit;
//! secrets travel over stdin, never command arguments, environment or logs.
use super::*;
use anyhow::Context;

const TTL: Duration = Duration::from_secs(30);

pub(super) async fn copy_code(code: &HumanCode) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut command = TokioCommand::new(std::env::current_exe()?);
    // Keep terminal Ctrl-C from killing the expiry worker together with recv.
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(0x00000200); // CREATE_NEW_PROCESS_GROUP disables inherited Ctrl-C.
    let mut child = command
        .arg("__clipboard-code")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut input = child
        .stdin
        .take()
        .context("Clipboard worker stdin unavailable")?;
    input.write_all(code.as_str().as_bytes()).await?;
    input.shutdown().await?;
    drop(input);
    let mut response = BufReader::new(
        child
            .stdout
            .take()
            .context("Clipboard worker output unavailable")?,
    )
    .lines();
    let ready = tokio::time::timeout(Duration::from_secs(5), response.next_line()).await;
    // Never kill the worker on a missing acknowledgement: it may have copied
    // already, and must retain the chance to expire that copy.
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    if !matches!(ready, Ok(Ok(Some(ref line))) if line == "ready") {
        anyhow::bail!("Clipboard copy could not be confirmed; enter the displayed code manually");
    }
    println!("Code copied for 30 seconds; clipboard history exclusion is requested.");
    Ok(())
}

fn wait_for_expiry(output: &mut impl Write, wait: impl FnOnce()) {
    // The receiver may exit while copying. A broken acknowledgement pipe must
    // not panic or short-circuit the expiry path after the clipboard was set.
    let _ = writeln!(output, "ready");
    let _ = output.flush();
    wait();
}

#[cfg(any(target_os = "macos", target_os = "windows", test))]
fn unchanged(receipt: u64, current: u64, expected: &str, actual: &str) -> bool {
    receipt == current && expected == actual
}

#[cfg(target_os = "macos")]
fn expire_mac(board: &objc2_app_kit::NSPasteboard, stamp: u64, code: &str) {
    // AppKit has no public atomic compare-and-clear. Recheck the generation
    // immediately before clearing. This cannot revoke a history manager's copy.
    let current = board.changeCount() as u64;
    // SAFETY: immutable framework constant, valid for the process lifetime.
    let text_type = unsafe { objc2_app_kit::NSPasteboardTypeString };
    if let Some(text) = board.stringForType(text_type)
        && unchanged(stamp, current, code, &text.to_string())
        && board.changeCount() as u64 == stamp
    {
        board.clearContents();
    }
}

pub(super) fn worker() -> anyhow::Result<()> {
    use std::io::Read;
    let mut input = String::new();
    std::io::stdin().take(257).read_to_string(&mut input)?;
    anyhow::ensure!(input.len() <= 256, "Invalid clipboard code");
    let code = HumanCode::parse(input)?;
    let mut clipboard = arboard::Clipboard::new()?;
    #[cfg(target_os = "macos")]
    {
        use arboard::SetExtApple;
        clipboard.set().exclude_from_history().text(code.as_str())?;
    }
    #[cfg(target_os = "windows")]
    {
        use arboard::SetExtWindows;
        clipboard
            .set()
            .exclude_from_history()
            .exclude_from_cloud()
            .text(code.as_str())?;
    }
    #[cfg(target_os = "linux")]
    {
        use arboard::SetExtLinux;
        clipboard.set().exclude_from_history().text(code.as_str())?;
    }

    #[cfg(target_os = "macos")]
    let stamp = objc2_app_kit::NSPasteboard::generalPasteboard().changeCount() as u64;
    #[cfg(target_os = "windows")]
    let stamp = clipboard_win::seq_num()
        .context("Clipboard sequence unavailable")?
        .get() as u64;
    wait_for_expiry(&mut std::io::stdout(), || std::thread::sleep(TTL));

    #[cfg(target_os = "macos")]
    expire_mac(
        &objc2_app_kit::NSPasteboard::generalPasteboard(),
        stamp,
        code.as_str(),
    );
    #[cfg(target_os = "windows")]
    {
        use clipboard_win::Getter;
        // Hold the native clipboard lock across the comparison and clear.
        let _lock = clipboard_win::Clipboard::new_attempts(10)?;
        let current = clipboard_win::seq_num()
            .context("Clipboard sequence unavailable")?
            .get() as u64;
        let mut text = String::new();
        if clipboard_win::formats::Unicode
            .read_clipboard(&mut text)
            .is_ok()
            && unchanged(stamp, current, code.as_str(), &text)
        {
            clipboard_win::empty()?;
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Exit closes only this process's X11/Wayland selection source, so a
        // later owner's selection survives. Skip arboard's Drop, which otherwise
        // asks a clipboard manager to persist the sensitive data after exit.
        std::mem::forget(clipboard);
        std::process::exit(0);
    }
    #[cfg(not(target_os = "linux"))]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lost_parent_acknowledgement_does_not_cancel_expiry() {
        struct ClosedPipe;
        impl Write for ClosedPipe {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.write(&[]).map(|_| ())
            }
        }
        let mut expired = false;
        wait_for_expiry(&mut ClosedPipe, || expired = true);
        assert!(expired);
    }
    #[test]
    fn expiry_preserves_newer_copies_even_if_text_is_identical() {
        assert!(unchanged(4, 4, "code", "code"));
        assert!(!unchanged(4, 5, "code", "code"));
        assert!(!unchanged(4, 4, "code", "notes"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn native_expiry_clears_only_the_owned_generation() {
        use objc2_app_kit::NSPasteboard;
        use objc2_foundation::NSString;
        // A private named pasteboard never reads or changes the user's clipboard.
        let board = NSPasteboard::pasteboardWithUniqueName();
        let code = "fig-mint-rose-123456";
        // SAFETY: immutable framework constant.
        let text_type = unsafe { objc2_app_kit::NSPasteboardTypeString };
        board.clearContents();
        assert!(board.setString_forType(&NSString::from_str(code), text_type));
        let stamp = board.changeCount() as u64;
        expire_mac(&board, stamp, code);
        assert!(board.stringForType(text_type).is_none());
        for replacement in ["new user content", code] {
            board.clearContents();
            assert!(board.setString_forType(&NSString::from_str(code), text_type));
            let stamp = board.changeCount() as u64;
            board.clearContents();
            assert!(board.setString_forType(&NSString::from_str(replacement), text_type));
            expire_mac(&board, stamp, code);
            assert_eq!(
                board.stringForType(text_type).unwrap().to_string(),
                replacement
            );
        }
        board.clearContents();
    }
}
