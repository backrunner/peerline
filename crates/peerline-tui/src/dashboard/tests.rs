use super::{
    Dashboard,
    logs::{LogEntry, LogKind, log_source_label, render_log_lines},
};
use crate::{RecvView, SendView};
use peerline_core::{
    ConnectionRoute, HumanCode, HumanName, PeerlineEvent, TransferId, TransferStage,
};
use ratatui::{layout::Rect, text::Line};
use std::time::Duration;

fn recv_dashboard() -> Dashboard {
    Dashboard::new_recv(RecvView {
        name: HumanName::parse("river-mango-42").unwrap(),
        code: HumanCode::parse("rose-lime-iris-jade-1234").unwrap(),
        bind: "127.0.0.1:43117".into(),
        route_status: "ready".into(),
        stage: TransferStage::Discovering,
        progress: None,
    })
}

fn send_dashboard() -> Dashboard {
    Dashboard::new_send(
        SendView {
            target_label: "peer".into(),
            target: "river-mango-42".into(),
            code: HumanCode::parse("rose-lime-iris-jade-1234").unwrap(),
            route_status: "ready".into(),
            stage: TransferStage::Discovering,
            progress: None,
        },
        true,
    )
}

fn line_text(line: Line<'_>) -> String {
    line.spans
        .into_iter()
        .map(|span| span.content.into_owned())
        .collect()
}

fn key(code: crossterm::event::KeyCode) -> crossterm::event::KeyEvent {
    crossterm::event::KeyEvent::new(code, crossterm::event::KeyModifiers::NONE)
}

#[test]
fn header_summary_does_not_repeat_the_block_title() {
    let dashboard = recv_dashboard();

    let summary = line_text(dashboard.summary_line(80));

    assert!(!summary.contains("Peerline receive"));
    assert_eq!(summary, "transfers 0 | active 0 | done 0 | failed 0");
}

#[test]
fn pairing_code_is_rendered_without_middle_truncation() {
    let dashboard = recv_dashboard();

    let code = line_text(dashboard.code_line(20));

    assert!(code.contains("rose-lime-iris-jade-1234"));
    assert!(!code.contains("..."));
}

#[test]
fn tracks_multiple_transfer_rows_with_peer_labels() {
    let mut dashboard = recv_dashboard();
    let first = TransferId::random();
    let second = TransferId::random();

    dashboard.apply_event(PeerlineEvent::TransferStarted {
        id: first,
        peer: "127.0.0.1:50001".into(),
        files: 2,
        bytes: 200,
    });
    dashboard.apply_event(PeerlineEvent::Progress {
        id: first,
        bytes_done: 100,
        bytes_total: 200,
    });
    dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Complete));
    dashboard.apply_event(PeerlineEvent::TransferStarted {
        id: second,
        peer: "12D3KooWExamplePeer".into(),
        files: 1,
        bytes: 50,
    });
    dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Connecting(
        ConnectionRoute::Libp2pDcutr,
    )));

    assert_eq!(dashboard.transfers.len(), 2);
    assert_eq!(dashboard.transfers[0].peer, "127.0.0.1:50001");
    assert_eq!(dashboard.transfers[1].peer, "12D3KooWExamplePeer");
    assert!(matches!(
        dashboard.transfers[0].stage,
        TransferStage::Complete
    ));
    assert!(matches!(
        dashboard.transfers[1].stage,
        TransferStage::Connecting(ConnectionRoute::Libp2pDcutr)
    ));
    assert!(dashboard.logs.len() >= 4);
}

#[test]
fn receive_dashboard_stays_open_after_completed_transfer() {
    let mut dashboard = recv_dashboard();

    let should_exit = dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Complete));

    assert!(!should_exit);
}

#[test]
fn send_dashboard_exits_on_success_but_waits_for_retry_on_failure() {
    let mut dashboard = send_dashboard();

    assert!(
        !dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Failed(
            "dial failed".into()
        ),))
    );
    assert!(dashboard.retry_available());
    assert!(dashboard.apply_event(PeerlineEvent::StageChanged(TransferStage::Complete,)));
}

#[test]
fn shutdown_event_closes_the_dashboard() {
    let mut dashboard = recv_dashboard();

    assert!(dashboard.apply_event(PeerlineEvent::Shutdown));
}

#[test]
fn activity_log_lines_wrap_to_the_available_width() {
    let entry = LogEntry {
        elapsed: Duration::from_millis(1250),
        kind: LogKind::Info,
        source: Some("peerline_cli::very_long_target_name".into()),
        text: "abcdefghijklmnopqrstuvwxyz0123456789\nsecond-line-with-more-text".into(),
        repeat_count: 1,
    };

    let lines = render_log_lines(&entry, 32);

    assert!(lines.len() > 2);
    assert!(lines.iter().all(|line| line.width() <= 32));
}

#[test]
fn activity_log_prefix_degrades_for_tiny_widths() {
    let entry = LogEntry {
        elapsed: Duration::from_millis(1250),
        kind: LogKind::Warn,
        source: Some("peerline_cli::very_long_target_name".into()),
        text: "abcdefghijklmnopqrstuvwxyz".into(),
        repeat_count: 1,
    };

    let lines = render_log_lines(&entry, 8);

    assert!(lines.len() > 1);
    assert!(lines.iter().all(|line| line.width() <= 8));
}

#[test]
fn activity_log_source_labels_omit_peerline_prefix() {
    assert_eq!(log_source_label("peerline_cli::main"), "cli::main");
    assert_eq!(
        log_source_label("peerline_net::libp2p_transfer::receiver"),
        "net::libp2p_transfer::receiver"
    );
    assert_eq!(
        log_source_label("external_crate::module"),
        "external_crate::module"
    );
}

#[test]
fn activity_log_prefix_never_invents_ellipses() {
    let entry = LogEntry {
        elapsed: Duration::from_millis(1250),
        kind: LogKind::Status,
        source: Some("peerline_net::libp2p_transfer::receiver".into()),
        text: "DHT provider record published".into(),
        repeat_count: 1,
    };

    let narrow = render_log_lines(&entry, 48)
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>()
        .join("\n");
    let wide = render_log_lines(&entry, 96)
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>()
        .join("\n");

    assert!(!narrow.contains("..."));
    assert!(!wide.contains("..."));
    assert!(wide.contains("[net::libp2p_transfer::receiver]"));
}

#[test]
fn activity_log_coalesces_repeated_infrastructure_messages() {
    let mut dashboard = recv_dashboard();
    let source = Some("peerline_net::libp2p_transfer::receiver".into());

    dashboard.push_log(
        LogKind::Info,
        "DHT descriptor published key=/peerline/descriptor/v1/example",
        source.clone(),
    );
    dashboard.push_log(
        LogKind::Info,
        "DHT provider record published key=/peerline/provider/v1/example",
        source.clone(),
    );
    dashboard.push_log(
        LogKind::Info,
        "DHT descriptor published key=/peerline/descriptor/v1/example",
        source,
    );

    assert_eq!(dashboard.logs.len(), 2);
    assert_eq!(dashboard.logs[1].repeat_count, 2);
    let text = dashboard
        .visible_activity_lines(Rect::new(0, 0, 96, 6))
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("(x2)"));
}

#[test]
fn activity_log_follows_the_bottom_by_default() {
    let mut dashboard = recv_dashboard();
    for index in 0..8 {
        dashboard.push_log(LogKind::Info, format!("log-{index}"), None);
    }

    let lines = dashboard
        .visible_activity_lines(Rect::new(0, 0, 80, 6))
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>();

    assert_eq!(lines.len(), 4);
    assert!(lines[0].contains("log-4"));
    assert!(lines[3].contains("log-7"));
}

#[test]
fn activity_log_scroll_keys_lock_and_release_history_view() {
    let mut dashboard = recv_dashboard();
    let area = Rect::new(0, 0, 80, 6);
    for index in 0..8 {
        dashboard.push_log(LogKind::Info, format!("log-{index}"), None);
    }

    assert!(dashboard.handle_activity_scroll_key(key(crossterm::event::KeyCode::Up), area));
    assert_eq!(dashboard.log_scroll_top, Some(3));

    let lines = dashboard
        .visible_activity_lines(area)
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>();
    assert!(lines[0].contains("log-3"));
    assert!(lines[3].contains("log-6"));

    assert!(dashboard.handle_activity_scroll_key(key(crossterm::event::KeyCode::End), area));
    assert_eq!(dashboard.log_scroll_top, None);
    let lines = dashboard
        .visible_activity_lines(area)
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>();
    assert!(lines[0].contains("log-4"));
    assert!(lines[3].contains("log-7"));
}

#[test]
fn activity_log_history_view_is_not_yanked_by_new_logs() {
    let mut dashboard = recv_dashboard();
    let area = Rect::new(0, 0, 80, 6);
    for index in 0..8 {
        dashboard.push_log(LogKind::Info, format!("log-{index}"), None);
    }

    assert!(dashboard.handle_activity_scroll_key(key(crossterm::event::KeyCode::Home), area));
    assert_eq!(dashboard.log_scroll_top, Some(0));

    dashboard.push_log(LogKind::Info, "log-8", None);
    let lines = dashboard
        .visible_activity_lines(area)
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>();

    assert!(lines[0].contains("log-0"));
    assert!(lines[3].contains("log-3"));
}

#[test]
fn q_and_escape_are_quit_keys() {
    assert!(super::is_quit_key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char('q'),
        crossterm::event::KeyModifiers::NONE,
    )));
    assert!(super::is_quit_key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Esc,
        crossterm::event::KeyModifiers::NONE,
    )));
    assert!(super::is_quit_key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char('c'),
        crossterm::event::KeyModifiers::CONTROL,
    )));
    assert!(!super::is_quit_key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char('x'),
        crossterm::event::KeyModifiers::NONE,
    )));
}
