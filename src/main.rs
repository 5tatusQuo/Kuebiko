mod codex;
mod debugger;
mod journal;
mod kernel;
mod mi;
mod protocol;
mod store;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Mutex, broadcast};
use tower_http::services::{ServeDir, ServeFile};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use codex::{CodexCommand, CodexHandle, TurnPurpose};
use debugger::{DebuggerCommand, DebuggerHandle};
use journal::Journal;
use kernel::{KernelCommand, KernelHandle};
use protocol::{
    ClientMessage, FullState, GDB_PTY_CHANNEL, LabConfig, LabStatus, ServerMessage, envelope,
};
use store::Store;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(long, default_value_t = 7878)]
    port: u16,
    #[arg(long, default_value = "web/dist")]
    assets_dir: PathBuf,
    #[arg(long)]
    no_open: bool,
}

struct Lab {
    id: String,
    config: LabConfig,
    debugger: DebuggerHandle,
    kernel: KernelHandle,
    journal: Journal,
    tutor_context: std::sync::Mutex<TutorContextCursor>,
}

#[derive(Default)]
struct TutorContextCursor {
    terminal: String,
}

struct WriteupCapture {
    journal: Journal,
    markdown: String,
}

struct AppState {
    lab: Mutex<Option<Lab>>,
    codex: CodexHandle,
    store: Store,
    events: broadcast::Sender<ServerMessage>,
    pty: broadcast::Sender<Vec<u8>>,
    token: String,
    port: u16,
    writeup: Mutex<Option<WriteupCapture>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "kuebiko=info,warn".into()),
        )
        .init();
    let args = Args::parse();
    let codex = codex::spawn().await?;
    let store = Store::new()?;
    let (events, _) = broadcast::channel(512);
    let (pty, _) = broadcast::channel(256);
    let token = random_token();
    let listener =
        tokio::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
            .await?;
    let port = listener.local_addr()?.port();
    let state = Arc::new(AppState {
        lab: Mutex::new(None),
        codex,
        store,
        events,
        pty,
        token: token.clone(),
        port,
        writeup: Mutex::new(None),
    });
    forward_codex(state.clone());

    let index = args.assets_dir.join("index.html");
    let assets = ServeDir::new(&args.assets_dir).fallback(ServeFile::new(index));
    let router = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/ws", get(ws_handler))
        .fallback_service(assets)
        .with_state(state.clone());

    let url = format!("http://127.0.0.1:{port}/?token={token}");
    info!(%url, "Kuebiko ready");
    if !args.no_open {
        let _ = tokio::process::Command::new("xdg-open")
            .arg(&url)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown(state))
        .await?;
    Ok(())
}

async fn shutdown(state: Arc<AppState>) {
    let _ = tokio::signal::ctrl_c().await;
    stop_lab(&state).await;
    let _ = state.codex.command(CodexCommand::Shutdown).await;
}

fn random_token() -> String {
    let mut bytes = [0u8; 24];
    rand::rng().fill(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Deserialize)]
struct WsQuery {
    token: String,
}

async fn ws_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WsQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if query.token != state.token {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) {
        let allowed = origin == format!("http://127.0.0.1:{}", state.port)
            || origin == format!("http://localhost:{}", state.port)
            || origin == "http://127.0.0.1:5173";
        if !allowed {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    upgrade.on_upgrade(move |socket| websocket(socket, state))
}

async fn websocket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    if send_json(
        &mut sender,
        &ServerMessage::StateFull(full_state(&state).await),
    )
    .await
    .is_err()
    {
        return;
    }
    if let Some(lab) = state.lab.lock().await.as_ref() {
        let replay = lab.debugger.replay();
        if !replay.is_empty() {
            let mut frame = Vec::with_capacity(replay.len() + 1);
            frame.push(GDB_PTY_CHANNEL);
            frame.extend(replay);
            let _ = sender.send(Message::Binary(frame.into())).await;
        }
    }
    let mut events = state.events.subscribe();
    let mut pty = state.pty.subscribe();
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => if send_json(&mut sender, &event).await.is_err() { break; },
                Err(broadcast::error::RecvError::Lagged(_)) => { let _ = send_json(&mut sender, &ServerMessage::StateFull(full_state(&state).await)).await; }
                Err(_) => break,
            },
            data = pty.recv() => if let Ok(data) = data {
                let mut frame = Vec::with_capacity(data.len() + 1); frame.push(GDB_PTY_CHANNEL); frame.extend(data);
                if sender.send(Message::Binary(frame.into())).await.is_err() { break; }
            },
            message = receiver.next() => match message {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(command) => if let Err(error) = handle_client(&state, command).await { emit_error(&state, "command", error.to_string()); },
                    Err(error) => emit_error(&state, "protocol", error.to_string()),
                },
                Some(Ok(Message::Binary(data))) if data.first() == Some(&GDB_PTY_CHANNEL) => {
                    let target = state.lab.lock().await.as_ref().map(|lab| (lab.debugger.clone(), lab.journal.clone()));
                    if let Some((debugger, journal)) = target {
                        let input = String::from_utf8_lossy(&data[1..]).into_owned();
                        journal.record("pwndbg", "input", &json!({"text": input})).await;
                        let _ = debugger.command(DebuggerCommand::Input(data[1..].to_vec())).await;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            }
        }
    }
}

async fn send_json(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &ServerMessage,
) -> Result<()> {
    sender
        .send(Message::Text(envelope(value).to_string().into()))
        .await?;
    Ok(())
}

async fn handle_client(state: &Arc<AppState>, command: ClientMessage) -> Result<()> {
    match command {
        ClientMessage::LabStart(config) => start_lab(state, config, None, None).await,
        ClientMessage::LabResume { lab_id } => {
            let record = state
                .store
                .get(&lab_id)
                .await
                .context("saved lab not found")?;
            start_lab(state, record.config, Some(record.id), record.thread_id).await
        }
        ClientMessage::LabStop => {
            stop_lab(state).await;
            Ok(())
        }
        ClientMessage::TerminalResize { cols, rows } => {
            with_lab(state, |lab| lab.debugger.clone())
                .await?
                .command(DebuggerCommand::Resize { cols, rows })
                .await
        }
        ClientMessage::DebuggerRefresh => {
            with_lab(state, |lab| lab.debugger.clone())
                .await?
                .command(DebuggerCommand::Refresh)
                .await
        }
        ClientMessage::KernelExecute { cell_id, code } => {
            with_lab(state, |lab| lab.kernel.clone())
                .await?
                .command(KernelCommand::Execute { cell_id, code })
                .await
        }
        ClientMessage::KernelInterrupt => {
            with_lab(state, |lab| lab.kernel.clone())
                .await?
                .command(KernelCommand::Interrupt)
                .await
        }
        ClientMessage::KernelRestart => restart_kernel(state).await,
        ClientMessage::KernelInputReply { request_id, value } => {
            with_lab(state, |lab| lab.kernel.clone())
                .await?
                .command(KernelCommand::InputReply { request_id, value })
                .await
        }
        ClientMessage::CodexSend { text } => {
            let (debugger, terminal_delta, kernel, config) = with_lab(state, |lab| {
                let terminal = lab.debugger.tutor_transcript();
                let mut cursor = lab.tutor_context.lock().expect("tutor context lock");
                let delta = terminal_delta(&cursor.terminal, &terminal, 8 * 1024);
                cursor.terminal = terminal;
                (
                    compact_debugger(&lab.debugger.snapshot.borrow()),
                    delta,
                    lab.kernel.context_history(2),
                    lab.config.clone(),
                )
            })
            .await?;
            let context = json!({ "objective": config.objective, "target": { "program": config.program, "args": config.args }, "debugger": debugger, "debuggerTerminal": { "format": "new plain text since the previous tutor turn; ANSI/control sequences removed", "delta": terminal_delta }, "recentIpPython": kernel });
            state
                .codex
                .command(CodexCommand::Send {
                    text,
                    context,
                    purpose: TurnPurpose::Tutor,
                })
                .await
        }
        ClientMessage::GenerateWriteup => generate_writeup(state).await,
        ClientMessage::CodexInterrupt => state.codex.command(CodexCommand::Interrupt).await,
        ClientMessage::CodexNewThread => {
            let _ = with_lab(state, |lab| {
                lab.tutor_context
                    .lock()
                    .expect("tutor context lock")
                    .terminal
                    .clear();
            })
            .await;
            state.codex.command(CodexCommand::NewThread).await
        }
        ClientMessage::AuthLogin => state.codex.command(CodexCommand::Login).await,
    }
}

async fn with_lab<T>(state: &Arc<AppState>, map: impl FnOnce(&Lab) -> T) -> Result<T> {
    let guard = state.lab.lock().await;
    guard.as_ref().map(map).context("no active lab")
}

async fn start_lab(
    state: &Arc<AppState>,
    mut config: LabConfig,
    existing_id: Option<String>,
    thread_id: Option<String>,
) -> Result<()> {
    debugger::validate_config(&mut config)?;
    stop_lab(state).await;
    emit(
        state,
        ServerMessage::LabStatus(LabStatus {
            phase: "starting".into(),
            config: Some(config.clone()),
            ..Default::default()
        }),
    );
    let debugger = debugger::spawn(&config).await?;
    let kernel = match kernel::spawn(&config).await {
        Ok(value) => value,
        Err(error) => {
            let _ = debugger.command(DebuggerCommand::Stop).await;
            return Err(error);
        }
    };
    let resumed = existing_id.is_some();
    let id = existing_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let journal = Journal::open(&id, &config).await?;
    journal
        .record("lab", "started", &json!({"resumed": resumed}))
        .await;
    state
        .codex
        .command(CodexCommand::Configure {
            cwd: config.workspace.clone(),
            objective: config.objective.clone(),
            resume_thread: thread_id.clone(),
        })
        .await?;
    state
        .store
        .put(id.clone(), config.clone(), thread_id)
        .await?;
    let lab = Lab {
        id: id.clone(),
        config: config.clone(),
        debugger: debugger.clone(),
        kernel: kernel.clone(),
        journal: journal.clone(),
        tutor_context: std::sync::Mutex::new(TutorContextCursor::default()),
    };
    *state.lab.lock().await = Some(lab);
    forward_debugger(state.clone(), debugger, journal.clone());
    forward_kernel(state.clone(), kernel, journal);
    emit(
        state,
        ServerMessage::LabStatus(LabStatus {
            phase: "running".into(),
            lab_id: Some(id),
            config: Some(config),
            message: None,
        }),
    );
    Ok(())
}

async fn stop_lab(state: &Arc<AppState>) {
    if let Some(lab) = state.lab.lock().await.take() {
        lab.journal.record("lab", "stopped", &json!({})).await;
        let _ = lab.kernel.command(KernelCommand::Stop).await;
        let _ = lab.debugger.command(DebuggerCommand::Stop).await;
        emit(
            state,
            ServerMessage::LabStatus(LabStatus {
                phase: "idle".into(),
                ..Default::default()
            }),
        );
    }
}

async fn generate_writeup(state: &Arc<AppState>) -> Result<()> {
    if state.writeup.lock().await.is_some() {
        anyhow::bail!("a writeup is already being generated");
    }
    let (journal, config) =
        with_lab(state, |lab| (lab.journal.clone(), lab.config.clone())).await?;
    let events = journal.writeup_context().await?;
    journal.record("writeup", "requested", &json!({})).await;
    *state.writeup.lock().await = Some(WriteupCapture {
        journal,
        markdown: String::new(),
    });
    let prompt = "Create a polished Markdown blog writeup from this lab journal. Be technically accurate and chronological. Include: objective, protections/checksec findings, investigation, important debugger observations, vulnerability/root cause, exploit-development reasoning, final technique, and lessons learned. Use commands and outputs as evidence, but remove repetitive terminal redraws and tutor chatter. Do not invent successful steps or facts absent from the journal. Redact local home-directory prefixes and any apparent secrets. Return only the Markdown document.".to_owned();
    let context = json!({"objective": config.objective, "target": {"program": config.program, "args": config.args}, "labJournalJsonl": events});
    if let Err(error) = state
        .codex
        .command(CodexCommand::Send {
            text: prompt,
            context,
            purpose: TurnPurpose::Writeup,
        })
        .await
    {
        *state.writeup.lock().await = None;
        return Err(error);
    }
    Ok(())
}

async fn restart_kernel(state: &Arc<AppState>) -> Result<()> {
    let (old, config, journal) = with_lab(state, |lab| {
        (lab.kernel.clone(), lab.config.clone(), lab.journal.clone())
    })
    .await?;
    old.command(KernelCommand::Stop).await?;
    let replacement = kernel::spawn(&config).await?;
    {
        let mut guard = state.lab.lock().await;
        guard.as_mut().context("lab stopped during restart")?.kernel = replacement.clone();
    }
    journal.record("ipython", "restarted", &json!({})).await;
    forward_kernel(state.clone(), replacement, journal);
    emit(
        state,
        ServerMessage::KernelEvent(protocol::KernelEvent::Restarted),
    );
    Ok(())
}

fn forward_debugger(state: Arc<AppState>, debugger: DebuggerHandle, journal: Journal) {
    let mut output = debugger.output.subscribe();
    let initial_transcript = debugger.tutor_transcript();
    let pty = state.pty.clone();
    let output_journal = journal.clone();
    tokio::spawn(async move {
        if !initial_transcript.is_empty() {
            output_journal
                .record(
                    "pwndbg",
                    "initialTranscript",
                    &json!({"text": initial_transcript}),
                )
                .await;
        }
        while let Ok(data) = output.recv().await {
            let text = debugger::strip_terminal_controls(&String::from_utf8_lossy(&data));
            let _ = pty.send(data);
            if !text.is_empty() {
                output_journal
                    .record("pwndbg", "output", &json!({"text": text}))
                    .await;
            }
        }
    });
    let mut snapshots = debugger.snapshot.clone();
    let events = state.events.clone();
    let snapshot_journal = journal;
    tokio::spawn(async move {
        let initial_snapshot = snapshots.borrow().clone();
        snapshot_journal
            .record("gdbMi", "snapshot", &initial_snapshot)
            .await;
        while snapshots.changed().await.is_ok() {
            let snapshot = snapshots.borrow().clone();
            let _ = events.send(ServerMessage::DebuggerState(snapshot.clone()));
            snapshot_journal
                .record("gdbMi", "snapshot", &snapshot)
                .await;
        }
    });
}

fn forward_kernel(state: Arc<AppState>, kernel: KernelHandle, journal: Journal) {
    let mut source = kernel.events.subscribe();
    let events = state.events.clone();
    tokio::spawn(async move {
        while let Ok(event) = source.recv().await {
            journal.record("ipython", "event", &event).await;
            let _ = events.send(ServerMessage::KernelEvent(event));
        }
    });
}

fn forward_codex(state: Arc<AppState>) {
    let mut source = state.codex.events.subscribe();
    tokio::spawn(async move {
        let mut assistant_response = String::new();
        while let Ok(event) = source.recv().await {
            let journal = state
                .lab
                .lock()
                .await
                .as_ref()
                .map(|lab| lab.journal.clone());
            match &event {
                protocol::CodexEvent::User { text } => {
                    if let Some(journal) = &journal {
                        assistant_response.clear();
                        journal
                            .record("codex", "userMessage", &json!({"text": text}))
                            .await;
                    }
                }
                protocol::CodexEvent::Delta { text } => assistant_response.push_str(text),
                protocol::CodexEvent::Completed { status } => {
                    if let Some(journal) = &journal {
                        journal
                            .record(
                                "codex",
                                "assistantMessage",
                                &json!({"text": assistant_response, "status": status}),
                            )
                            .await;
                        assistant_response.clear();
                    }
                }
                protocol::CodexEvent::Thread { thread_id } => {
                    if let Some(journal) = &journal {
                        journal
                            .record("codex", "thread", &json!({"threadId": thread_id}))
                            .await;
                    }
                }
                protocol::CodexEvent::Notice { text } => {
                    if let Some(journal) = &journal {
                        journal
                            .record("codex", "notice", &json!({"text": text}))
                            .await;
                    }
                }
                protocol::CodexEvent::TurnMetrics { .. } => {
                    if let Some(journal) = &journal {
                        journal.record("codex", "turnMetrics", &event).await;
                    }
                }
                _ => {}
            }
            match &event {
                protocol::CodexEvent::WriteupDelta { text } => {
                    if let Some(capture) = state.writeup.lock().await.as_mut() {
                        capture.markdown.push_str(text);
                    }
                }
                protocol::CodexEvent::WriteupCompleted { status } => {
                    if let Some(capture) = state.writeup.lock().await.take() {
                        if status == "completed" {
                            match capture.journal.save_writeup(&capture.markdown).await {
                                Ok(path) => {
                                    capture
                                        .journal
                                        .record("writeup", "saved", &json!({"path": path}))
                                        .await;
                                    emit(
                                        &state,
                                        ServerMessage::CodexEvent(
                                            protocol::CodexEvent::WriteupSaved {
                                                path: path.display().to_string(),
                                            },
                                        ),
                                    );
                                }
                                Err(error) => emit_error(&state, "writeup", error.to_string()),
                            }
                        } else {
                            emit_error(
                                &state,
                                "writeup",
                                format!("generation ended with status {status}"),
                            );
                        }
                    }
                }
                _ => {}
            }
            if let protocol::CodexEvent::Thread { thread_id } = &event
                && let Some((id, config)) = {
                    state
                        .lab
                        .lock()
                        .await
                        .as_ref()
                        .map(|lab| (lab.id.clone(), lab.config.clone()))
                }
            {
                let _ = state.store.put(id, config, Some(thread_id.clone())).await;
            }
            if !matches!(event, protocol::CodexEvent::WriteupDelta { .. }) {
                emit(&state, ServerMessage::CodexEvent(event));
                emit(
                    &state,
                    ServerMessage::AuthState(state.codex.auth.borrow().clone()),
                );
            }
        }
    });
}

async fn full_state(state: &Arc<AppState>) -> FullState {
    let recent_labs = state.store.list().await;
    let auth = state.codex.auth.borrow().clone();
    if let Some(lab) = state.lab.lock().await.as_ref() {
        FullState {
            lab: LabStatus {
                phase: "running".into(),
                lab_id: Some(lab.id.clone()),
                config: Some(lab.config.clone()),
                message: None,
            },
            debugger: lab.debugger.snapshot.borrow().clone(),
            kernel: lab.kernel.status.borrow().clone(),
            auth,
            recent_labs,
        }
    } else {
        FullState {
            lab: LabStatus {
                phase: "idle".into(),
                ..Default::default()
            },
            auth,
            recent_labs,
            ..Default::default()
        }
    }
}

fn emit(state: &Arc<AppState>, event: ServerMessage) {
    let _ = state.events.send(event);
}
fn emit_error(state: &Arc<AppState>, scope: &str, message: String) {
    warn!(%scope, %message);
    emit(
        state,
        ServerMessage::Error {
            scope: scope.into(),
            message,
        },
    );
}

fn terminal_delta(previous: &str, current: &str, limit: usize) -> String {
    let overlap = if current.starts_with(previous) {
        previous.len()
    } else {
        previous
            .char_indices()
            .find_map(|(index, _)| {
                current
                    .starts_with(&previous[index..])
                    .then_some(previous.len() - index)
            })
            .unwrap_or(0)
    };
    tail_chars(&current[overlap..], limit)
}

fn tail_chars(value: &str, limit: usize) -> String {
    let start = value
        .char_indices()
        .rev()
        .nth(limit)
        .map_or(0, |(index, _)| index);
    value[start..].to_owned()
}

fn compact_debugger(snapshot: &protocol::DebuggerSnapshot) -> serde_json::Value {
    const KEY_REGISTERS: &[&str] = &[
        "rip", "rsp", "rbp", "rax", "rbx", "rcx", "rdx", "rdi", "rsi", "r8", "r9",
    ];
    let registers: Vec<_> = snapshot
        .registers
        .iter()
        .filter(|register| KEY_REGISTERS.contains(&register.name.as_str()))
        .collect();
    let instruction_index = snapshot
        .frame
        .as_ref()
        .and_then(|frame| {
            snapshot
                .disassembly
                .iter()
                .position(|instruction| instruction.address == frame.address)
        })
        .unwrap_or(0);
    let instruction_start = instruction_index.saturating_sub(4);
    let instruction_end = (instruction_index + 8).min(snapshot.disassembly.len());
    json!({
        "revision": snapshot.revision,
        "state": snapshot.state,
        "stopReason": snapshot.stop_reason,
        "stale": snapshot.stale,
        "frame": snapshot.frame,
        "frames": snapshot.frames.iter().take(5).collect::<Vec<_>>(),
        "keyRegisters": registers,
        "nearbyInstructions": &snapshot.disassembly[instruction_start..instruction_end],
        "breakpoints": snapshot.breakpoints,
        "stack": snapshot.stack,
        "error": snapshot.error,
    })
}

#[cfg(test)]
mod context_tests {
    use super::terminal_delta;

    #[test]
    fn terminal_context_only_returns_new_text() {
        assert_eq!(
            terminal_delta("pwndbg> check", "pwndbg> checksec\nFull RELRO\n", 8192),
            "sec\nFull RELRO\n"
        );
    }

    #[test]
    fn terminal_context_handles_rolling_window_overlap() {
        assert_eq!(
            terminal_delta("old\nshared\n", "shared\nnew\n", 8192),
            "new\n"
        );
    }
}
