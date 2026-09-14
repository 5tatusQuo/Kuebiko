mod codex;
mod debugger;
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

use codex::{CodexCommand, CodexHandle};
use debugger::{DebuggerCommand, DebuggerHandle};
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
}

struct AppState {
    lab: Mutex<Option<Lab>>,
    codex: CodexHandle,
    store: Store,
    events: broadcast::Sender<ServerMessage>,
    pty: broadcast::Sender<Vec<u8>>,
    token: String,
    port: u16,
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
                    if let Some(lab) = state.lab.lock().await.as_ref() { let _ = lab.debugger.command(DebuggerCommand::Input(data[1..].to_vec())).await; }
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
            let (debugger, terminal, kernel, config) = with_lab(state, |lab| {
                (
                    lab.debugger.snapshot.borrow().clone(),
                    lab.debugger.tutor_transcript(),
                    lab.kernel.context_history(),
                    lab.config.clone(),
                )
            })
            .await?;
            let context = json!({ "objective": config.objective, "target": { "workspace": config.workspace, "program": config.program, "args": config.args }, "debugger": debugger, "debuggerTerminal": { "format": "plain text with ANSI/control sequences removed", "tail": terminal }, "ipython": kernel });
            state
                .codex
                .command(CodexCommand::Send { text, context })
                .await
        }
        ClientMessage::CodexInterrupt => state.codex.command(CodexCommand::Interrupt).await,
        ClientMessage::CodexNewThread => state.codex.command(CodexCommand::NewThread).await,
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
    let id = existing_id.unwrap_or_else(|| Uuid::new_v4().to_string());
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
    };
    *state.lab.lock().await = Some(lab);
    forward_debugger(state.clone(), debugger);
    forward_kernel(state.clone(), kernel);
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

async fn restart_kernel(state: &Arc<AppState>) -> Result<()> {
    let (old, config) = with_lab(state, |lab| (lab.kernel.clone(), lab.config.clone())).await?;
    old.command(KernelCommand::Stop).await?;
    let replacement = kernel::spawn(&config).await?;
    {
        let mut guard = state.lab.lock().await;
        guard.as_mut().context("lab stopped during restart")?.kernel = replacement.clone();
    }
    forward_kernel(state.clone(), replacement);
    emit(
        state,
        ServerMessage::KernelEvent(protocol::KernelEvent::Restarted),
    );
    Ok(())
}

fn forward_debugger(state: Arc<AppState>, debugger: DebuggerHandle) {
    let mut output = debugger.output.subscribe();
    let pty = state.pty.clone();
    tokio::spawn(async move {
        while let Ok(data) = output.recv().await {
            let _ = pty.send(data);
        }
    });
    let mut snapshots = debugger.snapshot.clone();
    let events = state.events.clone();
    tokio::spawn(async move {
        while snapshots.changed().await.is_ok() {
            let _ = events.send(ServerMessage::DebuggerState(snapshots.borrow().clone()));
        }
    });
}

fn forward_kernel(state: Arc<AppState>, kernel: KernelHandle) {
    let mut source = kernel.events.subscribe();
    let events = state.events.clone();
    tokio::spawn(async move {
        while let Ok(event) = source.recv().await {
            let _ = events.send(ServerMessage::KernelEvent(event));
        }
    });
}

fn forward_codex(state: Arc<AppState>) {
    let mut source = state.codex.events.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = source.recv().await {
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
            emit(&state, ServerMessage::CodexEvent(event));
            emit(
                &state,
                ServerMessage::AuthState(state.codex.auth.borrow().clone()),
            );
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
