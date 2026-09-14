use crate::protocol::{KernelEvent, KernelStatus, LabConfig};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{collections::HashMap, process::Stdio};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{broadcast, mpsc, watch},
};
use tracing::warn;

#[derive(Debug)]
pub enum KernelCommand {
    Execute { cell_id: String, code: String },
    InputReply { request_id: String, value: String },
    Interrupt,
    Stop,
}
#[derive(Clone)]
pub struct KernelHandle {
    commands: mpsc::Sender<KernelCommand>,
    pub events: broadcast::Sender<KernelEvent>,
    pub status: watch::Receiver<KernelStatus>,
    history: watch::Receiver<Vec<KernelHistory>>,
}
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelHistory {
    pub code: String,
    pub output: String,
    pub status: String,
}
impl KernelHandle {
    pub async fn command(&self, command: KernelCommand) -> Result<()> {
        self.commands.send(command).await.context("kernel stopped")
    }
    pub fn context_history(&self, limit: usize) -> Vec<KernelHistory> {
        self.history
            .borrow()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}

pub async fn spawn(config: &LabConfig) -> Result<KernelHandle> {
    let bridge = concat!(env!("CARGO_MANIFEST_DIR"), "/python/kernel_bridge.py");
    let mut child = Command::new(&config.python)
        .arg(bridge)
        .arg(&config.workspace)
        .current_dir(&config.workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("launch Jupyter bridge with {}", config.python))?;
    let mut stdin = child
        .stdin
        .take()
        .context("kernel bridge stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .context("kernel bridge stdout unavailable")?;
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                warn!(target:"ipykernel", %line);
            }
        });
    }
    let (command_tx, mut command_rx) = mpsc::channel(64);
    let (events, _) = broadcast::channel(256);
    let (status_tx, status) = watch::channel(KernelStatus {
        state: "starting".into(),
        execution_count: None,
    });
    let (history_tx, history) = watch::channel(Vec::new());
    let event_sender = events.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut items: Vec<KernelHistory> = Vec::new();
        let mut cells: HashMap<String, usize> = HashMap::new();
        loop {
            tokio::select! {
                command=command_rx.recv()=>{ let value=match command {
                    Some(KernelCommand::Execute{cell_id,code})=>{ cells.insert(cell_id.clone(),items.len()); items.push(KernelHistory{code:code.clone(),output:String::new(),status:"running".into()}); let _=history_tx.send(items.clone()); json!({"type":"execute","cellId":cell_id,"code":code}) },
                    Some(KernelCommand::InputReply{request_id,value})=>json!({"type":"inputReply","requestId":request_id,"value":value}),
                    Some(KernelCommand::Interrupt)=>json!({"type":"interrupt"}), Some(KernelCommand::Stop)|None=>json!({"type":"stop"}) };
                    if send(&mut stdin,&value).await.is_err()||value["type"]=="stop" { break; }
                },
                line=lines.next_line()=>match line { Ok(Some(line))=>if let Ok(value)=serde_json::from_str::<Value>(&line) {
                    if let Some(event)=parse(&value) { update(&mut items,&cells,&event); let _=history_tx.send(items.clone());
                        match &event { KernelEvent::Status{state}=>{let n=status_tx.borrow().execution_count;let _=status_tx.send(KernelStatus{state:state.clone(),execution_count:n});}, KernelEvent::Completed{execution_count,..}=>{let _=status_tx.send(KernelStatus{state:"idle".into(),execution_count:*execution_count});}, _=>{} }
                        let _=event_sender.send(event);
                    } else if value["kind"]=="bridgeError" { warn!(message=%value["message"],"kernel bridge error"); }
                }, _=>break },
                _=child.wait()=>{let _=status_tx.send(KernelStatus{state:"dead".into(),execution_count:None});break;}
            }
        }
        let _ = child.start_kill();
    });
    Ok(KernelHandle {
        commands: command_tx,
        events,
        status,
        history,
    })
}
async fn send(stdin: &mut tokio::process::ChildStdin, value: &Value) -> Result<()> {
    let mut b = serde_json::to_vec(value)?;
    b.push(b'\n');
    stdin.write_all(&b).await?;
    Ok(())
}
fn parse(v: &Value) -> Option<KernelEvent> {
    let s = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
    let count = || {
        v.get("executionCount")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
    };
    match v.get("kind")?.as_str()? {
        "status" => Some(KernelEvent::Status { state: s("state")? }),
        "started" => Some(KernelEvent::Started {
            cell_id: s("cellId")?,
            request_id: s("requestId")?,
        }),
        "stream" => Some(KernelEvent::Stream {
            cell_id: s("cellId"),
            name: s("name")?,
            text: s("text")?,
        }),
        "result" => Some(KernelEvent::Result {
            cell_id: s("cellId"),
            execution_count: count(),
            text: s("text"),
            image_png: s("imagePng"),
        }),
        "error" => Some(KernelEvent::Error {
            cell_id: s("cellId"),
            name: s("name")?,
            value: s("value")?,
            traceback: v["traceback"]
                .as_array()?
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        }),
        "inputRequest" => Some(KernelEvent::InputRequest {
            request_id: s("requestId")?,
            prompt: s("prompt")?,
            password: v["password"].as_bool().unwrap_or(false),
        }),
        "completed" => Some(KernelEvent::Completed {
            cell_id: s("cellId"),
            execution_count: count(),
            status: s("status")?,
        }),
        _ => None,
    }
}
fn update(items: &mut [KernelHistory], cells: &HashMap<String, usize>, event: &KernelEvent) {
    let (cell, text, status) = match event {
        KernelEvent::Stream { cell_id, text, .. } => (cell_id.as_ref(), Some(text.as_str()), None),
        KernelEvent::Result { cell_id, text, .. } => (cell_id.as_ref(), text.as_deref(), None),
        KernelEvent::Error {
            cell_id,
            name,
            value,
            ..
        } => (cell_id.as_ref(), None, Some(format!("{name}: {value}"))),
        KernelEvent::Completed {
            cell_id, status, ..
        } => (cell_id.as_ref(), None, Some(status.clone())),
        _ => return,
    };
    if let Some(item) = cell
        .and_then(|id| cells.get(id))
        .and_then(|i| items.get_mut(*i))
    {
        if let Some(text) = text {
            let room = 8192usize.saturating_sub(item.output.len());
            item.output.extend(text.chars().take(room));
        }
        if let Some(status) = status {
            item.status = status;
        }
    }
}
