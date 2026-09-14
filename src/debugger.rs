use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    os::fd::AsRawFd,
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use nix::{
    fcntl::{FcntlArg, OFlag, fcntl},
    pty::{Winsize, openpty},
    sys::signal::{Signal, killpg},
    unistd::{Pid, setsid, ttyname},
};
use tokio::{
    io::unix::AsyncFd,
    process::{Child, Command},
    sync::{broadcast, mpsc, watch},
};
use tracing::{debug, warn};

use crate::{
    mi::{MiRecord, MiValue, parse_line},
    protocol::{
        Breakpoint, DebuggerSnapshot, Frame, Instruction, LabConfig, MemoryBlock, Register,
    },
};

const REPLAY_LIMIT: usize = 1024 * 1024;
const TUTOR_TRANSCRIPT_LIMIT: usize = 12 * 1024;

#[derive(Debug)]
pub enum DebuggerCommand {
    Input(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    Refresh,
    Stop,
}

#[derive(Clone)]
pub struct DebuggerHandle {
    commands: mpsc::Sender<DebuggerCommand>,
    pub output: broadcast::Sender<Vec<u8>>,
    pub snapshot: watch::Receiver<DebuggerSnapshot>,
    replay: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl DebuggerHandle {
    pub async fn command(&self, command: DebuggerCommand) -> Result<()> {
        self.commands
            .send(command)
            .await
            .context("debugger stopped")
    }

    pub fn replay(&self) -> Vec<u8> {
        self.replay.lock().expect("replay lock").clone()
    }

    pub fn tutor_transcript(&self) -> String {
        let raw = self.replay.lock().expect("replay lock");
        let plain = strip_terminal_controls(&String::from_utf8_lossy(&raw));
        let start = plain
            .char_indices()
            .rev()
            .nth(TUTOR_TRANSCRIPT_LIMIT)
            .map_or(0, |(index, _)| index);
        plain[start..].to_owned()
    }
}

pub async fn spawn(config: &LabConfig) -> Result<DebuggerHandle> {
    let human = openpty(
        Some(&Winsize {
            ws_row: 32,
            ws_col: 110,
            ws_xpixel: 0,
            ws_ypixel: 0,
        }),
        None,
    )?;
    let machine = openpty(None, None)?;
    let mi_path = ttyname(&machine.slave)?.to_string_lossy().into_owned();
    set_nonblocking(&human.master)?;
    set_nonblocking(&machine.master)?;

    let human_master = File::from(nix::unistd::dup(&human.master)?);
    let human_writer = human_master.try_clone()?;
    let mi_master = File::from(nix::unistd::dup(&machine.master)?);
    let mi_writer = mi_master.try_clone()?;
    let human_slave_fd = human.slave.as_raw_fd();

    let mut command = Command::new(&config.debugger);
    command
        .arg("-q")
        .arg("-ex")
        .arg("set debuginfod enabled off")
        .arg("-ex")
        .arg("set pagination off")
        .arg("-ex")
        .arg("set mi-async on")
        .arg("-ex")
        .arg(format!("new-ui mi2 {mi_path}"))
        .arg("--args")
        .arg(&config.program)
        .args(&config.args)
        .current_dir(&config.workspace)
        .env("TERM", "xterm-256color")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(move || {
            setsid().map_err(std::io::Error::other)?;
            if libc::ioctl(human_slave_fd, libc::TIOCSCTTY, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            for target in [0, 1, 2] {
                if libc::dup2(human_slave_fd, target) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .with_context(|| format!("launch debugger {}", config.debugger))?;
    drop(human);
    drop(machine);

    let human_reader = AsyncFd::new(human_master)?;
    let machine_reader = AsyncFd::new(mi_master)?;
    let (command_tx, command_rx) = mpsc::channel(128);
    let (output, _) = broadcast::channel(256);
    let (snapshot_tx, snapshot) = watch::channel(DebuggerSnapshot {
        state: "starting".into(),
        ..Default::default()
    });
    let replay = Arc::new(std::sync::Mutex::new(Vec::new()));

    tokio::spawn(run(
        child,
        human_reader,
        human_writer,
        machine_reader,
        mi_writer,
        command_rx,
        output.clone(),
        snapshot_tx,
        replay.clone(),
    ));
    Ok(DebuggerHandle {
        commands: command_tx,
        output,
        snapshot,
        replay,
    })
}

fn set_nonblocking(fd: &impl std::os::fd::AsFd) -> Result<()> {
    let flags = OFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFL)?);
    fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run(
    mut child: Child,
    human_reader: AsyncFd<File>,
    mut human_writer: File,
    mi_reader: AsyncFd<File>,
    mut mi_writer: File,
    mut commands: mpsc::Receiver<DebuggerCommand>,
    output: broadcast::Sender<Vec<u8>>,
    snapshot_tx: watch::Sender<DebuggerSnapshot>,
    replay: Arc<std::sync::Mutex<Vec<u8>>>,
) {
    let mut human_buf = vec![0; 65536];
    let mut mi_buf = vec![0; 65536];
    let mut mi_pending = String::new();
    let mut snapshot = snapshot_tx.borrow().clone();
    let mut register_names = Vec::new();
    let mut next_token = 100u64;

    loop {
        tokio::select! {
            result = read_ready(&human_reader, &mut human_buf) => match result {
                Ok(0) => break,
                Ok(count) => {
                    let data = human_buf[..count].to_vec();
                    {
                        let mut saved = replay.lock().expect("replay lock");
                        saved.extend_from_slice(&data);
                        if saved.len() > REPLAY_LIMIT { let drain = saved.len() - REPLAY_LIMIT; saved.drain(..drain); }
                    }
                    let _ = output.send(data);
                }
                Err(error) => { warn!(%error, "human PTY read failed"); break; }
            },
            result = read_ready(&mi_reader, &mut mi_buf) => match result {
                Ok(0) => break,
                Ok(count) => {
                    mi_pending.push_str(&String::from_utf8_lossy(&mi_buf[..count]));
                    while let Some(newline) = mi_pending.find('\n') {
                        let line = mi_pending[..newline].trim_end_matches('\r').to_string();
                        mi_pending.drain(..=newline);
                        if line.is_empty() { continue; }
                        match parse_line(&line) {
                            Ok(record) => handle_record(record, &mut snapshot, &mut register_names, &snapshot_tx, &mut mi_writer, &mut next_token).await,
                            Err(error) => debug!(%error, %line, "ignored MI line"),
                        }
                    }
                }
                Err(error) => { warn!(%error, "MI PTY read failed"); break; }
            },
            command = commands.recv() => match command {
                Some(DebuggerCommand::Input(data)) => { let _ = write_all_async(&mut human_writer, &data).await; }
                Some(DebuggerCommand::Resize { cols, rows }) => {
                    let size = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
                    unsafe { libc::ioctl(human_writer.as_raw_fd(), libc::TIOCSWINSZ, &size); }
                }
                Some(DebuggerCommand::Refresh) => query_snapshot(&mut mi_writer, &mut next_token).await,
                Some(DebuggerCommand::Stop) | None => break,
            },
            status = child.wait() => {
                snapshot.state = "exited".into();
                snapshot.error = status.err().map(|e| e.to_string());
                snapshot.revision += 1;
                let _ = snapshot_tx.send(snapshot.clone());
                return;
            }
        }
    }
    if let Some(pid) = child.id() {
        let _ = killpg(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
    if let Some(pid) = child.id() {
        let _ = killpg(Pid::from_raw(pid as i32), Signal::SIGKILL);
    }
}

async fn read_ready(fd: &AsyncFd<File>, buffer: &mut [u8]) -> std::io::Result<usize> {
    loop {
        let mut guard = fd.readable().await?;
        match guard.try_io(|inner| {
            let mut file = inner.get_ref();
            file.read(buffer)
        }) {
            Ok(result) => return result,
            Err(_) => continue,
        }
    }
}

async fn write_all_async(file: &mut File, data: &[u8]) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < data.len() {
        match file.write(&data[offset..]) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(count) => offset += count,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn mi_command(writer: &mut File, next: &mut u64, command: &str) {
    *next += 1;
    let _ = write_all_async(writer, format!("{}{}\n", *next, command).as_bytes()).await;
}

async fn query_snapshot(writer: &mut File, next: &mut u64) {
    for command in [
        "-stack-list-frames 0 31",
        "-data-list-register-names",
        "-data-list-register-values x",
        "-data-disassemble -s '$pc-32' -e '$pc+96' -- 0",
        "-break-list",
        "-data-read-memory-bytes $sp 256",
    ] {
        mi_command(writer, next, command).await;
    }
}

async fn handle_record(
    record: MiRecord,
    snapshot: &mut DebuggerSnapshot,
    register_names: &mut Vec<String>,
    tx: &watch::Sender<DebuggerSnapshot>,
    writer: &mut File,
    next: &mut u64,
) {
    match record {
        MiRecord::Async { class, .. } if class == "running" => {
            snapshot.state = "running".into();
            snapshot.stale = true;
            snapshot.stop_reason = None;
            publish(snapshot, tx);
        }
        MiRecord::Async { class, results, .. } if class == "stopped" => {
            snapshot.state = "stopped".into();
            snapshot.stale = false;
            snapshot.stop_reason = text(&results, "reason").map(str::to_owned);
            snapshot.frame = results.get("frame").and_then(frame_from_value);
            publish(snapshot, tx);
            query_snapshot(writer, next).await;
        }
        MiRecord::Async { class, .. } if class.starts_with("breakpoint-") => {
            mi_command(writer, next, "-break-list").await;
        }
        MiRecord::Result { class, results, .. } if class == "error" => {
            snapshot.error = text(&results, "msg").map(str::to_owned);
            publish(snapshot, tx);
        }
        MiRecord::Result { class, results, .. } if class == "done" => {
            if snapshot.state == "starting" {
                snapshot.state = "ready".into();
                snapshot.stale = false;
            }
            if let Some(names) = results.get("register-names") {
                *register_names = names
                    .items()
                    .iter()
                    .filter_map(MiValue::text)
                    .map(str::to_owned)
                    .collect();
            }
            if let Some(values) = results.get("register-values") {
                snapshot.registers = values
                    .items()
                    .iter()
                    .filter_map(|value| {
                        let tuple = unwrap_named(value, "register-values").unwrap_or(value);
                        let number = tuple.field("number")?.text()?.parse::<usize>().ok()?;
                        Some(Register {
                            name: register_names
                                .get(number)
                                .cloned()
                                .unwrap_or_else(|| number.to_string()),
                            value: tuple.field("value")?.text()?.to_owned(),
                        })
                    })
                    .collect();
            }
            if let Some(stack) = results.get("stack") {
                snapshot.frames = stack
                    .items()
                    .iter()
                    .filter_map(|v| frame_from_value(unwrap_named(v, "frame").unwrap_or(v)))
                    .collect();
                snapshot.frame = snapshot
                    .frames
                    .first()
                    .cloned()
                    .or_else(|| snapshot.frame.clone());
            }
            if let Some(asm) = results.get("asm_insns") {
                snapshot.disassembly = asm
                    .items()
                    .iter()
                    .filter_map(instruction_from_value)
                    .collect();
            }
            if let Some(table) = results.get("BreakpointTable")
                && let Some(body) = table.field("body")
            {
                snapshot.breakpoints = body
                    .items()
                    .iter()
                    .filter_map(|v| breakpoint_from_value(unwrap_named(v, "bkpt").unwrap_or(v)))
                    .collect();
            }
            if let Some(memory) = results.get("memory").and_then(|m| m.items().first()) {
                snapshot.stack = Some(MemoryBlock {
                    address: memory
                        .field("begin")
                        .and_then(MiValue::text)
                        .unwrap_or_default()
                        .to_owned(),
                    bytes: memory
                        .field("contents")
                        .and_then(MiValue::text)
                        .unwrap_or_default()
                        .to_owned(),
                });
            }
            snapshot.error = None;
            publish(snapshot, tx);
        }
        MiRecord::Prompt if snapshot.state == "starting" => {
            snapshot.state = "ready".into();
            snapshot.stale = false;
            publish(snapshot, tx);
            query_snapshot(writer, next).await;
        }
        _ => {}
    }
}

fn strip_terminal_controls(input: &str) -> String {
    #[derive(Clone, Copy)]
    enum State {
        Text,
        Escape,
        Csi,
        Osc,
        OscEscape,
    }
    let normalized = input.replace("\r\n", "\n");
    let mut state = State::Text;
    let mut output = String::with_capacity(normalized.len());
    for ch in normalized.chars() {
        state = match state {
            State::Text => match ch {
                '\u{1b}' => State::Escape,
                '\n' | '\t' => {
                    output.push(ch);
                    State::Text
                }
                '\r' => {
                    output.push('\n');
                    State::Text
                }
                ch if !ch.is_control() => {
                    output.push(ch);
                    State::Text
                }
                _ => State::Text,
            },
            State::Escape => match ch {
                '[' => State::Csi,
                ']' => State::Osc,
                _ => State::Text,
            },
            State::Csi => {
                if ('@'..='~').contains(&ch) {
                    State::Text
                } else {
                    State::Csi
                }
            }
            State::Osc => match ch {
                '\u{7}' => State::Text,
                '\u{1b}' => State::OscEscape,
                _ => State::Osc,
            },
            State::OscEscape => {
                if ch == '\\' {
                    State::Text
                } else {
                    State::Osc
                }
            }
        };
    }
    output
}

fn publish(snapshot: &mut DebuggerSnapshot, tx: &watch::Sender<DebuggerSnapshot>) {
    snapshot.revision += 1;
    let _ = tx.send(snapshot.clone());
}

fn text<'a>(map: &'a BTreeMap<String, MiValue>, key: &str) -> Option<&'a str> {
    map.get(key)?.text()
}
fn unwrap_named<'a>(value: &'a MiValue, name: &str) -> Option<&'a MiValue> {
    value.field(name)
}
fn parse_u32(value: Option<&str>) -> Option<u32> {
    value?.parse().ok()
}

fn frame_from_value(value: &MiValue) -> Option<Frame> {
    Some(Frame {
        level: parse_u32(value.field("level").and_then(MiValue::text)),
        address: value
            .field("addr")
            .and_then(MiValue::text)
            .unwrap_or_default()
            .to_owned(),
        function: value
            .field("func")
            .and_then(MiValue::text)
            .map(str::to_owned),
        file: value
            .field("fullname")
            .or_else(|| value.field("file"))
            .and_then(MiValue::text)
            .map(str::to_owned),
        line: parse_u32(value.field("line").and_then(MiValue::text)),
    })
}

fn instruction_from_value(value: &MiValue) -> Option<Instruction> {
    Some(Instruction {
        address: value.field("address")?.text()?.to_owned(),
        function: value
            .field("func-name")
            .and_then(MiValue::text)
            .map(str::to_owned),
        offset: value
            .field("offset")
            .and_then(MiValue::text)
            .map(str::to_owned),
        instruction: value.field("inst")?.text()?.to_owned(),
    })
}

fn breakpoint_from_value(value: &MiValue) -> Option<Breakpoint> {
    Some(Breakpoint {
        number: value.field("number")?.text()?.to_owned(),
        enabled: value.field("enabled").and_then(MiValue::text) == Some("y"),
        address: value
            .field("addr")
            .and_then(MiValue::text)
            .map(str::to_owned),
        location: value
            .field("original-location")
            .or_else(|| value.field("what"))
            .and_then(MiValue::text)
            .map(str::to_owned),
    })
}

pub fn validate_config(config: &mut LabConfig) -> Result<()> {
    let workspace = std::fs::canonicalize(&config.workspace).context("workspace does not exist")?;
    if !workspace.is_dir() {
        bail!("workspace is not a directory");
    }
    let program = std::fs::canonicalize(&config.program).context("program does not exist")?;
    if !program.is_file() {
        bail!("program is not a file");
    }
    let debugger = resolve_executable(&config.debugger).context("debugger executable not found")?;
    let python = resolve_executable(&config.python).context("Python interpreter not found")?;
    config.workspace = workspace.to_string_lossy().into_owned();
    config.program = program.to_string_lossy().into_owned();
    config.debugger = debugger;
    config.python = python;
    Ok(())
}

fn resolve_executable(value: &str) -> Option<String> {
    if value.contains('/') {
        let path = std::path::PathBuf::from(value);
        let path = if path.is_absolute() {
            path
        } else {
            std::env::current_dir().ok()?.join(path)
        };
        // Preserve virtual-environment and wrapper symlinks: resolving them changes
        // Python's environment detection and can bypass pwndbg launch scripts.
        return path.is_file().then(|| path.to_string_lossy().into_owned());
    }
    std::env::var_os("PATH")?
        .to_string_lossy()
        .split(':')
        .map(|dir| std::path::Path::new(dir).join(value))
        .find(|path| path.is_file())
        .map(|path| path.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::strip_terminal_controls;

    #[test]
    fn terminal_transcript_removes_ansi_but_keeps_visible_output() {
        let raw = "\u{1b}[32mchecksec\u{1b}[0m\r\nRELRO: Full\u{1b}]0;pwndbg\u{7}\n";
        assert_eq!(strip_terminal_controls(raw), "checksec\nRELRO: Full\n");
    }
}
