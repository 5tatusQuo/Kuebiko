use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u8 = 1;
pub const GDB_PTY_CHANNEL: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LabConfig {
    pub workspace: String,
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub debugger: String,
    pub python: String,
    pub objective: String,
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    content = "payload",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ClientMessage {
    LabStart(LabConfig),
    LabResume { lab_id: String },
    LabStop,
    TerminalResize { cols: u16, rows: u16 },
    DebuggerRefresh,
    KernelExecute { cell_id: String, code: String },
    KernelInterrupt,
    KernelRestart,
    KernelInputReply { request_id: String, value: String },
    CodexSend { text: String },
    GenerateWriteup,
    CodexInterrupt,
    CodexNewThread,
    AuthLogin,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "camelCase")]
#[allow(clippy::large_enum_variant)]
pub enum ServerMessage {
    StateFull(FullState),
    LabStatus(LabStatus),
    DebuggerState(DebuggerSnapshot),
    KernelEvent(KernelEvent),
    CodexEvent(CodexEvent),
    AuthState(AuthState),
    Error { scope: String, message: String },
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullState {
    pub lab: LabStatus,
    pub debugger: DebuggerSnapshot,
    pub kernel: KernelStatus,
    pub auth: AuthState,
    pub recent_labs: Vec<LabRecord>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabStatus {
    pub phase: String,
    pub lab_id: Option<String>,
    pub config: Option<LabConfig>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DebuggerSnapshot {
    pub revision: u64,
    pub state: String,
    pub stop_reason: Option<String>,
    pub stale: bool,
    pub frame: Option<Frame>,
    pub frames: Vec<Frame>,
    pub registers: Vec<Register>,
    pub disassembly: Vec<Instruction>,
    pub breakpoints: Vec<Breakpoint>,
    pub stack: Option<MemoryBlock>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Frame {
    pub level: Option<u32>,
    pub address: String,
    pub function: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Register {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    pub address: String,
    pub function: Option<String>,
    pub offset: Option<String>,
    pub instruction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Breakpoint {
    pub number: String,
    pub enabled: bool,
    pub address: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBlock {
    pub address: String,
    pub bytes: String,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelStatus {
    pub state: String,
    pub execution_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum KernelEvent {
    Status {
        state: String,
    },
    Started {
        cell_id: String,
        request_id: String,
    },
    Stream {
        cell_id: Option<String>,
        name: String,
        text: String,
    },
    Result {
        cell_id: Option<String>,
        execution_count: Option<u32>,
        text: Option<String>,
        image_png: Option<String>,
    },
    Error {
        cell_id: Option<String>,
        name: String,
        value: String,
        traceback: Vec<String>,
    },
    InputRequest {
        request_id: String,
        prompt: String,
        password: bool,
    },
    Completed {
        cell_id: Option<String>,
        execution_count: Option<u32>,
        status: String,
    },
    Restarted,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CodexEvent {
    User {
        text: String,
    },
    Delta {
        text: String,
    },
    Completed {
        status: String,
    },
    Thread {
        thread_id: String,
    },
    LoginUrl {
        url: String,
    },
    Notice {
        text: String,
    },
    WriteupStarted,
    WriteupDelta {
        text: String,
    },
    WriteupCompleted {
        status: String,
    },
    WriteupSaved {
        path: String,
    },
    Activity {
        state: String,
        detail: Option<String>,
    },
    TurnMetrics {
        purpose: String,
        context_bytes: usize,
        acknowledgement_ms: Option<u64>,
        first_response_ms: Option<u64>,
        total_ms: u64,
    },
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthState {
    pub authenticated: bool,
    pub mode: Option<String>,
    pub plan: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabRecord {
    pub id: String,
    pub config: LabConfig,
    pub thread_id: Option<String>,
    pub updated_at: u64,
}

pub fn envelope(message: &ServerMessage) -> Value {
    let mut value = serde_json::to_value(message).expect("serializable server message");
    value
        .as_object_mut()
        .expect("tagged message object")
        .insert("v".into(), PROTOCOL_VERSION.into());
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_envelope_has_version_and_tag() {
        let value = envelope(&ServerMessage::Error {
            scope: "test".into(),
            message: "nope".into(),
        });
        assert_eq!(value["v"], 1);
        assert_eq!(value["type"], "error");
    }

    #[test]
    fn parses_tagged_client_message() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type":"terminalResize","payload":{"cols":80,"rows":24}}"#)
                .unwrap();
        assert!(matches!(
            msg,
            ClientMessage::TerminalResize { cols: 80, rows: 24 }
        ));
    }

    #[test]
    fn parses_camel_case_variant_fields() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type":"kernelExecute","payload":{"cellId":"cell-1","code":"print(1)"}}"#,
        )
        .unwrap();
        assert!(matches!(
            msg,
            ClientMessage::KernelExecute { cell_id, code }
                if cell_id == "cell-1" && code == "print(1)"
        ));
    }

    #[test]
    fn parses_generate_writeup_command() {
        let msg: ClientMessage = serde_json::from_str(r#"{"type":"generateWriteup"}"#).unwrap();
        assert!(matches!(msg, ClientMessage::GenerateWriteup));
    }
}
