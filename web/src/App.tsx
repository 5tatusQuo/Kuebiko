import { For, Show, createMemo, createSignal, onCleanup, onMount } from "solid-js";
import { connect, type TutorSocket } from "./socket";
import { TerminalPane } from "./TerminalPane";
import { SafeMarkdown } from "./SafeMarkdown";
import type { AuthState, DebuggerState, FullState, KernelStatus, LabConfig, LabRecord, LabStatus, ServerMessage } from "./types";

const emptyDebugger: DebuggerState = { revision: 0, state: "idle", stale: false, frames: [], registers: [], disassembly: [], breakpoints: [] };
const sampleRoot = "/home/quo/pwncollege/buffer_overflow_example";

interface Cell { id: string; code: string; status: string; count?: number; outputs: { kind: string; text?: string; image?: string }[] }
interface Chat { role: "user" | "assistant" | "notice"; text: string; streaming?: boolean }
interface TurnMetrics { purpose: string; contextBytes: number; acknowledgementMs?: number; firstResponseMs?: number; totalMs: number }

export default function App() {
  const [connection, setConnection] = createSignal("connecting");
  const [lab, setLab] = createSignal<LabStatus>({ phase: "idle" });
  const [debuggerState, setDebuggerState] = createSignal<DebuggerState>(emptyDebugger);
  const [kernel, setKernel] = createSignal<KernelStatus>({ state: "idle" });
  const [auth, setAuth] = createSignal<AuthState>({ authenticated: false });
  const [recent, setRecent] = createSignal<LabRecord[]>([]);
  const [cells, setCells] = createSignal<Cell[]>([]);
  const [chat, setChat] = createSignal<Chat[]>([]);
  const [codexActivity, setCodexActivity] = createSignal("ready");
  const [turnMetrics, setTurnMetrics] = createSignal<TurnMetrics>();
  const [error, setError] = createSignal("");
  const [inspector, setInspector] = createSignal("registers");
  let socket!: TutorSocket;
  let terminalWriter: (data: Uint8Array) => void = () => undefined;

  onMount(() => { socket = connect(handleMessage, (data) => terminalWriter(data), setConnection); });
  onCleanup(() => socket?.close());

  function handleMessage(message: ServerMessage) {
    const p = message.payload;
    switch (message.type) {
      case "stateFull": {
        const state = p as FullState; setLab(state.lab); setDebuggerState(state.debugger); setKernel(state.kernel); setAuth(state.auth); setRecent(state.recentLabs); break;
      }
      case "labStatus": setLab(p); break;
      case "debuggerState": setDebuggerState(p); break;
      case "authState": setAuth(p); break;
      case "recentLabs": setRecent(p.labs); break;
      case "kernelEvent": handleKernel(p); break;
      case "codexEvent": handleCodex(p); break;
      case "error": setError(`${p.scope}: ${p.message}`); break;
    }
  }

  function updateCell(id: string | undefined, update: (cell: Cell) => Cell) {
    if (!id) return; setCells((items) => items.map((cell) => cell.id === id ? update(cell) : cell));
  }
  function handleKernel(event: any) {
    if (event.kind === "status") setKernel((old) => ({ ...old, state: event.state }));
    if (event.kind === "stream") updateCell(event.cellId, (c) => ({ ...c, outputs: [...c.outputs, { kind: event.name, text: event.text }] }));
    if (event.kind === "result") updateCell(event.cellId, (c) => ({ ...c, count: event.executionCount ?? c.count, outputs: [...c.outputs, ...(event.text ? [{ kind: "result", text: event.text }] : []), ...(event.imagePng ? [{ kind: "image", image: event.imagePng }] : [])] }));
    if (event.kind === "error") updateCell(event.cellId, (c) => ({ ...c, status: "error", outputs: [...c.outputs, { kind: "error", text: event.traceback.join("\n") || `${event.name}: ${event.value}` }] }));
    if (event.kind === "completed") updateCell(event.cellId, (c) => ({ ...c, status: event.status, count: event.executionCount }));
    if (event.kind === "inputRequest") {
      const value = window.prompt(event.prompt) ?? ""; socket.send("kernelInputReply", { requestId: event.requestId, value });
    }
    if (event.kind === "restarted") { setCells([]); setKernel({ state: "starting" }); }
  }
  function handleCodex(event: any) {
    if (event.kind === "user") setChat((items) => [...items, { role: "user", text: event.text }]);
    if (event.kind === "delta") setChat((items) => {
      const copy = [...items]; const last = copy.at(-1);
      if (last?.role === "assistant" && last.streaming) copy[copy.length - 1] = { ...last, text: last.text + event.text };
      else copy.push({ role: "assistant", text: event.text, streaming: true });
      return copy;
    });
    if (event.kind === "completed") setChat((items) => items.map((item, index) => index === items.length - 1 ? { ...item, streaming: false } : item));
    if (event.kind === "notice") setChat((items) => [...items, { role: "notice", text: event.text }]);
    if (event.kind === "activity") setCodexActivity(event.detail ? `${event.state}: ${event.detail}` : event.state);
    if (event.kind === "turnMetrics") setTurnMetrics(event);
    if (event.kind === "writeupStarted") setChat((items) => [...items, { role: "notice", text: "Generating a Markdown writeup from the saved lab journal…" }]);
    if (event.kind === "writeupSaved") setChat((items) => [...items, { role: "notice", text: `Writeup saved to ${event.path}` }]);
    if (event.kind === "loginUrl") window.open(event.url, "_blank", "noopener,noreferrer");
  }

  const active = createMemo(() => lab().phase === "running");
  return <div class="app-shell">
    <header>
      <div class="brand"><img src="/kuebiko-logo.png" alt="" /><div><h1>Kuebiko</h1><small>binary exploitation workbench</small></div></div>
      <div class="statuses"><Status label="server" value={connection()} /><Status label="gdb" value={debuggerState().state} /><Status label="kernel" value={kernel().state} /><Status label="codex" value={auth().authenticated ? codexActivity() : "signed out"} /></div>
      <Show when={active()}><button class="danger" onClick={() => socket.send("labStop")}>Stop lab</button></Show>
    </header>
    <Show when={error()}><div class="error-banner"><span>{error()}</span><button onClick={() => setError("")}>×</button></div></Show>
    <Show when={active()} fallback={<Launch recent={recent()} onStart={(config) => socket.send("labStart", config)} onResume={(id) => socket.send("labResume", { labId: id })} />}>
      <main class="workbench">
        <section class="tools-column">
          <Panel title="pwndbg" badge={debuggerState().stopReason}>
            <TerminalPane onInput={(data) => socket.terminal(data)} onResize={(cols, rows) => socket.send("terminalResize", { cols, rows })} registerWriter={(writer) => terminalWriter = writer} />
          </Panel>
          <KernelPane cells={cells()} state={kernel().state} onExecute={(code) => {
            const id = crypto.randomUUID(); setCells((old) => [...old, { id, code, status: "queued", outputs: [] }]); socket.send("kernelExecute", { cellId: id, code });
          }} onInterrupt={() => socket.send("kernelInterrupt")} onRestart={() => socket.send("kernelRestart")} />
        </section>
        <Inspector state={debuggerState()} tab={inspector()} setTab={setInspector} refresh={() => socket.send("debuggerRefresh")} />
        <ChatPane chat={chat()} auth={auth()} metrics={turnMetrics()} send={(text) => socket.send("codexSend", { text })} generateWriteup={() => socket.send("generateWriteup")} interrupt={() => socket.send("codexInterrupt")} newThread={() => { setChat([]); setTurnMetrics(undefined); socket.send("codexNewThread"); }} login={() => socket.send("authLogin")} />
      </main>
    </Show>
  </div>;
}

function Status(props: { label: string; value: string }) { return <div class="status"><i class={props.value === "dead" || props.value.includes("error") ? "bad" : ""} /><span>{props.label}</span><b>{props.value}</b></div>; }
function Panel(props: { title: string; badge?: string; children: any }) { return <section class="panel"><div class="panel-title"><h2>{props.title}</h2><Show when={props.badge}><span class="badge">{props.badge}</span></Show></div>{props.children}</section>; }

function Launch(props: { recent: LabRecord[]; onStart: (config: LabConfig) => void; onResume: (id: string) => void }) {
  const [workspace, setWorkspace] = createSignal(sampleRoot); const [program, setProgram] = createSignal(`${sampleRoot}/buffer_overflow`);
  const [args, setArgs] = createSignal("AAAA"); const [debuggerPath, setDebuggerPath] = createSignal("pwndbg");
  const [python, setPython] = createSignal("/home/quo/projects/dbg-tutor/.venv/bin/python");
  const [objective, setObjective] = createSignal("Learn to analyze and exploit this buffer overflow step by step.");
  return <main class="launch"><section class="launch-card"><p class="eyebrow">NEW LOCAL LAB</p><h2>Open a debugging workspace</h2><p>Processes remain local. Codex receives structured state and a bounded, ANSI-stripped terminal tail only when you send a message.</p>
    <form onSubmit={(event) => { event.preventDefault(); props.onStart({ workspace: workspace(), program: program(), args: args().split("\n").filter(Boolean), debugger: debuggerPath(), python: python(), objective: objective() }); }}>
      <label>Workspace<input value={workspace()} onInput={(e) => setWorkspace(e.currentTarget.value)} /></label>
      <label>Executable<input value={program()} onInput={(e) => setProgram(e.currentTarget.value)} /></label>
      <label>Arguments <small>one per line</small><textarea rows="2" value={args()} onInput={(e) => setArgs(e.currentTarget.value)} /></label>
      <div class="form-row"><label>Debugger<input value={debuggerPath()} onInput={(e) => setDebuggerPath(e.currentTarget.value)} /></label><label>Python interpreter<input value={python()} onInput={(e) => setPython(e.currentTarget.value)} /></label></div>
      <label>Learning objective<textarea rows="3" value={objective()} onInput={(e) => setObjective(e.currentTarget.value)} /></label>
      <button class="primary" type="submit">Start lab <span>→</span></button>
    </form>
  </section><Show when={props.recent.length}><section class="recent"><p class="eyebrow">RECENT LABS</p><For each={props.recent}>{(record) => <button onClick={() => props.onResume(record.id)}><b>{record.config.program.split("/").at(-1)}</b><span>{record.config.objective}</span><small>{record.threadId ? "Codex thread saved" : "No chat yet"}</small></button>}</For></section></Show></main>;
}

function KernelPane(props: { cells: Cell[]; state: string; onExecute: (code: string) => void; onInterrupt: () => void; onRestart: () => void }) {
  const [code, setCode] = createSignal("from pwn import *\n");
  return <Panel title="IPython"><div class="kernel-toolbar"><span>{props.state}</span><button onClick={props.onInterrupt}>Interrupt</button><button onClick={props.onRestart}>Restart</button></div><div class="cells"><For each={props.cells}>{(cell) => <article class="cell"><div class="cell-input"><span>In [{cell.count ?? " "}]</span><pre>{cell.code}</pre></div><For each={cell.outputs}>{(output) => <div class={`cell-output ${output.kind}`}><Show when={output.image} fallback={<pre>{output.text}</pre>}><img alt="IPython output" src={`data:image/png;base64,${output.image}`} /></Show></div>}</For></article>}</For></div>
    <div class="composer kernel-composer"><textarea value={code()} onInput={(e) => setCode(e.currentTarget.value)} onKeyDown={(e) => { if (e.shiftKey && e.key === "Enter") { e.preventDefault(); props.onExecute(code()); setCode(""); } }} placeholder="Python code — Shift+Enter to run" /><button class="primary" disabled={!code().trim() || props.state === "busy"} onClick={() => { props.onExecute(code()); setCode(""); }}>Run</button></div></Panel>;
}

function Inspector(props: { state: DebuggerState; tab: string; setTab: (tab: string) => void; refresh: () => void }) {
  const tabs = ["registers", "frames", "disassembly", "breakpoints", "stack"];
  return <section class={`panel inspector ${props.state.stale ? "stale" : ""}`}><div class="panel-title"><h2>Debugger state</h2><button onClick={props.refresh}>↻</button></div><div class="stop-summary"><b>{props.state.state}</b><span>{props.state.stopReason ?? props.state.frame?.function ?? "waiting for inferior"}</span></div><nav class="tabs"><For each={tabs}>{(tab) => <button classList={{ active: props.tab === tab }} onClick={() => props.setTab(tab)}>{tab}</button>}</For></nav><div class="inspector-content">
    <Show when={props.tab === "registers"}><dl class="registers"><For each={props.state.registers}>{(reg) => <><dt>{reg.name}</dt><dd>{reg.value}</dd></>}</For></dl></Show>
    <Show when={props.tab === "frames"}><For each={props.state.frames}>{(frame) => <div class="row"><b>#{frame.level}</b><code>{frame.address}</code><span>{frame.function ?? "?"}{frame.line ? `:${frame.line}` : ""}</span></div>}</For></Show>
    <Show when={props.tab === "disassembly"}><For each={props.state.disassembly}>{(ins) => <div class="asm"><code>{ins.address}</code><span>{ins.instruction}</span></div>}</For></Show>
    <Show when={props.tab === "breakpoints"}><For each={props.state.breakpoints}>{(bp) => <div class="row"><b>#{bp.number}</b><span>{bp.enabled ? "on" : "off"}</span><code>{bp.location ?? bp.address}</code></div>}</For></Show>
    <Show when={props.tab === "stack"}><Memory address={props.state.stack?.address} bytes={props.state.stack?.bytes} /></Show>
    <Show when={props.state.error}><p class="inline-error">{props.state.error}</p></Show>
  </div></section>;
}
function Memory(props: { address?: string; bytes?: string }) {
  const rows = createMemo(() => (props.bytes?.match(/.{1,32}/g) ?? []).map((hex, index) => ({ address: props.address ? `0x${(BigInt(props.address) + BigInt(index * 16)).toString(16)}` : "", hex: hex.match(/../g)?.join(" ") })));
  return <For each={rows()}>{(row) => <div class="memory"><code>{row.address}</code><span>{row.hex}</span></div>}</For>;
}

function ChatPane(props: { chat: Chat[]; auth: AuthState; metrics?: TurnMetrics; send: (text: string) => void; generateWriteup: () => void; interrupt: () => void; newThread: () => void; login: () => void }) {
  const [text, setText] = createSignal("");
  return <section class="panel chat-panel"><div class="panel-title"><h2>Codex tutor</h2><div><button disabled={!props.auth.authenticated} title="Generate a Markdown writeup from this lab's journal" onClick={props.generateWriteup}>Writeup</button><button onClick={props.newThread}>New chat</button><button onClick={props.interrupt}>Stop</button></div></div><Show when={props.metrics}>{(metrics) => <div class="latency-bar">Last turn: {formatMs(metrics().firstResponseMs)} to first response · {formatMs(metrics().totalMs)} total · {Math.ceil(metrics().contextBytes / 1024)} KiB context</div>}</Show><Show when={props.auth.authenticated} fallback={<div class="login"><p>Sign in with your ChatGPT account to use Codex tutoring.</p><button class="primary" onClick={props.login}>Sign in</button></div>}><div class="messages"><Show when={!props.chat.length}><div class="empty-chat"><span>◎</span><p>Ask about what you see, request one hint, or generate a writeup from the recorded lab journal.</p></div></Show><For each={props.chat}>{(item) => <article class={`message ${item.role}`}><b>{item.role === "assistant" ? "Tutor" : item.role === "user" ? "You" : "System"}</b><SafeMarkdown text={item.text} /></article>}</For></div><div class="composer chat-composer"><textarea value={text()} onInput={(e) => setText(e.currentTarget.value)} onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); if (text().trim()) { props.send(text()); setText(""); } } }} placeholder="Ask for one hint…" /><button class="primary" disabled={!text().trim()} onClick={() => { props.send(text()); setText(""); }}>Send</button></div></Show></section>;
}

function formatMs(value?: number): string {
  if (value === undefined) return "—";
  return value < 1000 ? `${value} ms` : `${(value / 1000).toFixed(1)} s`;
}
