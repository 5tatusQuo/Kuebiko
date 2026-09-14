import { onCleanup, onMount } from "solid-js";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";

export function TerminalPane(props: {
  onInput: (data: Uint8Array) => void;
  onResize: (cols: number, rows: number) => void;
  registerWriter: (writer: (data: Uint8Array) => void) => void;
}) {
  let host!: HTMLDivElement;
  let terminal: Terminal | undefined;
  let observer: ResizeObserver | undefined;

  onMount(() => {
    terminal = new Terminal({
      cursorBlink: true,
      convertEol: false,
      fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
      fontSize: 13,
      scrollback: 10000,
      theme: { background: "#090d12", foreground: "#d7e0ea", cursor: "#62d7b6", selectionBackground: "#2a5260" },
    });
    const fit = new FitAddon(); terminal.loadAddon(fit); terminal.open(host); fit.fit();
    terminal.onData((value) => props.onInput(new TextEncoder().encode(value)));
    props.registerWriter((data) => terminal?.write(data));
    observer = new ResizeObserver(() => {
      try { fit.fit(); props.onResize(terminal!.cols, terminal!.rows); } catch { /* hidden pane */ }
    });
    observer.observe(host);
    import("@xterm/addon-webgl").then(({ WebglAddon }) => {
      if (!terminal) return;
      try { const webgl = new WebglAddon(); webgl.onContextLoss(() => webgl.dispose()); terminal.loadAddon(webgl); } catch { /* canvas fallback */ }
    });
    terminal.focus();
  });
  onCleanup(() => { observer?.disconnect(); terminal?.dispose(); terminal = undefined; });
  return <div class="terminal-host" ref={host} aria-label="pwndbg terminal" />;
}

