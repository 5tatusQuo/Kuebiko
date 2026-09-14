import { command, type ServerMessage } from "./types";

export interface TutorSocket {
  send(type: string, payload?: unknown): void;
  terminal(data: Uint8Array): void;
  close(): void;
}

export function connect(
  onMessage: (message: ServerMessage) => void,
  onTerminal: (data: Uint8Array) => void,
  onStatus: (status: string) => void,
): TutorSocket {
  const locationUrl = new URL(window.location.href);
  const supplied = locationUrl.searchParams.get("token");
  if (supplied) {
    sessionStorage.setItem("kuebiko-token", supplied);
    locationUrl.searchParams.delete("token");
    history.replaceState(null, "", locationUrl.pathname + locationUrl.search + locationUrl.hash);
  }
  const token = supplied ?? sessionStorage.getItem("kuebiko-token") ?? "";
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  let socket: WebSocket;
  let retry: number | undefined;
  let closed = false;

  const open = () => {
    onStatus("connecting");
    socket = new WebSocket(`${protocol}//${window.location.host}/ws?token=${encodeURIComponent(token)}`);
    socket.binaryType = "arraybuffer";
    socket.onopen = () => onStatus("connected");
    socket.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        const bytes = new Uint8Array(event.data);
        if (bytes[0] === 1) onTerminal(bytes.subarray(1));
      } else {
        try { onMessage(JSON.parse(event.data) as ServerMessage); }
        catch { onStatus("invalid server message"); }
      }
    };
    socket.onclose = () => {
      if (closed) return;
      onStatus("reconnecting");
      retry = window.setTimeout(open, 1000);
    };
    socket.onerror = () => onStatus("connection error");
  };
  open();

  return {
    send(type, payload) { if (socket?.readyState === WebSocket.OPEN) socket.send(command(type, payload)); },
    terminal(data) {
      if (socket?.readyState !== WebSocket.OPEN) return;
      const frame = new Uint8Array(data.length + 1); frame[0] = 1; frame.set(data, 1); socket.send(frame);
    },
    close() { closed = true; if (retry !== undefined) clearTimeout(retry); socket?.close(); },
  };
}
