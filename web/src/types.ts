export interface LabConfig {
  workspace: string;
  program: string;
  args: string[];
  debugger: string;
  python: string;
  objective: string;
}

export interface LabRecord { id: string; config: LabConfig; threadId?: string; updatedAt: number }
export interface Frame { level?: number; address: string; function?: string; file?: string; line?: number }
export interface Register { name: string; value: string }
export interface Instruction { address: string; function?: string; offset?: string; instruction: string }
export interface Breakpoint { number: string; enabled: boolean; address?: string; location?: string }
export interface DebuggerState {
  revision: number; state: string; stopReason?: string; stale: boolean; frame?: Frame;
  frames: Frame[]; registers: Register[]; disassembly: Instruction[];
  breakpoints: Breakpoint[]; stack?: { address: string; bytes: string }; error?: string;
}
export interface KernelStatus { state: string; executionCount?: number }
export interface AuthState { authenticated: boolean; mode?: string; plan?: string }
export interface LabStatus { phase: string; labId?: string; config?: LabConfig; message?: string }
export interface FullState {
  lab: LabStatus; debugger: DebuggerState; kernel: KernelStatus; auth: AuthState; recentLabs: LabRecord[];
}

export type ServerMessage = { v: 1; type: string; payload: any };

export function command(type: string, payload?: unknown): string {
  return JSON.stringify(payload === undefined ? { type } : { type, payload });
}

