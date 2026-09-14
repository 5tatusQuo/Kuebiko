import { describe, expect, it } from "vitest";
import { command } from "./types";

describe("command", () => {
  it("builds versionless client envelopes with camel-case payloads", () => {
    expect(command("kernelExecute", { cellId: "one", code: "print(1)" })).toBe(
      '{"type":"kernelExecute","payload":{"cellId":"one","code":"print(1)"}}',
    );
  });

  it("omits payload for signal commands", () => {
    expect(command("kernelInterrupt")).toBe('{"type":"kernelInterrupt"}');
  });
});
