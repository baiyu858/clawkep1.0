import { describe, expect, it } from "vitest";
import { resolveLatestRekepLogPanel } from "./chat.ts";

describe("resolveLatestRekepLogPanel", () => {
  it("extracts the latest rekep_sim output and metadata", () => {
    const panel = resolveLatestRekepLogPanel([
      {
        role: "assistant",
        toolCallId: "tool-1",
        content: [
          {
            type: "toolcall",
            name: "other_tool",
            arguments: {},
          },
          {
            type: "toolresult",
            name: "other_tool",
            text: "ignored",
          },
        ],
      },
      {
        role: "assistant",
        toolCallId: "tool-2",
        content: [
          {
            type: "toolcall",
            name: "rekep_sim",
            arguments: { action: "run", task: "pen" },
          },
          {
            type: "toolresult",
            name: "rekep_sim",
            text: "ReKep run running...\n[bridge] run_log_path: /tmp/rekep.log\nstage 1 ready",
          },
        ],
      },
    ]);

    expect(panel).toMatchObject({
      title: "ReKep Live Log · pen",
      action: "run",
      task: "pen",
      running: true,
      logPath: "/tmp/rekep.log",
    });
    expect(panel?.text).toContain("stage 1 ready");
  });

  it("returns null when there is no rekep_sim tool output", () => {
    expect(
      resolveLatestRekepLogPanel([
        {
          role: "assistant",
          content: [{ type: "toolcall", name: "exec", arguments: { command: "pwd" } }],
        },
      ]),
    ).toBeNull();
  });
});
