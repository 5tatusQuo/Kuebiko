import { For } from "solid-js";

type Block = { kind: "code" | "text"; text: string; language?: string };

function blocks(source: string): Block[] {
  const result: Block[] = [];
  const pattern = /```([^\n]*)\n([\s\S]*?)```/g;
  let start = 0;
  for (const match of source.matchAll(pattern)) {
    if (match.index! > start) result.push({ kind: "text", text: source.slice(start, match.index) });
    result.push({ kind: "code", language: match[1].trim(), text: match[2] });
    start = match.index! + match[0].length;
  }
  if (start < source.length) result.push({ kind: "text", text: source.slice(start) });
  return result;
}

export function SafeMarkdown(props: { text: string }) {
  return <div class="markdown"><For each={blocks(props.text)}>{(block) =>
    block.kind === "code" ? <pre data-language={block.language}><code>{block.text}</code></pre> : <div class="prose">{block.text}</div>
  }</For></div>;
}

