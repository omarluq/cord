import cytoscape from "cytoscape";
import { basicSetup } from "codemirror";
import { EditorView } from "@codemirror/view";
import { EditorState } from "@codemirror/state";
import { go } from "@codemirror/lang-go";
import { oneDark } from "@codemirror/theme-one-dark";

let editor;
let graph;
let worker;

const nodeColors = {
  queued: "#4c566a",
  running: "#88c0d0",
  completed: "#a3be8c",
  failed: "#bf616a",
};

export function mountEditor(parent, source) {
  editor?.destroy();
  editor = new EditorView({
    parent,
    state: EditorState.create({
      doc: source,
      extensions: [
        basicSetup,
        go(),
        oneDark,
        EditorView.theme({
          "&": { height: "100%", background: "#2e3440" },
          ".cm-scroller": { overflow: "auto", fontFamily: "ui-monospace, monospace" },
        }),
      ],
    }),
  });
}

export function source() {
  return editor?.state.doc.toString() ?? "";
}

export function mountGraph(container) {
  graph?.destroy();
  graph = cytoscape({
    container,
    elements: [],
    layout: { name: "breadthfirst", directed: true, padding: 36, spacingFactor: 1.35 },
    style: [
      { selector: "node", style: {
        "background-color": "#4c566a", "border-color": "#607087", "border-width": 2,
        color: "#eceff4", label: "data(label)", "font-family": "ui-monospace, monospace",
        "font-size": 12, height: 44, width: 118, shape: "round-rectangle",
        "text-valign": "center", "text-halign": "center"
      }},
      { selector: "edge", style: {
        width: 2, "line-color": "#607087", "target-arrow-color": "#607087",
        "target-arrow-shape": "triangle", "curve-style": "bezier"
      }},
    ],
  });
}

export function setGraph(data) {
  if (!graph) return;
  const nodes = (data.nodes ?? []).map(node => ({ data: { id: node.id, label: node.label || node.id, state: "queued" } }));
  const edges = (data.edges ?? []).map((edge, index) => ({ data: { id: `edge-${index}`, source: edge.from, target: edge.to } }));
  graph.elements().remove();
  graph.add([...nodes, ...edges]);
  graph.layout({ name: "breadthfirst", directed: true, padding: 36, spacingFactor: 1.35, animate: false }).run();
}

export function setNodeState(id, state) {
  const node = graph?.getElementById(id);
  if (!node || node.empty()) return;
  node.data("state", state);
  node.style("background-color", nodeColors[state] ?? nodeColors.queued);
  node.style("border-color", nodeColors[state] ?? nodeColors.queued);
}

export function runWasm(bytes, wasmExecURL, onMessage, onExit) {
  stopWasm();
  const workerSource = `
    self.importScripts(${JSON.stringify(wasmExecURL)});
    self.onmessage = async event => {
      const go = new Go();
      try {
        const result = await WebAssembly.instantiate(event.data, go.importObject);
        await go.run(result.instance);
        self.postMessage({ type: "exit" });
      } catch (error) {
        self.postMessage({ type: "error", message: error?.stack || String(error) });
      }
    };
  `;
  const url = URL.createObjectURL(new Blob([workerSource], { type: "text/javascript" }));
  worker = new Worker(url);
  URL.revokeObjectURL(url);
  worker.onmessage = event => {
    if (event.data?.type === "exit") {
      onExit();
      return;
    }
    if (typeof event.data === "string") {
      const envelope = JSON.parse(event.data);
      onMessage({ type: envelope.type, ...envelope.payload });
      return;
    }
    onMessage(event.data);
  };
  worker.onerror = event => onMessage({ type: "error", message: event.message });
  worker.postMessage(bytes, [bytes]);
}

export function stopWasm() {
  worker?.terminate();
  worker = undefined;
}

window.CordPlayground = {
  mountEditor,
  source,
  mountGraph,
  setGraph,
  setNodeState,
  runWasm,
  stopWasm,
};
