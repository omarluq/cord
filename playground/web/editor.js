import cytoscape from "cytoscape";
import { basicSetup } from "codemirror";
import { indentWithTab } from "@codemirror/commands";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState } from "@codemirror/state";
import { go } from "@codemirror/lang-go";
import { oneDark } from "@codemirror/theme-one-dark";

let editor;
let graph;
let graphContainer;
let graphSignature = "";
let graphRenderCount = 0;
let worker;

const nodeColors = {
  queued: "#4c566a",
  running: "#88c0d0",
  completed: "#a3be8c",
  failed: "#bf616a",
};

const edgeColors = {
  queued: "#607087",
  running: "#81a1c1",
  completed: "#8fbcbb",
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
        keymap.of([indentWithTab]),
        go(),
        oneDark,
        EditorView.theme({
          "&": { height: "100%", background: "#2e3440" },
          ".cm-scroller": { overflow: "auto", fontFamily: "ui-monospace, monospace" },
          ".cm-foldGutter .cm-gutterElement": {
            alignItems: "center",
            display: "flex",
            justifyContent: "center",
            padding: "0 3px",
          },
          ".cm-foldGutter span": {
            display: "block",
            fontSize: "0",
            height: "100%",
            position: "relative",
            width: "12px",
          },
          ".cm-foldGutter span::before": {
            content: "''",
            left: "50%",
            position: "absolute",
            top: "50%",
            transform: "translate(-50%, -50%)",
          },
          ".cm-foldGutter span[title='Fold line']::before": {
            borderLeft: "4px solid transparent",
            borderRight: "4px solid transparent",
            borderTop: "5px solid currentColor",
          },
          ".cm-foldGutter span[title='Unfold line']::before": {
            borderBottom: "4px solid transparent",
            borderLeft: "5px solid currentColor",
            borderTop: "4px solid transparent",
          }
        }),
      ],
    }),
  });
}

export function source() {
  return editor?.state.doc.toString() ?? "";
}

export function setSource(source) {
  if (!editor) return;
  editor.dispatch({ changes: { from: 0, to: editor.state.doc.length, insert: source } });
  editor.focus();
}

export function mountGraph(container) {
  graph?.destroy();
  graphContainer = container;
  graphSignature = "";
  graphRenderCount = 0;
  graph = cytoscape({
    container,
    elements: [],
    maxZoom: 2,
    minZoom: 0.25,
    layout: { name: "breadthfirst", directed: true, fit: false, padding: 36, spacingFactor: 1.35 },
    style: [
      { selector: "node", style: {
        "background-color": "#4c566a", "border-color": "#607087", "border-width": 2,
        color: "#eceff4", label: "data(label)", "font-family": "ui-monospace, monospace",
        // Cytoscape adds padding to both dimensions. Keep the outer node height
        // at 36px while allowing label-sized nodes 14px of horizontal breathing room.
        "font-size": 11, height: 8, width: "label", padding: 14, shape: "round-rectangle",
        "text-valign": "center", "text-halign": "center", "text-wrap": "none",
        "transition-property": "background-color, border-color, border-width",
        "transition-duration": "240ms"
      }},
      { selector: "node.running", style: {
        "border-color": "#88c0d0", "border-width": 4
      }},
      { selector: "node.completed", style: {
        "border-color": "#a3be8c", "border-width": 3
      }},
      { selector: "node.failed", style: {
        "border-color": "#bf616a", "border-width": 4
      }},
      { selector: "node.queued", style: {
        "background-color": "#4c566a", "border-color": "#607087", "border-width": 2
      }},
      { selector: "edge", style: {
        width: 2, "line-color": "#607087", "target-arrow-color": "#607087",
        "target-arrow-shape": "triangle", "curve-style": "bezier",
        "transition-property": "line-color, target-arrow-color, width",
        "transition-duration": "240ms"
      }},
    ],
  });
}

export function setGraph(data) {
  if (!graph) return;
  const signature = JSON.stringify({ nodes: data.nodes ?? [], edges: data.edges ?? [] });
  if (signature === graphSignature) return;

  const nodes = (data.nodes ?? []).map(node => ({
    data: { id: node.id, label: node.label || node.id, state: "queued" },
    classes: "queued",
  }));
  const edges = (data.edges ?? []).map((edge, index) => ({ data: { id: `edge-${index}`, source: edge.from, target: edge.to } }));
  graph.elements().remove();
  graph.add([...nodes, ...edges]);
  graphSignature = signature;
  graphRenderCount++;
  graphContainer.dataset.elements = String(nodes.length + edges.length);
  graphContainer.dataset.renderCount = String(graphRenderCount);
  graph.resize();
  graph.zoom(1);
  graph.layout({
    name: "breadthfirst",
    animate: false,
    directed: true,
    fit: false,
    padding: 36,
    spacingFactor: 1.35,
  }).run();
  graph.center();
}

export function zoomGraph(direction) {
  if (!graph) return;
  const nextZoom = Math.min(graph.maxZoom(), Math.max(graph.minZoom(), graph.zoom() + direction * 0.2));
  graph.zoom({ level: nextZoom, renderedPosition: { x: graph.width() / 2, y: graph.height() / 2 } });
}

export function setNodeState(id, state) {
  const node = graph?.getElementById(id);
  if (!node || node.empty()) return;
  applyNodeState(node, state);
  updateStateMarker();
}

export function setGraphState(state) {
  if (!graph) return;
  graph.nodes().forEach(node => applyNodeState(node, state));
  graph.edges().forEach(edge => {
    const color = edgeColors[state] ?? edgeColors.queued;
    edge.style("line-color", color);
    edge.style("target-arrow-color", color);
    edge.style("width", state === "running" ? 3 : 2);
  });
  updateStateMarker();
}

function applyNodeState(node, state) {
  const nextState = nodeColors[state] ? state : "queued";
  node.removeClass("queued running completed failed");
  node.addClass(nextState);
  node.data("state", nextState);
}

function updateStateMarker() {
  if (!graphContainer || !graph) return;
  graphContainer.dataset.nodeStates = graph.nodes().map(node => node.data("state")).join(",");
}

export function runWasm(bytes, wasmExecURL, onMessage, onExit) {
  stopWasm();
  const workerSource = `
    self.onerror = event => {
      if (String(event.message).includes("Go program has already exited")) {
        event.preventDefault();
        return true;
      }
      return false;
    };
    self.importScripts(${JSON.stringify(wasmExecURL)});
    const sendOutput = (type, values) => {
      const value = values.join(" ");
      const node = value.match(/^__CORD_NODE__:([^:]+):(running|completed|failed)$/);
      if (node) {
        self.postMessage({ type: "node", id: node[1], state: node[2] });
        return;
      }
      self.postMessage(type === "error" ? { type, message: value } : { type, value });
    };
    console.log = (...values) => sendOutput("output", values);
    console.error = (...values) => sendOutput("error", values);
    self.onmessage = async event => {
      const go = new Go();
      try {
        const result = await WebAssembly.instantiate(event.data, go.importObject);
        await go.run(result.instance);
        for (const timeout of go._scheduledTimeouts.values()) clearTimeout(timeout);
        go._scheduledTimeouts.clear();
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
  worker.onerror = event => {
    if (String(event.message).includes("Go program has already exited")) return;
    onMessage({ type: "error", message: event.message });
  };
  worker.postMessage(bytes, [bytes]);
}

export function stopWasm() {
  worker?.terminate();
  worker = undefined;
}

export function graphNodeStyles() {
  if (!graph) return [];
  return graph.nodes().map(node => ({
    state: node.data("state"),
    backgroundColor: node.style("background-color"),
    borderColor: node.style("border-color"),
    textColor: node.style("color"),
    width: node.renderedWidth(),
    height: node.renderedHeight(),
  }));
}

window.CordPlayground = {
  mountEditor,
  source,
  setSource,
  mountGraph,
  setGraph,
  zoomGraph,
  setGraphState,
  setNodeState,
  runWasm,
  stopWasm,
  graphNodeStyles,
};
