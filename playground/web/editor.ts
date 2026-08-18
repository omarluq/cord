import cytoscape, {
  type Core,
  type NodeSingular,
  type StylesheetJson,
} from "cytoscape";
import { basicSetup } from "codemirror";
import { indentWithTab } from "@codemirror/commands";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState } from "@codemirror/state";
import { go } from "@codemirror/lang-go";
import { oneDark } from "@codemirror/theme-one-dark";
import type { NodeState, WorkerMessage } from "./messages";

interface GraphNode {
  id: string;
  label?: string;
}

interface GraphEdge {
  from: string;
  to: string;
}

interface GraphData {
  nodes?: GraphNode[];
  edges?: GraphEdge[];
}

type GraphState = "queued" | NodeState;

interface GraphNodeStyle {
  state: GraphState;
  backgroundColor: string;
  borderColor: string;
  textColor: string;
  width: number;
  height: number;
}

interface CordPlaygroundAPI {
  mountEditor(parent: HTMLElement, source: string): void;
  source(): string;
  setSource(source: string): void;
  mountGraph(container: HTMLElement): void;
  setGraph(data: GraphData): void;
  zoomGraph(direction: number): void;
  setGraphState(state: GraphState): void;
  setNodeState(id: string, state: NodeState): void;
  runWasm(
    bytes: ArrayBuffer,
    wasmExecURL: string,
    onMessage: (message: WorkerMessage) => void,
    onExit: () => void,
  ): void;
  stopWasm(): void;
  graphNodeStyles(): GraphNodeStyle[];
}

declare global {
  interface Window {
    CordPlayground: CordPlaygroundAPI;
  }
}

let editor: EditorView | undefined;
let graph: Core | undefined;
let graphContainer: HTMLElement | undefined;
let graphSignature = "";
let graphRenderCount = 0;
let worker: Worker | undefined;

const nodeStates: readonly GraphState[] = [
  "queued",
  "running",
  "completed",
  "failed",
];

const edgeColors: Record<GraphState, string> = {
  queued: "#607087",
  running: "#81a1c1",
  completed: "#8fbcbb",
  failed: "#bf616a",
};

export function mountEditor(parent: HTMLElement, sourceText: string): void {
  editor?.destroy();
  editor = new EditorView({
    parent,
    state: EditorState.create({
      doc: sourceText,
      extensions: [
        basicSetup,
        keymap.of([indentWithTab]),
        go(),
        oneDark,
        EditorView.theme({
          "&": { height: "100%", background: "#2e3440" },
          ".cm-scroller": {
            overflow: "auto",
            fontFamily: "ui-monospace, monospace",
          },
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
          },
        }),
      ],
    }),
  });
}

export function source(): string {
  return editor?.state.doc.toString() ?? "";
}

export function setSource(sourceText: string): void {
  if (!editor) return;

  editor.dispatch({
    changes: {
      from: 0,
      to: editor.state.doc.length,
      insert: sourceText,
    },
  });
  editor.focus();
}

const graphStyles: StylesheetJson = [
  {
    selector: "node",
    style: {
      "background-color": "#4c566a",
      "border-color": "#607087",
      "border-width": 2,
      color: "#eceff4",
      label: "data(label)",
      "font-family": "ui-monospace, monospace",
      "font-size": 11,
      height: 8,
      width: "label",
      padding: "14px",
      shape: "round-rectangle",
      "text-valign": "center",
      "text-halign": "center",
      "text-wrap": "none",
      "transition-property": "background-color, border-color, border-width",
      "transition-duration": 240,
    },
  },
  {
    selector: "node.running",
    style: { "border-color": "#88c0d0", "border-width": 4 },
  },
  {
    selector: "node.completed",
    style: { "border-color": "#a3be8c", "border-width": 3 },
  },
  {
    selector: "node.failed",
    style: { "border-color": "#bf616a", "border-width": 4 },
  },
  {
    selector: "node.queued",
    style: {
      "background-color": "#4c566a",
      "border-color": "#607087",
      "border-width": 2,
    },
  },
  {
    selector: "edge",
    style: {
      width: 2,
      "line-color": "#607087",
      "target-arrow-color": "#607087",
      "target-arrow-shape": "triangle",
      "curve-style": "bezier",
      "transition-property": "line-color, target-arrow-color, width",
      "transition-duration": 240,
    },
  },
];

export function mountGraph(container: HTMLElement): void {
  graph?.destroy();
  graphContainer = container;
  graphSignature = "";
  graphRenderCount = 0;
  graph = cytoscape({
    container,
    elements: [],
    maxZoom: 2,
    minZoom: 0.25,
    layout: graphLayout(),
    style: graphStyles,
  });
}

function graphLayout(): cytoscape.BreadthFirstLayoutOptions {
  return {
    name: "breadthfirst",
    directed: true,
    fit: false,
    padding: 36,
    spacingFactor: 1.35,
  };
}

export function setGraph(data: GraphData): void {
  if (!graph || !graphContainer) return;

  const nodes = data.nodes ?? [];
  const edges = data.edges ?? [];
  const signature = JSON.stringify({ nodes, edges });
  if (signature === graphSignature) return;

  const graphNodes = nodes.map((node) => ({
    data: {
      id: node.id,
      label: node.label || node.id,
      state: "queued" satisfies GraphState,
    },
    classes: "queued",
  }));
  const graphEdges = edges.map((edge, index) => ({
    data: {
      id: `edge-${index}`,
      source: edge.from,
      target: edge.to,
    },
  }));

  graph.elements().remove();
  graph.add([...graphNodes, ...graphEdges]);
  graphSignature = signature;
  graphRenderCount++;
  graphContainer.dataset.elements = String(graphNodes.length + graphEdges.length);
  graphContainer.dataset.renderCount = String(graphRenderCount);
  graph.resize();
  graph.zoom(1);
  graph.layout(graphLayout()).run();
  graph.center();
}

export function zoomGraph(direction: number): void {
  if (!graph) return;

  const nextZoom = Math.min(
    graph.maxZoom(),
    Math.max(graph.minZoom(), graph.zoom() + direction * 0.2),
  );
  graph.zoom({
    level: nextZoom,
    renderedPosition: {
      x: graph.width() / 2,
      y: graph.height() / 2,
    },
  });
}

export function setNodeState(id: string, state: NodeState): void {
  const node = graph?.getElementById(id);
  if (!node || node.empty()) return;

  applyNodeState(node, state);
  updateStateMarker();
}

export function setGraphState(state: GraphState): void {
  if (!graph) return;

  graph.nodes().forEach((node) => applyNodeState(node, state));
  graph.edges().forEach((edge) => {
    const color = edgeColors[state];
    edge.style("line-color", color);
    edge.style("target-arrow-color", color);
    edge.style("width", state === "running" ? 3 : 2);
  });
  updateStateMarker();
}

function applyNodeState(node: NodeSingular, state: GraphState): void {
  node.removeClass(nodeStates.join(" "));
  node.addClass(state);
  node.data("state", state);
}

function updateStateMarker(): void {
  if (!graphContainer || !graph) return;

  graphContainer.dataset.nodeStates = graph
    .nodes()
    .map((node) => node.data("state") as GraphState)
    .join(",");
}

export function runWasm(
  bytes: ArrayBuffer,
  wasmExecURL: string,
  onMessage: (message: WorkerMessage) => void,
  onExit: () => void,
): void {
  stopWasm();
  const workerURL = new URL("web/worker.js", wasmExecURL);
  worker = new Worker(workerURL);
  const currentWorker = worker;
  const finish = (): void => {
    currentWorker.terminate();
    if (worker === currentWorker) {
      worker = undefined;
    }
    onExit();
  };
  currentWorker.onmessage = (event: MessageEvent<WorkerMessage>) => {
    if (event.data.type === "exit") {
      finish();
      return;
    }
    onMessage(event.data);
  };
  currentWorker.onerror = (event: ErrorEvent) => {
    if (!event.message.includes("Go program has already exited")) {
      onMessage({ type: "error", message: event.message });
    }
    finish();
  };
  currentWorker.postMessage({ bytes, wasmExecURL }, [bytes]);
}

export function stopWasm(): void {
  worker?.terminate();
  worker = undefined;
}

export function graphNodeStyles(): GraphNodeStyle[] {
  if (!graph) return [];

  return graph.nodes().map((node) => ({
    state: node.data("state") as GraphState,
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
