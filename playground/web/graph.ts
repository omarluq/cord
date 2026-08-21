import cytoscape, {
  type Core,
  type NodeSingular,
  type StylesheetJson,
} from "cytoscape";
import type { NodeState } from "./messages";

export interface GraphData {
  nodes?: { id: string; label?: string }[];
  edges?: { from: string; to: string }[];
}

export interface GraphNodeStyle {
  state: string;
  backgroundColor: string;
  borderColor: string;
  textColor: string;
  width: number;
  height: number;
}

export type GraphState = "queued" | NodeState;
interface GraphPalette {
  background: string;
  text: string;
  queued: string;
  running: string;
  completed: string;
  failed: string;
}

let graph: Core | undefined;
let container: HTMLElement | undefined;
let signature = "";
let renderCount = 0;
let resizeFrame = 0;
const nodeStates: readonly GraphState[] = [
  "queued",
  "running",
  "completed",
  "failed",
];

function themeColor(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function palette(): GraphPalette {
  return {
    background: themeColor("--color-nord-3"),
    text: themeColor("--color-nord-6"),
    queued: themeColor("--color-graph-queued"),
    running: themeColor("--color-nord-8"),
    completed: themeColor("--color-nord-14"),
    failed: themeColor("--color-nord-11"),
  };
}

function styles(colors: GraphPalette): StylesheetJson {
  return [
    {
      selector: "node",
      style: {
        "background-color": colors.background,
        "border-color": colors.queued,
        "border-width": 2,
        color: colors.text,
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
      style: { "border-color": colors.running, "border-width": 4 },
    },
    {
      selector: "node.completed",
      style: { "border-color": colors.completed, "border-width": 3 },
    },
    {
      selector: "node.failed",
      style: { "border-color": colors.failed, "border-width": 4 },
    },
    {
      selector: "node.queued",
      style: {
        "background-color": colors.background,
        "border-color": colors.queued,
        "border-width": 2,
      },
    },
    {
      selector: "edge",
      style: {
        width: 2,
        "line-color": colors.queued,
        "target-arrow-color": colors.queued,
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
        "transition-property": "line-color, target-arrow-color, width",
        "transition-duration": 240,
      },
    },
  ];
}

function layout(): cytoscape.BreadthFirstLayoutOptions {
  return {
    name: "breadthfirst",
    directed: true,
    fit: false,
    padding: 36,
    spacingFactor: 1.35,
  };
}

export function mountGraph(element: HTMLElement): void {
  if (resizeFrame !== 0) cancelAnimationFrame(resizeFrame);
  resizeFrame = 0;
  graph?.destroy();
  container = element;
  signature = "";
  renderCount = 0;
  graph = cytoscape({
    container: element,
    elements: [],
    maxZoom: 2,
    minZoom: 0.25,
    layout: layout(),
    style: styles(palette()),
  });
}

export function setGraph(data: GraphData): void {
  if (!graph || !container) return;

  const nodes = data.nodes ?? [];
  const edges = data.edges ?? [];
  const nextSignature = JSON.stringify({ nodes, edges });
  if (nextSignature === signature) return;

  const graphNodes = nodes.map((node) => ({
    data: { id: node.id, label: node.label || node.id, state: "queued" },
    classes: "queued",
  }));
  const graphEdges = edges.map((edge, index) => ({
    data: { id: `edge-${index}`, source: edge.from, target: edge.to },
  }));

  graph.elements().remove();
  graph.add([...graphNodes, ...graphEdges]);
  signature = nextSignature;
  renderCount++;
  container.dataset.elements = String(graphNodes.length + graphEdges.length);
  container.dataset.renderCount = String(renderCount);
  graph.resize();
  graph.zoom(1);
  graph.layout(layout()).run();
  graph.center();
}

export function resizeGraph(): void {
  if (resizeFrame !== 0) return;
  const mountedGraph = graph;
  resizeFrame = requestAnimationFrame(() => {
    resizeFrame = 0;
    if (graph === mountedGraph) graph?.resize();
  });
}

export function zoomGraph(direction: number): void {
  if (!graph) return;
  const nextZoom = Math.min(
    graph.maxZoom(),
    Math.max(graph.minZoom(), graph.zoom() + direction * 0.2),
  );
  graph.zoom({
    level: nextZoom,
    renderedPosition: { x: graph.width() / 2, y: graph.height() / 2 },
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
  const color = palette()[state];
  graph.nodes().forEach((node) => applyNodeState(node, state));
  graph.edges().forEach((edge) => {
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
  if (!container || !graph) return;
  container.dataset.nodeStates = graph.nodes()
    .map((node) => node.data("state") as GraphState)
    .join(",");
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

export function destroyGraph(): void {
  if (resizeFrame !== 0) cancelAnimationFrame(resizeFrame);
  resizeFrame = 0;
  graph?.destroy();
  graph = undefined;
  container = undefined;
}
