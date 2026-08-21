import {
  destroyEditor,
  mountEditor,
  setSource,
  source,
} from "./code-editor";
import {
  destroyGraph,
  graphNodeStyles,
  mountGraph,
  setGraph,
  setGraphState,
  setNodeState,
  zoomGraph,
  type GraphData,
  type GraphNodeStyle,
} from "./graph";
import {
  destroyResizableLayout,
  mountResizableLayout,
} from "./layout-resize";
import type { NodeState, WorkerMessage } from "./messages";
import { runWasm, stopWasm } from "./runtime";

interface CordPlaygroundAPI {
  mountEditor(parent: HTMLElement, source: string): void;
  source(): string;
  setSource(source: string): void;
  mountGraph(container: HTMLElement): void;
  setGraph(data: GraphData): void;
  zoomGraph(direction: number): void;
  setGraphState(state: "queued" | NodeState): void;
  setNodeState(id: string, state: NodeState): void;
  runWasm(
    bytes: ArrayBuffer,
    wasmExecURL: string,
    onMessage: (message: WorkerMessage) => void,
    onExit: () => void,
  ): void;
  stopWasm(): void;
  destroy(): void;
  graphNodeStyles(): GraphNodeStyle[];
}

declare global {
  interface Window {
    CordPlayground: CordPlaygroundAPI;
  }
}

function mountGraphAndLayout(container: HTMLElement): void {
  mountGraph(container);
  mountResizableLayout();
}

function destroy(): void {
  stopWasm();
  destroyResizableLayout();
  destroyEditor();
  destroyGraph();
}

window.CordPlayground = {
  mountEditor,
  source,
  setSource,
  mountGraph: mountGraphAndLayout,
  setGraph,
  zoomGraph,
  setGraphState,
  setNodeState,
  runWasm,
  stopWasm,
  destroy,
  graphNodeStyles,
};
