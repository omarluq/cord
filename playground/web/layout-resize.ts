import { resizeGraph } from "./graph";

const handleSize = 1;
const keyboardStep = 16;
const preferredFiles = 192;
const preferredResults = 640;
const preferredGraphRatio = 2 / 3;

let cleanupLayout: (() => void) | undefined;

interface HorizontalSizes {
  files: number;
  results: number;
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(Math.max(value, minimum), Math.max(minimum, maximum));
}

function horizontalMinimums(width: number): { files: number; editor: number; results: number } {
  const available = Math.max(0, width - handleSize * 2);
  if (available >= 720) return { files: 120, editor: 240, results: 240 };

  return {
    files: Math.min(96, available * 0.25),
    editor: available * 0.35,
    results: available * 0.3,
  };
}

function currentHorizontalSizes(layout: HTMLElement): HorizontalSizes {
  const children = layout.children;
  return {
    files: (children[0] as HTMLElement).getBoundingClientRect().width,
    results: (children[4] as HTMLElement).getBoundingClientRect().width,
  };
}

function applyHorizontalSizes(
  layout: HTMLElement,
  requestedFiles: number,
  requestedResults: number,
): void {
  const width = layout.clientWidth;
  const minimums = horizontalMinimums(width);
  const available = Math.max(0, width - handleSize * 2);
  const files = clamp(
    requestedFiles,
    minimums.files,
    available - minimums.editor - minimums.results,
  );
  const results = clamp(
    requestedResults,
    minimums.results,
    available - files - minimums.editor,
  );

  layout.style.gridTemplateColumns = `${files}px ${handleSize}px minmax(0, 1fr) ${handleSize}px ${results}px`;
  updateHandleValue(layout, "files", files, minimums.files, available - minimums.editor - minimums.results);
  updateHandleValue(layout, "results", results, minimums.results, available - files - minimums.editor);
  resizeGraph();
}

function applyVerticalSize(layout: HTMLElement, requestedGraph: number): void {
  const available = Math.max(0, layout.clientHeight - handleSize);
  const minimum = available >= 320 ? 120 : available * 0.3;
  const graphHeight = clamp(requestedGraph, minimum, available - minimum);
  layout.style.gridTemplateRows = `${graphHeight}px ${handleSize}px minmax(0, 1fr)`;
  updateHandleValue(layout, "output", graphHeight, minimum, available - minimum);
  resizeGraph();
}

function updateHandleValue(
  layout: HTMLElement,
  name: string,
  value: number,
  minimum: number,
  maximum: number,
): void {
  const handle = layout.querySelector<HTMLElement>(`[data-resize-handle="${name}"]`);
  if (!handle) return;
  handle.setAttribute("aria-valuemin", String(Math.round(minimum)));
  handle.setAttribute("aria-valuemax", String(Math.round(Math.max(minimum, maximum))));
  handle.setAttribute("aria-valuenow", String(Math.round(value)));
}

function keyboardMovement(key: string, vertical: boolean): number {
  if (vertical) {
    if (key === "ArrowLeft") return -keyboardStep;
    if (key === "ArrowRight") return keyboardStep;
    return 0;
  }
  if (key === "ArrowUp") return -keyboardStep;
  if (key === "ArrowDown") return keyboardStep;
  return 0;
}

function registerDrag(
  handle: HTMLElement,
  update: (movement: number) => void,
): () => void {
  let activePointer: number | undefined;
  let startPosition = 0;
  let previousCursor = "";
  let previousUserSelect = "";
  const vertical = handle.getAttribute("aria-orientation") === "vertical";

  const finish = (): void => {
    if (activePointer === undefined) return;
    activePointer = undefined;
    document.body.style.cursor = previousCursor;
    document.body.style.userSelect = previousUserSelect;
  };
  const pointerDown = (event: PointerEvent): void => {
    if (event.button !== 0) return;
    activePointer = event.pointerId;
    startPosition = vertical ? event.clientX : event.clientY;
    previousCursor = document.body.style.cursor;
    previousUserSelect = document.body.style.userSelect;
    document.body.style.cursor = vertical ? "col-resize" : "row-resize";
    document.body.style.userSelect = "none";
    handle.setPointerCapture(event.pointerId);
    event.preventDefault();
  };
  const pointerMove = (event: PointerEvent): void => {
    if (event.pointerId !== activePointer) return;
    const position = vertical ? event.clientX : event.clientY;
    update(position - startPosition);
    startPosition = position;
  };
  const keyDown = (event: KeyboardEvent): void => {
    const movement = keyboardMovement(event.key, vertical);
    if (movement === 0) return;
    event.preventDefault();
    update(movement);
  };

  handle.addEventListener("pointerdown", pointerDown);
  handle.addEventListener("pointermove", pointerMove);
  handle.addEventListener("pointerup", finish);
  handle.addEventListener("pointercancel", finish);
  handle.addEventListener("lostpointercapture", finish);
  handle.addEventListener("keydown", keyDown);
  window.addEventListener("blur", finish);

  return () => {
    finish();
    handle.removeEventListener("pointerdown", pointerDown);
    handle.removeEventListener("pointermove", pointerMove);
    handle.removeEventListener("pointerup", finish);
    handle.removeEventListener("pointercancel", finish);
    handle.removeEventListener("lostpointercapture", finish);
    handle.removeEventListener("keydown", keyDown);
    window.removeEventListener("blur", finish);
  };
}

export function mountResizableLayout(): void {
  cleanupLayout?.();
  const layout = document.getElementById("playground-layout");
  const results = document.getElementById("results-layout");
  if (!layout || !results) return;

  let horizontal = { files: preferredFiles, results: Math.min(preferredResults, layout.clientWidth / 2) };
  let graphHeight = results.clientHeight * preferredGraphRatio;
  const apply = (): void => {
    applyHorizontalSizes(layout, horizontal.files, horizontal.results);
    horizontal = currentHorizontalSizes(layout);
    applyVerticalSize(results, graphHeight);
    graphHeight = (results.children[0] as HTMLElement).getBoundingClientRect().height;
  };
  apply();

  const filesHandle = layout.querySelector<HTMLElement>('[data-resize-handle="files"]');
  const resultsHandle = layout.querySelector<HTMLElement>('[data-resize-handle="results"]');
  const outputHandle = results.querySelector<HTMLElement>('[data-resize-handle="output"]');
  const cleanups: (() => void)[] = [];
  if (filesHandle) {
    cleanups.push(registerDrag(filesHandle, (movement) => {
      applyHorizontalSizes(layout, horizontal.files + movement, horizontal.results);
      horizontal = currentHorizontalSizes(layout);
    }));
  }
  if (resultsHandle) {
    cleanups.push(registerDrag(resultsHandle, (movement) => {
      applyHorizontalSizes(layout, horizontal.files, horizontal.results - movement);
      horizontal = currentHorizontalSizes(layout);
    }));
  }
  if (outputHandle) {
    cleanups.push(registerDrag(outputHandle, (movement) => {
      applyVerticalSize(results, graphHeight + movement);
      graphHeight = (results.children[0] as HTMLElement).getBoundingClientRect().height;
    }));
  }

  const observer = new ResizeObserver(apply);
  observer.observe(layout);
  observer.observe(results);
  cleanupLayout = () => {
    observer.disconnect();
    cleanups.forEach((cleanup) => cleanup());
    cleanupLayout = undefined;
  };
}

export function destroyResizableLayout(): void {
  cleanupLayout?.();
}
