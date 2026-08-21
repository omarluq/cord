import type { WorkerMessage } from "./messages";

let worker: Worker | undefined;

/**
 * Starts a worker and transfers ownership of bytes to it. The transfer detaches
 * the buffer, so callers must not reuse bytes after this function returns.
 */
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
    if (worker === currentWorker) worker = undefined;
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
