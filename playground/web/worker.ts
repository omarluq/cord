import type { NodeState, WorkerMessage } from "./messages";

interface GoRuntime {
  importObject: WebAssembly.Imports;
  run(instance: WebAssembly.Instance): Promise<void>;
  _scheduledTimeouts?: Map<number, ReturnType<typeof setTimeout>>;
}

type GoConstructor = new () => GoRuntime;

interface WorkerGlobal extends DedicatedWorkerGlobalScope {
  Go: GoConstructor;
}

interface RunRequest {
  bytes: ArrayBuffer;
  wasmExecURL: string;
}

const worker = self as unknown as WorkerGlobal;

worker.onerror = (event: ErrorEvent): boolean => {
  if (event.message.includes("Go program has already exited")) {
    event.preventDefault();
    return true;
  }
  return false;
};

function send(message: WorkerMessage): void {
  worker.postMessage(message);
}

function sendOutput(type: "output" | "error", values: unknown[]): void {
  const value = values.map(String).join(" ");
  const node = /^__CORD_NODE__:([^:]+):(running|completed|failed)$/.exec(
    value,
  );
  if (node?.[1] && node[2]) {
    send({
      type: "node",
      id: node[1],
      state: node[2] as NodeState,
    });
    return;
  }

  if (type === "error") {
    send({ type, message: value });
    return;
  }
  send({ type, value });
}

console.log = (...values: unknown[]): void => sendOutput("output", values);
console.error = (...values: unknown[]): void => sendOutput("error", values);

worker.onmessage = async (event: MessageEvent<RunRequest>): Promise<void> => {
  try {
    const wasmExecURL = new URL(event.data.wasmExecURL, worker.location.href);
    if (wasmExecURL.origin !== worker.location.origin) {
      throw new Error("wasm_exec.js must use the playground origin");
    }
    worker.importScripts(wasmExecURL.href);
    const goRuntime = new worker.Go();
    const result = await WebAssembly.instantiate(
      event.data.bytes,
      goRuntime.importObject,
    );
    await goRuntime.run(result.instance);
    const timeouts = goRuntime._scheduledTimeouts;
    if (timeouts) {
      for (const timeout of timeouts.values()) {
        clearTimeout(timeout);
      }
      timeouts.clear();
    }
  } catch (error: unknown) {
    send({
      type: "error",
      message: error instanceof Error ? error.stack ?? error.message : String(error),
    });
  } finally {
    send({ type: "exit" });
  }
};

export {};
