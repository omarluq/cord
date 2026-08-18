export type NodeState = "running" | "completed" | "failed";

export type WorkerMessage =
  | { type: "node"; id: string; state: NodeState }
  | { type: "output"; value: string }
  | { type: "error"; message: string }
  | { type: "exit" };
