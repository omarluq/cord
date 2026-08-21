import { basicSetup } from "codemirror";
import { indentWithTab } from "@codemirror/commands";
import { go } from "@codemirror/lang-go";
import { EditorState } from "@codemirror/state";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView, keymap } from "@codemirror/view";

let editor: EditorView | undefined;

export function mountEditor(parent: HTMLElement, source: string): void {
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
        EditorView.contentAttributes.of({
          "aria-label": "Workflow Go source",
        }),
        EditorView.theme({
          "&": {
            height: "100%",
            background: "var(--color-nord-0)",
          },
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
            fontSize: "12px",
            lineHeight: "1",
            textAlign: "center",
            width: "12px",
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

export function destroyEditor(): void {
  editor?.destroy();
  editor = undefined;
}
