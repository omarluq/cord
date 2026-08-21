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

export function destroyEditor(): void {
  editor?.destroy();
  editor = undefined;
}
