import { expect, test, type Page } from "@playwright/test";

interface GraphNodeStyle {
  state: string;
  backgroundColor: string;
  borderColor: string;
  textColor: string;
  width: number;
  height: number;
}

interface StateTrackingElement extends HTMLElement {
  observedNodeStates?: string[];
}

const compilerURL = "http://127.0.0.1:4180/compile";

async function graphNodeStyles(page: Page): Promise<GraphNodeStyle[]> {
  return page.locator("#workflow-graph").evaluate(() => (
    (window as unknown as {
      CordPlayground: { graphNodeStyles(): GraphNodeStyle[] };
    }).CordPlayground.graphNodeStyles()
  ));
}

test(
  "compiles and executes user-authored Cord code in WebAssembly",
  async ({ page }) => {
    test.setTimeout(180_000);
    const errors: string[] = [];
    let compileRequests = 0;

    page.on("request", (request) => {
      if (request.url() === compilerURL) compileRequests++;
    });
    page.on("console", (message) => {
      const text = message.text();
      console.log(`browser ${message.type()}: ${text}`);
      const invalidGraphStyle = message.type() === "warning"
        && /shadow-(blur|color|opacity)/.test(text);
      const unexpectedError = message.type() === "error"
        && !text.includes("Go program has already exited");
      if (invalidGraphStyle || unexpectedError) errors.push(text);
    });
    page.on("pageerror", (error) => {
      if (!error.message.includes("Go program has already exited")) {
        errors.push(error.message);
      }
    });
    await page.route("**/web/playground.js", async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 3_000));
      await route.continue();
    });

    await page.goto(
      `http://127.0.0.1:4173/?compiler=${encodeURIComponent(compilerURL)}`,
    );
    await expect(page.locator('[data-testid="status"]')).toHaveText(
      "ready",
      { timeout: 30_000 },
    );
    await expect(page.locator("#workflow-editor .cm-editor")).toBeVisible();

    const examples = page.getByRole("navigation", {
      name: "Example workflows",
    });
    await expect(examples.getByRole("button")).toHaveCount(6);
    await expect(examples.getByRole("button", {
      name: "Open linear.go",
    })).toHaveClass(/text-nord-8/);
    await examples.getByRole("button", {
      name: "Open linear.go",
    }).click();
    await expect(page.locator("#workflow-editor")).toContainText(
      "func increment",
    );

    await page.locator("#workflow-graph").evaluate((element) => {
      const graph = element as StateTrackingElement;
      graph.observedNodeStates = [];
      new MutationObserver((mutations) => {
        for (const mutation of mutations) {
          graph.observedNodeStates?.push(mutation.oldValue ?? "");
        }
        graph.observedNodeStates?.push(graph.dataset.nodeStates ?? "");
      }).observe(graph, {
        attributes: true,
        attributeFilter: ["data-node-states"],
        attributeOldValue: true,
      });
    });

    await page.getByRole("button", {
      name: "Compile and run workflow",
    }).click();
    await expect(page.locator("#workflow-graph canvas").first()).toBeVisible({
      timeout: 120_000,
    });
    await expect(page.locator("#workflow-graph")).toHaveAttribute(
      "data-elements",
      "3",
      { timeout: 120_000 },
    );
    await expect(page.locator('[data-testid="run-result"]')).toContainText(
      "result: 10",
      { timeout: 120_000 },
    );
    await expect(page.locator("#workflow-graph")).toHaveAttribute(
      "data-node-states",
      "completed,completed",
    );
    const observedNodeStates = await page
      .locator("#workflow-graph")
      .evaluate((element) => (
        (element as StateTrackingElement).observedNodeStates ?? []
      ));
    expect(observedNodeStates.some((states) => states.includes("running")))
      .toBe(true);

    await expect.poll(async () => (
      (await graphNodeStyles(page)).map(({
        state,
        backgroundColor,
        borderColor,
        textColor,
      }) => ({ state, backgroundColor, borderColor, textColor }))
    )).toEqual([
      {
        state: "completed",
        backgroundColor: "rgb(76,86,106)",
        borderColor: "rgb(163,190,140)",
        textColor: "rgb(236,239,244)",
      },
      {
        state: "completed",
        backgroundColor: "rgb(76,86,106)",
        borderColor: "rgb(163,190,140)",
        textColor: "rgb(236,239,244)",
      },
    ]);

    const firstRunSizes = (await graphNodeStyles(page)).map(
      ({ width, height }) => ({ width, height }),
    );
    const firstRenderCount = await page
      .locator("#workflow-graph")
      .getAttribute("data-render-count");
    expect(firstRenderCount).not.toBeNull();
    const compileRequestsBeforeRerun = compileRequests;

    await page.getByRole("button", {
      name: "Compile and run workflow",
    }).click();
    await expect(page.locator("#workflow-graph")).toHaveAttribute(
      "data-node-states",
      "completed,completed",
      { timeout: 60_000 },
    );

    const secondRunSizes = (await graphNodeStyles(page)).map(
      ({ width, height }) => ({ width, height }),
    );
    expect(secondRunSizes).toEqual(firstRunSizes);
    await expect(page.locator("#workflow-graph")).toHaveAttribute(
      "data-render-count",
      firstRenderCount!,
    );
    expect(compileRequests).toBe(compileRequestsBeforeRerun);
    expect(errors).toEqual([]);
  },
);

test("resizes playground panels from their borders", async ({ page }) => {
  await page.goto(
    `http://127.0.0.1:4173/?compiler=${encodeURIComponent(compilerURL)}`,
  );
  await expect(page.locator('[data-testid="status"]')).toHaveText("ready", {
    timeout: 30_000,
  });

  const files = page.getByTestId("files-resize-handle");
  const results = page.getByTestId("results-resize-handle");
  const output = page.getByTestId("output-resize-handle");
  await expect(files).toHaveAttribute("aria-orientation", "vertical");
  await expect(output).toHaveAttribute("aria-orientation", "horizontal");

  const drag = async (
    handle: typeof files,
    deltaX: number,
    deltaY: number,
  ): Promise<void> => {
    const box = await handle.boundingBox();
    expect(box).not.toBeNull();
    const x = box!.x + box!.width / 2;
    const y = box!.y + box!.height / 2;
    await page.mouse.move(x, y);
    await page.mouse.down();
    await page.mouse.move(x + deltaX, y + deltaY);
    await page.mouse.up();
  };

  const fileWidth = await page.getByRole("navigation", {
    name: "Example workflows",
  }).evaluate((element) => element.getBoundingClientRect().width);
  await drag(files, 40, 0);
  await expect.poll(() => page.getByRole("navigation", {
    name: "Example workflows",
  }).evaluate((element) => element.getBoundingClientRect().width)).toBeGreaterThan(fileWidth);

  const resultWidth = await page.locator("#results-layout").evaluate(
    (element) => element.getBoundingClientRect().width,
  );
  await drag(results, -40, 0);
  await expect.poll(() => page.locator("#results-layout").evaluate(
    (element) => element.getBoundingClientRect().width,
  )).toBeGreaterThan(resultWidth);

  const graphHeight = await page.locator("#workflow-graph").evaluate(
    (element) => element.getBoundingClientRect().height,
  );
  await drag(output, 0, 40);
  await expect.poll(() => page.locator("#workflow-graph").evaluate(
    (element) => element.getBoundingClientRect().height,
  )).toBeGreaterThan(graphHeight);

  const beforeKey = Number(await files.getAttribute("aria-valuenow"));
  await files.focus();
  await files.press("ArrowRight");
  await expect.poll(async () => Number(await files.getAttribute("aria-valuenow")))
    .toBeGreaterThan(beforeKey);

  await page.setViewportSize({ width: 480, height: 520 });
  await expect(page.locator("#workflow-editor")).toBeVisible();
  await expect(page.locator("#workflow-graph")).toBeVisible();
  await expect(page.locator('[data-testid="run-result"]')).toBeVisible();
});

test(
  "compiles and executes the large pipeline",
  async ({ page }) => {
    test.setTimeout(240_000);
    const requestFailures: string[] = [];

    page.on("requestfailed", (request) => {
      if (request.url() === compilerURL) {
        requestFailures.push(request.failure()?.errorText ?? "unknown failure");
      }
    });

    await page.goto(
      `http://127.0.0.1:4173/?compiler=${encodeURIComponent(compilerURL)}`,
    );
    await expect(page.locator('[data-testid="status"]')).toHaveText(
      "ready",
      { timeout: 30_000 },
    );

    await page.getByRole("button", {
      name: "Open large_pipeline.go",
    }).click();
    await page.getByRole("button", {
      name: "Compile and run workflow",
    }).click();

    await expect(page.locator('[data-testid="run-result"]')).toContainText(
      "order charges: $72",
      { timeout: 180_000 },
    );
    await expect(page.locator('[data-testid="status"]')).toHaveText("ready");
    expect(requestFailures).toEqual([]);
  },
);
