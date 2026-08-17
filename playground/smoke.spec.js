const { test, expect } = require('@playwright/test');

test('compiles and executes user-authored Cord code in WebAssembly', async ({ page }) => {
  test.setTimeout(180_000);
  const errors = [];
  page.on('console', message => {
    console.log(`browser ${message.type()}: ${message.text()}`);
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', error => errors.push(error.message));

  await page.goto('http://127.0.0.1:4173/?compiler=http%3A%2F%2F127.0.0.1%3A4180%2Fcompile');
  await expect(page.locator('[data-testid="status"]')).toHaveText('ready', { timeout: 30_000 });
  await expect(page.locator('#workflow-editor .cm-editor')).toBeVisible();
  await page.getByRole('button', { name: 'Compile & run' }).click();
  await expect(page.locator('[data-testid="run-result"]')).toHaveText('result: 10', { timeout: 60_000 });

  expect(errors).toEqual([]);
});
