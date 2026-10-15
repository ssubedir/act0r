// Regression tests for the repository's actual documentation-checking script.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, mkdir, copyFile, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';

const checker = fileURLToPath(new URL('./check-docs.mjs', import.meta.url));
async function check(markdown) {
  const root = await mkdtemp(join(tmpdir(), 'act0r-docs-check-'));
  try {
    for (const directory of ['scripts', 'docs', 'examples']) await mkdir(join(root, directory));
    await copyFile(checker, join(root, 'scripts/check-docs.mjs'));
    await writeFile(join(root, 'README.md'), markdown);
    const result = spawnSync(process.execPath, [join(root, 'scripts/check-docs.mjs')], {encoding:'utf8', timeout:10000, windowsHide:true});
    assert.ifError(result.error);
    return result;
  } finally {
    await rm(root, {recursive:true, force:true});
  }
}

test('a shorter backtick fence cannot close a longer fenced block', async () => {
  const result = await check('# Document\n\n````markdown\n```\n[example](missing.md)\n````\n');
  assert.equal(result.status, 0, result.stderr);
});
test('a shorter tilde fence cannot close a longer fenced block', async () => {
  const result = await check('# Document\n\n~~~~markdown\n~~~\n[example](missing.md)\n~~~~\n');
  assert.equal(result.status, 0, result.stderr);
});
test('text after a fence marker does not close the current fenced block', async () => {
  const result = await check('# Document\n\n```markdown\n```not-a-close\n[example](missing.md)\n```\n');
  assert.equal(result.status, 0, result.stderr);
});
test('a different fence character does not close the current block', async () => {
  const result = await check('# Document\n\n```markdown\n~~~~\n[example](missing.md)\n```\n');
  assert.equal(result.status, 0, result.stderr);
});
test('a longer matching closing fence restores visible-link checking and line numbers', async () => {
  const result = await check('# Document\n\n```markdown\n[example](ignored.md)\n````  \n[broken](missing.md)\n');
  assert.equal(result.status, 1);
  assert.match(result.stderr, /README\.md:6:.*missing\.md.*missing target/);
  assert.doesNotMatch(result.stderr, /ignored\.md/);
});
test('links after a nested shorter marker are checked only after the real closing fence', async () => {
  const result = await check('# Document\n\n````markdown\n```\n[example](ignored.md)\n````\n[broken](missing.md)\n');
  assert.equal(result.status, 1);
  assert.match(result.stderr, /README\.md:7:.*missing\.md.*missing target/);
  assert.doesNotMatch(result.stderr, /ignored\.md/);
});
test('normal local anchors and remote links retain their current behavior', async () => {
  const result = await check('# Document\n\n[local](#document)\n[remote](https://example.invalid/missing)\n');
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Checked 1 local links/);
});
test('an unclosed fenced block keeps its example links hidden through end of file', async () => {
  const result = await check('# Document\n\n```markdown\n[example](missing.md)\n');
  assert.equal(result.status, 0, result.stderr);
});
