// Local files/fragments only: CI never fetches remote pages or executes examples.
import {readFile, readdir, stat} from "node:fs/promises";
import {dirname, relative, resolve, sep} from "node:path";
import {fileURLToPath} from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const documents = [];
const excluded = new Set([".git", ".build", ".act0r", "target", "node_modules"]);
async function collect(directory) {
  for (const entry of await readdir(directory, {withFileTypes: true})) {
    if (excluded.has(entry.name) || entry.isSymbolicLink()) continue;
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) await collect(path);
    else if (entry.name.endsWith(".md")) documents.push(path);
  }
}
for (const entry of await readdir(root, {withFileTypes: true})) {
  if (entry.isFile() && entry.name.endsWith(".md")) documents.push(resolve(root, entry.name));
}
await collect(resolve(root, "docs"));
await collect(resolve(root, "examples"));
const cache = new Map();
function visibleLines(text) {
  let fence;
  return text.split("\n").map(line => {
    const marker = line.match(/^\s*(`{3,}|~{3,})/);
    if (marker) {
      if (!fence) fence = marker[1][0];
      else if (fence === marker[1][0]) fence = undefined;
      return "";
    }
    return fence ? "" : line;
  }).join("\n");
}
async function document(path) {
  if (cache.has(path)) return cache.get(path);
  const text = visibleLines(await readFile(path, "utf8"));
  const anchors = new Set();
  const counts = new Map();
  for (const match of text.matchAll(/<a\s+[^>]*id=["']([^"']+)["'][^>]*>/g)) anchors.add(match[1]);
  for (const match of text.matchAll(/^#{1,6}\s+(.+?)\s*#*\s*$/gm)) {
    const base = match[1].replace(/<[^>]+>/g, "").toLowerCase()
      .replace(/[^\p{L}\p{N}_ -]/gu, "").replace(/ /g, "-");
    const count = counts.get(base) ?? 0;
    counts.set(base, count + 1);
    anchors.add(base + (count ? `-${count}` : ""));
  }
  const value = {text, anchors};
  cache.set(path, value);
  return value;
}
let checked = 0;
const errors = [];
for (const source of documents) {
  const {text} = await document(source);
  for (const match of text.matchAll(/\[[^\]\n]*\]\(([^)\n]+)\)/g)) {
    const link = match[1].trim().replace(/^<|>$/g, "");
    if (/^[a-z][a-z0-9+.-]*:/i.test(link) || link.startsWith("//")) continue;
    const line = text.slice(0, match.index).split("\n").length;
    const location = `${relative(root, source).split(sep).join("/")}:${line}`;
    try {
      const hash = link.indexOf("#");
      const path = hash < 0 ? link : link.slice(0, hash);
      const fragment = hash < 0 ? "" : decodeURIComponent(link.slice(hash + 1));
      const target = path ? resolve(dirname(source), decodeURIComponent(path)) : source;
      const within = relative(root, target);
      if (within === ".." || within.startsWith(`..${sep}`)) throw new Error("outside repository");
      const entry = await stat(target);
      if (fragment && entry.isFile() && target.endsWith(".md") && !(await document(target)).anchors.has(fragment)) {
        throw new Error(`missing section #${fragment}`);
      }
      checked++;
    } catch (error) {
      errors.push(`${location}: ${link} (${error.code === "ENOENT" ? "missing target" : error.message})`);
    }
  }
}
if (errors.length) {
  console.error(errors.join("\n"));
  process.exitCode = 1;
} else {
  console.log(`Checked ${checked} local links and fragments in ${documents.length} Markdown files.`);
}
