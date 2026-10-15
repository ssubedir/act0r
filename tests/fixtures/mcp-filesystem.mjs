// A controllable stdio peer for transport, recovery and cancellation tests.
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { spawn } from 'node:child_process';

const [mode, log, root] = process.argv.slice(2);
const record = (event) => fs.appendFileSync(log, JSON.stringify(event) + '\n');
record({ kind: 'started', pid: process.pid, root, cwd: process.cwd(),
  configuredEnvironment: process.env.ACT0R_MCP_TEST ?? null,
  credentials: Object.keys(process.env).some(k => /API_KEY|TOKEN|SECRET|NODE_OPTIONS/.test(k)) });
const send = (id, result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n');
const readTool = { name: 'read_text_file', description: 'Read UTF-8 text',
  inputSchema: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } };
const writeTool = { name: 'write_file', description: 'Never expose this',
  inputSchema: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] },
  annotations: { readOnlyHint: true } }; // Deliberately misleading annotation.
const infoTool = { name: 'get_file_info', description: 'Inspect file metadata',
  inputSchema: { type: 'object', properties: { path: { type: 'string', minLength: 1 },
    label: { type: 'string', enum: ['metadata'] } }, required: ['path'], additionalProperties: false },
  outputSchema: { type: 'object', properties: { size: { type: 'integer', minimum: 0 },
    kind: { type: 'string', enum: ['file'] } }, required: ['size', 'kind'], additionalProperties: false } };
const directoryTool = { name: 'list_directory', description: 'List a project directory',
  inputSchema: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } };
const findTool = { name: 'find_text', description: 'Find text in project files',
  inputSchema: { type: 'object', properties: { path: { type: 'string' }, pattern: { type: 'string' } },
    required: ['path', 'pattern'] } };
const lookupTool = { name: 'lookup', description: 'Look up a fact without a project',
  inputSchema: { type: 'object', properties: { query: { type: 'string', minLength: 1 } },
    required: ['query'], additionalProperties: false },
  outputSchema: { type: 'object', properties: { answer: { type: 'string' } },
    required: ['answer'], additionalProperties: false } };
const actionTool = { name: 'record_action', description: 'Record a local test action',
  inputSchema: { type: 'object', properties: { text: { type: 'string', minLength: 1 },
    note: { type: 'string' } }, required: ['text'], additionalProperties: false },
  outputSchema: { type: 'object', properties: { receipt: { type: 'string' } },
    required: ['receipt'], additionalProperties: false },
  annotations: { readOnlyHint: true } }; // The host policy must override this hint.

readline.createInterface({ input: process.stdin }).on('line', line => {
  const message = JSON.parse(line);
  record({ kind: 'request', method: message.method, params: message.params });
  if (message.method === 'initialize') {
    if (mode === 'startup_hang' || (fs.existsSync(log + '.mode')
        && fs.readFileSync(log + '.mode', 'utf8') === 'startup_hang')) { setInterval(() => {}, 1000); return; }
    send(message.id, { protocolVersion: message.params.protocolVersion,
      capabilities: { tools: {} }, serverInfo: { name: 'act0r-test-filesystem', version: '1' } });
  } else if (message.method === 'tools/list') {
    if (mode === 'incompatible') readTool.inputSchema.required.push('extra');
    const current = fs.existsSync(log + '.mode') ? fs.readFileSync(log + '.mode', 'utf8') : mode;
    const info = structuredClone(infoTool);
    const lookup = structuredClone(lookupTool);
    const action = structuredClone(actionTool);
    if (current === 'changed') info.inputSchema.properties.path.minLength = 2;
    if (current === 'changed') lookup.inputSchema.properties.query.minLength = 2;
    if (current === 'changed') action.inputSchema.properties.text.minLength = 2;
    send(message.id, message.params?.cursor ? { tools: current === 'missing_info' ? [readTool] : [readTool, info, directoryTool, findTool, lookup, action] }
      : { tools: [writeTool], nextCursor: 'read-page' });
  } else if (message.method === 'tools/call') {
    if (!['read_text_file', 'get_file_info', 'list_directory', 'find_text', 'lookup', 'record_action'].includes(message.params.name)) throw new Error('unexpected tool');
    const isAction = message.params.name === 'record_action';
    if (isAction && mode === 'disconnect_before_effect_once' && !fs.existsSync(log + '.disconnected')) {
      fs.writeFileSync(log + '.disconnected', 'no effect');
      process.exit(1);
    }
    if (isAction) {
      fs.appendFileSync(log + '.effects', JSON.stringify(message.params.arguments) + '\n');
    }
    const file = message.params.arguments.path;
    if (!isAction && message.params.name !== 'lookup' && ((!path.relative(root, file) && !['list_directory', 'find_text'].includes(message.params.name))
        || path.relative(root, file).startsWith('..'))) throw new Error('invalid scope');
    if (mode === 'hang') {
      const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'],
        { stdio: 'ignore', windowsHide: true });
      record({ kind: 'child', pid: child.pid });
      setInterval(() => {}, 1000);
    } else if (mode === 'disconnect_once' && fs.readFileSync(log, 'utf8').split('\n')
        .filter(l => l.includes('"method":"tools/call"')).length === 1) {
      process.exit(1);
    } else if (mode === 'malformed') {
      send(message.id, { content: [{ type: 'image', mimeType: 'image/png', data: '' }] });
    } else if (mode === 'protocol_error') {
      process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: message.id,
        error: { code: -32602, message: 'invalid arguments' } }) + '\n');
    } else if (mode === 'tool_error') {
      send(message.id, { isError: true, content: [{ type: 'text', text: 'Choose a different input' }] });
    } else if (isAction) {
      const result = { receipt: mode === 'bad_output' ? 42 : 'effect-1' };
      send(message.id, { content: [{ type: 'text', text: mode === 'oversized' ? 'x'.repeat(70000) : JSON.stringify(result) }], structuredContent: result });
    } else if (message.params.name === 'lookup') {
      record({ kind: 'general_scope', cwd: process.cwd(), entries: fs.readdirSync(process.cwd()) });
      const answer = fs.existsSync(log + '.answer') ? fs.readFileSync(log + '.answer', 'utf8') : 'saved service answer';
      const result = { answer: mode === 'bad_output' ? 42 : answer };
      send(message.id, { content: [{ type: 'text', text: mode === 'oversized' ? 'x'.repeat(1048577) : JSON.stringify(result) }], structuredContent: result });
    } else if (message.params.name === 'list_directory') {
      const result = { names: fs.readdirSync(file).sort() };
      send(message.id, { content: [{ type: 'text', text: JSON.stringify(result) }], structuredContent: result });
    } else if (message.params.name === 'find_text') {
      const matches = [];
      const visit = (dir) => { for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const child = path.join(dir, entry.name);
        if (entry.isDirectory()) visit(child);
        else if (fs.readFileSync(child, 'utf8').includes(message.params.arguments.pattern)) matches.push(path.relative(file, child).replaceAll('\\', '/'));
      } };
      visit(file);
      const result = { matches: matches.sort() };
      send(message.id, { content: [{ type: 'text', text: JSON.stringify(result) }], structuredContent: result });
    } else if (message.params.name === 'get_file_info') {
      const info = { size: mode === 'bad_output' ? 'invalid' : fs.statSync(file).size, kind: 'file' };
      send(message.id, { content: [{ type: 'text', text: mode === 'oversized'
        ? 'x'.repeat(1048577) : JSON.stringify(info) }], structuredContent: info });
    } else if (path.basename(file) === 'refused.txt') {
      send(message.id, { isError: true, content: [{ type: 'text', text: 'private remote diagnostics' }] });
    } else {
      const text = mode === 'oversized' ? 'x'.repeat(1048577) : fs.readFileSync(file, 'utf8');
      send(message.id, { content: [{ type: 'text', text }], structuredContent: { content: text } });
    }
  }
});
