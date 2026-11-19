/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { spawn } from 'node:child_process';
export const name = 'tqts-discovery-tools';
export const inject = ['tools'];
export async function apply(ctx, config) {
  const { defineTool } = await import(config.toolsModule);
  let calls = 0;
  for (const kind of ['context', 'sql']) {
    ctx.tools.register(defineTool({
      name: kind === 'sql' ? 'iotdb_sql' : 'dataset_context',
      description: kind === 'sql'
        ? 'Execute one read-only IoTDB TREE SQL statement in the assigned database. Multiple calls are permitted for discovery, queries and correction. SELECT; SHOW TIMESERIES/DEVICES/CHILD PATHS/CHILD NODES/VERSION; COUNT TIMESERIES/DEVICES/NODES. Use explicit root paths. No other SQL statements.'
        : 'Retrieve original benchmark public dataset documentation on demand. This contains field meanings, units, device mappings, time conventions and dialect notes, never questions or answers. Results are scalar entries with JSON-pointer paths. Start with query or pointer=/database; use a returned parent pointer to inspect a related record. query is a case-insensitive literal substring; empty query matches all. Follow next_offset with identical query/pointer.',
      parameters: kind === 'sql' ? {
        sql: { type: 'string', required: true, description: 'One read-only IoTDB tree-model SQL statement.' },
      } : {
        pointer: { type: 'string', description: 'JSON-pointer prefix; default empty (all public context).' },
        query: { type: 'string', description: 'Literal substring filter over each path and value; default empty.' },
        offset: { type: 'integer', description: 'Result offset, default 0.' },
        limit: { type: 'integer', description: 'Page size, 1..40, default 20.' },
      },
      output: {
        schema: { type: 'object', additionalProperties: false, properties: {
          content: { type: 'string', required: true },
          elapsedMs: { type: 'number', required: true },
          isError: { type: 'boolean', required: true },
        } },
        render: (_args, value) => [{ type: 'text', text: value.content }],
        presentationMeta: (_args, value) => ({ interface: kind, cliProcessMs: kind === 'sql' ? value.elapsedMs : 0, toolError: value.isError }),
      },
      timeoutMs: 55000,
      async execute(args, exec) {
        if (++calls > 30) throw new Error('Tool budget exhausted (30 calls). Return your best supported final answer now.');
        return await new Promise((resolve, reject) => {
          const child = spawn(config.python, [config.runner, config.configPath], { stdio: ['pipe','pipe','pipe'] });
          const stdout = [], stderr = [];
          child.stdout.on('data', x => stdout.push(x));
          child.stderr.on('data', x => stderr.push(x));
          const abort = () => child.kill('SIGTERM');
          exec.signal.addEventListener('abort', abort, { once: true });
          child.on('error', reject);
          child.on('close', code => {
            exec.signal.removeEventListener('abort', abort);
            if (code !== 0) return reject(new Error(Buffer.concat(stderr).toString()));
            try { resolve(JSON.parse(Buffer.concat(stdout).toString())); } catch (e) { reject(e); }
          });
          child.stdin.end(JSON.stringify({ kind, arguments: args, callId: String(exec.callId) }));
        });
      },
    }));
  }
}
