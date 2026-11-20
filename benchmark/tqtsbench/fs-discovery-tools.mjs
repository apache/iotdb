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
export const name = 'tqts-fs-discovery-tools';
export const inject = ['tools'];
export async function apply(ctx, config) {
  const { defineTool } = await import(config.toolsModule);
  const features = new Set(config.features ?? []);
  const temporal = features.has('temporal'), extreme = features.has('extreme'), grid = features.has('grid'), reduce = features.has('reduce');
  const commands = ['help','ls','find','tree','schema','meta','stats','count','cat','head','tail','stat','file','grep','cut','paste','join','wc'];
  const analytic = temporal || extreme || reduce;
  const aggregates = ['count','min','max','sum','avg','median','first','last','min_time','max_time','null_count', ...(reduce ? ['delta','elapsed_ms','rate_per_ms'] : []), ...(extreme ? ['argmin','argmax'] : [])];
  const groupings = [...(reduce ? ['entity','pooled'] : []), ...(temporal ? ['session'] : [])];
  let calls = 0;
  for (const kind of ['context', 'fs']) {
    ctx.tools.register(defineTool({
      name: kind === 'fs' ? 'iotdb_fs' : 'dataset_context',
      description: kind === 'fs'
        ? 'Execute a native read-only IoTDB Tree FS command in the assigned database. Use help for full typed parameters and return contract. No SQL escape or local files. Read results are structured pages; statistics are native FS scan statistics. Only existing FS capabilities are exposed.'
        : 'Retrieve original benchmark public dataset documentation on demand. This contains field meanings, units, device mappings, time conventions and dialect notes, never questions or answers. Results are scalar entries with JSON-pointer paths. Start with query or pointer=/database; use a returned parent pointer to inspect a related record. query is a case-insensitive literal substring; empty query matches all. Follow next_offset with identical query/pointer.',
      parameters: kind === 'fs' ? {
        command: { type: 'string', enum: commands, required: true, description: 'Native read-only FS operation. Call help for the complete parameter and return contract.' },
        ...(temporal ? {
          gaps: { type: 'boolean', description: 'stats mode: true computes complete successive non-null observation gaps of the single measurement; see help.' },
          gap_ms: { type: 'integer', description: 'stats with group_by=session only: positive threshold; split iff gap > threshold.' },
        } : {}),
        ...(groupings.length ? { group_by: { type: 'string', enum: groupings, description: 'stats mode: ' + [reduce ? 'entity (default) or pooled reduction grain' : '', temporal ? 'session segments by gap_ms' : ''].filter(Boolean).join('; ') + '. See help.' } } : {}),
        ...(extreme ? { by: { type: 'string', description: 'stats with aggregates [argmin] or [argmax] only: target measurement; ties choose earliest time.' } } : {}),
        ...(reduce ? {
          sort_by: { type: 'string', description: 'stats reduction mode: selected aggregate used for global ordering of result rows AFTER grouping, before paging.' },
          order: { type: 'string', enum: ['asc','desc'], description: 'sort direction, default desc; requires sort_by; NULL last.' },
        } : {}),
        ...(grid ? {
          origin: { type: 'string', description: 'stats grid: epoch-ms string or ISO datetime, interpreted with explicit timezone for local time; required with width and start_ms/end_ms.' },
          width: { type: 'integer', description: 'Positive grid width in unit; explicit windows are mutually exclusive.' },
          step: { type: 'integer', description: 'Positive grid stride, defaults to width; supports overlapping or gapped windows.' },
          unit: { type: 'string', enum: ['ms','day','month'], description: 'Fixed milliseconds vs origin-anchored calendar days/months. Default ms.' },
        } : {}),
        ...((grid || reduce) ? { timezone: { type: 'string', description: 'stats: IANA timezone default UTC for grids; in reduction mode it also labels epoch bounds. Ambiguous/nonexistent boundaries rejected.' } } : {}),
        path: { type: 'string', description: 'Absolute virtual path inside assigned /root/database. Default assigned root. No local paths or .csv suffix for Tree model.' },
        paths: { type: 'array', items: { type: 'string' }, description: 'For paste (2..8) or join (exactly 2) only; absolute virtual paths.' },
        measurements: { type: 'array', items: { type: 'string' }, description: 'Field identifiers, 1..64; schema/count/stats/cat/head. Analytic stats modes take exactly one target here' + (extreme ? ' (argmin/argmax: associated fields instead)' : '') + '. Omit for native defaults.' },
        start_ms: { type: 'integer', description: 'Inclusive epoch-ms lower bound; stats/cat/head only.' },
        end_ms: { type: 'integer', description: 'Inclusive epoch-ms upper bound; stats/cat/head only.' },
        limit: { type: 'integer', description: 'cat/head/tail page size 1..2000; defaults 200/200/10.' + (analytic ? ' Also output-row page size for analytic stats modes, default 200.' : '') },
        offset: { type: 'integer', description: 'cat/head row offset >=0; follow next_offset with identical filters.' + (analytic ? ' Also output-row offset for analytic stats modes.' : '') },
        aggregates: { type: 'array', items: { type: 'string', enum: aggregates }, description: 'stats only; omit for all native statistics including first/last and timeline min_time/max_time.' + (analytic ? ' Some values select an analytic mode; see help.' : '') },
        windows: { type: 'array', items: { type: 'object', properties: { start_ms: { type: 'integer', required: true }, end_ms: { type: 'integer', required: true } }, additionalProperties: false }, description: 'stats (all modes except group_by=session): 1..256 inclusive epoch-ms windows, mutually exclusive with top-level start_ms/end_ms. One scan per device; output includes window_index/start/end. Overlap and duplicates allowed.' },
        max_depth: { type: 'integer', description: 'find/tree recursion depth 1..12, defaults 6/3.' },
        pattern: { type: 'string', description: 'find filename glob or grep regular expression; up to 256 chars, no command separators or quotes.' },
        type: { type: 'string', enum: ['f','d'], description: 'find object kind filter only.' },
        fields: { type: 'string', description: 'cut only: positive comma-separated CSV field indices/ranges, e.g. 1,3-5.' },
        left_field: { type: 'integer', description: 'join left CSV field index, default 1.' },
        right_field: { type: 'integer', description: 'join right CSV field index, default 1.' },
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
        presentationMeta: (_args, value) => ({ interface: kind, cliProcessMs: kind === 'fs' ? value.elapsedMs : 0, toolError: value.isError }),
      },
      timeoutMs: 195000,
      async execute(args, exec) {
        if (++calls > 120) throw new Error('Tool budget exhausted (120 calls). Return your best supported final answer now.');
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
