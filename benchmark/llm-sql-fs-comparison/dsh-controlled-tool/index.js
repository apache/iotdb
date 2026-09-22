/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { spawn } from "node:child_process";

export const name = "iotdb-controlled-tool";
export const inject = ["tools"];

const ARMS = new Set(["sql", "filesystem"]);
const DATABASE_NAME = /^[A-Za-z_][A-Za-z_0-9]*$/;
const FS_COMMANDS = [
  "help",
  "ls",
  "schema",
  "meta",
  "cat",
  "head",
  "tail",
  "count",
  "stats",
  "stat",
  "file",
];
const FS_TAG_OPERATORS = ["eq", "neq", "regexp", "is-null", "not-null"];
const FS_AGGREGATES = ["count", "min", "max", "sum", "avg", "median"];
const MAX_MODEL_OUTPUT_BYTES = 40000;

function boundedText(value) {
  if (Buffer.byteLength(value, "utf8") <= MAX_MODEL_OUTPUT_BYTES) return value;
  const suffix = "\n[output omitted; narrow the request or use structured pagination]";
  const budget = MAX_MODEL_OUTPUT_BYTES - Buffer.byteLength(suffix, "utf8");
  let prefix = "";
  let bytes = 0;
  for (const character of value) {
    const size = Buffer.byteLength(character, "utf8");
    if (bytes + size > budget) break;
    prefix += character;
    bytes += size;
  }
  return prefix + suffix;
}

function requiredString(config, key) {
  const value = config[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`iotdb-controlled-tool: ${key} must be a nonempty string`);
  }
  return value;
}

function booleanConfig(config, key, fallback) {
  const value = config[key];
  if (value === undefined) return fallback;
  if (typeof value === "boolean") return value;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error(`iotdb-controlled-tool: ${key} must be true or false`);
}

function run(request, config, signal) {
  return new Promise((resolve, reject) => {
    const child = spawn(config.python, [config.runner], {
      detached: process.platform !== "win32",
      env: {
        PATH: process.env.PATH ?? "",
        HOME: process.env.HOME ?? "",
        LANG: process.env.LANG ?? "C.UTF-8",
        ...(process.env.LC_ALL === undefined ? {} : { LC_ALL: process.env.LC_ALL }),
        ...(process.env.TZ === undefined ? {} : { TZ: process.env.TZ }),
        ...(process.env.TMPDIR === undefined ? {} : { TMPDIR: process.env.TMPDIR }),
        ...(process.env.JAVA_HOME === undefined
          ? {}
          : { JAVA_HOME: process.env.JAVA_HOME }),
        ...(process.env.IOTDB_USERNAME === undefined
          ? {}
          : { IOTDB_USERNAME: process.env.IOTDB_USERNAME }),
        ...(process.env.IOTDB_PASSWORD === undefined
          ? {}
          : { IOTDB_PASSWORD: process.env.IOTDB_PASSWORD }),
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
    const stdout = [];
    const stderr = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    const cap = 1024 * 1024;
    const collect = (chunks, field) => (chunk) => {
      if (field === "stdout") stdoutBytes += chunk.length;
      else stderrBytes += chunk.length;
      const size = field === "stdout" ? stdoutBytes : stderrBytes;
      if (size <= cap) chunks.push(chunk);
    };
    child.stdout.on("data", collect(stdout, "stdout"));
    child.stderr.on("data", collect(stderr, "stderr"));
    const abort = () => {
      try {
        if (process.platform === "win32") child.kill("SIGKILL");
        else process.kill(-child.pid, "SIGKILL");
      } catch (error) {
        if (error?.code !== "ESRCH") reject(error);
      }
    };
    signal.addEventListener("abort", abort, { once: true });
    child.once("error", reject);
    child.once("close", (code, childSignal) => {
      signal.removeEventListener("abort", abort);
      const err = Buffer.concat(stderr).toString("utf8");
      if (stdoutBytes > cap || stderrBytes > cap) {
        reject(new Error("controlled runner output exceeded 1 MiB"));
        return;
      }
      if (code !== 0) {
        reject(
          new Error(
            `controlled runner exited with ${code ?? childSignal ?? "unknown"}: ${err}`,
          ),
        );
        return;
      }
      let response;
      try {
        response = JSON.parse(Buffer.concat(stdout).toString("utf8"));
      } catch {
        reject(new Error(`controlled runner returned invalid JSON: ${err}`));
        return;
      }
      if (response?.ok !== true) {
        reject(
          new Error(
            `IoTDB ${response?.errorKind ?? "tool_error"}: ${response?.message ?? "command rejected"}`,
          ),
        );
        return;
      }
      resolve(response);
    });
    child.stdin.end(JSON.stringify(request));
  });
}

export async function apply(ctx, rawConfig = {}) {
  const config = {
    arm: requiredString(rawConfig, "arm"),
    database: requiredString(rawConfig, "database"),
    filesystemPath: rawConfig.filesystemPath,
    structuredPages: booleanConfig(rawConfig, "structuredPages", true),
    compactPages: booleanConfig(rawConfig, "compactPages", false),
    filteredStats: booleanConfig(rawConfig, "filteredStats", false),
    runner: requiredString(rawConfig, "runner"),
    executable: requiredString(rawConfig, "executable"),
    outputRoot: requiredString(rawConfig, "outputRoot"),
    python: requiredString(rawConfig, "python"),
    toolsModule: requiredString(rawConfig, "toolsModule"),
    timeoutSeconds: rawConfig.timeoutSeconds ?? 30,
  };
  if (!ARMS.has(config.arm)) {
    throw new Error("iotdb-controlled-tool: arm must be sql or filesystem");
  }
  if (!DATABASE_NAME.test(config.database)) {
    throw new Error("iotdb-controlled-tool: invalid database identifier");
  }
  if (
    config.arm === "filesystem" &&
    (typeof config.filesystemPath !== "string" || config.filesystemPath.length === 0)
  ) {
    throw new Error("iotdb-controlled-tool: filesystemPath must be a nonempty string");
  }
  if (config.compactPages && !config.structuredPages) {
    throw new Error(
      "iotdb-controlled-tool: compactPages requires structuredPages",
    );
  }
  if (
    typeof config.timeoutSeconds !== "number" ||
    !Number.isFinite(config.timeoutSeconds) ||
    config.timeoutSeconds <= 0 ||
    config.timeoutSeconds > 300
  ) {
    throw new Error("iotdb-controlled-tool: timeoutSeconds must be in (0, 300]");
  }
  const sql = config.arm === "sql";
  const toolName = sql ? "iotdb_sql" : "iotdb_fs";
  const parameters = sql
    ? {
        sql: {
          type: "string",
          required: true,
          description:
            "A single SELECT, SHOW TABLES, DESC, or DESCRIBE statement. Qualify all tables with the task database.",
        },
      }
    : {
        command: {
          type: "string",
          enum: FS_COMMANDS,
          required: true,
          description:
            "Read-only operation. Use help with no other parameters to receive the complete contract.",
        },
        startMs: {
          type: "integer",
          description:
            `Inclusive UTC epoch-millisecond lower bound; cat and head${config.filteredStats ? ", plus stats," : ""} only. Omit for no lower bound. Do not pass an ISO timestamp.`,
        },
        endMs: {
          type: "integer",
          description:
            `Inclusive UTC epoch-millisecond upper bound; cat and head${config.filteredStats ? ", plus stats," : ""} only. Omit for no upper bound. Must be >= startMs.`,
        },
        measurement: {
          type: "string",
          description:
            "One FIELD measurement identifier; schema, count, stats, cat, and head only. Omit for all applicable columns.",
        },
        limit: {
          type: "integer",
          description:
            "Requested page size from 1 through 500; cat, head, and tail only. Defaults to 200 for cat/head and 10 for tail.",
        },
        offset: {
          type: "integer",
          description:
            "Number of rows to skip, starting at 0; cat and head only. Omit for zero.",
        },
        tagName: {
          type: "string",
          description:
            `TAG column identifier; cat and head${config.filteredStats ? ", plus stats," : ""} only. Supply tagOperator too.`,
        },
        tagOperator: {
          type: "string",
          enum: FS_TAG_OPERATORS,
          description:
            "TAG predicate operator. eq, neq, and regexp require tagValue; is-null and not-null forbid it.",
        },
        tagValue: {
          type: "string",
          description: "TAG predicate value for eq, neq, or regexp.",
        },
      };
  if (!sql && config.filteredStats) {
    parameters.aggregates = {
      type: "array",
      items: { type: "string", enum: FS_AGGREGATES },
      description:
        "One or more controlled statistics to compute after applying filters; stats only. Use min and max together for a range.",
    };
  }
  let callCount = 0;
  const { defineTool } = await import(config.toolsModule);
  ctx.tools.register(
    defineTool({
      name: toolName,
      description: sql
        ? `Execute one read-only IoTDB table-dialect SQL statement restricted to database ${config.database}.`
        : `Read the fixed IoTDB filesystem object ${config.filesystemPath} through typed, read-only operations. The path and CSV output format are fixed. ${config.structuredPages ? `Data reads return bounded structured pages with next_offset.${config.compactPages ? " Time and numeric measurements use JSON numbers while TAG values remain strings." : ""}` : "Data reads return raw CSV text for the typed-interface ablation."}${config.filteredStats ? " Stats can apply typed time and TAG filters before controlled aggregation." : ""} Call help for complete command-specific parameter rules.`,
      parameters,
      output: {
        schema: {
          type: "object",
          additionalProperties: false,
          properties: {
            ok: { type: "boolean", required: true },
            resultKind: {
              type: "string",
              enum: ["text", "page"],
              required: true,
            },
            stdout: { type: "string", required: true },
            stderr: { type: "string", required: true },
            page: {
              oneOf: [
                {
                  type: "object",
                  additionalProperties: false,
                  properties: {
                    columns: {
                      type: "array",
                      items: { type: "string" },
                      required: true,
                    },
                    rows: {
                      type: "array",
                      items: {
                        type: "array",
                        items: {
                          oneOf: [
                            { type: "string" },
                            { type: "number" },
                            { type: "null" },
                          ],
                        },
                      },
                      required: true,
                    },
                    offset: { type: "integer", required: true },
                    limit: { type: "integer", required: true },
                    returned_rows: { type: "integer", required: true },
                    has_more: { type: "boolean", required: true },
                    next_offset: {
                      oneOf: [{ type: "integer" }, { type: "null" }],
                      required: true,
                    },
                    null_value: { type: "string", required: true },
                  },
                },
                { type: "null" },
              ],
              required: true,
            },
            exitCode: { type: "integer", required: true },
            errorKind: {
              oneOf: [{ type: "string" }, { type: "null" }],
              required: true,
            },
            timedOut: { type: "boolean", required: true },
            truncated: { type: "boolean", required: true },
            stdoutBytes: { type: "integer", required: true },
            stderrBytes: { type: "integer", required: true },
            cliProcessMs: { type: "number", required: true },
          },
        },
        render: (_args, value) => {
          if (value.resultKind === "page") {
            return [
              {
                type: "text",
                text: JSON.stringify({ type: "page", ...value.page }),
              },
            ];
          }
          let text = value.stdout;
          if (value.stderr.length > 0) {
            text += `${text.endsWith("\n") || text.length === 0 ? "" : "\n"}[stderr]\n${value.stderr}`;
          }
          if (value.truncated) {
            text += `${text.endsWith("\n") ? "" : "\n"}[output truncated; full output retained by benchmark]`;
          }
          text = text || "(no output)";
          return [
            {
              type: "text",
              text: config.structuredPages ? boundedText(text) : text,
            },
          ];
        },
        presentationMeta: (_args, value) => ({
          interface: config.arm,
          cliProcessMs: value.cliProcessMs,
          stdoutBytes: value.stdoutBytes,
          stderrBytes: value.stderrBytes,
          truncated: value.truncated,
          resultKind: value.resultKind,
          returnedRows: value.page?.returned_rows ?? null,
          nextOffset: value.page?.next_offset ?? null,
        }),
      },
      timeoutMs: (config.timeoutSeconds + 5) * 1000,
      async execute(args, exec) {
        callCount += 1;
        if (sql && callCount > 1) {
          throw new Error("the SQL baseline permits exactly one tool call");
        }
        const request = {
          arm: config.arm,
          database: config.database,
          executable: config.executable,
          outputRoot: config.outputRoot,
          callId: String(exec.callId),
          timeoutSeconds: config.timeoutSeconds,
          ...(sql
            ? { command: args.sql }
            : {
                filesystemPath: config.filesystemPath,
                structuredPages: config.structuredPages,
                compactPages: config.compactPages,
                filteredStats: config.filteredStats,
                parameters: args,
              }),
        };
        const response = await run(
          request,
          config,
          exec.signal,
        );
        const { artifactDirectory: _artifactDirectory, ...modelValue } = response;
        return modelValue;
      },
    }),
  );
}
