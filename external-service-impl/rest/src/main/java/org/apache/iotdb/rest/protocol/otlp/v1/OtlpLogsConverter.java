/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rest.protocol.otlp.v1;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.rest.protocol.otlp.v1.OtlpIngestor.OtlpTableBatch;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class OtlpLogsConverter {

  private static final String TABLE = "logs";

  // TAG(3) + ATTRIBUTE(4) + FIELD(18) = 25 columns
  private static final String[] COLUMN_NAMES = {
    "user_id",
    "session_id",
    "event_name",
    "terminal_type",
    "service_version",
    "os_type",
    "host_arch",
    "prompt_id",
    "event_sequence",
    "body",
    "prompt_length",
    "prompt",
    "model",
    "cost_usd",
    "duration_ms",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "request_id",
    "speed",
    "error",
    "status_code",
    "attempt",
    "tool_name",
    "success",
    "tool_duration_ms",
    "decision",
    "decision_source",
    "tool_result_size_bytes"
  };
  private static final TSDataType[] DATA_TYPES = {
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.INT32,
    TSDataType.STRING,
    TSDataType.INT32,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.DOUBLE,
    TSDataType.INT64,
    TSDataType.INT64,
    TSDataType.INT64,
    TSDataType.INT64,
    TSDataType.INT64,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.INT32,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.INT64,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.INT64
  };
  private static final TsTableColumnCategory[] CATEGORIES = {
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD,
    TsTableColumnCategory.FIELD
  };

  private static final int C_USER_ID = 0;
  private static final int C_SESSION_ID = 1;
  private static final int C_EVENT_NAME = 2;
  private static final int C_TERMINAL_TYPE = 3;
  private static final int C_SERVICE_VERSION = 4;
  private static final int C_OS_TYPE = 5;
  private static final int C_HOST_ARCH = 6;
  private static final int C_PROMPT_ID = 7;
  private static final int C_EVENT_SEQUENCE = 8;
  private static final int C_BODY = 9;
  private static final int C_PROMPT_LENGTH = 10;
  private static final int C_PROMPT = 11;
  private static final int C_MODEL = 12;
  private static final int C_COST_USD = 13;
  private static final int C_DURATION_MS = 14;
  private static final int C_INPUT_TOKENS = 15;
  private static final int C_OUTPUT_TOKENS = 16;
  private static final int C_CACHE_READ_TOKENS = 17;
  private static final int C_CACHE_CREATION_TOKENS = 18;
  private static final int C_REQUEST_ID = 19;
  private static final int C_SPEED = 20;
  private static final int C_ERROR = 21;
  private static final int C_STATUS_CODE = 22;
  private static final int C_ATTEMPT = 23;
  private static final int C_TOOL_NAME = 24;
  private static final int C_SUCCESS = 25;
  private static final int C_TOOL_DURATION_MS = 26;
  private static final int C_DECISION = 27;
  private static final int C_DECISION_SOURCE = 28;
  private static final int C_TOOL_RESULT_SIZE_BYTES = 29;

  private OtlpLogsConverter() {}

  static boolean convertAndInsert(
      final OtlpService service, final ExportLogsServiceRequest request) {
    final Map<String, List<ResourceLogs>> byDb = new HashMap<>();
    for (final ResourceLogs rl : request.getResourceLogsList()) {
      final String db =
          OtlpConverter.deriveDatabaseName(
              OtlpConverter.extractServiceName(rl.getResource().getAttributesList()));
      byDb.computeIfAbsent(db, k -> new ArrayList<>()).add(rl);
    }
    boolean allOk = true;
    for (final Map.Entry<String, List<ResourceLogs>> entry : byDb.entrySet()) {
      if (!insertForDatabase(service, entry.getKey(), entry.getValue())) {
        allOk = false;
      }
    }
    return allOk;
  }

  private static boolean insertForDatabase(
      final OtlpService service, final String db, final List<ResourceLogs> rls) {
    int capacity = 0;
    for (final ResourceLogs rl : rls) {
      for (final ScopeLogs sl : rl.getScopeLogsList()) {
        capacity += sl.getLogRecordsCount();
      }
    }
    if (capacity == 0) {
      return true;
    }
    final IClientSession session = service.sessionFor(db);
    final OtlpTableBatch batch =
        new OtlpTableBatch(TABLE, COLUMN_NAMES, DATA_TYPES, CATEGORIES, capacity);

    for (final ResourceLogs rl : rls) {
      final List<KeyValue> resAttrs = rl.getResource().getAttributesList();
      final String serviceVersion = OtlpConverter.extractAttribute(resAttrs, "service.version");
      final String osType = OtlpConverter.extractAttribute(resAttrs, "os.type");
      final String hostArch = OtlpConverter.extractAttribute(resAttrs, "host.arch");
      for (final ScopeLogs sl : rl.getScopeLogsList()) {
        for (final LogRecord log : sl.getLogRecordsList()) {
          final long ts =
              log.getTimeUnixNano() != 0 ? log.getTimeUnixNano() : log.getObservedTimeUnixNano();
          batch.startRow(OtlpConverter.nanoToDbPrecision(ts));

          final List<KeyValue> attrs = log.getAttributesList();
          final String eventName = OtlpConverter.extractAttribute(attrs, "event.name");

          // TAGs
          batch.setString(C_USER_ID, OtlpConverter.extractAttribute(attrs, "user.id"));
          batch.setString(C_SESSION_ID, OtlpConverter.extractAttribute(attrs, "session.id"));
          batch.setString(C_EVENT_NAME, eventName);
          // ATTRIBUTEs
          batch.setString(C_TERMINAL_TYPE, OtlpConverter.extractAttribute(attrs, "terminal.type"));
          batch.setString(C_SERVICE_VERSION, serviceVersion);
          batch.setString(C_OS_TYPE, osType);
          batch.setString(C_HOST_ARCH, hostArch);
          // Common FIELDs
          batch.setString(C_PROMPT_ID, OtlpConverter.extractAttribute(attrs, "prompt.id"));
          setIntFromAttr(batch, C_EVENT_SEQUENCE, attrs, "event.sequence");
          batch.setString(C_BODY, bodyToString(log.getBody()));

          // Event-specific FIELDs
          if ("user_prompt".equals(eventName)) {
            setIntFromAttr(batch, C_PROMPT_LENGTH, attrs, "prompt_length");
            batch.setString(C_PROMPT, OtlpConverter.extractAttribute(attrs, "prompt"));
          } else if ("api_request".equals(eventName)) {
            batch.setString(C_MODEL, OtlpConverter.extractAttribute(attrs, "model"));
            setDoubleFromAttr(batch, C_COST_USD, attrs, "cost_usd");
            setLongFromAttr(batch, C_DURATION_MS, attrs, "duration_ms");
            setLongFromAttr(batch, C_INPUT_TOKENS, attrs, "input_tokens");
            setLongFromAttr(batch, C_OUTPUT_TOKENS, attrs, "output_tokens");
            setLongFromAttr(batch, C_CACHE_READ_TOKENS, attrs, "cache_read_tokens");
            setLongFromAttr(batch, C_CACHE_CREATION_TOKENS, attrs, "cache_creation_tokens");
            batch.setString(C_REQUEST_ID, OtlpConverter.extractAttribute(attrs, "request_id"));
            batch.setString(C_SPEED, OtlpConverter.extractAttribute(attrs, "speed"));
          } else if ("api_error".equals(eventName)) {
            batch.setString(C_MODEL, OtlpConverter.extractAttribute(attrs, "model"));
            batch.setString(C_ERROR, OtlpConverter.extractAttribute(attrs, "error"));
            batch.setString(C_STATUS_CODE, OtlpConverter.extractAttribute(attrs, "status_code"));
            setLongFromAttr(batch, C_DURATION_MS, attrs, "duration_ms");
            setIntFromAttr(batch, C_ATTEMPT, attrs, "attempt");
            batch.setString(C_REQUEST_ID, OtlpConverter.extractAttribute(attrs, "request_id"));
            batch.setString(C_SPEED, OtlpConverter.extractAttribute(attrs, "speed"));
          } else if ("tool_result".equals(eventName)) {
            batch.setString(C_TOOL_NAME, OtlpConverter.extractAttribute(attrs, "tool_name"));
            batch.setString(C_SUCCESS, OtlpConverter.extractAttribute(attrs, "success"));
            setLongFromAttr(batch, C_TOOL_DURATION_MS, attrs, "duration_ms");
            batch.setString(
                C_DECISION_SOURCE, OtlpConverter.extractAttribute(attrs, "decision_source"));
            batch.setString(C_DECISION, OtlpConverter.extractAttribute(attrs, "decision_type"));
            setLongFromAttr(batch, C_TOOL_RESULT_SIZE_BYTES, attrs, "tool_result_size_bytes");
          } else if ("tool_decision".equals(eventName)) {
            batch.setString(C_TOOL_NAME, OtlpConverter.extractAttribute(attrs, "tool_name"));
            batch.setString(C_DECISION, OtlpConverter.extractAttribute(attrs, "decision"));
            batch.setString(C_DECISION_SOURCE, OtlpConverter.extractAttribute(attrs, "source"));
          }
        }
      }
    }
    return OtlpIngestor.insert(db, session, batch);
  }

  private static void setLongFromAttr(
      final OtlpTableBatch batch, final int col, final List<KeyValue> attrs, final String key) {
    final String v = OtlpConverter.extractAttribute(attrs, key);
    if (!v.isEmpty()) {
      try {
        batch.setLong(col, Long.parseLong(v));
      } catch (final NumberFormatException ignored) {
      }
    }
  }

  private static void setIntFromAttr(
      final OtlpTableBatch batch, final int col, final List<KeyValue> attrs, final String key) {
    final String v = OtlpConverter.extractAttribute(attrs, key);
    if (!v.isEmpty()) {
      try {
        batch.setInt(col, Integer.parseInt(v));
      } catch (final NumberFormatException ignored) {
      }
    }
  }

  private static void setDoubleFromAttr(
      final OtlpTableBatch batch, final int col, final List<KeyValue> attrs, final String key) {
    final String v = OtlpConverter.extractAttribute(attrs, key);
    if (!v.isEmpty()) {
      try {
        batch.setDouble(col, Double.parseDouble(v));
      } catch (final NumberFormatException ignored) {
      }
    }
  }

  private static String bodyToString(final AnyValue body) {
    if (body == null) {
      return "";
    }
    switch (body.getValueCase()) {
      case STRING_VALUE:
        return body.getStringValue();
      case BOOL_VALUE:
        return Boolean.toString(body.getBoolValue());
      case INT_VALUE:
        return Long.toString(body.getIntValue());
      case DOUBLE_VALUE:
        return Double.toString(body.getDoubleValue());
      case BYTES_VALUE:
        return OtlpConverter.bytesToHex(body.getBytesValue());
      default:
        return body.toString();
    }
  }
}
