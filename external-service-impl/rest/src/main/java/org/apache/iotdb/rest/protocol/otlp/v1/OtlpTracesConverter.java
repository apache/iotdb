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

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Flattens OTLP trace export requests into rows for each service's {@code traces} table. */
final class OtlpTracesConverter {

  private static final String TABLE = "traces";

  // TAG(2) + ATTRIBUTE(3) + FIELD(12) = 17 columns
  private static final String[] COLUMN_NAMES = {
    "service_name",
    "span_name",
    "service_version",
    "os_type",
    "host_arch",
    "trace_id",
    "span_id",
    "parent_span_id",
    "span_kind",
    "start_time_unix_nano",
    "end_time_unix_nano",
    "duration_nano",
    "status_code",
    "status_message",
    "attributes",
    "resource_attributes",
    "scope_name",
    "scope_version"
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
    TSDataType.STRING,
    TSDataType.INT64,
    TSDataType.INT64,
    TSDataType.INT64,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING
  };
  private static final TsTableColumnCategory[] COLUMN_CATEGORIES = {
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
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
    TsTableColumnCategory.FIELD
  };

  private OtlpTracesConverter() {}

  static boolean convertAndInsert(
      final OtlpService service, final ExportTraceServiceRequest request) {
    final Map<String, List<ResourceSpans>> byDatabase = new HashMap<>();
    for (final ResourceSpans rs : request.getResourceSpansList()) {
      final String db =
          OtlpConverter.deriveDatabaseName(
              OtlpConverter.extractServiceName(rs.getResource().getAttributesList()));
      byDatabase.computeIfAbsent(db, k -> new ArrayList<>()).add(rs);
    }

    boolean allOk = true;
    for (final Map.Entry<String, List<ResourceSpans>> entry : byDatabase.entrySet()) {
      if (!insertForDatabase(service, entry.getKey(), entry.getValue())) {
        allOk = false;
      }
    }
    return allOk;
  }

  private static boolean insertForDatabase(
      final OtlpService service, final String database, final List<ResourceSpans> resourceSpans) {
    int capacity = 0;
    for (final ResourceSpans rs : resourceSpans) {
      for (final ScopeSpans ss : rs.getScopeSpansList()) {
        capacity += ss.getSpansCount();
      }
    }
    if (capacity == 0) {
      return true;
    }
    final IClientSession session = service.sessionFor(database);
    final OtlpTableBatch batch =
        new OtlpTableBatch(TABLE, COLUMN_NAMES, DATA_TYPES, COLUMN_CATEGORIES, capacity);

    for (final ResourceSpans rs : resourceSpans) {
      final java.util.List<io.opentelemetry.proto.common.v1.KeyValue> resAttrs =
          rs.getResource().getAttributesList();
      final String serviceName = OtlpConverter.extractServiceName(resAttrs);
      final String serviceVersion = OtlpConverter.extractAttribute(resAttrs, "service.version");
      final String osType = OtlpConverter.extractAttribute(resAttrs, "os.type");
      final String hostArch = OtlpConverter.extractAttribute(resAttrs, "host.arch");
      final String resourceAttrsJson = OtlpConverter.attributesToJson(resAttrs);
      for (final ScopeSpans ss : rs.getScopeSpansList()) {
        final String scopeName = ss.getScope().getName();
        final String scopeVersion = ss.getScope().getVersion();
        for (final Span span : ss.getSpansList()) {
          final long startNano = span.getStartTimeUnixNano();
          final long endNano = span.getEndTimeUnixNano();
          batch.startRow(OtlpConverter.nanoToDbPrecision(startNano));
          int c = 0;
          // TAGs
          batch.setString(c++, serviceName);
          batch.setString(c++, span.getName());
          // ATTRIBUTEs
          batch.setString(c++, serviceVersion);
          batch.setString(c++, osType);
          batch.setString(c++, hostArch);
          // FIELDs
          batch.setString(c++, OtlpConverter.bytesToHex(span.getTraceId()));
          batch.setString(c++, OtlpConverter.bytesToHex(span.getSpanId()));
          batch.setString(c++, OtlpConverter.bytesToHex(span.getParentSpanId()));
          batch.setString(c++, span.getKind().name());
          batch.setLong(c++, startNano);
          batch.setLong(c++, endNano);
          batch.setLong(c++, endNano - startNano);
          batch.setString(c++, statusCode(span));
          batch.setString(c++, span.getStatus().getMessage());
          batch.setString(c++, OtlpConverter.attributesToJson(span.getAttributesList()));
          batch.setString(c++, resourceAttrsJson);
          batch.setString(c++, scopeName);
          batch.setString(c, scopeVersion);
        }
      }
    }
    return OtlpIngestor.insert(database, session, batch);
  }

  private static String statusCode(final Span span) {
    // Status is a top-level proto type in opentelemetry-proto, not nested in Span.
    // OTLP status codes: STATUS_CODE_UNSET = 0, STATUS_CODE_OK = 1, STATUS_CODE_ERROR = 2.
    if (!span.hasStatus()) {
      return "UNSET";
    }
    final io.opentelemetry.proto.trace.v1.Status status = span.getStatus();
    switch (status.getCode()) {
      case STATUS_CODE_OK:
        return "OK";
      case STATUS_CODE_ERROR:
        return "ERROR";
      case STATUS_CODE_UNSET:
      case UNRECOGNIZED:
      default:
        return "UNSET";
    }
  }
}
