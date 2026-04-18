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

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class OtlpMetricsConverter {

  private static final String TABLE = "metrics";

  // TAG(5) + ATTRIBUTE(8) + FIELD(1) = 14 columns
  private static final String[] COLUMN_NAMES = {
    "user_id",
    "session_id",
    "metric_name",
    "model",
    "type",
    "terminal_type",
    "service_version",
    "os_type",
    "os_version",
    "host_arch",
    "unit",
    "metric_type",
    "description",
    "value"
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
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.STRING,
    TSDataType.DOUBLE
  };
  private static final TsTableColumnCategory[] CATEGORIES = {
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.TAG,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.ATTRIBUTE,
    TsTableColumnCategory.FIELD
  };

  // Column indices
  private static final int C_USER_ID = 0;
  private static final int C_SESSION_ID = 1;
  private static final int C_METRIC_NAME = 2;
  private static final int C_MODEL = 3;
  private static final int C_TYPE = 4;
  private static final int C_TERMINAL_TYPE = 5;
  private static final int C_SERVICE_VERSION = 6;
  private static final int C_OS_TYPE = 7;
  private static final int C_OS_VERSION = 8;
  private static final int C_HOST_ARCH = 9;
  private static final int C_UNIT = 10;
  private static final int C_METRIC_TYPE = 11;
  private static final int C_DESCRIPTION = 12;
  private static final int C_VALUE = 13;

  private OtlpMetricsConverter() {}

  static boolean convertAndInsert(
      final OtlpService service, final ExportMetricsServiceRequest request) {
    final Map<String, List<ResourceMetrics>> byDb = new HashMap<>();
    for (final ResourceMetrics rm : request.getResourceMetricsList()) {
      final String db =
          OtlpConverter.deriveDatabaseName(
              OtlpConverter.extractServiceName(rm.getResource().getAttributesList()));
      byDb.computeIfAbsent(db, k -> new ArrayList<>()).add(rm);
    }
    boolean allOk = true;
    for (final Map.Entry<String, List<ResourceMetrics>> entry : byDb.entrySet()) {
      if (!insertForDatabase(service, entry.getKey(), entry.getValue())) {
        allOk = false;
      }
    }
    return allOk;
  }

  private static boolean insertForDatabase(
      final OtlpService service, final String db, final List<ResourceMetrics> rms) {
    int capacity = 0;
    for (final ResourceMetrics rm : rms) {
      for (final ScopeMetrics sm : rm.getScopeMetricsList()) {
        for (final Metric m : sm.getMetricsList()) {
          capacity += countDataPoints(m);
        }
      }
    }
    if (capacity == 0) {
      return true;
    }
    final IClientSession session = service.sessionFor(db);
    final OtlpTableBatch batch =
        new OtlpTableBatch(TABLE, COLUMN_NAMES, DATA_TYPES, CATEGORIES, capacity);

    for (final ResourceMetrics rm : rms) {
      final List<KeyValue> resAttrs = rm.getResource().getAttributesList();
      final String serviceVersion = OtlpConverter.extractAttribute(resAttrs, "service.version");
      final String osType = OtlpConverter.extractAttribute(resAttrs, "os.type");
      final String osVersion = OtlpConverter.extractAttribute(resAttrs, "os.version");
      final String hostArch = OtlpConverter.extractAttribute(resAttrs, "host.arch");
      for (final ScopeMetrics sm : rm.getScopeMetricsList()) {
        for (final Metric metric : sm.getMetricsList()) {
          writeMetric(batch, metric, serviceVersion, osType, osVersion, hostArch);
        }
      }
    }
    return OtlpIngestor.insert(db, session, batch);
  }

  private static void writeMetric(
      final OtlpTableBatch batch,
      final Metric metric,
      final String serviceVersion,
      final String osType,
      final String osVersion,
      final String hostArch) {
    switch (metric.getDataCase()) {
      case GAUGE:
        for (final NumberDataPoint dp : metric.getGauge().getDataPointsList()) {
          writeNumberPoint(batch, metric, "gauge", dp, serviceVersion, osType, osVersion, hostArch);
        }
        break;
      case SUM:
        for (final NumberDataPoint dp : metric.getSum().getDataPointsList()) {
          writeNumberPoint(batch, metric, "sum", dp, serviceVersion, osType, osVersion, hostArch);
        }
        break;
      case HISTOGRAM:
        for (final HistogramDataPoint dp : metric.getHistogram().getDataPointsList()) {
          writeRow(
              batch,
              dp.getTimeUnixNano(),
              dp.getAttributesList(),
              metric.getName(),
              "histogram",
              dp.hasSum() ? dp.getSum() : 0.0,
              metric.getUnit(),
              metric.getDescription(),
              serviceVersion,
              osType,
              osVersion,
              hostArch);
        }
        break;
      case EXPONENTIAL_HISTOGRAM:
        for (final io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint dp :
            metric.getExponentialHistogram().getDataPointsList()) {
          writeRow(
              batch,
              dp.getTimeUnixNano(),
              dp.getAttributesList(),
              metric.getName(),
              "exponential_histogram",
              dp.hasSum() ? dp.getSum() : 0.0,
              metric.getUnit(),
              metric.getDescription(),
              serviceVersion,
              osType,
              osVersion,
              hostArch);
        }
        break;
      case SUMMARY:
        for (final SummaryDataPoint dp : metric.getSummary().getDataPointsList()) {
          writeRow(
              batch,
              dp.getTimeUnixNano(),
              dp.getAttributesList(),
              metric.getName(),
              "summary",
              dp.getSum(),
              metric.getUnit(),
              metric.getDescription(),
              serviceVersion,
              osType,
              osVersion,
              hostArch);
        }
        break;
      default:
        break;
    }
  }

  private static void writeNumberPoint(
      final OtlpTableBatch batch,
      final Metric metric,
      final String metricType,
      final NumberDataPoint dp,
      final String serviceVersion,
      final String osType,
      final String osVersion,
      final String hostArch) {
    writeRow(
        batch,
        dp.getTimeUnixNano(),
        dp.getAttributesList(),
        metric.getName(),
        metricType,
        numericValue(dp),
        metric.getUnit(),
        metric.getDescription(),
        serviceVersion,
        osType,
        osVersion,
        hostArch);
  }

  private static void writeRow(
      final OtlpTableBatch batch,
      final long timeUnixNano,
      final List<KeyValue> dpAttrs,
      final String metricName,
      final String metricType,
      final double value,
      final String unit,
      final String description,
      final String serviceVersion,
      final String osType,
      final String osVersion,
      final String hostArch) {
    batch.startRow(OtlpConverter.nanoToDbPrecision(timeUnixNano));
    // TAGs
    batch.setString(C_USER_ID, OtlpConverter.extractAttribute(dpAttrs, "user.id"));
    batch.setString(C_SESSION_ID, OtlpConverter.extractAttribute(dpAttrs, "session.id"));
    batch.setString(C_METRIC_NAME, metricName);
    batch.setString(C_MODEL, OtlpConverter.extractAttribute(dpAttrs, "model"));
    batch.setString(C_TYPE, OtlpConverter.extractAttribute(dpAttrs, "type"));
    // ATTRIBUTEs
    batch.setString(C_TERMINAL_TYPE, OtlpConverter.extractAttribute(dpAttrs, "terminal.type"));
    batch.setString(C_SERVICE_VERSION, serviceVersion);
    batch.setString(C_OS_TYPE, osType);
    batch.setString(C_OS_VERSION, osVersion);
    batch.setString(C_HOST_ARCH, hostArch);
    batch.setString(C_UNIT, unit);
    batch.setString(C_METRIC_TYPE, metricType);
    batch.setString(C_DESCRIPTION, description);
    // FIELD
    batch.setDouble(C_VALUE, value);
  }

  private static double numericValue(final NumberDataPoint dp) {
    switch (dp.getValueCase()) {
      case AS_DOUBLE:
        return dp.getAsDouble();
      case AS_INT:
        return (double) dp.getAsInt();
      default:
        return 0.0;
    }
  }

  private static int countDataPoints(final Metric metric) {
    switch (metric.getDataCase()) {
      case GAUGE:
        return metric.getGauge().getDataPointsCount();
      case SUM:
        return metric.getSum().getDataPointsCount();
      case HISTOGRAM:
        return metric.getHistogram().getDataPointsCount();
      case EXPONENTIAL_HISTOGRAM:
        return metric.getExponentialHistogram().getDataPointsCount();
      case SUMMARY:
        return metric.getSummary().getDataPointsCount();
      default:
        return 0;
    }
  }
}
