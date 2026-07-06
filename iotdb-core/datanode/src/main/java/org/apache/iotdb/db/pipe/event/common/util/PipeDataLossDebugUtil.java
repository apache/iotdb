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

package org.apache.iotdb.db.pipe.event.common.util;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class PipeDataLossDebugUtil {

  public static final String PREFIX = "[PipeDataLossDebug]";

  private static final int MAX_PRINTED_MEASUREMENTS = 16;

  private PipeDataLossDebugUtil() {}

  public static String formatPipe(final String pipeName, final long creationTime) {
    return "pipeName=" + pipeName + ", creationTime=" + creationTime;
  }

  public static String formatTablet(final Tablet tablet) {
    if (Objects.isNull(tablet)) {
      return "tablet=null";
    }

    final int rowSize = tablet.getRowSize();
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final int schemaSize = Objects.isNull(schemas) ? 0 : schemas.size();
    final long[] timestamps = tablet.getTimestamps();
    final String firstTime =
        rowSize > 0 && Objects.nonNull(timestamps) && timestamps.length > 0
            ? String.valueOf(timestamps[0])
            : "null";
    final String lastTime =
        rowSize > 0 && Objects.nonNull(timestamps) && timestamps.length >= rowSize
            ? String.valueOf(timestamps[rowSize - 1])
            : "null";

    return "device="
        + safeGetDeviceId(tablet)
        + ", rowSize="
        + rowSize
        + ", schemaSize="
        + schemaSize
        + ", measurements="
        + formatMeasurements(schemas)
        + ", firstTime="
        + firstTime
        + ", lastTime="
        + lastTime
        + ", markedNullCells="
        + countMarkedCells(tablet.getBitMaps(), rowSize);
  }

  public static String formatStatement(final Statement statement) {
    if (Objects.isNull(statement)) {
      return "statement=null";
    }

    if (statement instanceof InsertTabletStatement) {
      final InsertTabletStatement insertTabletStatement = (InsertTabletStatement) statement;
      return "type="
          + statement.getType()
          + ", database="
          + insertTabletStatement.getDatabaseName().orElse(null)
          + ", writeToTable="
          + insertTabletStatement.isWriteToTable()
          + ", device="
          + insertTabletStatement.getDevicePath()
          + ", rowCount="
          + insertTabletStatement.getRowCount()
          + ", measurements="
          + formatMeasurements(insertTabletStatement.getMeasurements())
          + ", firstTime="
          + firstTime(insertTabletStatement.getTimes(), insertTabletStatement.getRowCount())
          + ", lastTime="
          + lastTime(insertTabletStatement.getTimes(), insertTabletStatement.getRowCount())
          + ", markedNullCells="
          + countMarkedCells(
              insertTabletStatement.getBitMaps(), insertTabletStatement.getRowCount())
          + ", failedMeasurements="
          + insertTabletStatement.getFailedMeasurements();
    }

    if (statement instanceof InsertMultiTabletsStatement) {
      final InsertMultiTabletsStatement insertMultiTabletsStatement =
          (InsertMultiTabletsStatement) statement;
      final List<InsertTabletStatement> statements =
          insertMultiTabletsStatement.getInsertTabletStatementList();
      return "type="
          + statement.getType()
          + ", database="
          + insertMultiTabletsStatement.getDatabaseName().orElse(null)
          + ", tabletCount="
          + statements.size()
          + ", totalRows="
          + statements.stream().mapToInt(InsertTabletStatement::getRowCount).sum()
          + ", firstTablet={"
          + (statements.isEmpty() ? "null" : formatStatement(statements.get(0)))
          + "}";
    }

    if (statement instanceof InsertRowsStatement) {
      final InsertRowsStatement insertRowsStatement = (InsertRowsStatement) statement;
      return "type="
          + statement.getType()
          + ", database="
          + insertRowsStatement.getDatabaseName().orElse(null)
          + ", rowStatementCount="
          + insertRowsStatement.getInsertRowStatementList().size();
    }

    if (statement instanceof InsertRowStatement) {
      final InsertRowStatement insertRowStatement = (InsertRowStatement) statement;
      return "type="
          + statement.getType()
          + ", database="
          + insertRowStatement.getDatabaseName().orElse(null)
          + ", device="
          + insertRowStatement.getDevicePath()
          + ", time="
          + insertRowStatement.getTime()
          + ", measurements="
          + formatMeasurements(insertRowStatement.getMeasurements())
          + ", failedMeasurements="
          + insertRowStatement.getFailedMeasurements();
    }

    return "type=" + statement.getType() + ", class=" + statement.getClass().getName();
  }

  public static String formatStatus(final TSStatus status) {
    if (Objects.isNull(status)) {
      return "status=null";
    }
    return "code="
        + status.getCode()
        + ", subStatusSize="
        + status.getSubStatusSize()
        + ", message="
        + status.getMessage();
  }

  public static long countMarkedCells(final BitMap[] bitMaps, final int rowSize) {
    if (Objects.isNull(bitMaps) || rowSize <= 0) {
      return 0;
    }

    long count = 0;
    for (final BitMap bitMap : bitMaps) {
      if (Objects.isNull(bitMap)) {
        continue;
      }
      for (int i = 0; i < rowSize; ++i) {
        if (bitMap.isMarked(i)) {
          ++count;
        }
      }
    }
    return count;
  }

  private static String safeGetDeviceId(final Tablet tablet) {
    try {
      return tablet.getDeviceId();
    } catch (final Exception e) {
      return "unknown";
    }
  }

  private static String formatMeasurements(final List<IMeasurementSchema> schemas) {
    if (Objects.isNull(schemas)) {
      return "null";
    }
    return schemas.stream()
            .limit(MAX_PRINTED_MEASUREMENTS)
            .map(schema -> Objects.isNull(schema) ? "null" : schema.getMeasurementName())
            .collect(Collectors.toList())
        + (schemas.size() > MAX_PRINTED_MEASUREMENTS ? "...(" + schemas.size() + ")" : "");
  }

  private static String formatMeasurements(final String[] measurements) {
    if (Objects.isNull(measurements)) {
      return "null";
    }
    return Arrays.stream(measurements).limit(MAX_PRINTED_MEASUREMENTS).collect(Collectors.toList())
        + (measurements.length > MAX_PRINTED_MEASUREMENTS
            ? "...(" + measurements.length + ")"
            : "");
  }

  private static String firstTime(final long[] times, final int rowCount) {
    return rowCount > 0 && Objects.nonNull(times) && times.length > 0
        ? String.valueOf(times[0])
        : "null";
  }

  private static String lastTime(final long[] times, final int rowCount) {
    return rowCount > 0 && Objects.nonNull(times) && times.length >= rowCount
        ? String.valueOf(times[rowCount - 1])
        : "null";
  }
}
