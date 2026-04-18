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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Packs a column-oriented batch of OTLP rows into an {@link InsertTabletStatement} and hands it off
 * to the {@link Coordinator} for the table model. The receiver builds rows via {@link
 * OtlpTableBatch}; this class only cares about serializing those rows in the layout IoTDB expects.
 */
final class OtlpIngestor {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpIngestor.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final Binary EMPTY_BINARY = new Binary("".getBytes(StandardCharsets.UTF_8));

  private OtlpIngestor() {}

  /** Inserts a batch. No-ops if batch is empty. Returns true on success. */
  static boolean insert(
      final String database, final IClientSession session, final OtlpTableBatch batch) {
    if (batch.rowCount() == 0) {
      return true;
    }

    final SessionManager sessionManager = SessionManager.getInstance();
    final long queryId = sessionManager.requestQueryId();
    try {
      session.setDatabaseName(database);
      session.setSqlDialect(IClientSession.SqlDialect.TABLE);

      final InsertTabletStatement statement = buildStatement(batch);
      final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

      // The legacy 8-arg overload takes the tree-model Statement subtype (InsertTabletStatement).
      // The newer overloads require a relational.sql.ast.Statement, which does not apply here.
      // This is the same overload the REST /insertTablet and MQTT handlers use.
      @SuppressWarnings("deprecation")
      final ExecutionResult result =
          Coordinator.getInstance()
              .executeForTableModel(
                  statement,
                  new SqlParser(),
                  session,
                  queryId,
                  sessionManager.getSessionInfo(session),
                  "",
                  metadata,
                  CONFIG.getQueryTimeoutThreshold());
      final TSStatus status = result.status;
      final int code = status.getCode();
      if (code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        LOGGER.warn(
            "OTLP insert into {}.{} failed: code={}, message={}, rows={}",
            database,
            batch.tableName(),
            code,
            status.getMessage(),
            batch.rowCount());
        return false;
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn("OTLP insert into {}.{} threw", database, batch.tableName(), e);
      return false;
    } finally {
      Coordinator.getInstance().cleanupQueryExecution(queryId);
    }
  }

  private static InsertTabletStatement buildStatement(final OtlpTableBatch batch) {
    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDevicePath(new PartialPath(batch.tableName(), false));
    statement.setMeasurements(batch.columnNames());
    statement.setTimes(batch.times());
    statement.setDataTypes(batch.dataTypes());
    statement.setColumnCategories(batch.columnCategories());
    statement.setColumns(batch.columnValues());
    statement.setBitMaps(batch.bitMaps());
    statement.setRowCount(batch.rowCount());
    statement.setAligned(false);
    statement.setWriteToTable(true);
    return statement;
  }

  /**
   * Column-major row buffer with a fixed schema. Callers push rows with {@link #startRow(long)}
   * followed by {@code set*} calls, then hand the batch to {@link OtlpIngestor#insert}. Unset cells
   * become IoTDB nulls by way of the BitMap: the default IoTDB convention is that a fresh BitMap
   * has every position cleared (= not-null), so we pre-mark every slot and let the per-column
   * {@code set*} methods do nothing on null input — this way any slot that ends up untouched stays
   * marked.
   */
  static final class OtlpTableBatch {
    private final String tableName;
    private final String[] columnNames;
    private final TSDataType[] dataTypes;
    private final TsTableColumnCategory[] columnCategories;
    private final int capacity;

    private final long[] times;
    private final Object[] columnValues;
    private final BitMap[] bitMaps;
    private int rowCount;

    OtlpTableBatch(
        final String tableName,
        final String[] columnNames,
        final TSDataType[] dataTypes,
        final TsTableColumnCategory[] columnCategories,
        final int capacity) {
      this.tableName = tableName;
      this.columnNames = columnNames;
      this.dataTypes = dataTypes;
      this.columnCategories = columnCategories;
      this.capacity = Math.max(capacity, 1);
      this.times = new long[this.capacity];
      this.columnValues = new Object[columnNames.length];
      this.bitMaps = new BitMap[columnNames.length];
      for (int c = 0; c < columnNames.length; c++) {
        this.columnValues[c] = allocateColumn(dataTypes[c], this.capacity);
        this.bitMaps[c] = null;
      }
    }

    /** Begins a new row at {@code time}. Must be followed by set* calls for every non-null cell. */
    void startRow(final long time) {
      if (rowCount >= capacity) {
        throw new IllegalStateException(
            "OtlpTableBatch overflow: row " + rowCount + " >= capacity " + capacity);
      }
      times[rowCount] = time;
      rowCount++;
    }

    void setString(final int column, final String value) {
      if (value == null) {
        return;
      }
      final Binary[] arr = (Binary[]) columnValues[column];
      arr[rowCount - 1] =
          value.isEmpty() ? EMPTY_BINARY : new Binary(value.getBytes(StandardCharsets.UTF_8));
    }

    void setLong(final int column, final long value) {
      final long[] arr = (long[]) columnValues[column];
      arr[rowCount - 1] = value;
    }

    void setInt(final int column, final int value) {
      final int[] arr = (int[]) columnValues[column];
      arr[rowCount - 1] = value;
    }

    void setDouble(final int column, final double value) {
      final double[] arr = (double[]) columnValues[column];
      arr[rowCount - 1] = value;
    }

    String tableName() {
      return tableName;
    }

    String[] columnNames() {
      return columnNames;
    }

    TSDataType[] dataTypes() {
      return dataTypes;
    }

    TsTableColumnCategory[] columnCategories() {
      return columnCategories;
    }

    int rowCount() {
      return rowCount;
    }

    long[] times() {
      if (rowCount == capacity) {
        return times;
      }
      final long[] trimmed = new long[rowCount];
      System.arraycopy(times, 0, trimmed, 0, rowCount);
      return trimmed;
    }

    Object[] columnValues() {
      // For string columns, replace any unassigned (null) slot with EMPTY_BINARY so the writer
      // has a real Binary object to work with; the bit stays cleared so the cell still reads as
      // not-null. If you need true nullability, mark the bit explicitly before calling this.
      for (int c = 0; c < columnNames.length; c++) {
        if (columnValues[c] instanceof Binary[]) {
          final Binary[] arr = (Binary[]) columnValues[c];
          for (int r = 0; r < rowCount; r++) {
            if (arr[r] == null) {
              arr[r] = EMPTY_BINARY;
            }
          }
        }
      }
      if (rowCount == capacity) {
        return columnValues;
      }
      final Object[] trimmed = new Object[columnNames.length];
      for (int c = 0; c < columnNames.length; c++) {
        trimmed[c] = trimColumn(columnValues[c], dataTypes[c], rowCount);
      }
      return trimmed;
    }

    BitMap[] bitMaps() {
      // Fresh BitMaps: every bit zero = every cell not-null. OTLP rows always provide scalar
      // defaults (empty strings, 0 ints) rather than true nulls, so we never mark anything.
      final BitMap[] out = new BitMap[columnNames.length];
      for (int c = 0; c < columnNames.length; c++) {
        out[c] = new BitMap(rowCount);
      }
      return out;
    }

    private static Object allocateColumn(final TSDataType type, final int capacity) {
      switch (type) {
        case INT32:
          return new int[capacity];
        case INT64:
        case TIMESTAMP:
          return new long[capacity];
        case FLOAT:
          return new float[capacity];
        case DOUBLE:
          return new double[capacity];
        case BOOLEAN:
          return new boolean[capacity];
        case TEXT:
        case STRING:
        case BLOB:
          return new Binary[capacity];
        default:
          throw new UnsupportedOperationException("OTLP: unsupported TSDataType " + type);
      }
    }

    private static Object trimColumn(final Object source, final TSDataType type, final int length) {
      switch (type) {
        case INT32:
          {
            final int[] trimmed = new int[length];
            System.arraycopy(source, 0, trimmed, 0, length);
            return trimmed;
          }
        case INT64:
        case TIMESTAMP:
          {
            final long[] trimmed = new long[length];
            System.arraycopy(source, 0, trimmed, 0, length);
            return trimmed;
          }
        case FLOAT:
          {
            final float[] trimmed = new float[length];
            System.arraycopy(source, 0, trimmed, 0, length);
            return trimmed;
          }
        case DOUBLE:
          {
            final double[] trimmed = new double[length];
            System.arraycopy(source, 0, trimmed, 0, length);
            return trimmed;
          }
        case BOOLEAN:
          {
            final boolean[] trimmed = new boolean[length];
            System.arraycopy(source, 0, trimmed, 0, length);
            return trimmed;
          }
        case TEXT:
        case STRING:
        case BLOB:
          {
            final Binary[] trimmed = new Binary[length];
            System.arraycopy(source, 0, trimmed, 0, length);
            return trimmed;
          }
        default:
          throw new UnsupportedOperationException("OTLP: unsupported TSDataType " + type);
      }
    }
  }
}
