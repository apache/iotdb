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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Converts IoTConsensus WAL log entries (InsertNode) to Tablet format for subscription. */
public class ConsensusLogToTabletConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusLogToTabletConverter.class);

  private final TreePattern treePattern;
  private final TablePattern tablePattern;

  /**
   * The actual database name of the DataRegion this converter processes (table-model format without
   * "root." prefix). Null for tree-model topics.
   */
  private final String databaseName;

  public ConsensusLogToTabletConverter(
      final TreePattern treePattern, final TablePattern tablePattern, final String databaseName) {
    this.treePattern = treePattern;
    this.tablePattern = tablePattern;
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  static String safeDeviceIdForLog(final InsertNode node) {
    try {
      final Object deviceId = node.getDeviceID();
      return deviceId != null ? deviceId.toString() : "null";
    } catch (final Exception e) {
      return "N/A(" + node.getType() + ")";
    }
  }

  public List<Tablet> convert(final InsertNode insertNode) {
    if (Objects.isNull(insertNode)) {
      return Collections.emptyList();
    }

    final PlanNodeType nodeType = insertNode.getType();
    if (nodeType == null) {
      LOGGER.warn("InsertNode type is null, skipping conversion");
      return Collections.emptyList();
    }

    LOGGER.debug(
        "ConsensusLogToTabletConverter: converting InsertNode type={}, deviceId={}",
        nodeType,
        safeDeviceIdForLog(insertNode));

    switch (nodeType) {
      case INSERT_ROW:
        return convertInsertRowNode((InsertRowNode) insertNode);
      case INSERT_TABLET:
        return convertInsertTabletNode((InsertTabletNode) insertNode);
      case INSERT_ROWS:
        return convertInsertRowsNode((InsertRowsNode) insertNode);
      case INSERT_ROWS_OF_ONE_DEVICE:
        return convertInsertRowsOfOneDeviceNode((InsertRowsOfOneDeviceNode) insertNode);
      case INSERT_MULTI_TABLET:
        return convertInsertMultiTabletsNode((InsertMultiTabletsNode) insertNode);
      case RELATIONAL_INSERT_ROW:
        return convertRelationalInsertRowNode((RelationalInsertRowNode) insertNode);
      case RELATIONAL_INSERT_TABLET:
        return convertRelationalInsertTabletNode((RelationalInsertTabletNode) insertNode);
      case RELATIONAL_INSERT_ROWS:
        return convertRelationalInsertRowsNode((RelationalInsertRowsNode) insertNode);
      default:
        LOGGER.debug("Unsupported InsertNode type for subscription: {}", nodeType);
        return Collections.emptyList();
    }
  }

  // ======================== Tree Model Conversion ========================

  private List<Tablet> convertInsertRowNode(final InsertRowNode node) {
    final IDeviceID deviceId = node.getDeviceID();

    // Device-level path filtering
    if (treePattern != null && !treePattern.mayOverlapWithDevice(deviceId)) {
      return Collections.emptyList();
    }

    final long time = node.getTime();

    // Determine which columns match the pattern
    final String[] measurements = node.getMeasurements();
    final TSDataType[] dataTypes = node.getDataTypes();
    final Object[] values = node.getValues();
    final List<Integer> matchedColumnIndices = getMatchedTreeColumnIndices(deviceId, measurements);

    if (matchedColumnIndices.isEmpty()) {
      return Collections.emptyList();
    }

    // Build Tablet with matched columns
    final int columnCount = matchedColumnIndices.size();
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (final int colIdx : matchedColumnIndices) {
      schemas.add(new MeasurementSchema(measurements[colIdx], dataTypes[colIdx]));
    }

    final Tablet tablet = new Tablet(deviceId.toString(), schemas, 1 /* maxRowNumber */);
    tablet.addTimestamp(0, time);

    for (int i = 0; i < columnCount; i++) {
      final int originalColIdx = matchedColumnIndices.get(i);
      final Object value = values[originalColIdx];
      if (value == null) {
        if (tablet.getBitMaps() == null) {
          tablet.initBitMaps();
        }
        tablet.getBitMaps()[i].mark(0);
      } else {
        addValueToTablet(tablet, 0, i, dataTypes[originalColIdx], value);
      }
    }
    tablet.setRowSize(1);

    return Collections.singletonList(tablet);
  }

  private List<Tablet> convertInsertTabletNode(final InsertTabletNode node) {
    final IDeviceID deviceId = node.getDeviceID();

    // Device-level path filtering
    if (treePattern != null && !treePattern.mayOverlapWithDevice(deviceId)) {
      return Collections.emptyList();
    }

    final String[] measurements = node.getMeasurements();
    final TSDataType[] dataTypes = node.getDataTypes();
    final long[] times = node.getTimes();
    final Object[] columns = node.getColumns();
    final BitMap[] bitMaps = node.getBitMaps();
    final int rowCount = node.getRowCount();

    // Column filtering
    final List<Integer> matchedColumnIndices = getMatchedTreeColumnIndices(deviceId, measurements);
    if (matchedColumnIndices.isEmpty()) {
      return Collections.emptyList();
    }

    final int columnCount = matchedColumnIndices.size();
    final boolean allColumnsMatch = (columnCount == measurements.length);

    // Build schemas (always needed)
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (final int colIdx : matchedColumnIndices) {
      schemas.add(new MeasurementSchema(measurements[colIdx], dataTypes[colIdx]));
    }

    // Build column arrays and bitmaps using bulk copy
    final long[] newTimes = Arrays.copyOf(times, rowCount);
    final Object[] newColumns = new Object[columnCount];
    final BitMap[] newBitMaps = new BitMap[columnCount];

    for (int i = 0; i < columnCount; i++) {
      final int originalColIdx = allColumnsMatch ? i : matchedColumnIndices.get(i);
      newColumns[i] = copyColumnArray(dataTypes[originalColIdx], columns[originalColIdx], rowCount);
      if (bitMaps != null && bitMaps[originalColIdx] != null) {
        newBitMaps[i] = new BitMap(rowCount);
        BitMap.copyOfRange(bitMaps[originalColIdx], 0, newBitMaps[i], 0, rowCount);
      }
    }

    final Tablet tablet =
        new Tablet(deviceId.toString(), schemas, newTimes, newColumns, newBitMaps, rowCount);

    return Collections.singletonList(tablet);
  }

  private List<Tablet> convertInsertRowsNode(final InsertRowsNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    for (final InsertRowNode rowNode : node.getInsertRowNodeList()) {
      // Handle merge bug: RelationalInsertRowNode.mergeInsertNode() is not overridden,
      // so merged relational nodes arrive as InsertRowsNode (tree) with RelationalInsertRowNode
      // children. Dispatch correctly by checking the actual child type.
      if (rowNode instanceof RelationalInsertRowNode) {
        tablets.addAll(convertRelationalInsertRowNode((RelationalInsertRowNode) rowNode));
      } else {
        tablets.addAll(convertInsertRowNode(rowNode));
      }
    }
    return tablets;
  }

  private List<Tablet> convertInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    for (final InsertRowNode rowNode : node.getInsertRowNodeList()) {
      tablets.addAll(convertInsertRowNode(rowNode));
    }
    return tablets;
  }

  private List<Tablet> convertInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    for (final InsertTabletNode tabletNode : node.getInsertTabletNodeList()) {
      tablets.addAll(convertInsertTabletNode(tabletNode));
    }
    return tablets;
  }

  // ======================== Table Model Conversion ========================

  private List<Tablet> convertRelationalInsertRowNode(final RelationalInsertRowNode node) {
    final String tableName = node.getTableName();

    // Table-level pattern filtering
    if (tablePattern != null) {
      if (databaseName != null && !tablePattern.matchesDatabase(databaseName)) {
        return Collections.emptyList();
      }
      if (tableName != null && !tablePattern.matchesTable(tableName)) {
        return Collections.emptyList();
      }
    }

    final long time = node.getTime();
    final String[] measurements = node.getMeasurements();
    final TSDataType[] dataTypes = node.getDataTypes();
    final Object[] values = node.getValues();

    final int columnCount = measurements.length;
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      schemas.add(new MeasurementSchema(measurements[i], dataTypes[i]));
    }

    final Tablet tablet = new Tablet(tableName != null ? tableName : "", schemas, 1);
    tablet.addTimestamp(0, time);

    for (int i = 0; i < columnCount; i++) {
      final Object value = values[i];
      if (value == null) {
        if (tablet.getBitMaps() == null) {
          tablet.initBitMaps();
        }
        tablet.getBitMaps()[i].mark(0);
      } else {
        addValueToTablet(tablet, 0, i, dataTypes[i], value);
      }
    }
    tablet.setRowSize(1);

    return Collections.singletonList(tablet);
  }

  private List<Tablet> convertRelationalInsertTabletNode(final RelationalInsertTabletNode node) {
    final String tableName = node.getTableName();

    // Table-level pattern filtering
    if (tablePattern != null) {
      if (databaseName != null && !tablePattern.matchesDatabase(databaseName)) {
        return Collections.emptyList();
      }
      if (tableName != null && !tablePattern.matchesTable(tableName)) {
        return Collections.emptyList();
      }
    }

    final String[] measurements = node.getMeasurements();
    final TSDataType[] dataTypes = node.getDataTypes();
    final long[] times = node.getTimes();
    final Object[] columns = node.getColumns();
    final BitMap[] bitMaps = node.getBitMaps();
    final int rowCount = node.getRowCount();

    final int columnCount = measurements.length;
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      schemas.add(new MeasurementSchema(measurements[i], dataTypes[i]));
    }

    // Build column arrays and bitmaps using bulk copy
    final long[] newTimes = Arrays.copyOf(times, rowCount);
    final Object[] newColumns = new Object[columnCount];
    final BitMap[] newBitMaps = new BitMap[columnCount];

    for (int colIdx = 0; colIdx < columnCount; colIdx++) {
      newColumns[colIdx] = copyColumnArray(dataTypes[colIdx], columns[colIdx], rowCount);
      if (bitMaps != null && bitMaps[colIdx] != null) {
        newBitMaps[colIdx] = new BitMap(rowCount);
        BitMap.copyOfRange(bitMaps[colIdx], 0, newBitMaps[colIdx], 0, rowCount);
      }
    }

    final Tablet tablet =
        new Tablet(
            tableName != null ? tableName : "",
            schemas,
            newTimes,
            newColumns,
            newBitMaps,
            rowCount);

    return Collections.singletonList(tablet);
  }

  private List<Tablet> convertRelationalInsertRowsNode(final RelationalInsertRowsNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    for (final InsertRowNode rowNode : node.getInsertRowNodeList()) {
      tablets.addAll(convertRelationalInsertRowNode((RelationalInsertRowNode) rowNode));
    }
    return tablets;
  }

  // ======================== Helper Methods ========================

  /**
   * Returns indices of columns that match the tree pattern. If no tree pattern is specified, all
   * column indices are returned.
   */
  private List<Integer> getMatchedTreeColumnIndices(
      final IDeviceID deviceId, final String[] measurements) {
    if (treePattern == null || treePattern.isRoot() || treePattern.coversDevice(deviceId)) {
      // All columns match
      final List<Integer> allIndices = new ArrayList<>(measurements.length);
      for (int i = 0; i < measurements.length; i++) {
        if (measurements[i] != null) {
          allIndices.add(i);
        }
      }
      return allIndices;
    }

    final List<Integer> matchedIndices = new ArrayList<>();
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null && treePattern.matchesMeasurement(deviceId, measurements[i])) {
        matchedIndices.add(i);
      }
    }
    return matchedIndices;
  }

  /**
   * Bulk-copies a typed column array using System.arraycopy. Returns a new array of the same type
   * containing the first {@code rowCount} elements.
   */
  private Object copyColumnArray(
      final TSDataType dataType, final Object sourceColumn, final int rowCount) {
    switch (dataType) {
      case BOOLEAN:
        {
          final boolean[] src = (boolean[]) sourceColumn;
          final boolean[] dst = new boolean[rowCount];
          System.arraycopy(src, 0, dst, 0, rowCount);
          return dst;
        }
      case INT32:
      case DATE:
        {
          final int[] src = (int[]) sourceColumn;
          final int[] dst = new int[rowCount];
          System.arraycopy(src, 0, dst, 0, rowCount);
          return dst;
        }
      case INT64:
      case TIMESTAMP:
        {
          final long[] src = (long[]) sourceColumn;
          final long[] dst = new long[rowCount];
          System.arraycopy(src, 0, dst, 0, rowCount);
          return dst;
        }
      case FLOAT:
        {
          final float[] src = (float[]) sourceColumn;
          final float[] dst = new float[rowCount];
          System.arraycopy(src, 0, dst, 0, rowCount);
          return dst;
        }
      case DOUBLE:
        {
          final double[] src = (double[]) sourceColumn;
          final double[] dst = new double[rowCount];
          System.arraycopy(src, 0, dst, 0, rowCount);
          return dst;
        }
      case TEXT:
      case BLOB:
      case STRING:
        {
          final Binary[] src = (Binary[]) sourceColumn;
          final Binary[] dst = new Binary[rowCount];
          System.arraycopy(src, 0, dst, 0, rowCount);
          return dst;
        }
      default:
        LOGGER.warn("Unsupported data type for bulk copy: {}", dataType);
        return sourceColumn;
    }
  }

  /**
   * Adds a single value to the tablet at the specified position.
   *
   * <p>IMPORTANT: In tsfile-2.2.1, Tablet.addTimestamp() calls initBitMapsWithApiUsage() which
   * creates bitMaps and marks ALL positions as null via markAll(). Since we write values directly
   * to the underlying typed arrays (bypassing the Tablet.addValue() API which would call
   * updateBitMap to unmark), we must explicitly unmark the bitmap position to indicate the value is
   * NOT null.
   */
  private void addValueToTablet(
      final Tablet tablet,
      final int rowIndex,
      final int columnIndex,
      final TSDataType dataType,
      final Object value) {
    switch (dataType) {
      case BOOLEAN:
        ((boolean[]) tablet.getValues()[columnIndex])[rowIndex] = (boolean) value;
        break;
      case INT32:
      case DATE:
        ((int[]) tablet.getValues()[columnIndex])[rowIndex] = (int) value;
        break;
      case INT64:
      case TIMESTAMP:
        ((long[]) tablet.getValues()[columnIndex])[rowIndex] = (long) value;
        break;
      case FLOAT:
        ((float[]) tablet.getValues()[columnIndex])[rowIndex] = (float) value;
        break;
      case DOUBLE:
        ((double[]) tablet.getValues()[columnIndex])[rowIndex] = (double) value;
        break;
      case TEXT:
      case BLOB:
      case STRING:
        ((Binary[]) tablet.getValues()[columnIndex])[rowIndex] = (Binary) value;
        break;
      default:
        LOGGER.warn("Unsupported data type: {}", dataType);
        return;
    }
    // Unmark the bitmap position to indicate this value is NOT null.
    // addTimestamp() triggers initBitMapsWithApiUsage() which marks all positions as null.
    final BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null && bitMaps[columnIndex] != null) {
      bitMaps[columnIndex].unmark(rowIndex);
    }
  }

  /** Copies a single column value from the source column array to the tablet. */
  private void copyColumnValue(
      final Tablet tablet,
      final int targetRowIndex,
      final int targetColumnIndex,
      final TSDataType dataType,
      final Object sourceColumn,
      final int sourceRowIndex) {
    switch (dataType) {
      case BOOLEAN:
        ((boolean[]) tablet.getValues()[targetColumnIndex])[targetRowIndex] =
            ((boolean[]) sourceColumn)[sourceRowIndex];
        break;
      case INT32:
      case DATE:
        ((int[]) tablet.getValues()[targetColumnIndex])[targetRowIndex] =
            ((int[]) sourceColumn)[sourceRowIndex];
        break;
      case INT64:
      case TIMESTAMP:
        ((long[]) tablet.getValues()[targetColumnIndex])[targetRowIndex] =
            ((long[]) sourceColumn)[sourceRowIndex];
        break;
      case FLOAT:
        ((float[]) tablet.getValues()[targetColumnIndex])[targetRowIndex] =
            ((float[]) sourceColumn)[sourceRowIndex];
        break;
      case DOUBLE:
        ((double[]) tablet.getValues()[targetColumnIndex])[targetRowIndex] =
            ((double[]) sourceColumn)[sourceRowIndex];
        break;
      case TEXT:
      case BLOB:
      case STRING:
        ((Binary[]) tablet.getValues()[targetColumnIndex])[targetRowIndex] =
            ((Binary[]) sourceColumn)[sourceRowIndex];
        break;
      default:
        LOGGER.warn("Unsupported data type for copy: {}", dataType);
        return;
    }
    // Unmark the bitmap position to indicate this value is NOT null.
    final BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null && bitMaps[targetColumnIndex] != null) {
      bitMaps[targetColumnIndex].unmark(targetRowIndex);
    }
  }
}
