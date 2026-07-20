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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.columnfilter.TabletColumnPruner;

import org.apache.tsfile.enums.ColumnCategory;
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
  private final String topicName;
  private final ColumnFilterMatcher fallbackColumnFilterMatcher;

  /**
   * The actual database name of the DataRegion this converter processes (table-model format without
   * "root." prefix). Null for tree-model topics.
   */
  private final String databaseName;

  public ConsensusLogToTabletConverter(
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final ColumnFilterMatcher columnFilterMatcher,
      final String databaseName) {
    this(treePattern, tablePattern, null, columnFilterMatcher, databaseName);
  }

  public ConsensusLogToTabletConverter(
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String topicName,
      final ColumnFilterMatcher columnFilterMatcher,
      final String databaseName) {
    this.treePattern = treePattern;
    this.tablePattern = tablePattern;
    this.topicName = topicName;
    this.fallbackColumnFilterMatcher =
        Objects.nonNull(columnFilterMatcher) ? columnFilterMatcher : ColumnFilterMatcher.matchAll();
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

  /**
   * Deserializes the IConsensusRequest entries within an IndexedConsensusRequest to produce an
   * InsertNode. WAL entries are typically stored as IoTConsensusRequest (serialized ByteBuffers),
   * and a single logical write may be split across multiple fragments (SearchNode).
   *
   * <p>The deserialization follows the same pattern as {@code
   * DataRegionStateMachine.grabPlanNode()}.
   */
  static InsertNode deserializeToInsertNode(final IndexedConsensusRequest indexedRequest) {
    final List<SearchNode> searchNodes = new ArrayList<>();
    PlanNode nonSearchNode = null;

    for (final IConsensusRequest req : indexedRequest.getRequests()) {
      PlanNode planNode;
      try {
        if (req instanceof IoTConsensusRequest) {
          // WAL entries read from file are wrapped as IoTConsensusRequest (ByteBuffer).
          planNode = WALEntry.deserializeForConsensus(req.serializeToByteBuffer());
        } else if (req instanceof InsertNode) {
          // In-memory entries that are not yet flushed to WAL file may already be PlanNode.
          planNode = (PlanNode) req;
        } else {
          planNode = PlanNodeType.deserialize(req.serializeToByteBuffer());
        }
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_FAILED_TO_DESERIALIZE_ICONSENSUSREQUEST_EC1F6BAD,
            req.getClass().getSimpleName(),
            indexedRequest.getSearchIndex(),
            e.getMessage(),
            e);
        continue;
      }

      if (planNode instanceof SearchNode) {
        final SearchNode searchNode = (SearchNode) planNode;
        searchNode.setSearchIndex(indexedRequest.getSearchIndex());
        if (indexedRequest.getSyncIndex() >= 0) {
          searchNode.setSyncIndex(indexedRequest.getSyncIndex());
        }
        if (indexedRequest.getPhysicalTime() > 0) {
          searchNode.setPhysicalTime(indexedRequest.getPhysicalTime());
        }
        if (indexedRequest.getNodeId() >= 0) {
          searchNode.setNodeId(indexedRequest.getNodeId());
        }
        searchNodes.add(searchNode);
      } else {
        nonSearchNode = planNode;
      }
    }

    if (!searchNodes.isEmpty()) {
      final PlanNode merged = searchNodes.get(0).merge(searchNodes);
      if (merged instanceof InsertNode) {
        final InsertNode mergedInsert = (InsertNode) merged;
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_DESERIALIZED_MERGED_INSERTNODE_51FB8295,
            indexedRequest.getSearchIndex(),
            mergedInsert.getType(),
            safeDeviceIdForLog(mergedInsert),
            searchNodes.size());

        return mergedInsert;
      }
    }

    if (nonSearchNode != null) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_SEARCHINDEX_CONTAINS_NON_INSERTNODE_CFA9FA49,
          indexedRequest.getSearchIndex(),
          nonSearchNode.getClass().getSimpleName());
    }

    return null;
  }

  public List<Tablet> convert(final InsertNode insertNode) {
    if (Objects.isNull(insertNode)) {
      return Collections.emptyList();
    }

    final PlanNodeType nodeType = insertNode.getType();
    if (nodeType == null) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_LOG_INSERTNODE_TYPE_IS_NULL_SKIPPING_CONVERSION_A2F1ADF7);
      return Collections.emptyList();
    }

    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_CONVERTING_INSERTNODE_TYPE_B80428A0,
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
        LOGGER.debug(
            DataNodePipeMessages.PIPE_LOG_UNSUPPORTED_INSERTNODE_TYPE_FOR_SUBSCRIPTION_E488EF74,
            nodeType);
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
    final List<Integer> matchedColumnIndices =
        getMatchedTreeColumnIndices(deviceId, measurements, dataTypes, values, false);

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
    if (node instanceof RelationalInsertTabletNode) {
      return convertRelationalInsertTabletNode((RelationalInsertTabletNode) node);
    }

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
    final List<Integer> matchedColumnIndices =
        getMatchedTreeColumnIndices(deviceId, measurements, dataTypes, columns, true);
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

    // Column filtering changes only the tablet shape. The selected value arrays come from WAL
    // InsertNodes and are reused by the subscription read path.
    final long[] newTimes = times;
    final Object[] newColumns = new Object[columnCount];
    final BitMap[] newBitMaps = new BitMap[columnCount];

    for (int i = 0; i < columnCount; i++) {
      final int originalColIdx = allColumnsMatch ? i : matchedColumnIndices.get(i);
      newColumns[i] = columns[originalColIdx];
      if (bitMaps != null && bitMaps[originalColIdx] != null) {
        newBitMaps[i] = bitMaps[originalColIdx];
      }
    }

    final Tablet tablet =
        new Tablet(deviceId.toString(), schemas, newTimes, newColumns, newBitMaps, rowCount);

    return Collections.singletonList(tablet);
  }

  private List<Tablet> convertInsertRowsNode(final InsertRowsNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    final List<InsertRowNode> pendingTreeRows = new ArrayList<>();
    for (final InsertRowNode rowNode : node.getInsertRowNodeList()) {
      // Handle merge bug: RelationalInsertRowNode.mergeInsertNode() is not overridden,
      // so merged relational nodes arrive as InsertRowsNode (tree) with RelationalInsertRowNode
      // children. Dispatch correctly by checking the actual child type.
      if (rowNode instanceof RelationalInsertRowNode) {
        tablets.addAll(convertTreeInsertRowNodes(pendingTreeRows));
        pendingTreeRows.clear();
        tablets.addAll(convertRelationalInsertRowNode((RelationalInsertRowNode) rowNode));
      } else {
        pendingTreeRows.add(rowNode);
      }
    }
    tablets.addAll(convertTreeInsertRowNodes(pendingTreeRows));
    return tablets;
  }

  private List<Tablet> convertInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    return convertTreeInsertRowNodes(node.getInsertRowNodeList());
  }

  private List<Tablet> convertTreeInsertRowNodes(final List<InsertRowNode> rowNodes) {
    if (rowNodes.isEmpty()) {
      return Collections.emptyList();
    }

    final List<Tablet> tablets = new ArrayList<>();
    final List<MatchedTreeRow> pendingRows = new ArrayList<>();
    TreeRowGroupKey pendingKey = null;

    for (final InsertRowNode rowNode : rowNodes) {
      final MatchedTreeRow matchedRow = matchTreeInsertRowNode(rowNode);
      if (matchedRow == null) {
        continue;
      }

      if (pendingKey != null && !pendingKey.equals(matchedRow.groupKey)) {
        tablets.add(buildTreeRowsTablet(pendingKey, pendingRows));
        pendingRows.clear();
      }
      // Batch only adjacent rows with the same emitted schema, preserving cross-device row order.
      pendingKey = matchedRow.groupKey;
      pendingRows.add(matchedRow);
    }

    if (!pendingRows.isEmpty()) {
      tablets.add(buildTreeRowsTablet(pendingKey, pendingRows));
    }
    return tablets;
  }

  private MatchedTreeRow matchTreeInsertRowNode(final InsertRowNode node) {
    final IDeviceID deviceId = node.getDeviceID();
    if (treePattern != null && !treePattern.mayOverlapWithDevice(deviceId)) {
      return null;
    }

    final String[] measurements = node.getMeasurements();
    final TSDataType[] dataTypes = node.getDataTypes();
    final Object[] values = node.getValues();
    final List<Integer> matchedColumnIndices =
        getMatchedTreeColumnIndices(deviceId, measurements, dataTypes, values, false);
    return matchedColumnIndices.isEmpty()
        ? null
        : new MatchedTreeRow(
            node,
            new TreeRowGroupKey(deviceId, measurements, dataTypes, matchedColumnIndices),
            matchedColumnIndices);
  }

  private Tablet buildTreeRowsTablet(
      final TreeRowGroupKey groupKey, final List<MatchedTreeRow> rows) {
    final int columnCount = groupKey.measurements.length;
    final List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (int colIdx = 0; colIdx < columnCount; colIdx++) {
      schemas.add(new MeasurementSchema(groupKey.measurements[colIdx], groupKey.dataTypes[colIdx]));
    }

    final Tablet tablet = new Tablet(groupKey.deviceId, schemas, rows.size());
    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      final MatchedTreeRow matchedRow = rows.get(rowIndex);
      final InsertRowNode rowNode = matchedRow.rowNode;
      final Object[] values = rowNode.getValues();
      final TSDataType[] dataTypes = rowNode.getDataTypes();
      tablet.addTimestamp(rowIndex, rowNode.getTime());

      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        final int originalColIdx = matchedRow.matchedColumnIndices.get(columnIndex);
        final Object value = values[originalColIdx];
        if (value == null) {
          if (tablet.getBitMaps() == null) {
            tablet.initBitMaps();
          }
          tablet.getBitMaps()[columnIndex].mark(rowIndex);
        } else {
          addValueToTablet(tablet, rowIndex, columnIndex, dataTypes[originalColIdx], value);
        }
      }
    }
    tablet.setRowSize(rows.size());
    return tablet;
  }

  private List<Tablet> convertInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    for (final InsertTabletNode tabletNode : node.getInsertTabletNodeList()) {
      // Handle merge bug: RelationalInsertTabletNode.mergeInsertNode() is not overridden,
      // so merged relational tablets arrive as InsertMultiTabletsNode (tree) with
      // RelationalInsertTabletNode children. Dispatch correctly by checking the actual child type.
      if (tabletNode instanceof RelationalInsertTabletNode) {
        tablets.addAll(convertRelationalInsertTabletNode((RelationalInsertTabletNode) tabletNode));
      } else {
        tablets.addAll(convertInsertTabletNode(tabletNode));
      }
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
    final Tablet tablet =
        buildTableModelTabletFromRow(
            tableName, time, measurements, dataTypes, values, node.getColumnCategories());
    if (Objects.isNull(tablet)) {
      return Collections.emptyList();
    }

    final Tablet prunedTablet =
        TabletColumnPruner.pruneTableModelTablet(tablet, databaseName, getColumnFilterMatcher());
    return Objects.nonNull(prunedTablet)
        ? Collections.singletonList(prunedTablet)
        : Collections.emptyList();
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
    if (Objects.isNull(measurements)
        || Objects.isNull(dataTypes)
        || Objects.isNull(node.getColumns())) {
      LOGGER.warn(
          "Malformed RelationalInsertTabletNode with null measurements/dataTypes/columns, "
              + "skipping conversion");
      return Collections.emptyList();
    }
    final Object[] columns = node.getColumns();
    final BitMap[] bitMaps = node.getBitMaps();
    final List<IMeasurementSchema> schemas = new ArrayList<>(measurements.length);
    final List<ColumnCategory> columnTypes = new ArrayList<>(measurements.length);
    final List<Object> validColumns = new ArrayList<>(measurements.length);
    final List<BitMap> validBitMaps =
        Objects.nonNull(bitMaps) ? new ArrayList<>(measurements.length) : null;
    for (int i = 0; i < measurements.length; i++) {
      if (!isValidColumn(measurements, dataTypes, columns, i, false)) {
        LOGGER.warn(
            "Skipping malformed RelationalInsertTabletNode column at index {} "
                + "(measurements={}, dataTypes={}, columns={})",
            i,
            measurements.length,
            dataTypes.length,
            columns.length);
        continue;
      }
      schemas.add(new MeasurementSchema(measurements[i], dataTypes[i]));
      columnTypes.add(toTsFileColumnCategory(node.getColumnCategories(), i));
      validColumns.add(columns[i]);
      if (Objects.nonNull(validBitMaps)) {
        validBitMaps.add(i < bitMaps.length ? bitMaps[i] : null);
      }
    }
    if (schemas.isEmpty()) {
      return Collections.emptyList();
    }
    final Tablet tablet =
        new Tablet(
            tableName != null ? tableName : "",
            schemas,
            columnTypes,
            node.getTimes(),
            validColumns.toArray(new Object[0]),
            Objects.nonNull(validBitMaps) ? validBitMaps.toArray(new BitMap[0]) : null,
            node.getRowCount());

    final Tablet prunedTablet =
        TabletColumnPruner.pruneTableModelTablet(tablet, databaseName, getColumnFilterMatcher());
    return Objects.nonNull(prunedTablet)
        ? Collections.singletonList(prunedTablet)
        : Collections.emptyList();
  }

  private List<Tablet> convertRelationalInsertRowsNode(final RelationalInsertRowsNode node) {
    final List<Tablet> tablets = new ArrayList<>();
    for (final InsertRowNode rowNode : node.getInsertRowNodeList()) {
      tablets.addAll(convertRelationalInsertRowNode((RelationalInsertRowNode) rowNode));
    }
    return tablets;
  }

  private Tablet buildTableModelTabletFromRow(
      final String tableName,
      final long time,
      final String[] measurements,
      final TSDataType[] dataTypes,
      final Object[] values,
      final TsTableColumnCategory[] columnCategories) {
    if (Objects.isNull(measurements) || Objects.isNull(dataTypes) || Objects.isNull(values)) {
      return null;
    }

    final List<String> columnNames = new ArrayList<>(measurements.length);
    final List<TSDataType> columnDataTypes = new ArrayList<>(measurements.length);
    final List<ColumnCategory> columnTypes = new ArrayList<>(measurements.length);
    final List<Integer> originalColumnIndexes = new ArrayList<>(measurements.length);
    for (int i = 0; i < measurements.length && i < dataTypes.length && i < values.length; i++) {
      if (Objects.isNull(measurements[i]) || Objects.isNull(dataTypes[i])) {
        continue;
      }
      columnNames.add(measurements[i]);
      columnDataTypes.add(dataTypes[i]);
      columnTypes.add(toTsFileColumnCategory(columnCategories, i));
      originalColumnIndexes.add(i);
    }
    if (columnNames.isEmpty()) {
      return null;
    }

    final Tablet tablet =
        new Tablet(
            Objects.nonNull(tableName) ? tableName : "",
            columnNames,
            columnDataTypes,
            columnTypes,
            1);
    tablet.addTimestamp(0, time);
    for (int i = 0; i < originalColumnIndexes.size(); i++) {
      final int originalColumnIndex = originalColumnIndexes.get(i);
      final Object value = values[originalColumnIndex];
      if (Objects.isNull(value)) {
        if (Objects.isNull(tablet.getBitMaps())) {
          tablet.initBitMaps();
        }
        tablet.getBitMaps()[i].mark(0);
      } else {
        addValueToTablet(tablet, 0, i, dataTypes[originalColumnIndex], value);
      }
    }
    tablet.setRowSize(1);
    return tablet;
  }

  // ======================== Helper Methods ========================

  /**
   * Returns indices of columns that match the tree pattern. If no tree pattern is specified, all
   * column indices are returned.
   */
  private List<Integer> getMatchedTreeColumnIndices(
      final IDeviceID deviceId,
      final String[] measurements,
      final TSDataType[] dataTypes,
      final Object[] valuesOrColumns,
      final boolean requireNonNullValue) {
    if (measurements == null) {
      return Collections.emptyList();
    }
    if (treePattern == null || treePattern.isRoot() || treePattern.coversDevice(deviceId)) {
      // All columns match
      final List<Integer> allIndices = new ArrayList<>(measurements.length);
      for (int i = 0; i < measurements.length; i++) {
        if (isValidColumn(measurements, dataTypes, valuesOrColumns, i, requireNonNullValue)) {
          allIndices.add(i);
        }
      }
      return allIndices;
    }

    final List<Integer> matchedIndices = new ArrayList<>();
    for (int i = 0; i < measurements.length; i++) {
      if (isValidColumn(measurements, dataTypes, valuesOrColumns, i, requireNonNullValue)
          && treePattern.matchesMeasurement(deviceId, measurements[i])) {
        matchedIndices.add(i);
      }
    }
    return matchedIndices;
  }

  private ColumnCategory toTsFileColumnCategory(
      final TsTableColumnCategory[] columnCategories, final int columnIndex) {
    return columnCategories != null
            && columnIndex < columnCategories.length
            && columnCategories[columnIndex] != null
        ? columnCategories[columnIndex].toTsFileColumnType()
        : ColumnCategory.FIELD;
  }

  private ColumnFilterMatcher getColumnFilterMatcher() {
    return Objects.nonNull(topicName)
        ? SubscriptionAgent.broker().getColumnFilterMatcher(topicName)
        : fallbackColumnFilterMatcher;
  }

  private boolean isValidColumn(
      final String[] measurements,
      final TSDataType[] dataTypes,
      final Object[] valuesOrColumns,
      final int index,
      final boolean requireNonNullValue) {
    return measurements != null
        && index >= 0
        && index < measurements.length
        && measurements[index] != null
        && dataTypes != null
        && index < dataTypes.length
        && dataTypes[index] != null
        && valuesOrColumns != null
        && index < valuesOrColumns.length
        && (!requireNonNullValue || valuesOrColumns[index] != null);
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
        LOGGER.warn(DataNodePipeMessages.PIPE_LOG_UNSUPPORTED_DATA_TYPE_C8929F11, dataType);
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
        LOGGER.warn(
            DataNodePipeMessages.PIPE_LOG_UNSUPPORTED_DATA_TYPE_FOR_COPY_8AD25FE7, dataType);
        return;
    }
    // Unmark the bitmap position to indicate this value is NOT null.
    final BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null && bitMaps[targetColumnIndex] != null) {
      bitMaps[targetColumnIndex].unmark(targetRowIndex);
    }
  }

  private static final class MatchedTreeRow {

    private final InsertRowNode rowNode;
    private final TreeRowGroupKey groupKey;
    private final List<Integer> matchedColumnIndices;

    private MatchedTreeRow(
        final InsertRowNode rowNode,
        final TreeRowGroupKey groupKey,
        final List<Integer> matchedColumnIndices) {
      this.rowNode = rowNode;
      this.groupKey = groupKey;
      this.matchedColumnIndices = matchedColumnIndices;
    }
  }

  private static final class TreeRowGroupKey {

    private final String deviceId;
    private final String[] measurements;
    private final TSDataType[] dataTypes;

    private TreeRowGroupKey(
        final IDeviceID deviceId,
        final String[] measurements,
        final TSDataType[] dataTypes,
        final List<Integer> matchedColumnIndices) {
      this.deviceId = deviceId.toString();
      this.measurements = new String[matchedColumnIndices.size()];
      this.dataTypes = new TSDataType[matchedColumnIndices.size()];
      for (int i = 0; i < matchedColumnIndices.size(); i++) {
        final int originalColIdx = matchedColumnIndices.get(i);
        this.measurements[i] = measurements[originalColIdx];
        this.dataTypes[i] = dataTypes[originalColIdx];
      }
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TreeRowGroupKey)) {
        return false;
      }
      final TreeRowGroupKey that = (TreeRowGroupKey) o;
      return Objects.equals(deviceId, that.deviceId)
          && Arrays.equals(measurements, that.measurements)
          && Arrays.equals(dataTypes, that.dataTypes);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(deviceId);
      result = 31 * result + Arrays.hashCode(measurements);
      result = 31 * result + Arrays.hashCode(dataTypes);
      return result;
    }
  }
}
