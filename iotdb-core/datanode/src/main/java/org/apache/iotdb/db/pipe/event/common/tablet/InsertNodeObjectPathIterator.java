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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("java:S107")
public class InsertNodeObjectPathIterator implements Iterator<String> {

  public static final class ExtractContext {
    private final TreePattern treePattern;
    private final TablePattern tablePattern;
    private final long startTime;
    private final long endTime;
    private final Boolean isTableModelEvent;
    private final String tableModelDatabaseName;
    private final UserEntity entity;

    public ExtractContext(
        final TreePattern treePattern,
        final TablePattern tablePattern,
        final long startTime,
        final long endTime,
        final Boolean isTableModelEvent,
        final String tableModelDatabaseName,
        final UserEntity entity) {
      this.treePattern = treePattern;
      this.tablePattern = tablePattern;
      this.startTime = startTime;
      this.endTime = endTime;
      this.isTableModelEvent = isTableModelEvent;
      this.tableModelDatabaseName = tableModelDatabaseName;
      this.entity = entity;
    }
  }

  private final Iterator<String> pathIterator;

  public InsertNodeObjectPathIterator(
      final InsertNode insertNode, final Boolean hasObjectData, final ExtractContext context) {

    if (Boolean.FALSE.equals(hasObjectData) || insertNode == null) {
      this.pathIterator = Collections.emptyIterator();
    } else {
      this.pathIterator =
          extractPathsFromInsertNode(
                  insertNode,
                  context.treePattern,
                  context.tablePattern,
                  context.startTime,
                  context.endTime,
                  context.isTableModelEvent,
                  context.tableModelDatabaseName,
                  context.entity)
              .filter(Objects::nonNull)
              .iterator();
    }
  }

  public InsertNodeObjectPathIterator(final Tablet tablet, final ExtractContext context) {

    if (tablet == null) {
      this.pathIterator = Collections.emptyIterator();
    } else {
      this.pathIterator =
          extractPathsFromTablet(
                  tablet,
                  context.treePattern,
                  context.tablePattern,
                  context.startTime,
                  context.endTime,
                  context.isTableModelEvent,
                  context.tableModelDatabaseName,
                  context.entity)
              .filter(Objects::nonNull)
              .iterator();
    }
  }

  @Override
  public boolean hasNext() {
    return pathIterator.hasNext();
  }

  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more object paths available.");
    }
    return pathIterator.next();
  }

  private Stream<String> extractPathsFromInsertNode(
      final InsertNode insertNode,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String tableModelDatabaseName,
      final UserEntity entity) {

    if (insertNode instanceof InsertRowNode) {
      return extractPathsFromRow(
          (InsertRowNode) insertNode,
          treePattern,
          tablePattern,
          startTime,
          endTime,
          isTableModelEvent,
          tableModelDatabaseName,
          entity);
    }

    if (insertNode instanceof InsertTabletNode) {
      final InsertTabletNode node = (InsertTabletNode) insertNode;
      return extractPathsFromColumns(
          node.getTimes(),
          node.getRowCount(),
          node.getColumns(),
          node.getDataTypes(),
          node.getBitMaps(),
          node.getMeasurements(),
          node.getDeviceID(0),
          node.getTableName(),
          treePattern,
          tablePattern,
          startTime,
          endTime,
          isTableModelEvent,
          tableModelDatabaseName,
          entity);
    }

    if (insertNode instanceof InsertRowsNode) {
      final List<InsertRowNode> rows = ((InsertRowsNode) insertNode).getInsertRowNodeList();
      if (rows == null || rows.isEmpty()) {
        return Stream.empty();
      }
      return rows.stream()
          .flatMap(
              row ->
                  extractPathsFromRow(
                      row,
                      treePattern,
                      tablePattern,
                      startTime,
                      endTime,
                      isTableModelEvent,
                      tableModelDatabaseName,
                      entity));
    }

    return Stream.empty();
  }

  private static Stream<String> extractPathsFromTablet(
      final Tablet tablet,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String tableModelDatabaseName,
      final UserEntity entity) {

    if (tablet.getSchemas() == null || tablet.getSchemas().isEmpty()) {
      return Stream.empty();
    }

    final IDeviceID deviceID =
        (Boolean.FALSE.equals(isTableModelEvent) && tablet.getDeviceId() != null)
            ? new StringArrayDeviceID(tablet.getDeviceId())
            : null;

    final TSDataType[] dataTypes =
        tablet.getSchemas().stream().map(IMeasurementSchema::getType).toArray(TSDataType[]::new);

    final String[] measurements =
        tablet.getSchemas().stream()
            .map(IMeasurementSchema::getMeasurementName)
            .toArray(String[]::new);

    return extractPathsFromColumns(
        tablet.getTimestamps(),
        tablet.getRowSize(),
        tablet.getValues(),
        dataTypes,
        tablet.getBitMaps(),
        measurements,
        deviceID,
        tablet.getTableName(),
        treePattern,
        tablePattern,
        startTime,
        endTime,
        isTableModelEvent,
        tableModelDatabaseName,
        entity);
  }

  private static Stream<String> extractPathsFromColumns(
      final long[] times,
      final int rowCount,
      final Object[] columns,
      final TSDataType[] types,
      final BitMap[] bitMaps,
      final String[] measurements,
      final IDeviceID deviceID,
      final String tableName,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String databaseName,
      final UserEntity entity) {

    if (times == null
        || times.length == 0
        || columns == null
        || types == null
        || measurements == null) {
      return Stream.empty();
    }

    if (Boolean.TRUE.equals(isTableModelEvent)
        && tablePattern != null
        && (!tablePattern.matchesDatabase(databaseName) || !tablePattern.matchesTable(tableName))) {
      return Stream.empty();
    }

    return IntStream.range(0, types.length)
        .filter(
            colIdx ->
                isValidObjectColumnArray(
                    types[colIdx],
                    columns[colIdx],
                    measurements[colIdx],
                    deviceID,
                    treePattern,
                    isTableModelEvent,
                    entity))
        .boxed()
        .flatMap(
            colIdx -> {
              final Binary[] colData = (Binary[]) columns[colIdx];
              final BitMap bitMap =
                  (bitMaps != null && colIdx < bitMaps.length) ? bitMaps[colIdx] : null;
              final int limit = Math.min(colData.length, Math.min(rowCount, times.length));

              return IntStream.range(0, limit)
                  .filter(
                      rowIdx ->
                          isRowDataValid(
                              times[rowIdx], startTime, endTime, colData[rowIdx], bitMap, rowIdx))
                  .mapToObj(
                      rowIdx ->
                          ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair((colData[rowIdx]))
                              .getRight());
            });
  }

  private static Stream<String> extractPathsFromRow(
      final InsertRowNode node,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final Boolean isTableModelEvent,
      final String tableModelDatabaseName,
      final UserEntity entity) {

    if (node.getTime() < startTime || node.getTime() > endTime) {
      return Stream.empty();
    }

    if (Boolean.TRUE.equals(isTableModelEvent)
        && tablePattern != null
        && (!tablePattern.matchesDatabase(tableModelDatabaseName)
            || !tablePattern.matchesTable(node.getTableName()))) {
      return Stream.empty();
    }

    final Object[] values = node.getValues();
    final TSDataType[] types = node.getDataTypes();
    final String[] measurements = node.getMeasurements();

    if (values == null || types == null || measurements == null) {
      return Stream.empty();
    }

    final int maxIndex = Math.min(types.length, Math.min(values.length, measurements.length));

    return IntStream.range(0, maxIndex)
        .filter(
            i ->
                isValidObjectSingleValue(
                    types[i],
                    values[i],
                    measurements[i],
                    node.getDeviceID(),
                    treePattern,
                    isTableModelEvent,
                    entity))
        .mapToObj(
            i ->
                ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(((Binary) values[i]))
                    .getRight());
  }

  private static boolean isValidObjectColumnArray(
      final TSDataType type,
      final Object columnData,
      final String measurement,
      final IDeviceID deviceID,
      final TreePattern treePattern,
      final Boolean isTableModelEvent,
      final UserEntity entity) {

    if (type != TSDataType.OBJECT || !(columnData instanceof Binary[]) || measurement == null) {
      return false;
    }
    return matchesPatternAndPrivilege(
        measurement, deviceID, treePattern, isTableModelEvent, entity);
  }

  private static boolean isValidObjectSingleValue(
      final TSDataType type,
      final Object valueData,
      final String measurement,
      final IDeviceID deviceID,
      final TreePattern treePattern,
      final Boolean isTableModelEvent,
      final UserEntity entity) {

    if (type != TSDataType.OBJECT || !(valueData instanceof Binary) || measurement == null) {
      return false;
    }
    return matchesPatternAndPrivilege(
        measurement, deviceID, treePattern, isTableModelEvent, entity);
  }

  private static boolean matchesPatternAndPrivilege(
      final String measurement,
      final IDeviceID deviceID,
      final TreePattern treePattern,
      final Boolean isTableModelEvent,
      final UserEntity entity) {

    if (Boolean.TRUE.equals(isTableModelEvent)) {
      return true;
    }
    if (treePattern != null
        && (deviceID == null || !treePattern.matchesMeasurement(deviceID, measurement))) {
      return false;
    }
    return checkSeriesReadPrivilege(entity, deviceID, measurement);
  }

  private static boolean isRowDataValid(
      final long time,
      final long startTime,
      final long endTime,
      final Binary data,
      final BitMap bitMap,
      final int rowIdx) {

    return time >= startTime
        && time <= endTime
        && data != null
        && (bitMap == null || !bitMap.isMarked(rowIdx));
  }

  private static boolean checkSeriesReadPrivilege(
      final UserEntity entity, final IDeviceID deviceId, final String measurement) {
    if (entity == null) {
      return true;
    }
    try {
      return AuthorityChecker.getAccessControl()
              .checkSeriesPrivilege4Pipe(
                  entity,
                  Collections.singletonList(new MeasurementPath(deviceId, measurement)),
                  PrivilegeType.READ_DATA)
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (final Exception e) {
      return false;
    }
  }
}
