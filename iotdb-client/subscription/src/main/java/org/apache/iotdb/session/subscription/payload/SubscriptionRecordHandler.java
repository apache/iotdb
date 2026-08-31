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

package org.apache.iotdb.session.subscription.payload;

import org.apache.iotdb.rpc.subscription.annotation.TableModel;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeException;
import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;

import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.AbstractResultSet;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubscriptionRecordHandler implements Iterable<ResultSet>, SubscriptionMessageHandler {

  private final List<SubscriptionResultSet> resultSets;

  private final List<ResultSet> resultSetView;

  public SubscriptionRecordHandler(final Map<String, List<Tablet>> tablets) {
    this(tablets, true);
  }

  public SubscriptionRecordHandler(
      final Map<String, List<Tablet>> tablets, final boolean timeSelected) {
    this(tablets, timeSelected, Collections.emptyMap());
  }

  public SubscriptionRecordHandler(
      final Map<String, List<Tablet>> tablets,
      final boolean timeSelected,
      final Map<String, Map<String, Boolean>> timeSelectedByTable) {
    final List<SubscriptionResultSet> resultSets = new ArrayList<>();
    for (final Map.Entry<String, List<Tablet>> entry : tablets.entrySet()) {
      final String databaseName = entry.getKey();
      final List<Tablet> tabletList = entry.getValue();
      if (Objects.isNull(tabletList)) {
        continue;
      }
      for (final Tablet tablet : tabletList) {
        if (Objects.isNull(tablet)) {
          continue;
        }
        resultSets.add(
            new SubscriptionResultSet(
                tablet,
                databaseName,
                resolveTimeSelected(timeSelectedByTable, timeSelected, databaseName, tablet)));
      }
    }
    this.resultSets = Collections.unmodifiableList(resultSets);
    final List<ResultSet> resultSetView = new ArrayList<>();
    resultSetView.addAll(resultSets);
    this.resultSetView = Collections.unmodifiableList(resultSetView);
  }

  public List<ResultSet> getResultSets() {
    return resultSetView;
  }

  @Override
  public Iterator<ResultSet> iterator() {
    return resultSetView.iterator();
  }

  @Override
  public void removeUserData() {
    resultSets.forEach(SubscriptionResultSet::removeUserData);
  }

  private static boolean resolveTimeSelected(
      final Map<String, Map<String, Boolean>> timeSelectedByTable,
      final boolean defaultTimeSelected,
      final String databaseName,
      final Tablet tablet) {
    if (Objects.isNull(timeSelectedByTable) || timeSelectedByTable.isEmpty()) {
      return defaultTimeSelected;
    }
    if (Objects.isNull(tablet)
        || Objects.isNull(databaseName)
        || Objects.isNull(tablet.getTableName())) {
      return defaultTimeSelected;
    }

    final Map<String, Boolean> tableMap =
        timeSelectedByTable.get(databaseName.trim().toLowerCase(Locale.ROOT));
    if (Objects.isNull(tableMap)) {
      return defaultTimeSelected;
    }

    final Boolean tableTimeSelected =
        tableMap.get(tablet.getTableName().trim().toLowerCase(Locale.ROOT));
    return Objects.nonNull(tableTimeSelected)
        ? Boolean.TRUE.equals(tableTimeSelected)
        : defaultTimeSelected;
  }

  public static class SubscriptionResultSet extends AbstractResultSet {

    private Tablet tablet;

    @Nullable private final String databaseName;

    private final boolean timeSelected;

    private final int visibleColumnCount;

    private final List<RowPosition> sortedRowPositions;

    private int rowIndex = -1;

    @TableModel private List<ColumnCategory> columnCategoryList;

    private volatile boolean userDataRemoved = false;

    private SubscriptionResultSet(
        final Tablet tablet, @Nullable final String databaseName, final boolean timeSelected) {
      super(generateColumnNames(tablet, databaseName), generateColumnTypes(tablet));
      this.tablet = tablet;
      this.databaseName = databaseName;
      this.timeSelected = timeSelected;
      this.visibleColumnCount = tablet.getSchemas().size() + (shouldExposeTime() ? 1 : 0);
      if (!shouldExposeTime()) {
        resultSetMetadata = new SubscriptionResultSetMetadata(tablet);
        columnNameToColumnIndexMap.clear();
        final List<IMeasurementSchema> schemas = tablet.getSchemas();
        for (int i = 0; i < schemas.size(); ++i) {
          columnNameToColumnIndexMap.put(schemas.get(i).getMeasurementName(), i + 1);
        }
      }
      this.sortedRowPositions = generateSortedRowPositions(tablet);
    }

    @TableModel
    public String getDatabaseName() {
      return databaseName;
    }

    @TableModel
    public String getTableName() {
      ensureUserDataAvailable();
      return tablet.getTableName();
    }

    @TableModel
    public List<ColumnCategory> getColumnCategories() {
      ensureUserDataAvailable();
      if (Objects.nonNull(columnCategoryList)) {
        return columnCategoryList;
      }

      if (!isTableData()) {
        return Collections.emptyList();
      }

      return columnCategoryList =
          Stream.concat(
                  shouldExposeTime() ? Stream.of(ColumnCategory.TIME) : Stream.empty(),
                  tablet.getColumnTypes().stream()
                      .map(SubscriptionResultSet::convertColumnCategory))
              .collect(Collectors.toList());
    }

    public boolean isTimeSelected() {
      return timeSelected;
    }

    public Tablet getTablet() {
      ensureUserDataAvailable();
      return tablet;
    }

    public boolean hasNext() {
      ensureUserDataAvailable();
      return Objects.nonNull(tablet) && rowIndex + 1 < sortedRowPositions.size();
    }

    @Nullable
    public RowRecord nextRecord() throws IOException {
      return next() ? currentRow : null;
    }

    public int getColumnCount() {
      ensureUserDataAvailable();
      return visibleColumnCount;
    }

    public List<String> getColumnNames() {
      final int columnCount = getColumnCount();
      final List<String> columnNames = new ArrayList<>(columnCount);
      for (int i = 1; i <= columnCount; ++i) {
        columnNames.add(resultSetMetadata.getColumnName(i));
      }
      return columnNames;
    }

    public List<String> getColumnTypes() {
      final int columnCount = getColumnCount();
      final List<String> columnTypes = new ArrayList<>(columnCount);
      for (int i = 1; i <= columnCount; ++i) {
        columnTypes.add(resultSetMetadata.getColumnType(i).toString());
      }
      return columnTypes;
    }

    @Override
    public boolean next() throws IOException {
      ensureUserDataAvailable();
      if (Objects.isNull(tablet)) {
        return false;
      }

      ++rowIndex;
      if (rowIndex >= sortedRowPositions.size()) {
        return false;
      }

      final RowPosition position = sortedRowPositions.get(rowIndex);
      currentRow = generateRowRecord(position.timestamp, position.rowIndex);
      return true;
    }

    @Override
    public void close() {
      tablet = null;
      currentRow = null;
    }

    @Override
    public Iterator<TSRecord> iterator() {
      ensureUserDataAvailable();
      final Tablet currentTablet = this.tablet;
      if (Objects.isNull(currentTablet)) {
        return Collections.emptyIterator();
      }
      return new Iterator<TSRecord>() {
        private int index = 0;

        @Override
        public boolean hasNext() {
          return index < sortedRowPositions.size();
        }

        @Override
        public TSRecord next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          final RowPosition position = sortedRowPositions.get(index++);
          return generateTsRecord(currentTablet, position.timestamp, position.rowIndex);
        }
      };
    }

    private void removeUserData() {
      if (userDataRemoved) {
        return;
      }

      userDataRemoved = true;
      sortedRowPositions.clear();
      close();
    }

    public enum ColumnCategory {
      TIME,
      TAG,
      FIELD,
      ATTRIBUTE
    }

    private boolean isTableData() {
      return Objects.nonNull(databaseName);
    }

    private void ensureUserDataAvailable() {
      if (userDataRemoved) {
        throw new SubscriptionRuntimeException(
            String.format(
                SubscriptionMessages.EXCEPTION_USER_DATA_HAS_BEEN_REMOVED_ARG_7093644B,
                getClass().getSimpleName()));
      }
    }

    private static List<String> generateColumnNames(
        final Tablet tablet, @Nullable final String databaseName) {
      final List<IMeasurementSchema> schemas = tablet.getSchemas();
      if (Objects.nonNull(databaseName)) {
        return schemas.stream()
            .map(IMeasurementSchema::getMeasurementName)
            .collect(Collectors.toList());
      }
      final String deviceId = tablet.getDeviceId();
      return schemas.stream()
          .map(schema -> deviceId + "." + schema.getMeasurementName())
          .collect(Collectors.toList());
    }

    private static List<TSDataType> generateColumnTypes(final Tablet tablet) {
      return tablet.getSchemas().stream()
          .map(IMeasurementSchema::getType)
          .collect(Collectors.toList());
    }

    private static List<RowPosition> generateSortedRowPositions(final Tablet tablet) {
      final int rowSize = tablet.getRowSize();
      final List<RowPosition> positions = new ArrayList<>(rowSize);
      for (int i = 0; i < rowSize; ++i) {
        positions.add(new RowPosition(tablet.getTimestamp(i), i));
      }
      positions.sort(
          (left, right) -> {
            final int timeComparison = Long.compare(left.timestamp, right.timestamp);
            return timeComparison != 0
                ? timeComparison
                : Integer.compare(left.rowIndex, right.rowIndex);
          });
      return positions;
    }

    private RowRecord generateRowRecord(final long timestamp, final int rowPosition) {
      final int columnSize = tablet.getSchemas().size();
      final List<Field> fields = new ArrayList<>(columnSize);

      final BitMap[] bitMaps = tablet.getBitMaps();
      for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
        final Field field;
        if (isNullValue(tablet.getValues(), bitMaps, columnIndex, rowPosition)) {
          field = new Field(null);
        } else {
          final TSDataType dataType = tablet.getSchemas().get(columnIndex).getType();
          field =
              generateFieldFromTabletValue(dataType, tablet.getValues()[columnIndex], rowPosition);
        }
        fields.add(field);
      }
      return new RowRecord(timestamp, fields);
    }

    private TSRecord generateTsRecord(
        final Tablet currentTablet, final long timestamp, final int currentRowIndex) {
      final TSRecord record =
          isTableData()
              ? new TSRecord(currentTablet.getTableName(), timestamp)
              : new TSRecord(currentTablet.getDeviceId(), timestamp);

      final BitMap[] bitMaps = currentTablet.getBitMaps();
      for (int columnIndex = 0; columnIndex < currentTablet.getSchemas().size(); ++columnIndex) {
        if (isNullValue(currentTablet.getValues(), bitMaps, columnIndex, currentRowIndex)) {
          continue;
        }

        final String measurement = currentTablet.getSchemas().get(columnIndex).getMeasurementName();
        final TSDataType dataType = currentTablet.getSchemas().get(columnIndex).getType();
        final Object value = currentTablet.getValues()[columnIndex];
        switch (dataType) {
          case BOOLEAN:
            record.addPoint(measurement, ((boolean[]) value)[currentRowIndex]);
            break;
          case INT32:
            record.addPoint(measurement, ((int[]) value)[currentRowIndex]);
            break;
          case DATE:
            record.addPoint(measurement, ((LocalDate[]) value)[currentRowIndex]);
            break;
          case INT64:
          case TIMESTAMP:
            record.addPoint(measurement, ((long[]) value)[currentRowIndex]);
            break;
          case FLOAT:
            record.addPoint(measurement, ((float[]) value)[currentRowIndex]);
            break;
          case DOUBLE:
            record.addPoint(measurement, ((double[]) value)[currentRowIndex]);
            break;
          case TEXT:
          case STRING:
          case BLOB:
          case OBJECT:
            final Binary binary = ((Binary[]) value)[currentRowIndex];
            if (Objects.nonNull(binary)) {
              record.addPoint(measurement, binary.getValues());
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    SubscriptionMessages.EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160, dataType));
        }
      }
      return record;
    }

    private static boolean isNullValue(
        final Object[] values, final BitMap[] bitMaps, final int columnIndex, final int rowIndex) {
      if (Objects.isNull(values)
          || columnIndex >= values.length
          || Objects.isNull(values[columnIndex])) {
        return true;
      }
      return bitMaps != null
          && columnIndex < bitMaps.length
          && bitMaps[columnIndex] != null
          && bitMaps[columnIndex].isMarked(rowIndex);
    }

    @Override
    protected Field getField(final int index) {
      if (shouldExposeTime()) {
        return super.getField(index);
      }
      if (index <= 0 || index > visibleColumnCount) {
        throw new IndexOutOfBoundsException(
            String.format(
                SubscriptionMessages.EXCEPTION_RESULTSET_COLUMN_INDEX_OUT_OF_BOUND_ARG_2D8CC5A3,
                index));
      }
      return currentRow.getField(index - 1);
    }

    private boolean shouldExposeTime() {
      return !isTableData() || timeSelected;
    }

    private static ColumnCategory convertColumnCategory(
        final org.apache.tsfile.enums.ColumnCategory columnCategory) {
      switch (columnCategory) {
        case FIELD:
          return ColumnCategory.FIELD;
        case TAG:
          return ColumnCategory.TAG;
        case ATTRIBUTE:
          return ColumnCategory.ATTRIBUTE;
        default:
          throw new IllegalArgumentException(
              SubscriptionMessages.EXCEPTION_UNKNOWN_COLUMN_CATEGORY_4F49F64B + columnCategory);
      }
    }

    private static Field generateFieldFromTabletValue(
        final TSDataType dataType, final Object value, final int index) {
      final Field field = new Field(dataType);
      switch (dataType) {
        case BOOLEAN:
          field.setBoolV(((boolean[]) value)[index]);
          break;
        case INT32:
          field.setIntV(((int[]) value)[index]);
          break;
        case DATE:
          field.setIntV(DateUtils.parseDateExpressionToInt(((LocalDate[]) value)[index]));
          break;
        case INT64:
        case TIMESTAMP:
          field.setLongV(((long[]) value)[index]);
          break;
        case FLOAT:
          field.setFloatV(((float[]) value)[index]);
          break;
        case DOUBLE:
          field.setDoubleV(((double[]) value)[index]);
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          field.setBinaryV(new Binary((((Binary[]) value)[index]).getValues()));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  SubscriptionMessages.EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160, dataType));
      }
      return field;
    }

    private static class RowPosition {
      private final long timestamp;
      private final int rowIndex;

      private RowPosition(final long timestamp, final int rowIndex) {
        this.timestamp = timestamp;
        this.rowIndex = rowIndex;
      }
    }

    private static class SubscriptionResultSetMetadata implements ResultSetMetadata {

      private final List<IMeasurementSchema> schemas;

      private SubscriptionResultSetMetadata(final Tablet tablet) {
        this.schemas = tablet.getSchemas();
      }

      @Override
      public String getColumnName(final int index) {
        return schemas.get(index - 1).getMeasurementName();
      }

      @Override
      public TSDataType getColumnType(final int index) {
        return schemas.get(index - 1).getType();
      }
    }
  }
}
