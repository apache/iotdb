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

import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.AbstractResultSet;
import org.apache.tsfile.read.query.dataset.ResultSet;
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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubscriptionRecordHandler implements Iterable<ResultSet>, SubscriptionMessageHandler {

  private final List<ResultSet> records;

  public SubscriptionRecordHandler(final Map<String, List<Tablet>> tablets) {
    final List<ResultSet> records = new ArrayList<>();
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
        records.add(new SubscriptionRecord(tablet, databaseName));
      }
    }
    this.records = Collections.unmodifiableList(records);
  }

  public List<ResultSet> getRecords() {
    return records;
  }

  @Override
  public Iterator<ResultSet> iterator() {
    return records.iterator();
  }

  public static class SubscriptionRecord extends AbstractResultSet {

    private Tablet tablet;

    @Nullable private final String databaseName;

    private final List<RowPosition> sortedRowPositions;

    private int rowIndex = -1;

    @TableModel private List<ColumnCategory> columnCategoryList;

    private SubscriptionRecord(final Tablet tablet, @Nullable final String databaseName) {
      super(generateColumnNames(tablet, databaseName), generateColumnTypes(tablet));
      this.tablet = tablet;
      this.databaseName = databaseName;
      this.sortedRowPositions = generateSortedRowPositions(tablet);
    }

    @TableModel
    public String getDatabaseName() {
      return databaseName;
    }

    @TableModel
    public String getTableName() {
      return tablet.getTableName();
    }

    @TableModel
    public List<ColumnCategory> getColumnCategories() {
      if (Objects.nonNull(columnCategoryList)) {
        return columnCategoryList;
      }

      if (!isTableData()) {
        return Collections.emptyList();
      }

      return columnCategoryList =
          Stream.concat(
                  Stream.of(ColumnCategory.TIME),
                  tablet.getColumnTypes().stream()
                      .map(
                          columnCategory -> {
                            switch (columnCategory) {
                              case FIELD:
                                return ColumnCategory.FIELD;
                              case TAG:
                                return ColumnCategory.TAG;
                              case ATTRIBUTE:
                                return ColumnCategory.ATTRIBUTE;
                              default:
                                throw new IllegalArgumentException(
                                    "Unknown column category: " + columnCategory);
                            }
                          }))
              .collect(Collectors.toList());
    }

    public Tablet getTablet() {
      return tablet;
    }

    public boolean hasNext() {
      return Objects.nonNull(tablet) && rowIndex + 1 < sortedRowPositions.size();
    }

    @Nullable
    public RowRecord nextRecord() throws IOException {
      return next() ? currentRow : null;
    }

    public int getColumnCount() {
      return tablet.getSchemas().size() + 1;
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

    public enum ColumnCategory {
      TIME,
      TAG,
      FIELD,
      ATTRIBUTE
    }

    private boolean isTableData() {
      return Objects.nonNull(databaseName);
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
        if (bitMaps != null
            && bitMaps[columnIndex] != null
            && bitMaps[columnIndex].isMarked(rowPosition)) {
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
        if (bitMaps != null
            && bitMaps[columnIndex] != null
            && bitMaps[columnIndex].isMarked(currentRowIndex)) {
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
                String.format("Data type %s is not supported.", dataType));
        }
      }
      return record;
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
              String.format("Data type %s is not supported.", dataType));
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
  }
}
