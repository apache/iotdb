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

import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SubscriptionRecordHandler
    implements Iterable<SubscriptionRecordHandler.SubscriptionResultSet>,
        SubscriptionMessageHandler {

  private final List<SubscriptionResultSet> resultSets;

  public SubscriptionRecordHandler(final List<Tablet> tablets) {
    final List<SubscriptionResultSet> resultSets = new ArrayList<>();
    for (final Tablet tablet : tablets) {
      if (Objects.isNull(tablet)) {
        continue;
      }
      resultSets.add(new SubscriptionResultSet(tablet));
    }
    this.resultSets = Collections.unmodifiableList(resultSets);
  }

  public List<SubscriptionResultSet> getResultSets() {
    return resultSets;
  }

  @Override
  public Iterator<SubscriptionResultSet> iterator() {
    return resultSets.iterator();
  }

  @Override
  public void removeUserData() {
    resultSets.forEach(SubscriptionResultSet::removeUserData);
  }

  public static class SubscriptionResultSet implements ISessionDataSet {

    private Tablet tablet;

    private final List<RowPosition> sortedRowPositions;

    private int rowIndex = -1;

    private List<String> columnNameList;

    private List<String> columnTypeList;

    private volatile boolean userDataRemoved = false;

    private SubscriptionResultSet(final Tablet tablet) {
      this.tablet = tablet;
      this.sortedRowPositions = generateSortedRowPositions(tablet);
    }

    public Tablet getTablet() {
      ensureUserDataAvailable();
      return tablet;
    }

    @Override
    public List<String> getColumnNames() {
      ensureUserDataAvailable();
      if (Objects.nonNull(columnNameList)) {
        return columnNameList;
      }

      columnNameList = new ArrayList<>();
      columnNameList.add("Time");

      final List<MeasurementSchema> schemas = tablet.getSchemas();
      final String deviceId = tablet.deviceId;
      columnNameList.addAll(
          schemas.stream()
              .map(schema -> deviceId + "." + schema.getMeasurementId())
              .collect(Collectors.toList()));
      return columnNameList;
    }

    @Override
    public List<String> getColumnTypes() {
      ensureUserDataAvailable();
      if (Objects.nonNull(columnTypeList)) {
        return columnTypeList;
      }

      columnTypeList = new ArrayList<>();
      columnTypeList.add(TSDataType.INT64.toString());

      final List<MeasurementSchema> schemas = tablet.getSchemas();
      columnTypeList.addAll(
          schemas.stream().map(schema -> schema.getType().toString()).collect(Collectors.toList()));
      return columnTypeList;
    }

    public boolean hasNext() {
      ensureUserDataAvailable();
      return Objects.nonNull(tablet) && rowIndex + 1 < sortedRowPositions.size();
    }

    @Override
    public RowRecord next() {
      ensureUserDataAvailable();
      final RowPosition position = sortedRowPositions.get(++rowIndex);
      return generateRowRecord(position.timestamp, position.rowIndex);
    }

    @Override
    public void close() {
      tablet = null;
    }

    private void removeUserData() {
      if (userDataRemoved) {
        return;
      }

      userDataRemoved = true;
      sortedRowPositions.clear();
      close();
    }

    private RowRecord generateRowRecord(final long timestamp, final int rowPosition) {
      final int columnSize = tablet.getSchemas().size();
      final List<Field> fields = new ArrayList<>(columnSize);

      for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
        final Field field;
        if (tablet.bitMaps != null
            && tablet.bitMaps[columnIndex] != null
            && tablet.bitMaps[columnIndex].isMarked(rowPosition)) {
          field = new Field(null);
        } else {
          final TSDataType dataType = tablet.getSchemas().get(columnIndex).getType();
          field = generateFieldFromTabletValue(dataType, tablet.values[columnIndex], rowPosition);
        }
        fields.add(field);
      }
      return new RowRecord(timestamp, fields);
    }

    private static List<RowPosition> generateSortedRowPositions(final Tablet tablet) {
      final List<RowPosition> positions = new ArrayList<>(tablet.timestamps.length);
      for (int rowPosition = 0; rowPosition < tablet.timestamps.length; ++rowPosition) {
        positions.add(new RowPosition(tablet.timestamps[rowPosition], rowPosition));
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

    private void ensureUserDataAvailable() {
      if (userDataRemoved) {
        throw new SubscriptionRuntimeException(
            String.format("User data has been removed from %s.", getClass().getSimpleName()));
      }
    }
  }
}
