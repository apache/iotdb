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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class SubscriptionSessionDataSet implements ISessionDataSet {

  private Tablet tablet;

  public Tablet getTablet() {
    return tablet;
  }

  public SubscriptionSessionDataSet(final Tablet tablet) {
    this.tablet = tablet;
    generateRowIterator();
  }

  /////////////////////////////// override ///////////////////////////////

  private List<String> columnNameList;
  private List<String> columnTypeList;

  @Override
  public List<String> getColumnNames() {
    if (Objects.nonNull(columnNameList)) {
      return columnNameList;
    }

    columnNameList = new ArrayList<>();
    columnNameList.add("Time");

    String deviceId = tablet.getDeviceId();
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    columnNameList.addAll(
        schemas.stream()
            .map((schema) -> deviceId + "." + schema.getMeasurementName())
            .collect(Collectors.toList()));
    return columnNameList;
  }

  @Override
  public List<String> getColumnTypes() {
    if (Objects.nonNull(columnTypeList)) {
      return columnTypeList;
    }

    columnTypeList = new ArrayList<>();
    columnTypeList.add(TSDataType.INT64.toString());

    List<IMeasurementSchema> schemas = tablet.getSchemas();
    columnTypeList.addAll(
        schemas.stream().map(schema -> schema.getType().toString()).collect(Collectors.toList()));
    return columnTypeList;
  }

  public boolean hasNext() {
    return rowIterator.hasNext();
  }

  @Override
  public RowRecord next() {
    final Map.Entry<Long, Integer> entry = this.rowIterator.next();
    final int columnSize = getColumnSize();

    final List<Field> fields = new ArrayList<>();
    final long timestamp = entry.getKey();
    final int rowIndex = entry.getValue();

    for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
      final Field field;
      if (tablet.bitMaps != null
          && tablet.bitMaps[columnIndex] != null
          && tablet.bitMaps[columnIndex].isMarked(rowIndex)) {
        field = new Field(null);
      } else {
        final TSDataType dataType = tablet.getSchemas().get(columnIndex).getType();
        field = generateFieldFromTabletValue(dataType, tablet.values[columnIndex], rowIndex);
      }
      fields.add(field);
    }
    return new RowRecord(timestamp, fields);
  }

  @Override
  public void close() throws Exception {
    // maybe friendly for gc
    tablet = null;
  }

  /////////////////////////////// utility ///////////////////////////////

  private Iterator<Map.Entry<Long, Integer>> rowIterator;

  private int getColumnSize() {
    return tablet.getSchemas().size();
  }

  private void generateRowIterator() {
    // timestamp -> row index
    final long[] timestamps = tablet.timestamps;
    final TreeMap<Long, Integer> timestampToRowIndex = new TreeMap<>();
    final int rowSize = timestamps.length;
    for (int rowIndex = 0; rowIndex < rowSize; ++rowIndex) {
      final Long timestamp = timestamps[rowIndex];
      timestampToRowIndex.put(timestamp, rowIndex);
    }
    this.rowIterator = timestampToRowIndex.entrySet().iterator();
  }

  private static Field generateFieldFromTabletValue(
      final TSDataType dataType, final Object value, final int index) {
    final Field field = new Field(dataType);
    switch (dataType) {
      case BOOLEAN:
        final boolean booleanValue = ((boolean[]) value)[index];
        field.setBoolV(booleanValue);
        break;
      case INT32:
        final int intValue = ((int[]) value)[index];
        field.setIntV(intValue);
        break;
      case DATE:
        final LocalDate dateValue = ((LocalDate[]) value)[index];
        field.setIntV(DateUtils.parseDateExpressionToInt(dateValue));
        break;
      case INT64:
      case TIMESTAMP:
        final long longValue = ((long[]) value)[index];
        field.setLongV(longValue);
        break;
      case FLOAT:
        final float floatValue = ((float[]) value)[index];
        field.setFloatV(floatValue);
        break;
      case DOUBLE:
        final double doubleValue = ((double[]) value)[index];
        field.setDoubleV(doubleValue);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        final Binary binaryValue = new Binary((((Binary[]) value)[index]).getValues());
        field.setBinaryV(binaryValue);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    return field;
  }
}
