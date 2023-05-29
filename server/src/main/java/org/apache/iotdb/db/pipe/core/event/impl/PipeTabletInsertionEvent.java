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

package org.apache.iotdb.db.pipe.core.event.impl;

import org.apache.iotdb.db.pipe.core.event.view.access.PipeRow;
import org.apache.iotdb.db.pipe.core.event.view.access.PipeRowIterator;
import org.apache.iotdb.db.pipe.core.event.view.collector.PipeRowCollector;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.access.RowIterator;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class PipeTabletInsertionEvent implements TabletInsertionEvent {

  private Tablet tablet;

  private final String pattern;

  private List<TSDataType> columnTypeList;
  private List<Path> columnNameList;
  private final String deviceId;
  private Object[][] rowRecords;

  public PipeTabletInsertionEvent(Tablet tablet) {
    this(tablet, null);
  }

  public PipeTabletInsertionEvent(Tablet tablet, String pattern) {
    this.tablet = tablet;
    this.pattern = pattern;
    this.deviceId = tablet.deviceId;

    matchPattern();
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    for (Object[] rowRecord : rowRecords) {
      Row row = new PipeRow(columnNameList, columnTypeList).setRowRecord(rowRecord);
      consumer.accept(row, rowCollector);
    }

    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processByIterator(BiConsumer<RowIterator, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();
    List<Row> rowList = new ArrayList<>();

    for (Object[] rowRecord : rowRecords) {
      Row row = new PipeRow(columnNameList, columnTypeList).setRowRecord(rowRecord);
      rowList.add(row);
    }

    RowIterator rowIterator = new PipeRowIterator(rowList, 0, rowList.size());
    consumer.accept(rowIterator, rowCollector);
    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    consumer.accept(tablet, rowCollector);
    return rowCollector.toTabletInsertionEvent();
  }

  public void matchPattern() {
    if (tablet == null) {
      return;
    }

    List<MeasurementSchema> originSchemaList = tablet.getSchemas();
    List<Integer> indexList = new ArrayList<>();
    this.columnTypeList = new ArrayList<>();
    this.columnNameList = new ArrayList<>();

    boolean collectAllColumns =
        pattern == null || (pattern.length() <= deviceId.length() && deviceId.startsWith(pattern));

    for (int i = 0; i < originSchemaList.size(); i++) {
      MeasurementSchema measurementSchema = tablet.getSchemas().get(i);
      if (collectAllColumns
          || (pattern.length() > deviceId.length()
              && pattern.startsWith(deviceId)
              && pattern.length()
                  == deviceId.length() + measurementSchema.getMeasurementId().length() + 1
              && pattern.endsWith(
                  TsFileConstant.PATH_SEPARATOR + measurementSchema.getMeasurementId()))) {
        this.columnNameList.add(new Path(deviceId, measurementSchema.getMeasurementId(), false));
        this.columnTypeList.add(measurementSchema.getType());
        indexList.add(i);
      }
    }

    if (!collectAllColumns && !indexList.isEmpty()) {
      int rowSize = tablet.rowSize;
      int columnSize = indexList.size();
      this.rowRecords = new Object[rowSize][columnSize];

      List<MeasurementSchema> newSchemaList = new ArrayList<>();
      for (int i = 0; i < columnSize; i++) {
        newSchemaList.add(
            new MeasurementSchema(columnNameList.get(i).getMeasurement(), columnTypeList.get(i)));
      }

      Object[][] columns = new Object[columnSize][rowSize];
      for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
        Object[] column = (Object[]) tablet.values[indexList.get(columnIndex)];
        System.arraycopy(column, 0, columns[columnIndex], 0, rowSize);
        for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
          rowRecords[rowIndex][columnIndex] = column[rowIndex];
        }
      }

      Tablet newTablet = new Tablet(deviceId, newSchemaList);
      newTablet.rowSize = rowSize;
      newTablet.values = columns;
      newTablet.bitMaps = tablet.bitMaps;
      this.tablet = newTablet;
    } else {
      this.tablet = null;
    }
  }

  public String getPattern() {
    return pattern;
  }

  public List<MeasurementSchema> getMeasureSchemaList() {
    return tablet.getSchemas();
  }

  public String getDeviceId() {
    return deviceId;
  }

  public Tablet getTablet() {
    return tablet;
  }
}
