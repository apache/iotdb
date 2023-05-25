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

import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
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
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public class PipeTabletInsertionEvent implements TabletInsertionEvent, EnrichedEvent {

  private List<Tablet> tabletList;

  private String pattern;

  private List<TSDataType> columnTypeList;
  private List<Path> columnNameList;
  private String deviceFullPath;

  public PipeTabletInsertionEvent(List<Tablet> tabletList) {
    this.tabletList = tabletList;
    pattern = null;

    matchPattern();
  }

  @Override
  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    for (Tablet tablet : tabletList) {
      int rowSize = tablet.rowSize;
      int columnSize = tablet.values.length;

      for (int i = 0; i < rowSize; i++) {
        Object[] rowRecord = new Object[columnSize];
        for (int j = 0; j < columnSize; j++) {
          rowRecord[j] = ((Object[]) tablet.values[j])[i];
        }
        Row row = new PipeRow(columnNameList, columnTypeList).setRowRecord(rowRecord);
        consumer.accept(row, rowCollector);
      }
    }
    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processByIterator(BiConsumer<RowIterator, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    for (Tablet tablet : tabletList) {
      int rowSize = tablet.rowSize;
      int columnSize = tablet.values.length;
      List<Row> rowList = new ArrayList<>();

      for (int i = 0; i < rowSize; i++) {
        Object[] rowRecord = new Object[columnSize];
        for (int j = 0; j < columnSize; j++) {
          rowRecord[j] = ((Object[]) tablet.values[j])[i];
        }

        rowList.add(new PipeRow(columnNameList, columnTypeList).setRowRecord(rowRecord));
      }

      RowIterator rowIterator = new PipeRowIterator(rowList, 0, rowList.size());
      consumer.accept(rowIterator, rowCollector);
    }
    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();
    for (Tablet tablet : tabletList) {
      consumer.accept(tablet, rowCollector);
    }
    return rowCollector.toTabletInsertionEvent();
  }

  public void matchPattern() {
    if (tabletList.isEmpty()) {
      return;
    }

    Tablet tmpTablet = tabletList.get(0);
    List<MeasurementSchema> originSchemaList = tmpTablet.getSchemas();
    this.deviceFullPath = tmpTablet.deviceId;
    this.columnTypeList = new ArrayList<>();
    this.columnNameList = new ArrayList<>();
    List<Integer> indexList = new ArrayList<>();

    if (pattern != null) {
      if (deviceFullPath.indexOf(pattern) != 0) {
        tabletList = Collections.emptyList();
      } else {
        for (int i = 0; i < originSchemaList.size(); i++) {
          MeasurementSchema measurementSchema = tmpTablet.getSchemas().get(i);
          if (pattern.length()
                  == deviceFullPath.length() + measurementSchema.getMeasurementId().length() + 1
              && pattern.endsWith(
                  TsFileConstant.PATH_SEPARATOR + measurementSchema.getMeasurementId())) {
            this.columnNameList.add(
                new Path(deviceFullPath, measurementSchema.getMeasurementId(), false));
            this.columnTypeList.add(measurementSchema.getType());
            indexList.add(i);
          }
        }

        int columnSize = indexList.size();
        List<Tablet> newTabletList = new ArrayList<>();
        List<MeasurementSchema> newSchemaList = new ArrayList<>();
        for (int i = 0; i < columnSize; i++) {
          newSchemaList.add(
              new MeasurementSchema(columnNameList.get(i).getMeasurement(), columnTypeList.get(i)));
        }

        for (Tablet originTablet : tabletList) {
          int rowSize = originTablet.rowSize;
          Object[][] columns = new Object[columnSize][rowSize];

          for (int i = 0; i < columnSize; i++) {
            Object[] columnRecord = (Object[]) originTablet.values[indexList.get(i)];
            System.arraycopy(columnRecord, 0, columns[i], 0, rowSize);
          }

          Tablet newTablet = new Tablet(deviceFullPath, newSchemaList);
          newTablet.rowSize = rowSize;
          newTablet.values = columns;
          newTabletList.add(newTablet);
        }

        tabletList = newTabletList;
      }
    }
  }

  //  private Object[][] convertColumnsToRows(Object[] columnRecords) {
  //    int rowSize = ((Object[]) columnRecords[0]).length;
  //    int columnSize = columnRecords.length;
  //    Object[][] rowRecords = new Object[rowSize][columnSize];
  //
  //    for (int i = 0; i < rowSize; i++) {
  //      for (int j = 0; j < columnSize; j++) {
  //        rowRecords[i][j] = ((Object[]) columnRecords[j])[i];
  //      }
  //    }
  //    return rowRecords;
  //  }

  @Override
  public boolean increaseReferenceCount(String holderMessage) {
    return false;
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage) {
    return false;
  }

  @Override
  public int getReferenceCount() {
    return 0;
  }

  @Override
  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  @Override
  public String getPattern() {
    return pattern;
  }
}
