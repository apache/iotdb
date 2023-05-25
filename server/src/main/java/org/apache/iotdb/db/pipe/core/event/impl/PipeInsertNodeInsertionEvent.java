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

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.core.event.view.access.PipeRow;
import org.apache.iotdb.db.pipe.core.event.view.access.PipeRowIterator;
import org.apache.iotdb.db.pipe.core.event.view.collector.PipeRowCollector;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.access.RowIterator;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class PipeInsertNodeInsertionEvent implements TabletInsertionEvent, EnrichedEvent {

  private final InsertNode insertNode;

  private final AtomicInteger referenceCount;

  private String pattern;

  private List<TSDataType> dataTypeList;
  private List<Path> columnNameList;
  private String deviceFullPath;
  private Object[][] columns;
  private Object[][] rowRecords;

  public PipeInsertNodeInsertionEvent(InsertNode insertNode) {
    this.insertNode = insertNode;
    this.referenceCount = new AtomicInteger(0);
    this.pattern = null;

    matchPattern();
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  @Override
  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    if (insertNode instanceof InsertRowNode) {
      Row row = new PipeRow(columnNameList, dataTypeList).setRowRecord(rowRecords[0]);
      consumer.accept(row, rowCollector);
    } else if (insertNode instanceof InsertTabletNode) {
      for (Object[] rowRecord : rowRecords) {
        Row row = new PipeRow(columnNameList, dataTypeList).setRowRecord(rowRecord);
        consumer.accept(row, rowCollector);
      }
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processByIterator(BiConsumer<RowIterator, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    if (insertNode instanceof InsertRowNode) {
      Row row = new PipeRow(columnNameList, dataTypeList).setRowRecord(rowRecords[0]);

      PipeRowIterator rowIterator = new PipeRowIterator(Collections.singletonList(row), 0, 1);
      consumer.accept(rowIterator, rowCollector);
    } else if (insertNode instanceof InsertTabletNode) {
      List<Row> rows = new ArrayList<>();
      for (Object[] rowRecord : rowRecords) {
        Row row = new PipeRow(columnNameList, dataTypeList).setRowRecord(rowRecord);
        rows.add(row);
      }
      RowIterator rowIterator = new PipeRowIterator(rows, 0, rows.size());
      consumer.accept(rowIterator, rowCollector);
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }

    return this;
  }

  @Override
  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer)
      throws IOException {
    RowCollector rowCollector = new PipeRowCollector();
    List<MeasurementSchema> schemas = createMeasurementSchemas();
    Tablet tablet = new Tablet(deviceFullPath, schemas);

    if (insertNode instanceof InsertRowNode) {
      Row row = new PipeRow(columnNameList, dataTypeList).setRowRecord(rowRecords[0]);

      for (int i = 0; i < row.size(); i++) {
        tablet.addValue(columnNameList.get(i).getMeasurement(), 0, row.getObject(i));
      }
      consumer.accept(tablet, rowCollector);
    } else if (insertNode instanceof InsertTabletNode) {
      for (Object[] row : rowRecords) {
        for (int j = 0; j < row.length; j++) {
          tablet.addValue(columnNameList.get(j).getMeasurement(), j, row[j]);
        }
      }
      consumer.accept(tablet, rowCollector);
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
    return this;
  }

  private List<MeasurementSchema> createMeasurementSchemas() {
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < columnNameList.size(); i++) {
      schemas.add(
          new MeasurementSchema(columnNameList.get(i).getMeasurement(), dataTypeList.get(i)));
    }
    return schemas;
  }

  private void matchPattern() {
    // origin schemas from insertNode
    TSDataType[] originDataTypeList = insertNode.getDataTypes();
    String[] originMeasurementList = insertNode.getMeasurements();
    Object[] originColumns;

    if(insertNode instanceof InsertRowNode){
      originColumns = ((InsertRowNode)insertNode).getValues();
    } else if(insertNode instanceof InsertTabletNode){
      originColumns = ((InsertTabletNode) insertNode).getColumns();
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }

    int originColumnSize = originMeasurementList.length;
    int rowSize = ((InsertTabletNode) insertNode).getRowCount();

    this.deviceFullPath = insertNode.getDevicePath().getFullPath();
    this.dataTypeList = new ArrayList<>();
    this.columnNameList = new ArrayList<>();
    List<Integer> indexList = new ArrayList<>();

    if (pattern != null) {
      if (deviceFullPath.indexOf(pattern) != 0) {
        // TODO：it means match nothing
      } else {
        int patternLength = pattern.length();
        int deviceFullPathLength = deviceFullPath.length();

        for (int i = 0; i < originColumnSize; i++) {
          String measurement = originMeasurementList[i];

          // 如果匹配成功，就收集元数据
          if (patternLength == deviceFullPathLength + measurement.length() + 1
              && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {

            columnNameList.add(new Path(deviceFullPath, measurement, false));
            dataTypeList.add(originDataTypeList[i]);
            indexList.add(i);
          }
        }

        int columnSize = indexList.size();
        this.columns = new Object[columnSize][rowSize];

        for (int i = 0; i < columnSize; i++) {
          Object[] column = (Object[]) originColumns[indexList.get(i)];
          System.arraycopy(column, 0, columns[i], 0, rowSize);
        }

        convertColumnsToRows();
      }
    }
  }

  private void convertColumnsToRows() {
    int rowSize = columns[0].length;
    int columnSize = columns.length;
    this.rowRecords = new Object[rowSize][columnSize];

    for (int i = 0; i < rowSize; i++) {
      Object[] row = new Object[columnSize];
      for (int j = 0; j < columnSize; j++) {
        row[j] = columns[j][i];
      }
      rowRecords[i] = row;
    }
  }

  @Override
  public boolean increaseReferenceCount(String holderMessage) {
    // TODO: use WALPipeHandler pinMemtable
    referenceCount.incrementAndGet();
    return true;
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage) {
    // TODO: use WALPipeHandler unpinMemetable
    referenceCount.decrementAndGet();
    return true;
  }

  @Override
  public int getReferenceCount() {
    // TODO: use WALPipeHandler unpinMemetable
    return referenceCount.get();
  }

  @Override
  public void setPattern(String pathPattern) {
    this.pattern = pathPattern;
  }

  @Override
  public String getPattern() {
    return pattern;
  }

  @Override
  public String toString() {
    return "PipeTabletInsertionEvent{" + "insertNode=" + insertNode + '}';
  }
}
