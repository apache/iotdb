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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.core.event.view.access.PipeRow;
import org.apache.iotdb.db.pipe.core.event.view.access.PipeRowIterator;
import org.apache.iotdb.db.pipe.core.event.view.collector.PipeRowCollector;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.wal.exception.WALPipeException;
import org.apache.iotdb.db.wal.utils.WALEntryHandler;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class PipeInsertNodeInsertionEvent extends EnrichedEvent implements TabletInsertionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInsertNodeInsertionEvent.class);

  private final WALEntryHandler walEntryHandler;
  private final ProgressIndex progressIndex;

  private List<TSDataType> columnTypeList;
  private List<Path> columnNameList;
  private String deviceId;
  private Object[][] rowRecords;
  private long[] timestamps;

  public PipeInsertNodeInsertionEvent(
      WALEntryHandler walEntryHandler, ProgressIndex progressIndex) {
    this(walEntryHandler, progressIndex, null, null);
  }

  private PipeInsertNodeInsertionEvent(
      WALEntryHandler walEntryHandler,
      ProgressIndex progressIndex,
      PipeTaskMeta pipeTaskMeta,
      String pattern) {
    super(pipeTaskMeta, pattern);
    this.walEntryHandler = walEntryHandler;
    this.progressIndex = progressIndex;

    matchPattern();
  }

  public InsertNode getInsertNode() throws WALPipeException {
    return walEntryHandler.getValue();
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean increaseResourceReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.wal().pin(walEntryHandler.getMemTableId(), walEntryHandler);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for memtable %d error. Holder Message: %s",
              walEntryHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean decreaseResourceReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.wal().unpin(walEntryHandler.getMemTableId());
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for memtable %d error. Holder Message: %s",
              walEntryHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public PipeInsertNodeInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta) {
    return new PipeInsertNodeInsertionEvent(walEntryHandler, progressIndex, pipeTaskMeta, pattern);
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();

    for (int i = 0; i < timestamps.length; i++) {
      Row row =
          new PipeRow(columnNameList, columnTypeList, timestamps[i]).setRowRecord(rowRecords[i]);
      consumer.accept(row, rowCollector);
    }

    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processByIterator(BiConsumer<Iterable<Row>, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();
    PipeRowIterator rowIterator =
        new PipeRowIterator(
            columnNameList, columnTypeList, timestamps, rowRecords, 0, timestamps.length);
    consumer.accept(rowIterator, rowCollector);
    return rowCollector.toTabletInsertionEvent();
  }

  @Override
  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    PipeRowCollector rowCollector = new PipeRowCollector();
    List<MeasurementSchema> schemas = createMeasurementSchemas();
    Tablet tablet = new Tablet(deviceId, schemas);

    for (int i = 0; i < timestamps.length; i++) {
      Row row =
          new PipeRow(columnNameList, columnTypeList, timestamps[i]).setRowRecord(rowRecords[i]);
      for (int rowIndex = 0; rowIndex < row.size(); rowIndex++) {
        tablet.addValue(columnNameList.get(i).getMeasurement(), rowIndex, row.getObject(i));
      }
    }

    return rowCollector.toTabletInsertionEvent();
  }

  private List<MeasurementSchema> createMeasurementSchemas() {
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < columnNameList.size(); i++) {
      schemas.add(
          new MeasurementSchema(columnNameList.get(i).getMeasurement(), columnTypeList.get(i)));
    }
    return schemas;
  }

  private void matchPattern() {
    try {
      InsertNode insertNode = walEntryHandler.getValue();
      if (insertNode instanceof InsertRowNode) {
        processRowNode((InsertRowNode) insertNode);
      } else if (insertNode instanceof InsertTabletNode) {
        processTabletNode((InsertTabletNode) insertNode);
      } else {
        throw new UnSupportedDataTypeException(
            String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
      }
    } catch (WALPipeException e) {
      LOGGER.error("Get InsertNode from WALEntryHandler error.", e);
    }
  }

  private void processRowNode(InsertRowNode insertRowNode) {
    TSDataType[] originDataTypeList = insertRowNode.getDataTypes();
    String[] originMeasurementList = insertRowNode.getMeasurements();
    Object[] originValues = insertRowNode.getValues();
    this.deviceId = insertRowNode.getDevicePath().getFullPath();
    this.timestamps = new long[] {insertRowNode.getTime()};

    processPatternWithSingleRow(originDataTypeList, originMeasurementList, originValues);
  }

  private void processTabletNode(InsertTabletNode insertTabletNode) {
    TSDataType[] originDataTypeList = insertTabletNode.getDataTypes();
    String[] originMeasurementList = insertTabletNode.getMeasurements();
    Object[] originColumns = insertTabletNode.getColumns();
    int rowSize = insertTabletNode.getRowCount();
    this.deviceId = insertTabletNode.getDevicePath().getFullPath();
    this.timestamps = insertTabletNode.getTimes();

    processPatternWithColumns(originDataTypeList, originMeasurementList, originColumns, rowSize);
  }

  private void processPatternWithSingleRow(
      TSDataType[] originDataTypeList, String[] originMeasurementList, Object[] originValues) {
    this.columnTypeList = new ArrayList<>();
    this.columnNameList = new ArrayList<>();
    List<Integer> indexList = new ArrayList<>();

    processPatternByDevice(originMeasurementList, originDataTypeList, indexList);

    for (int i = 0; i < indexList.size(); i++) {
      rowRecords[0][i] = originValues[indexList.get(i)];
    }
  }

  private void processPatternWithColumns(
      TSDataType[] originDataTypeList,
      String[] originMeasurementList,
      Object[] originColumns,
      int rowSize) {
    this.columnTypeList = new ArrayList<>();
    this.columnNameList = new ArrayList<>();
    List<Integer> indexList = new ArrayList<>();

    processPatternByDevice(originMeasurementList, originDataTypeList, indexList);

    int columnSize = indexList.size();
    this.rowRecords = new Object[rowSize][columnSize];

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      Object[] column = (Object[]) originColumns[indexList.get(columnIndex)];
      for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        rowRecords[rowIndex][columnIndex] = column[rowIndex];
      }
    }
  }

  private void processPatternByDevice(
      String[] originMeasurementList, TSDataType[] originDataTypeList, List<Integer> indexList) {
    int originColumnSize = originMeasurementList.length;
    // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
    // in this case, all data can be matched without checking the measurements
    if (pattern == null || pattern.length() <= deviceId.length() && deviceId.startsWith(pattern)) {
      for (int i = 0; i < originColumnSize; i++) {
        columnNameList.add(new Path(deviceId, originMeasurementList[i], false));
        columnTypeList.add(originDataTypeList[i]);
        indexList.add(i);
      }
    }

    // case 2: for example, pattern is root.a.b.c and device is root.a.b
    // in this case, we need to check the full path
    else if (pattern.length() > deviceId.length() && pattern.startsWith(deviceId)) {
      for (int i = 0; i < originColumnSize; i++) {
        String measurement = originMeasurementList[i];

        // low cost check comes first
        if (pattern.length() == deviceId.length() + measurement.length() + 1
            // high cost check comes later
            && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
          columnNameList.add(new Path(deviceId, measurement, false));
          columnTypeList.add(originDataTypeList[i]);
          indexList.add(i);
        }
      }
    }
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return "PipeTabletInsertionEvent{"
        + "walEntryHandler="
        + walEntryHandler
        + ", progressIndex="
        + progressIndex
        + '}';
  }
}
