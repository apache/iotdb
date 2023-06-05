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

package org.apache.iotdb.db.pipe.core.event.view.datastructure;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.pipe.core.event.view.access.PipeRow;
import org.apache.iotdb.db.pipe.core.event.view.collector.PipeRowCollector;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

public class TabletInsertionDataContainer {

  private String deviceId;
  private MeasurementSchema[] measurementSchemaList;
  private String[] columnNameStringList;

  private long[] timestampColumn;
  private TSDataType[] valueColumnTypes;
  // each column of Object[] is a column of primitive type array
  private Object[] valueColumns;
  private BitMap[] nullValueColumnBitmaps;
  private int rowCount;

  private Tablet tablet;

  public TabletInsertionDataContainer(InsertNode insertNode, String pattern) {
    try {
      if (insertNode instanceof InsertRowNode) {
        parse((InsertRowNode) insertNode, pattern);
      } else if (insertNode instanceof InsertTabletNode) {
        parse((InsertTabletNode) insertNode, pattern);
      } else {
        throw new UnSupportedDataTypeException(
            String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
      }
    } catch (IllegalPathException e) {
      throw new PipeException(
          String.format("Failed to parse insertNode with pattern %s.", pattern), e);
    }
  }

  public TabletInsertionDataContainer(Tablet tablet, String pattern) {
    parse(tablet, pattern);
  }

  //////////////////////////// parse ////////////////////////////

  private void parse(InsertRowNode insertRowNode, String pattern) throws IllegalPathException {
    final int originColumnSize = insertRowNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = insertRowNode.getDevicePath().getFullPath();
    this.timestampColumn = new long[] {insertRowNode.getTime()};

    generateColumnIndexMapper(
        insertRowNode.getMeasurements(), pattern, originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumnTypes = new TSDataType[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final MeasurementSchema[] originMeasurementSchemaList = insertRowNode.getMeasurementSchemas();
    final String[] originColumnNameStringList = insertRowNode.getMeasurements();
    final TSDataType[] originValueColumnTypes = insertRowNode.getDataTypes();
    final Object[] originValueColumns = insertRowNode.getValues();

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        switch (originValueColumnTypes[i]) {
          case INT32:
            this.valueColumns[filteredColumnIndex] = new int[] {(Integer) originValueColumns[i]};
            break;
          case INT64:
            this.valueColumns[filteredColumnIndex] = new long[] {(Long) originValueColumns[i]};
            break;
          case FLOAT:
            this.valueColumns[filteredColumnIndex] = new float[] {(Float) originValueColumns[i]};
            break;
          case DOUBLE:
            this.valueColumns[filteredColumnIndex] = new double[] {(Double) originValueColumns[i]};
            break;
          case BOOLEAN:
            this.valueColumns[filteredColumnIndex] =
                new boolean[] {(Boolean) originValueColumns[i]};
            break;
          case TEXT:
            this.valueColumns[filteredColumnIndex] = new Binary[] {(Binary) originValueColumns[i]};
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.", originValueColumnTypes[i].toString()));
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = new BitMap(1);
      }
    }

    rowCount = 1;
  }

  private void parse(InsertTabletNode insertTabletNode, String pattern)
      throws IllegalPathException {
    final int originColumnSize = insertTabletNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = insertTabletNode.getDevicePath().getFullPath();
    this.timestampColumn = insertTabletNode.getTimes();

    generateColumnIndexMapper(
        insertTabletNode.getMeasurements(),
        pattern,
        originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumnTypes = new TSDataType[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final MeasurementSchema[] originMeasurementSchemaList =
        insertTabletNode.getMeasurementSchemas();
    final String[] originColumnNameStringList = insertTabletNode.getMeasurements();
    final TSDataType[] originValueColumnTypes = insertTabletNode.getDataTypes();
    final Object[] originValueColumns = insertTabletNode.getColumns();
    final BitMap[] originBitMapList =
        (insertTabletNode.getBitMaps() == null
            ? IntStream.range(0, originColumnSize)
                .boxed()
                .map(o -> new BitMap(timestampColumn.length))
                .toArray(BitMap[]::new)
            : insertTabletNode.getBitMaps());
    for (int i = 0; i < originBitMapList.length; i++) {
      if (originBitMapList[i] == null) {
        originBitMapList[i] = new BitMap(timestampColumn.length);
      }
    }

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        this.valueColumns[filteredColumnIndex] = originValueColumns[i];
        this.nullValueColumnBitmaps[filteredColumnIndex] = originBitMapList[i];
      }
    }

    rowCount = timestampColumn.length;
  }

  private void parse(Tablet tablet, String pattern) {
    final int originColumnSize = tablet.getSchemas().size();
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = tablet.deviceId;
    this.timestampColumn = tablet.timestamps;

    final List<MeasurementSchema> originMeasurementSchemaList = tablet.getSchemas();
    final String[] originMeasurementList = new String[originMeasurementSchemaList.size()];
    for (int i = 0; i < originMeasurementSchemaList.size(); i++) {
      originMeasurementList[i] = originMeasurementSchemaList.get(i).getMeasurementId();
    }
    generateColumnIndexMapper(
        originMeasurementList, pattern, originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumnTypes = new TSDataType[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final String[] originColumnNameStringList = new String[originColumnSize];
    final TSDataType[] originValueColumnTypes = new TSDataType[originColumnSize];
    for (int i = 0; i < originColumnSize; i++) {
      originColumnNameStringList[i] = originMeasurementSchemaList.get(i).getMeasurementId();
      originValueColumnTypes[i] = originMeasurementSchemaList.get(i).getType();
    }
    final Object[] originValueColumns = tablet.values;
    final BitMap[] originBitMapList =
        tablet.bitMaps == null
            ? IntStream.range(0, originColumnSize)
                .boxed()
                .map(o -> new BitMap(timestampColumn.length))
                .toArray(BitMap[]::new)
            : tablet.bitMaps;
    for (int i = 0; i < originBitMapList.length; i++) {
      if (originBitMapList[i] == null) {
        originBitMapList[i] = new BitMap(timestampColumn.length);
      }
    }

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList.get(i);
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        this.valueColumns[filteredColumnIndex] = originValueColumns[i];
        this.nullValueColumnBitmaps[filteredColumnIndex] = originBitMapList[i];
      }
    }

    rowCount = tablet.rowSize;
  }

  // TODO: cache the result keyed by deviceId to improve performance
  private void generateColumnIndexMapper(
      String[] originMeasurementList,
      String pattern,
      Integer[] originColumnIndex2FilteredColumnIndexMapperList) {
    final int originColumnSize = originMeasurementList.length;

    // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
    // in this case, all data can be matched without checking the measurements
    if (pattern == null || pattern.length() <= deviceId.length() && deviceId.startsWith(pattern)) {
      for (int i = 0; i < originColumnSize; i++) {
        originColumnIndex2FilteredColumnIndexMapperList[i] = i;
      }
    }

    // case 2: for example, pattern is root.a.b.c and device is root.a.b
    // in this case, we need to check the full path
    else if (pattern.length() > deviceId.length() && pattern.startsWith(deviceId)) {
      int filteredCount = 0;

      for (int i = 0; i < originColumnSize; i++) {
        final String measurement = originMeasurementList[i];

        // low cost check comes first
        if (pattern.length() == deviceId.length() + measurement.length() + 1
            // high cost check comes later
            && pattern.startsWith(deviceId)
            && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
          originColumnIndex2FilteredColumnIndexMapperList[i] = filteredCount++;
        }
      }
    }
  }

  ////////////////////////////  process  ////////////////////////////

  public Iterable<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (valueColumns.length == 0 || timestampColumn.length == 0) {
      return Collections.emptyList();
    }

    final PipeRowCollector rowCollector = new PipeRowCollector();
    for (int i = 0; i < timestampColumn.length; i++) {
      consumer.accept(
          new PipeRow(
              i,
              deviceId,
              measurementSchemaList,
              timestampColumn,
              valueColumnTypes,
              valueColumns,
              nullValueColumnBitmaps,
              columnNameStringList),
          rowCollector);
    }
    return rowCollector.convertToTabletInsertionEvents();
  }

  public Iterable<TabletInsertionEvent> processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    final PipeRowCollector rowCollector = new PipeRowCollector();
    consumer.accept(convertToTablet(), rowCollector);
    return rowCollector.convertToTabletInsertionEvents();
  }

  ////////////////////////////  convert  ////////////////////////////

  public Tablet convertToTablet() {
    if (tablet != null) {
      return tablet;
    }

    final int columnSize = measurementSchemaList.length;
    final List<MeasurementSchema> measurementSchemaArrayList =
        new ArrayList<>(Arrays.asList(measurementSchemaList).subList(0, columnSize));
    final Tablet newTablet = new Tablet(deviceId, measurementSchemaArrayList, rowCount);
    newTablet.timestamps = timestampColumn;
    newTablet.bitMaps = nullValueColumnBitmaps;
    newTablet.values = valueColumns;
    newTablet.rowSize = rowCount;

    tablet = newTablet;

    return tablet;
  }
}
