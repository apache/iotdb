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
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

public class TabletInsertionDataContainer {

  private String deviceId;
  private MeasurementSchema[] measurementSchemaList;
  private String[] columnNameStringList;

  private long[] timestampColumn;
  private Object[][] valueColumns;
  private TSDataType[] valueColumnTypes;
  private BitMap[] nullValueColumnBitmaps;

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
        insertRowNode, pattern, originColumnIndex2FilteredColumnIndexMapperList);
    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize][1];
    this.valueColumnTypes = new TSDataType[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final MeasurementSchema[] originMeasurementSchemaList = insertRowNode.getMeasurementSchemas();
    final String[] originColumnNameStringList = insertRowNode.getMeasurements();
    final Object[] originValueColumns = insertRowNode.getValues();
    final TSDataType[] originValueColumnTypes = insertRowNode.getDataTypes();

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumns[filteredColumnIndex][0] = originValueColumns[i];
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        this.nullValueColumnBitmaps[filteredColumnIndex] = new BitMap(1);
      }
    }
  }

  private void parse(InsertTabletNode insertTabletNode, String pattern)
      throws IllegalPathException {
    final int originColumnSize = insertTabletNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = insertTabletNode.getDevicePath().getFullPath();
    this.timestampColumn = insertTabletNode.getTimes();

    generateColumnIndexMapper(
        insertTabletNode, pattern, originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize][];
    this.valueColumnTypes = new TSDataType[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final MeasurementSchema[] originMeasurementSchemaList =
        insertTabletNode.getMeasurementSchemas();
    final String[] originColumnNameStringList = insertTabletNode.getMeasurements();
    final Object[] originValueColumns = insertTabletNode.getColumns();
    final TSDataType[] originValueColumnTypes = insertTabletNode.getDataTypes();
    final BitMap[] originBitMapList = insertTabletNode.getBitMaps();

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumns[filteredColumnIndex] =
            convertToColumn(originValueColumns[i], originValueColumnTypes[i], originBitMapList[i]);
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        this.nullValueColumnBitmaps[filteredColumnIndex] = originBitMapList[i];
      }
    }
  }

  private void parse(Tablet tablet, String pattern) {
    final int originColumnSize = tablet.getSchemas().size();
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = tablet.deviceId;
    this.timestampColumn = tablet.timestamps;

    generateColumnIndexMapper(tablet, pattern, originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize][];
    this.valueColumnTypes = new TSDataType[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final List<MeasurementSchema> originMeasurementSchemaList = tablet.getSchemas();
    final String[] originColumnNameStringList = new String[originColumnSize];
    final TSDataType[] originValueColumnTypes = new TSDataType[originColumnSize];
    for (int i = 0; i < originColumnSize; i++) {
      originColumnNameStringList[i] = originMeasurementSchemaList.get(i).getMeasurementId();
      originValueColumnTypes[i] = originMeasurementSchemaList.get(i).getType();
    }
    final Object[] originValueColumns = tablet.values;
    final BitMap[] originBitMapList = tablet.bitMaps;

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList.get(i);
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumns[filteredColumnIndex] =
            convertToColumn(originValueColumns[i], originValueColumnTypes[i], originBitMapList[i]);
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        this.nullValueColumnBitmaps[filteredColumnIndex] = originBitMapList[i];
      }
    }
  }

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
            && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
          originColumnIndex2FilteredColumnIndexMapperList[i] = filteredCount++;
        }
      }
    }
  }

  private void generateColumnIndexMapper(
      InsertNode insertNode,
      String pattern,
      Integer[] originColumnIndex2FilteredColumnIndexMapperList) {
    generateColumnIndexMapper(
        insertNode.getMeasurements(), pattern, originColumnIndex2FilteredColumnIndexMapperList);
  }

  private void generateColumnIndexMapper(
      Tablet tablet, String pattern, Integer[] originColumnIndex2FilteredColumnIndexMapperList) {
    final List<MeasurementSchema> originMeasurementSchemaList = tablet.getSchemas();
    final String[] originMeasurementList = new String[originMeasurementSchemaList.size()];
    for (int i = 0; i < originMeasurementSchemaList.size(); i++) {
      originMeasurementList[i] = originMeasurementSchemaList.get(i).getMeasurementId();
    }
    generateColumnIndexMapper(
        originMeasurementList, pattern, originColumnIndex2FilteredColumnIndexMapperList);
  }

  private Object[] convertToColumn(Object originColumn, TSDataType dataType, BitMap bitMap) {
    switch (dataType) {
      case INT32:
        final int[] intValues = (int[]) originColumn;
        final Integer[] integerValues = new Integer[intValues.length];
        for (int i = 0; i < intValues.length; i++) {
          integerValues[i] = bitMap != null && bitMap.isMarked(i) ? null : intValues[i];
        }
        return integerValues;
      case INT64:
        final long[] longValues = (long[]) originColumn;
        final Long[] longValues2 = new Long[longValues.length];
        for (int i = 0; i < longValues.length; i++) {
          longValues2[i] = bitMap != null && bitMap.isMarked(i) ? null : longValues[i];
        }
        return longValues2;
      case FLOAT:
        final float[] floatValues = (float[]) originColumn;
        final Float[] floatValues2 = new Float[floatValues.length];
        for (int i = 0; i < floatValues.length; i++) {
          floatValues2[i] = bitMap != null && bitMap.isMarked(i) ? null : floatValues[i];
        }
        return floatValues2;
      case DOUBLE:
        final double[] doubleValues = (double[]) originColumn;
        final Double[] doubleValues2 = new Double[doubleValues.length];
        for (int i = 0; i < doubleValues.length; i++) {
          doubleValues2[i] = bitMap != null && bitMap.isMarked(i) ? null : doubleValues[i];
        }
        return doubleValues2;
      case BOOLEAN:
        final boolean[] booleanValues = (boolean[]) originColumn;
        final Boolean[] booleanValues2 = new Boolean[booleanValues.length];
        for (int i = 0; i < booleanValues.length; i++) {
          booleanValues2[i] = bitMap != null && bitMap.isMarked(i) ? null : booleanValues[i];
        }
        return booleanValues2;
      case TEXT:
        final Binary[] binaryValues = (Binary[]) originColumn;
        final String[] stringValues = new String[binaryValues.length];
        for (int i = 0; i < binaryValues.length; i++) {
          stringValues[i] =
              bitMap != null && bitMap.isMarked(i) ? null : binaryValues[i].getStringValue();
        }
        return stringValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  ////////////////////////////  process  ////////////////////////////

  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    final PipeRowCollector rowCollector = new PipeRowCollector();
    for (int i = 0; i < timestampColumn.length; i++) {
      consumer.accept(
          new PipeRow(
              i,
              deviceId,
              measurementSchemaList,
              timestampColumn,
              valueColumns,
              valueColumnTypes,
              columnNameStringList),
          rowCollector);
    }
    return rowCollector.toTabletInsertionEvent();
  }

  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    final PipeRowCollector rowCollector = new PipeRowCollector();
    consumer.accept(convertToTablet(), rowCollector);
    return rowCollector.toTabletInsertionEvent();
  }

  ////////////////////////////  convert  ////////////////////////////

  public Tablet convertToTablet() {
    if (tablet != null) {
      return tablet;
    }

    final int columnSize = measurementSchemaList.length;
    final int rowSize = valueColumns[0].length;

    final List<MeasurementSchema> measurementSchemaArrayList =
        new ArrayList<>(Arrays.asList(measurementSchemaList).subList(0, columnSize));

    final Tablet newTablet = new Tablet(deviceId, measurementSchemaArrayList, rowSize);
    newTablet.timestamps = timestampColumn;
    newTablet.bitMaps = nullValueColumnBitmaps;
    newTablet.values = squashFromColumnList(valueColumns, valueColumnTypes);

    tablet = newTablet;
    return tablet;
  }

  private Object[] squashFromColumnList(Object[][] valueColumns, TSDataType[] valueColumnTypes) {
    final Object[] values = new Object[valueColumns.length];
    for (int i = 0; i < valueColumns.length; i++) {
      values[i] = squashFromColumn(valueColumns[i], valueColumnTypes[i]);
    }
    return values;
  }

  private Object squashFromColumn(Object[] valueColumn, TSDataType valueColumnType) {
    switch (valueColumnType) {
      case INT32:
        final Integer[] intValues = (Integer[]) valueColumn;
        final int[] intValues2 = new int[intValues.length];
        for (int i = 0; i < intValues.length; i++) {
          intValues2[i] = intValues[i] == null ? 0 : intValues[i];
        }
        return intValues2;
      case INT64:
        final Long[] longValues = (Long[]) valueColumn;
        final long[] longValues2 = new long[longValues.length];
        for (int i = 0; i < longValues.length; i++) {
          longValues2[i] = longValues[i] == null ? 0 : longValues[i];
        }
        return longValues2;
      case FLOAT:
        final Float[] floatValues = (Float[]) valueColumn;
        final float[] floatValues2 = new float[floatValues.length];
        for (int i = 0; i < floatValues.length; i++) {
          floatValues2[i] = floatValues[i] == null ? 0 : floatValues[i];
        }
        return floatValues2;
      case DOUBLE:
        final Double[] doubleValues = (Double[]) valueColumn;
        final double[] doubleValues2 = new double[doubleValues.length];
        for (int i = 0; i < doubleValues.length; i++) {
          doubleValues2[i] = doubleValues[i] == null ? 0 : doubleValues[i];
        }
        return doubleValues2;
      case BOOLEAN:
        final Boolean[] booleanValues = (Boolean[]) valueColumn;
        final boolean[] booleanValues2 = new boolean[booleanValues.length];
        for (int i = 0; i < booleanValues.length; i++) {
          booleanValues2[i] = booleanValues[i] != null && booleanValues[i];
        }
        return booleanValues2;
      case TEXT:
        final String[] stringValues = (String[]) valueColumn;
        final Binary[] binaryValues = new Binary[stringValues.length];
        for (int i = 0; i < stringValues.length; i++) {
          binaryValues[i] =
              stringValues[i] == null ? Binary.EMPTY_VALUE : Binary.valueOf(stringValues[i]);
        }
        return binaryValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", valueColumnType));
    }
  }
}
