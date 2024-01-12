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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.event.common.row.PipeRowCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TabletInsertionDataContainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TabletInsertionDataContainer.class);

  private final PipeTaskMeta pipeTaskMeta; // used to report progress
  private final EnrichedEvent
      sourceEvent; // used to report progress and filter value columns by time range

  private String deviceId;
  private boolean isAligned;
  private MeasurementSchema[] measurementSchemaList;
  private String[] columnNameStringList;

  private long[] timestampColumn;
  private TSDataType[] valueColumnTypes;
  // Each column of Object[] is a column of primitive type array
  private Object[] valueColumns;
  private BitMap[] nullValueColumnBitmaps;
  private int rowCount;

  private Tablet tablet;

  private static final Integer CACHED_FULL_ROW_INDEX_LIST_ROW_COUNT_UPPER = 16;
  private static final Map<Integer, List<Integer>> cachedFullRowIndexList = new HashMap<>();

  static {
    for (int rowCount = 0; rowCount <= CACHED_FULL_ROW_INDEX_LIST_ROW_COUNT_UPPER; ++rowCount) {
      cachedFullRowIndexList.put(
          rowCount, IntStream.range(0, rowCount).boxed().collect(Collectors.toList()));
    }
  }

  public TabletInsertionDataContainer(
      PipeTaskMeta pipeTaskMeta, EnrichedEvent sourceEvent, InsertNode insertNode, String pattern) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;

    if (insertNode instanceof InsertRowNode) {
      parse((InsertRowNode) insertNode, pattern);
    } else if (insertNode instanceof InsertTabletNode) {
      parse((InsertTabletNode) insertNode, pattern);
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
  }

  public TabletInsertionDataContainer(
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      Tablet tablet,
      boolean isAligned,
      String pattern) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;

    parse(tablet, isAligned, pattern);
  }

  @TestOnly
  public TabletInsertionDataContainer(InsertNode insertNode, String pattern) {
    this(null, null, insertNode, pattern);
  }

  public boolean isAligned() {
    return isAligned;
  }

  //////////////////////////// parse ////////////////////////////

  private void parse(InsertRowNode insertRowNode, String pattern) {
    final int originColumnSize = insertRowNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = insertRowNode.getDevicePath().getFullPath();
    this.isAligned = insertRowNode.isAligned();

    final long[] originTimestampColumn = new long[] {insertRowNode.getTime()};
    List<Integer> rowIndexList = generateRowIndexList(originTimestampColumn);
    this.timestampColumn = rowIndexList.stream().mapToLong(i -> originTimestampColumn[i]).toArray();

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
        BitMap bitMap = new BitMap(this.timestampColumn.length);
        if (Objects.isNull(originValueColumns[i]) || Objects.isNull(originValueColumnTypes[i])) {
          this.valueColumns[filteredColumnIndex] = null;
          bitMap.markAll();
        } else {
          this.valueColumns[filteredColumnIndex] =
              filterValueColumnsByRowIndexList(
                  originValueColumnTypes[i],
                  originValueColumns[i],
                  rowIndexList,
                  true,
                  bitMap, // use the output bitmap since there is no bitmap in InsertRowNode
                  bitMap);
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = bitMap;
      }
    }

    this.rowCount = this.timestampColumn.length;
    if (this.rowCount == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "InsertRowNode({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], the corresponding source event({}) will be ignored.",
          insertRowNode,
          pattern,
          this.sourceEvent.getStartTime(),
          this.sourceEvent.getEndTime(),
          this.sourceEvent);
    }
  }

  private void parse(InsertTabletNode insertTabletNode, String pattern) {
    final int originColumnSize = insertTabletNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = insertTabletNode.getDevicePath().getFullPath();
    this.isAligned = insertTabletNode.isAligned();

    final long[] originTimestampColumn = insertTabletNode.getTimes();
    final int originRowSize = originTimestampColumn.length;
    List<Integer> rowIndexList = generateRowIndexList(originTimestampColumn);
    this.timestampColumn = rowIndexList.stream().mapToLong(i -> originTimestampColumn[i]).toArray();

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
                .map(o -> new BitMap(originRowSize))
                .toArray(BitMap[]::new)
            : insertTabletNode.getBitMaps());
    for (int i = 0; i < originBitMapList.length; i++) {
      if (originBitMapList[i] == null) {
        originBitMapList[i] = new BitMap(originRowSize);
      }
    }

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        BitMap bitMap = new BitMap(this.timestampColumn.length);
        if (Objects.isNull(originValueColumns[i]) || Objects.isNull(originValueColumnTypes[i])) {
          this.valueColumns[filteredColumnIndex] = null;
          bitMap.markAll();
        } else {
          this.valueColumns[filteredColumnIndex] =
              filterValueColumnsByRowIndexList(
                  originValueColumnTypes[i],
                  originValueColumns[i],
                  rowIndexList,
                  false,
                  originBitMapList[i],
                  bitMap);
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = bitMap;
      }
    }

    this.rowCount = this.timestampColumn.length;
    if (rowCount == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "InsertTabletNode({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], the corresponding source event({}) will be ignored.",
          insertTabletNode,
          pattern,
          sourceEvent.getStartTime(),
          sourceEvent.getEndTime(),
          sourceEvent);
    }
  }

  private void parse(Tablet tablet, boolean isAligned, String pattern) {
    final int originColumnSize = tablet.getSchemas().size();
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    this.deviceId = tablet.deviceId;
    this.isAligned = isAligned;

    final long[] originTimestampColumn =
        Arrays.copyOf(
            tablet.timestamps, tablet.rowSize); // tablet.timestamps.length == tablet.maxRowNumber
    List<Integer> rowIndexList = generateRowIndexList(originTimestampColumn);
    this.timestampColumn = rowIndexList.stream().mapToLong(i -> originTimestampColumn[i]).toArray();

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
    final Object[] originValueColumns =
        tablet.values; // we do not reduce value columns here by origin row size
    final BitMap[] originBitMapList =
        tablet.bitMaps == null
            ? IntStream.range(0, originColumnSize)
                .boxed()
                .map(o -> new BitMap(tablet.getMaxRowNumber()))
                .toArray(BitMap[]::new)
            : tablet.bitMaps; // we do not reduce bitmaps here by origin row size
    for (int i = 0; i < originBitMapList.length; i++) {
      if (originBitMapList[i] == null) {
        originBitMapList[i] = new BitMap(tablet.getMaxRowNumber());
      }
    }

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList.get(i);
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] = originValueColumnTypes[i];
        BitMap bitMap = new BitMap(this.timestampColumn.length);
        if (Objects.isNull(originValueColumns[i]) || Objects.isNull(originValueColumnTypes[i])) {
          this.valueColumns[filteredColumnIndex] = null;
          bitMap.markAll();
        } else {
          this.valueColumns[filteredColumnIndex] =
              filterValueColumnsByRowIndexList(
                  originValueColumnTypes[i],
                  originValueColumns[i],
                  rowIndexList,
                  false,
                  originBitMapList[i],
                  bitMap);
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = bitMap;
      }
    }

    this.rowCount = this.timestampColumn.length;
    if (this.rowCount == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Tablet({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], the corresponding source event({}) will be ignored.",
          tablet,
          pattern,
          this.sourceEvent.getStartTime(),
          this.sourceEvent.getEndTime(),
          this.sourceEvent);
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

        // ignore null measurement for partial insert
        if (measurement == null) {
          continue;
        }

        // low cost check comes first
        if (pattern.length() == deviceId.length() + measurement.length() + 1
            // high cost check comes later
            && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
          originColumnIndex2FilteredColumnIndexMapperList[i] = filteredCount++;
        }
      }
    }
  }

  private List<Integer> generateRowIndexList(final long[] originTimestampColumn) {
    final int rowCount = originTimestampColumn.length;
    if (Objects.isNull(sourceEvent) || !sourceEvent.shouldParseTime()) {
      return generateFullRowIndexList(rowCount);
    }

    List<Integer> rowIndexList = new ArrayList<>();
    // We assume that `originTimestampColumn` is ordered.
    if (originTimestampColumn[originTimestampColumn.length - 1] < sourceEvent.getStartTime()
        || originTimestampColumn[0] > sourceEvent.getEndTime()) {
      return rowIndexList;
    }

    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
      if (sourceEvent.getStartTime() <= originTimestampColumn[rowIndex]
          && originTimestampColumn[rowIndex] <= sourceEvent.getEndTime()) {
        rowIndexList.add(rowIndex);
      }
    }

    return rowIndexList;
  }

  private static List<Integer> generateFullRowIndexList(int rowCount) {
    if (rowCount <= CACHED_FULL_ROW_INDEX_LIST_ROW_COUNT_UPPER) {
      return cachedFullRowIndexList.get(rowCount);
    }
    return IntStream.range(0, rowCount).boxed().collect(Collectors.toList());
  }

  private static Object filterValueColumnsByRowIndexList(
      @NonNull TSDataType type,
      @NonNull Object originValueColumn,
      @NonNull List<Integer> rowIndexList,
      boolean isSingleOriginValueColumn,
      @NonNull BitMap originNullValueColumnBitmap,
      @NonNull BitMap nullValueColumnBitmap /* output parameters */) {
    switch (type) {
      case INT32:
        {
          int[] intValueColumns =
              isSingleOriginValueColumn
                  ? new int[] {(int) originValueColumn}
                  : (int[]) originValueColumn;
          int[] valueColumns = new int[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = intValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case INT64:
        {
          long[] longValueColumns =
              isSingleOriginValueColumn
                  ? new long[] {(long) originValueColumn}
                  : (long[]) originValueColumn;
          long[] valueColumns = new long[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0L;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = longValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case FLOAT:
        {
          float[] floatValueColumns =
              isSingleOriginValueColumn
                  ? new float[] {(float) originValueColumn}
                  : (float[]) originValueColumn;
          float[] valueColumns = new float[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0F;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = floatValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case DOUBLE:
        {
          double[] doubleValueColumns =
              isSingleOriginValueColumn
                  ? new double[] {(double) originValueColumn}
                  : (double[]) originValueColumn;
          double[] valueColumns = new double[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0D;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = doubleValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case BOOLEAN:
        {
          boolean[] booleanValueColumns =
              isSingleOriginValueColumn
                  ? new boolean[] {(boolean) originValueColumn}
                  : (boolean[]) originValueColumn;
          boolean[] valueColumns = new boolean[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = false;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = booleanValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case TEXT:
        {
          Binary[] binaryValueColumns =
              isSingleOriginValueColumn
                  ? new Binary[] {(Binary) originValueColumn}
                  : (Binary[]) originValueColumn;
          Binary[] valueColumns = new Binary[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (Objects.isNull(binaryValueColumns[rowIndexList.get(i)])
                || Objects.isNull(binaryValueColumns[rowIndexList.get(i)].getValues())
                || originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = Binary.EMPTY_VALUE;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = new Binary(binaryValueColumns[rowIndexList.get(i)].getValues());
            }
          }
          return valueColumns;
        }
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", type));
    }
  }

  ////////////////////////////  process  ////////////////////////////

  public Iterable<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (valueColumns.length == 0 || timestampColumn.length == 0) {
      return Collections.emptyList();
    }

    final PipeRowCollector rowCollector = new PipeRowCollector(pipeTaskMeta, sourceEvent);
    for (int i = 0; i < rowCount; i++) {
      consumer.accept(
          new PipeRow(
              i,
              deviceId,
              isAligned,
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
    final PipeRowCollector rowCollector = new PipeRowCollector(pipeTaskMeta, sourceEvent);
    consumer.accept(convertToTablet(), rowCollector);
    return rowCollector.convertToTabletInsertionEvents();
  }

  ////////////////////////////  convertToTablet  ////////////////////////////

  public Tablet convertToTablet() {
    if (tablet != null) {
      return tablet;
    }

    final Tablet newTablet = new Tablet(deviceId, Arrays.asList(measurementSchemaList), rowCount);
    newTablet.timestamps = timestampColumn;
    newTablet.bitMaps = nullValueColumnBitmaps;
    newTablet.values = valueColumns;
    newTablet.rowSize = rowCount;

    tablet = newTablet;

    return tablet;
  }
}
