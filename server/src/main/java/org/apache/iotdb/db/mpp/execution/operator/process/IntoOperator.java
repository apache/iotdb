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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IntoOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator child;

  private final List<InsertTabletStatementGenerator> insertTabletStatementGenerators;
  private final List<Pair<String, PartialPath>> sourceTargetPathPairList;
  private final Map<String, InputLocation> sourceColumnToInputLocationMap;

  public IntoOperator(
      OperatorContext operatorContext,
      Operator child,
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Pair<String, PartialPath>> sourceTargetPathPairList,
      Map<String, InputLocation> sourceColumnToInputLocationMap) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.insertTabletStatementGenerators =
        constructInsertTabletStatementGenerators(
            targetPathToSourceInputLocationMap, targetPathToDataTypeMap, targetDeviceToAlignedMap);
    this.sourceTargetPathPairList = sourceTargetPathPairList;
    this.sourceColumnToInputLocationMap = sourceColumnToInputLocationMap;
  }

  private List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap) {
    List<InsertTabletStatementGenerator> insertTabletStatementGenerators =
        new ArrayList<>(targetPathToSourceInputLocationMap.size());
    for (PartialPath targetDevice : targetPathToSourceInputLocationMap.keySet()) {
      InsertTabletStatementGenerator generator =
          new InsertTabletStatementGenerator(
              targetDevice,
              targetPathToSourceInputLocationMap.get(targetDevice),
              targetPathToDataTypeMap.get(targetDevice),
              targetDeviceToAlignedMap.get(targetDevice.toString()));
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    TsBlock inputTsBlock = child.next();
    int lastReadIndex = 0;
    while (lastReadIndex < inputTsBlock.getPositionCount()) {
      for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
        lastReadIndex = generator.processTsBlock(inputTsBlock, lastReadIndex);
      }
      insertMultiTabletsInternally(true);
    }

    if (child.hasNext()) {
      return null;
    } else {
      insertMultiTabletsInternally(false);
      return constructResultTsBlock();
    }
  }

  private void insertMultiTabletsInternally(boolean needCheck) {
    if ((needCheck && !insertTabletStatementGenerators.get(0).isFull())
        || insertTabletStatementGenerators.get(0).isEmpty()) {
      return;
    }

    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      insertTabletStatementList.add(generator.constructInsertTabletStatement());
    }

    InsertMultiTabletsStatement insertMultiTabletsStatement = new InsertMultiTabletsStatement();
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    // TODO: execute insertMultiTabletsStatement

    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      generator.reset();
    }
  }

  private TsBlock constructResultTsBlock() {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.selectIntoColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder resultTsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
      timeColumnBuilder.writeLong(0);
      columnBuilders[0].writeBinary(new Binary(sourceTargetPathPair.left));
      columnBuilders[1].writeBinary(new Binary(sourceTargetPathPair.right.toString()));
      columnBuilders[2].writeInt(findWritten(sourceTargetPathPair.left));
      resultTsBlockBuilder.declarePosition();
    }
    return resultTsBlockBuilder.build();
  }

  private int findWritten(String sourceColumn) {
    InputLocation inputLocation = sourceColumnToInputLocationMap.get(sourceColumn);
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      int written = generator.getWrittenCount(inputLocation);
      if (written != -1) {
        return written;
      }
    }
    return 0;
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  public static class InsertTabletStatementGenerator {

    private final int TABLET_ROW_LIMIT =
        IoTDBDescriptor.getInstance().getConfig().getSelectIntoInsertTabletPlanRowLimit();

    private final PartialPath devicePath;
    private final boolean isAligned;
    private final String[] measurements;
    private final TSDataType[] dataTypes;
    private final InputLocation[] inputLocations;

    private int rowCount = 0;

    private long[] times;
    private Object[] columns;
    private BitMap[] bitMaps;

    private final Map<InputLocation, AtomicInteger> writtenCounter;

    public InsertTabletStatementGenerator(
        PartialPath devicePath,
        Map<String, InputLocation> measurementToInputLocationMap,
        Map<String, TSDataType> measurementToDataTypeMap,
        Boolean isAligned) {
      this.devicePath = devicePath;
      this.isAligned = isAligned;
      this.measurements = measurementToInputLocationMap.keySet().toArray(new String[0]);
      this.dataTypes = measurementToDataTypeMap.values().toArray(new TSDataType[0]);
      this.inputLocations = measurementToInputLocationMap.values().toArray(new InputLocation[0]);
      this.writtenCounter = new HashMap<>();
      for (InputLocation inputLocation : inputLocations) {
        writtenCounter.put(inputLocation, new AtomicInteger(0));
      }
    }

    public void reset() {
      this.rowCount = 0;
      this.times = new long[TABLET_ROW_LIMIT];
      this.columns = new Object[this.measurements.length];
      this.bitMaps = new BitMap[this.measurements.length];
      for (int i = 0; i < this.bitMaps.length; ++i) {
        this.bitMaps[i] = new BitMap(TABLET_ROW_LIMIT);
        this.bitMaps[i].markAll();
      }
    }

    public int processTsBlock(TsBlock tsBlock, int lastReadIndex) {
      for (; lastReadIndex < tsBlock.getPositionCount(); lastReadIndex++) {

        times[rowCount] = tsBlock.getTimeByIndex(lastReadIndex);

        for (int i = 0; i < measurements.length; ++i) {
          Column valueColumn = tsBlock.getValueColumns()[inputLocations[i].getValueColumnIndex()];

          // if the value is NULL
          if (valueColumn.isNull(lastReadIndex)) {
            // bit in bitMaps are marked as 1 (NULL) by default
            continue;
          }

          bitMaps[i].unmark(rowCount);
          writtenCounter.get(inputLocations[i]).getAndIncrement();
          switch (valueColumn.getDataType()) {
            case INT32:
              ((int[]) columns[i])[rowCount] = valueColumn.getInt(lastReadIndex);
              break;
            case INT64:
              ((long[]) columns[i])[rowCount] = valueColumn.getLong(lastReadIndex);
              break;
            case FLOAT:
              ((float[]) columns[i])[rowCount] = valueColumn.getFloat(lastReadIndex);
              break;
            case DOUBLE:
              ((double[]) columns[i])[rowCount] = valueColumn.getDouble(lastReadIndex);
              break;
            case BOOLEAN:
              ((boolean[]) columns[i])[rowCount] = valueColumn.getBoolean(lastReadIndex);
              break;
            case TEXT:
              ((Binary[]) columns[i])[rowCount] = valueColumn.getBinary(lastReadIndex);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format(
                      "data type %s is not supported when convert data at client",
                      valueColumn.getDataType()));
          }
        }

        ++rowCount;
        if (rowCount == TABLET_ROW_LIMIT) {
          break;
        }
      }
      return lastReadIndex;
    }

    public boolean isFull() {
      return rowCount == TABLET_ROW_LIMIT;
    }

    public boolean isEmpty() {
      return rowCount == 0;
    }

    public InsertTabletStatement constructInsertTabletStatement() {
      InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
      insertTabletStatement.setDevicePath(devicePath);
      insertTabletStatement.setAligned(isAligned);
      insertTabletStatement.setMeasurements(measurements);
      insertTabletStatement.setDataTypes(dataTypes);
      insertTabletStatement.setRowCount(rowCount);

      if (rowCount != TABLET_ROW_LIMIT) {
        times = Arrays.copyOf(times, rowCount);
        for (int i = 0; i < columns.length; i++) {
          switch (dataTypes[i]) {
            case BOOLEAN:
              columns[i] = Arrays.copyOf((boolean[]) columns[i], rowCount);
              break;
            case INT32:
              columns[i] = Arrays.copyOf((int[]) columns[i], rowCount);
              break;
            case INT64:
              columns[i] = Arrays.copyOf((long[]) columns[i], rowCount);
              break;
            case FLOAT:
              columns[i] = Arrays.copyOf((float[]) columns[i], rowCount);
              break;
            case DOUBLE:
              columns[i] = Arrays.copyOf((double[]) columns[i], rowCount);
              break;
            case TEXT:
              columns[i] = Arrays.copyOf((Binary[]) columns[i], rowCount);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", dataTypes[i]));
          }
        }
      }

      insertTabletStatement.setTimes(times);
      insertTabletStatement.setColumns(columns);
      insertTabletStatement.setBitMaps(bitMaps);

      return insertTabletStatement;
    }

    public int getWrittenCount(InputLocation inputLocation) {
      if (!writtenCounter.containsKey(inputLocation)) {
        return -1;
      }
      return writtenCounter.get(inputLocation).get();
    }
  }
}
