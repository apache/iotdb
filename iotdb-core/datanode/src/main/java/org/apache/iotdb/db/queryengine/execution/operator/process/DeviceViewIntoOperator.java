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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class DeviceViewIntoOperator extends AbstractTreeIntoOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceViewIntoOperator.class);

  private static final long maxTsBlockSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long tsBlockInitialSize =
      TsBlock.INSTANCE_SIZE
          + RamUsageEstimator.shallowSizeOfInstance(TimeColumn.class)
          + 3 * RamUsageEstimator.shallowSizeOfInstance(BinaryColumn.class)
          + RamUsageEstimator.shallowSizeOfInstance(LongColumn.class);

  private final Map<String, Map<PartialPath, Map<String, InputLocation>>>
      deviceToTargetPathSourceInputLocationMap;
  private final Map<String, Map<PartialPath, Map<String, TSDataType>>>
      deviceToTargetPathDataTypeMap;
  private final Map<String, Boolean> targetDeviceToAlignedMap;
  private final Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap;
  private Iterator<String> deviceIterator;
  // Device -> current index in the source-target path pair list, used for batching result TsBlocks
  private final Map<String, Integer> deviceToSourceTargetPathPairIndex;
  // Target path -> number of written rows, used to show operation results to users
  private final Map<PartialPath, Long> targetPathToWriteCountMap;

  private final int deviceColumnIndex;
  private String currentDevice;

  private final TsBlockBuilder resultTsBlockBuilder;
  private String resultTsBlockDevice;

  @SuppressWarnings("squid:S107")
  public DeviceViewIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      Map<String, Map<PartialPath, Map<String, InputLocation>>>
          deviceToTargetPathSourceInputLocationMap,
      Map<String, Map<PartialPath, Map<String, TSDataType>>> deviceToTargetPathDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap,
      Map<String, InputLocation> sourceColumnToInputLocationMap,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    super(operatorContext, child, inputColumnTypes, intoOperationExecutor, statementSizePerLine);
    this.deviceToTargetPathSourceInputLocationMap = deviceToTargetPathSourceInputLocationMap;
    this.deviceToTargetPathDataTypeMap = deviceToTargetPathDataTypeMap;
    this.targetDeviceToAlignedMap = targetDeviceToAlignedMap;
    this.deviceToSourceTargetPathPairListMap = deviceToSourceTargetPathPairListMap;
    this.deviceToSourceTargetPathPairIndex =
        deviceToSourceTargetPathPairListMap.keySet().stream()
            .collect(Collectors.toMap(device -> device, device -> 0));
    this.targetPathToWriteCountMap = new HashMap<>();

    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.selectIntoAlignByDeviceColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    this.resultTsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    this.deviceColumnIndex =
        sourceColumnToInputLocationMap.get(ColumnHeaderConstant.DEVICE).getValueColumnIndex();
  }

  @Override
  protected boolean processTsBlock(TsBlock inputTsBlock) {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return true;
    }

    String device = String.valueOf(inputTsBlock.getValueColumns()[deviceColumnIndex].getBinary(0));
    if (!Objects.equals(device, currentDevice)) {
      final InsertMultiTabletsStatement insertMultiTabletsStatement =
          constructInsertMultiTabletsStatement(false);
      updateResultTsBlock();

      insertTabletStatementGenerators = constructInsertTabletStatementGeneratorsByDevice(device);
      currentDevice = device;

      if (insertMultiTabletsStatement != null) {
        executeInsertMultiTabletsStatement(insertMultiTabletsStatement);
        cachedTsBlock = inputTsBlock;
        return false;
      }
    }

    int readIndex = 0;
    while (readIndex < inputTsBlock.getPositionCount()) {
      int lastReadIndex = readIndex;
      for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
        lastReadIndex = Math.max(lastReadIndex, generator.processTsBlock(inputTsBlock, readIndex));
      }
      readIndex = lastReadIndex;
      if (insertMultiTabletsInternally(true)) {
        cachedTsBlock = inputTsBlock.subTsBlock(readIndex);
        return false;
      }
    }
    return true;
  }

  @Override
  protected TsBlock tryToReturnResultTsBlock() {
    InsertMultiTabletsStatement insertMultiTabletsStatement =
        constructInsertMultiTabletsStatement(false);
    updateResultTsBlock();
    currentDevice = null;

    if (insertMultiTabletsStatement != null) {
      executeInsertMultiTabletsStatement(insertMultiTabletsStatement);
      return null;
    }
    if (deviceIterator == null) {
      deviceIterator = deviceToSourceTargetPathPairListMap.keySet().iterator();
    }
    return constructResultTsBlock();
  }

  private TsBlock constructResultTsBlock() {
    resultTsBlockBuilder.reset();
    long estimatedTsBlockSize = tsBlockInitialSize;
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    while (resultTsBlockDevice != null || deviceIterator.hasNext()) {
      if (resultTsBlockDevice == null) {
        resultTsBlockDevice = deviceIterator.next();
      }
      int sourceTargetPathPairIndex = deviceToSourceTargetPathPairIndex.get(resultTsBlockDevice);
      List<Pair<String, PartialPath>> sourceTargetPathPairList =
          deviceToSourceTargetPathPairListMap.get(resultTsBlockDevice);
      for (;
          sourceTargetPathPairIndex < sourceTargetPathPairList.size();
          sourceTargetPathPairIndex++) {
        Pair<String, PartialPath> sourceTargetPathPair =
            sourceTargetPathPairList.get(sourceTargetPathPairIndex);
        timeColumnBuilder.writeLong(0);
        Binary device = new Binary(resultTsBlockDevice, TSFileConfig.STRING_CHARSET);
        columnBuilders[0].writeBinary(device);
        Binary sourceColumn = new Binary(sourceTargetPathPair.left, TSFileConfig.STRING_CHARSET);
        columnBuilders[1].writeBinary(sourceColumn);
        Binary targetPath =
            new Binary(sourceTargetPathPair.right.toString(), TSFileConfig.STRING_CHARSET);
        columnBuilders[2].writeBinary(targetPath);
        columnBuilders[3].writeLong(
            targetPathToWriteCountMap.getOrDefault(sourceTargetPathPair.right, 0L));
        resultTsBlockBuilder.declarePosition();

        estimatedTsBlockSize +=
            TimeColumn.SIZE_IN_BYTES_PER_POSITION
                + device.ramBytesUsed()
                + sourceColumn.ramBytesUsed()
                + targetPath.ramBytesUsed()
                + LongColumn.SIZE_IN_BYTES_PER_POSITION;
        if (estimatedTsBlockSize >= maxTsBlockSize) {
          deviceToSourceTargetPathPairIndex.put(resultTsBlockDevice, sourceTargetPathPairIndex + 1);
          return resultTsBlockBuilder.build();
        }
      }
      resultTsBlockDevice = null;
    }
    finished = true;
    return resultTsBlockBuilder.build();
  }

  private List<InsertTabletStatementGenerator> constructInsertTabletStatementGeneratorsByDevice(
      String currentDevice) {
    Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap =
        deviceToTargetPathSourceInputLocationMap.get(currentDevice);
    Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap =
        deviceToTargetPathDataTypeMap.get(currentDevice);
    return constructInsertTabletStatementGenerators(
        targetPathToSourceInputLocationMap,
        targetPathToDataTypeMap,
        targetDeviceToAlignedMap,
        inputColumnTypes,
        maxRowNumberInStatement);
  }

  private void updateResultTsBlock() {
    if (currentDevice == null) {
      return;
    }
    for (Pair<String, PartialPath> sourceTargetPathPair :
        deviceToSourceTargetPathPairListMap.get(currentDevice)) {
      targetPathToWriteCountMap.put(
          sourceTargetPathPair.right,
          findWritten(
              sourceTargetPathPair.right.getIDeviceID().toString(),
              sourceTargetPathPair.right.getMeasurement()));
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultTsBlockBuilder.getRetainedSizeInBytes()
        + (insertTabletStatementGenerators == null
            ? 0
            : insertTabletStatementGenerators.stream()
                .mapToLong(InsertTabletStatementGenerator::ramBytesUsed)
                .sum());
  }
}
