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
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class DeviceViewIntoOperator extends AbstractIntoOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceViewIntoOperator.class);

  private final Map<String, Map<PartialPath, Map<String, InputLocation>>>
      deviceToTargetPathSourceInputLocationMap;
  private final Map<String, Map<PartialPath, Map<String, TSDataType>>>
      deviceToTargetPathDataTypeMap;
  private final Map<String, Boolean> targetDeviceToAlignedMap;
  private final Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap;

  private final int deviceColumnIndex;
  private String currentDevice;

  private final TsBlockBuilder resultTsBlockBuilder;

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
      for (AbstractIntoOperator.InsertTabletStatementGenerator generator :
          insertTabletStatementGenerators) {
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

    finished = true;
    return resultTsBlockBuilder.build();
  }

  private List<AbstractIntoOperator.InsertTabletStatementGenerator>
      constructInsertTabletStatementGeneratorsByDevice(String currentDevice) {
    Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap =
        deviceToTargetPathSourceInputLocationMap.get(currentDevice);
    Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap =
        deviceToTargetPathDataTypeMap.get(currentDevice);
    return constructInsertTabletStatementGenerators(
        targetPathToSourceInputLocationMap,
        targetPathToDataTypeMap,
        targetDeviceToAlignedMap,
        typeConvertors,
        maxRowNumberInStatement);
  }

  private void updateResultTsBlock() {
    if (currentDevice == null) {
      return;
    }

    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    for (Pair<String, PartialPath> sourceTargetPathPair :
        deviceToSourceTargetPathPairListMap.get(currentDevice)) {
      timeColumnBuilder.writeLong(0);
      columnBuilders[0].writeBinary(new Binary(currentDevice, TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(sourceTargetPathPair.left, TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeBinary(
          new Binary(sourceTargetPathPair.right.toString(), TSFileConfig.STRING_CHARSET));
      columnBuilders[3].writeInt(
          findWritten(
              sourceTargetPathPair.right.getIDeviceID().toString(),
              sourceTargetPathPair.right.getMeasurement()));
      resultTsBlockBuilder.declarePosition();
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultTsBlockBuilder.getRetainedSizeInBytes();
  }
}
