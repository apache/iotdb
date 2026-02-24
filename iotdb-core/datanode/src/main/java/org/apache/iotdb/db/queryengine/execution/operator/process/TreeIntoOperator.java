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

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class TreeIntoOperator extends AbstractTreeIntoOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TreeIntoOperator.class);

  private final List<Pair<String, PartialPath>> sourceTargetPathPairList;

  private int outputIndex = 0;

  @SuppressWarnings("squid:S107")
  public TreeIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Pair<String, PartialPath>> sourceTargetPathPairList,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    super(
        operatorContext,
        child,
        inputColumnTypes,
        intoOperationExecutor,
        statementSizePerLine,
        ColumnHeaderConstant.selectIntoColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList()));
    this.sourceTargetPathPairList = sourceTargetPathPairList;
    insertTabletStatementGenerators =
        constructInsertTabletStatementGenerators(
            targetPathToSourceInputLocationMap,
            targetPathToDataTypeMap,
            targetDeviceToAlignedMap,
            inputColumnTypes,
            maxRowNumberInStatement);
  }

  @Override
  protected boolean processTsBlock(TsBlock inputTsBlock) {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return true;
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
    if (insertMultiTabletsInternally(false)) {
      return null;
    }

    TsBlock res = constructResultTsBlock();
    finished = (outputIndex == sourceTargetPathPairList.size());
    return res;
  }

  @Override
  protected TsBlock tryToReturnPartialResult() {
    return null;
  }

  private TsBlock constructResultTsBlock() {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    for (int size = sourceTargetPathPairList.size();
        outputIndex < size && !resultTsBlockBuilder.isFull();
        outputIndex++) {
      Pair<String, PartialPath> sourceTargetPathPair = sourceTargetPathPairList.get(outputIndex);
      timeColumnBuilder.writeLong(0);
      columnBuilders[0].writeBinary(
          new Binary(sourceTargetPathPair.left, TSFileConfig.STRING_CHARSET));
      columnBuilders[1].writeBinary(
          new Binary(sourceTargetPathPair.right.toString(), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].writeLong(
          findWritten(
              sourceTargetPathPair.right.getIDeviceID().toString(),
              sourceTargetPathPair.right.getMeasurement()));
      resultTsBlockBuilder.declarePosition();
    }
    TsBlock res = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return res;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + (sourceTargetPathPairList == null
            ? 0
            : sourceTargetPathPairList.stream()
                .mapToLong(
                    pair ->
                        RamUsageEstimator.sizeOf(pair.left)
                            + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(pair.right))
                .sum())
        + (insertTabletStatementGenerators == null
            ? 0
            : insertTabletStatementGenerators.stream()
                .mapToLong(InsertTabletStatementGenerator::ramBytesUsed)
                .sum());
  }
}
