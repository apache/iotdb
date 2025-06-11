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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class TableIntoOperator extends AbstractIntoOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableIntoOperator.class);

  private final IDeviceID deviceID;
  private final List<Pair<String, PartialPath>> sourceTargetPathPairList;

  public TableIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      String databaseName,
      IDeviceID deviceID,
      List<TSDataType> inputColumnTypes,
      List<TsTableColumnCategory> inputColumnCategories,
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Pair<String, PartialPath>> sourceTargetPathPairList,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    super(operatorContext, child, inputColumnTypes, intoOperationExecutor, statementSizePerLine);
    this.deviceID = deviceID;
    this.sourceTargetPathPairList = sourceTargetPathPairList;
    insertTabletStatementGenerators =
        constructInsertTabletStatementGenerators(
            databaseName,
            targetPathToSourceInputLocationMap,
            targetPathToDataTypeMap,
            targetDeviceToAlignedMap,
            typeConvertors,
            inputColumnCategories,
            maxRowNumberInStatement);
  }

  protected static List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      String databaseName,
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Type> sourceTypeConvertors,
      List<TsTableColumnCategory> tsTableColumnCategory,
      int maxRowNumberInStatement) {
    List<InsertTabletStatementGenerator> insertTabletStatementGenerators =
        new ArrayList<>(targetPathToSourceInputLocationMap.size());
    for (Map.Entry<PartialPath, Map<String, InputLocation>> entry :
        targetPathToSourceInputLocationMap.entrySet()) {
      PartialPath targetDevice = entry.getKey();
      TableInsertTabletStatementGenerator generator =
          new TableInsertTabletStatementGenerator(
              databaseName,
              targetDevice,
              entry.getValue(),
              targetPathToDataTypeMap.get(targetDevice),
              targetDeviceToAlignedMap.get(targetDevice.toString()),
              sourceTypeConvertors,
              tsTableColumnCategory,
              maxRowNumberInStatement);
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
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

    finished = true;
    return constructResultTsBlock();
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
                .sum());
  }

  @Override
  protected InsertMultiTabletsStatement constructInsertMultiTabletsStatement(boolean needCheck) {
    InsertMultiTabletsStatement insertMultiTabletsStatement =
        super.constructInsertMultiTabletsStatement(needCheck);
    if (insertMultiTabletsStatement != null) {
      insertMultiTabletsStatement.setWriteToTable(true);
    }
    return insertMultiTabletsStatement;
  }

  private TsBlock constructResultTsBlock() {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.selectIntoTableColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder resultTsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(0);
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    columnBuilders[0].writeInt(findWritten(deviceID.toString()));
    resultTsBlockBuilder.declarePosition();
    return resultTsBlockBuilder.build();
  }

  private int findWritten(String device) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!Objects.equals(generator.getDevice(), device)) {
        continue;
      }
      return generator.getWrittenCount();
    }
    return 0;
  }
}
