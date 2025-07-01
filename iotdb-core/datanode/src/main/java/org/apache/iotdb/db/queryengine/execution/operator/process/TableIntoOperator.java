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
import org.apache.iotdb.db.protocol.client.DataNodeInternalClient;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import com.google.common.util.concurrent.Futures;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class TableIntoOperator extends AbstractIntoOperator {
  protected InsertTabletStatementGenerator insertTabletStatementGenerator;

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableIntoOperator.class);

  private final PartialPath targetDevice;

  public TableIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      String databaseName,
      PartialPath targetDevice,
      List<TSDataType> inputColumnTypes,
      List<TsTableColumnCategory> inputColumnCategories,
      Map<String, InputLocation> measurementToInputLocationMap,
      Map<String, TSDataType> measurementToDataTypeMap,
      Boolean isAligned,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    super(operatorContext, child, inputColumnTypes, intoOperationExecutor, statementSizePerLine);
    this.targetDevice = targetDevice;
    insertTabletStatementGenerator =
        new TableInsertTabletStatementGenerator(
            databaseName,
            targetDevice,
            measurementToInputLocationMap,
            measurementToDataTypeMap,
            isAligned,
            typeConvertors,
            inputColumnCategories,
            maxRowNumberInStatement);
  }

  @Override
  protected void resetInsertTabletStatementGenerators() {
    insertTabletStatementGenerator.reset();
  }

  @Override
  protected boolean processTsBlock(TsBlock inputTsBlock) {
    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return true;
    }

    int readIndex = 0;
    while (readIndex < inputTsBlock.getPositionCount()) {
      readIndex = insertTabletStatementGenerator.processTsBlock(inputTsBlock, readIndex);
      if (insertTabletInternally(true)) {
        cachedTsBlock = inputTsBlock.subTsBlock(readIndex);
        return false;
      }
    }
    return true;
  }

  @Override
  protected TsBlock tryToReturnResultTsBlock() {
    if (insertTabletInternally(false)) {
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
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(targetDevice);
  }

  private boolean insertTabletInternally(boolean needCheck) {
    final InsertTabletStatement insertTabletStatement = constructInsertTabletStatement(needCheck);
    if (insertTabletStatement == null) {
      return false;
    }

    executeInsertTabletStatement(insertTabletStatement);
    return true;
  }

  private InsertTabletStatement constructInsertTabletStatement(boolean needCheck) {
    if (insertTabletStatementGenerator == null
        || (needCheck && !insertTabletStatementGenerator.isFull())) {
      return null;
    }
    if (insertTabletStatementGenerator.isEmpty()) {
      return null;
    }
    return insertTabletStatementGenerator.constructInsertTabletStatement();
  }

  private void executeInsertTabletStatement(InsertTabletStatement insertTabletStatement) {
    if (client == null) {
      client = new DataNodeInternalClient(operatorContext.getSessionInfo());
    }
    writeOperationFuture =
        Futures.submit(
            () -> client.insertRelationalTablets(insertTabletStatement), writeOperationExecutor);
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
    columnBuilders[0].writeInt(findWritten());
    resultTsBlockBuilder.declarePosition();
    return resultTsBlockBuilder.build();
  }

  private int findWritten() {
    return insertTabletStatementGenerator.getWrittenCount();
  }
}
