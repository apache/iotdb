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
import org.apache.iotdb.db.protocol.client.DataNodeInternalClient;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import com.google.common.util.concurrent.Futures;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public abstract class AbstractTreeIntoOperator extends AbstractIntoOperator {
  protected List<InsertTabletStatementGenerator> insertTabletStatementGenerators;
  protected final TsBlockBuilder resultTsBlockBuilder;

  protected AbstractTreeIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine,
      List<TSDataType> outputDataTypes) {
    super(operatorContext, child, inputColumnTypes, intoOperationExecutor, statementSizePerLine);
    this.maxReturnSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    this.resultTsBlockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  protected static List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<TSDataType> inputColumnTypes,
      int maxRowNumberInStatement) {
    List<InsertTabletStatementGenerator> insertTabletStatementGenerators =
        new ArrayList<>(targetPathToSourceInputLocationMap.size());
    for (Map.Entry<PartialPath, Map<String, InputLocation>> entry :
        targetPathToSourceInputLocationMap.entrySet()) {
      PartialPath targetDevice = entry.getKey();
      TreeInsertTabletStatementGenerator generator =
          new TreeInsertTabletStatementGenerator(
              targetDevice,
              entry.getValue(),
              targetPathToDataTypeMap.get(targetDevice),
              inputColumnTypes,
              targetDeviceToAlignedMap.get(targetDevice.toString()),
              maxRowNumberInStatement);
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
  }

  @Override
  protected void resetInsertTabletStatementGenerators() {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      generator.reset();
    }
  }

  protected long findWritten(String device, String measurement) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!Objects.equals(generator.getDevice(), device)) {
        continue;
      }
      return generator.getWrittenCount(measurement);
    }
    return 0;
  }

  /** Return true if write task is submitted successfully. */
  protected boolean insertMultiTabletsInternally(boolean needCheck) {
    final InsertMultiTabletsStatement insertMultiTabletsStatement =
        constructInsertMultiTabletsStatement(needCheck);
    if (insertMultiTabletsStatement == null) {
      return false;
    }

    executeInsertMultiTabletsStatement(insertMultiTabletsStatement);
    return true;
  }

  protected InsertMultiTabletsStatement constructInsertMultiTabletsStatement(boolean needCheck) {
    if (insertTabletStatementGenerators == null
        || (needCheck && !existFullStatement(insertTabletStatementGenerators))) {
      return null;
    }

    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!generator.isEmpty()) {
        insertTabletStatementList.add(generator.constructInsertTabletStatement());
      }
    }
    if (insertTabletStatementList.isEmpty()) {
      return null;
    }

    InsertMultiTabletsStatement insertMultiTabletsStatement = new InsertMultiTabletsStatement();
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    return insertMultiTabletsStatement;
  }

  protected void executeInsertMultiTabletsStatement(
      InsertMultiTabletsStatement insertMultiTabletsStatement) {
    if (client == null) {
      client = new DataNodeInternalClient(operatorContext.getSessionInfo());
    }
    writeOperationFuture =
        Futures.submit(
            () -> client.insertTablets(insertMultiTabletsStatement), writeOperationExecutor);
  }

  protected boolean existFullStatement(
      List<InsertTabletStatementGenerator> insertTabletStatementGenerators) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (generator.isFull()) {
        return true;
      }
    }
    return false;
  }
}
