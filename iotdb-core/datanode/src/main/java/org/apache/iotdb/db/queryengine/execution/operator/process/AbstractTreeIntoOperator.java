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
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public abstract class AbstractTreeIntoOperator extends AbstractIntoOperator {

  protected AbstractTreeIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    super(operatorContext, child, inputColumnTypes, intoOperationExecutor, statementSizePerLine);
  }

  protected static List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Type> sourceTypeConvertors,
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
              targetDeviceToAlignedMap.get(targetDevice.toString()),
              sourceTypeConvertors,
              maxRowNumberInStatement);
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
  }

  protected int findWritten(String device, String measurement) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!Objects.equals(generator.getDevice(), device)) {
        continue;
      }
      return generator.getWrittenCount(measurement);
    }
    return 0;
  }
}
