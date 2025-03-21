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

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.Map;

public class TableLeftOuterTimeJoinOperator extends LeftOuterTimeJoinOperator {

  private final Map<InputLocation, Integer> outputColumnMap;

  public TableLeftOuterTimeJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      Operator rightChild,
      int leftColumnCount,
      Map<InputLocation, Integer> outputColumnMap,
      List<TSDataType> dataTypes,
      TimeComparator comparator) {
    super(operatorContext, leftChild, leftColumnCount, rightChild, dataTypes, comparator);
    this.outputColumnMap = outputColumnMap;
  }

  @Override
  protected int getOutputColumnIndex(int inputColumnIndex, boolean isLeftTable) {
    return outputColumnMap.get(
        isLeftTable
            ? new InputLocation(0, inputColumnIndex)
            : new InputLocation(1, inputColumnIndex - leftColumnCount));
  }
}
