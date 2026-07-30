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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.calc.utils.sort.TableDiskSpiller;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.Comparator;
import java.util.List;

public class TableSortOperator extends SortOperator {
  public TableSortOperator(
      CommonOperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      String folderPath,
      Comparator<SortKey> comparator) {
    super(
        operatorContext,
        inputOperator,
        dataTypes,
        new TableDiskSpiller(folderPath, folderPath + operatorContext.getOperatorId(), dataTypes),
        comparator);
  }

  @Override
  protected void appendTime(TimeColumnBuilder timeBuilder, long time) {
    // do nothing for table related operator
  }

  @Override
  protected TsBlock buildFinalResult(TsBlockBuilder resultBuilder) {
    return resultBuilder.build(
        new RunLengthEncodedColumn(
            CommonOperatorUtils.TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
  }
}
