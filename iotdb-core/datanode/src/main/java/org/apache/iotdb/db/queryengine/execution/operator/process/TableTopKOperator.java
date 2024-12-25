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

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableTopKOperator extends TopKOperator {
  public TableTopKOperator(
      OperatorContext operatorContext,
      List<Operator> childrenOperators,
      List<TSDataType> dataTypes,
      Comparator<SortKey> comparator,
      int topValue,
      boolean childrenDataInOrder) {
    super(operatorContext, childrenOperators, dataTypes, comparator, topValue, childrenDataInOrder);
  }

  @Override
  protected TsBlock constrcutResultTsBlock(int positionCount, Column[] columns) {
    return new TsBlock(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount), columns);
  }

  @Override
  protected void updateTsBlock(
      TsBlock resultTsBlock, int updateIdx, TsBlock sourceTsBlock, int sourceIndex) {
    resultTsBlock.updateWithoutTimeColumn(updateIdx, sourceTsBlock, sourceIndex);
  }
}
