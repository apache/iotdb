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
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;

public class TableLinearFillOperator extends AbstractLinearFillOperator {

  // start from 0
  private final int helperColumnIndex;

  public TableLinearFillOperator(
      OperatorContext operatorContext,
      ILinearFill[] fillArray,
      Operator child,
      int helperColumnIndex) {
    super(operatorContext, fillArray, child);
    this.helperColumnIndex = helperColumnIndex;
  }

  @Override
  Column getHelperColumn(TsBlock tsBlock) {
    // if helperColumnIndex is -1, HelperColumn won't be used, so just return TimeColumn in TsBlock
    // instead
    return tsBlock.getColumn(helperColumnIndex);
  }

  @Override
  Integer getLastRowIndexForNonNullHelperColumn(TsBlock tsBlock) {
    Column helperColumn = getHelperColumn(tsBlock);
    int size = tsBlock.getPositionCount();
    if (!helperColumn.mayHaveNull()) {
      return size - 1;
    } else {
      int i = size - 1;
      for (; i >= 0; i--) {
        if (!helperColumn.isNull(i)) {
          break;
        }
      }
      return i;
    }
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed() + Integer.BYTES;
  }
}
