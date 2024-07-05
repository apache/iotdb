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

package org.apache.iotdb.db.queryengine.transformation.dag.input;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

public class TsBlockInputDataSet implements IUDFInputDataSet {

  private final Operator operator;
  private final List<TSDataType> dataTypes;
  private TsBlock tsBlock;

  public TsBlockInputDataSet(Operator operator, List<TSDataType> dataTypes) {
    this.operator = operator;
    this.dataTypes = dataTypes;
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  @Override
  public YieldableState yield() throws Exception {
    // Request from child operator if there is no TsBlock
    if (tsBlock == null) {
      if (operator.isBlocked() != Operator.NOT_BLOCKED) {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
      if (!operator.hasNextWithTimer()) {
        return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
      }
      tsBlock = operator.nextWithTimer();
      if (tsBlock == null) {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
    }

    return YieldableState.YIELDABLE;
  }

  @Override
  public Column[] currentBlock() {
    Column[] rows = tsBlock.getAllColumns();
    // Prepare for next TsBlock
    tsBlock = null;
    return rows;
  }
}
