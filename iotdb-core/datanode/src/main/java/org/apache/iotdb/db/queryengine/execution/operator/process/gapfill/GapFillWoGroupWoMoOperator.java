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

package org.apache.iotdb.db.queryengine.execution.operator.process.gapfill;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer.nextDateBin;

public class GapFillWoGroupWoMoOperator extends AbstractGapFillWoGroupOperator {

  // used when the first parameter(timeInterval) of date_bin_gapfill is like 1ns, 1us, 1ms, 1s, 1m,
  // 1h, 1d, 1w and so on which only containing time interval unit less than month
  private final long nonMonthDuration;

  public GapFillWoGroupWoMoOperator(
      OperatorContext operatorContext,
      Operator child,
      int timeColumnIndex,
      long startTime,
      long endTime,
      List<TSDataType> dataTypes,
      long nonMonthDuration) {
    super(operatorContext, child, timeColumnIndex, startTime, endTime, dataTypes);
    this.nonMonthDuration = nonMonthDuration;
  }

  @Override
  void nextTime() {
    this.currentTime = nextDateBin(nonMonthDuration, this.currentTime);
  }
}
