/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

import static org.apache.iotdb.db.mpp.execution.operator.AggregationUtil.calculateAggregationFromRawData;

/**
 * RawDataAggregationOperator is used to process raw data tsBlock input calculating using value
 * filter. It's possible that there is more than one tsBlock input in one time interval. And it's
 * also possible that one tsBlock can cover multiple time intervals too.
 *
 * <p>Since raw data query with value filter is processed by FilterOperator above TimeJoinOperator,
 * there we can see RawDataAggregateOperator as a one-to-one(one input, ont output) operator.
 *
 * <p>Return aggregation result in many time intervals once.
 */
public class RawDataAggregationOperator extends SingleInputAggregationOperator {

  public RawDataAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      Operator child,
      boolean ascending,
      long maxReturnSize) {
    super(operatorContext, aggregators, child, ascending, timeRangeIterator, maxReturnSize);
  }

  @Override
  protected boolean calculateNextAggregationResult() {
    while (!calcFromRawData()) {
      inputTsBlock = null;

      // NOTE: child.next() can only be invoked once
      if (child.hasNext() && canCallNext) {
        inputTsBlock = child.next();
        canCallNext = false;
      } else if (child.hasNext()) {
        // if child still has next but can't be invoked now
        return false;
      } else {
        break;
      }
    }

    // update result using aggregators
    updateResultTsBlock();

    return true;
  }

  private boolean calcFromRawData() {
    Pair<Boolean, TsBlock> calcResult =
        calculateAggregationFromRawData(inputTsBlock, aggregators, curTimeRange, ascending);
    inputTsBlock = calcResult.getRight();
    return calcResult.getLeft();
  }
}
