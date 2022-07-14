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

package org.apache.iotdb.db.mpp.execution.timer;

import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.schedule.DriverTaskThread;

import io.airlift.units.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

public class RuleBasedTimeSliceAllocator implements ITimeSliceAllocator {

  private final long EXECUTION_TIME_SLICE_IN_MS =
      DriverTaskThread.EXECUTION_TIME_SLICE.roundTo(TimeUnit.MILLISECONDS);

  private final Map<OperatorContext, Integer> operatorToWeightMap;

  private int totalWeight;

  public RuleBasedTimeSliceAllocator() {
    this.operatorToWeightMap = new HashMap<>();
    this.totalWeight = 0;
  }

  public void recordExecutionWeight(OperatorContext operatorContext, int weight) {
    checkState(
        !operatorToWeightMap.containsKey(operatorContext), "Same operator has been weighted");
    operatorToWeightMap.put(operatorContext, weight);
    totalWeight += weight;
  }

  private int getWeight(OperatorContext operatorContext) {
    checkState(
        operatorToWeightMap.containsKey(operatorContext), "This operator has not been weighted");
    return operatorToWeightMap.get(operatorContext);
  }

  @Override
  public Duration getMaxRunTime(OperatorContext operatorContext) {
    return new Duration(
        (double) EXECUTION_TIME_SLICE_IN_MS * getWeight(operatorContext) / totalWeight,
        TimeUnit.MILLISECONDS);
  }
}
