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

package org.apache.iotdb.db.pipe.processor.aggregate.datastructure.window;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class TimeSeriesWindowState {
  private long currentBoundaryTime = Long.MIN_VALUE;
  private final Map<String, Object> intermediateResultName2ValueMap = new HashMap<>();
  private final Map<String, Object> runtimeStateName2ValueMap = new HashMap<>();

  public long getCurrentBoundaryTime() {
    return currentBoundaryTime;
  }

  public void setCurrentBoundaryTime(long currentBoundaryTime) {
    this.currentBoundaryTime = currentBoundaryTime;
  }

  public synchronized void tryInitBoundaryTime(
      long timeStamp, long slidingBoundaryTime, long slidingInterval) {
    if (currentBoundaryTime == Long.MIN_VALUE) {
      // Initiate the slidingBoundaryTime on the first incoming value
      currentBoundaryTime =
          timeStamp <= slidingBoundaryTime
              ? slidingBoundaryTime
              : ((timeStamp - slidingBoundaryTime - 1) / slidingInterval + 1) * slidingInterval
                  + slidingBoundaryTime;
    }
  }

  public synchronized void updateCurrentValue(
      long timeStamp,
      long slidingInterval,
      Map<String, IntermediateResultOperator> intermediateResult2AttributesMap) {
    if (timeStamp < currentBoundaryTime) {
      return;
    }
    // Compute the value if the timestamp is in the current window
    else if (timeStamp < currentBoundaryTime + slidingInterval) {

    }
    // Terminate the window and emit the output if the timestamp is later than the window right
    // bound
    else {

    }
  }

  private void updateIntermediateResult(String name, UnaryOperator<Object> updateFunction) {
    intermediateResultName2ValueMap.compute(
        name, (resultName, value) -> updateFunction.apply(value));
  }
}
