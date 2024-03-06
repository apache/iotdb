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

package org.apache.iotdb.db.pipe.processor.aggregate.datastructure.timeseries;

import java.util.Map;

public class TimeSeriesAttributes {
  private long currentBoundaryTime = Long.MIN_VALUE;
  private Map<String, Object> intermediateResultName2ValueMap;

  public long getCurrentBoundaryTime() {
    return currentBoundaryTime;
  }

  public void setCurrentBoundaryTime(long currentBoundaryTime) {
    this.currentBoundaryTime = currentBoundaryTime;
  }

  public Map<String, Object> getIntermediateResultName2ValueMap() {
    return intermediateResultName2ValueMap;
  }

  public void updateIntermediateResult(String name, Object value) {
    intermediateResultName2ValueMap.put(name, value);
  }
}
