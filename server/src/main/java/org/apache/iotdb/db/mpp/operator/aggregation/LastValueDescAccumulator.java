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

package org.apache.iotdb.db.mpp.operator.aggregation;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

public class LastValueDescAccumulator extends LastValueAccumulator {

  private boolean hasCandidateResult = false;

  public LastValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    // Data inside tsBlock is still in ascending order, we have to traverse the first tsBlock
    for (int i = 0; i < column[0].getPositionCount(); i++) {
      long curTime = column[0].getLong(i);
      if (curTime >= timeRange.getMin() && curTime < timeRange.getMax()) {
        updateLastValue(column[1].getObject(i), curTime);
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return hasCandidateResult;
  }

  @Override
  public void reset() {
    hasCandidateResult = false;
    super.reset();
  }

  protected void updateLastValue(Object value, long curTime) {
    hasCandidateResult = true;
    super.updateLastValue(value, curTime);
  }
}
