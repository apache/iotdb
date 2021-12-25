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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

import static org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet.calcIntervalByMonth;

public class GroupByTimeFillPlan extends GroupByTimePlan {

  private long queryStartTime;
  private long queryEndTime;

  private Map<TSDataType, IFill> fillTypes;
  private IFill singleFill;

  public GroupByTimeFillPlan() {
    super();
    setOperatorType(Operator.OperatorType.GROUP_BY_FILL);
  }

  public IFill getSingleFill() {
    return singleFill;
  }

  public Map<TSDataType, IFill> getFillType() {
    return fillTypes;
  }

  public void setSingleFill(IFill singleFill) {
    this.singleFill = singleFill;
  }

  public void setFillType(Map<TSDataType, IFill> fillTypes) {
    this.fillTypes = fillTypes;
  }

  public long getQueryStartTime() {
    return queryStartTime;
  }

  public long getQueryEndTime() {
    return queryEndTime;
  }

  /** union the query time range with the extra fill range */
  public void initFillRange() {
    long minQueryStartTime = startTime;
    long maxQueryEndTime = endTime;
    if (fillTypes != null) {
      // old type fill logic
      for (Map.Entry<TSDataType, IFill> IFillEntry : fillTypes.entrySet()) {
        IFill fill = IFillEntry.getValue();
        fill.convertRange(startTime, endTime);
        minQueryStartTime = Math.min(minQueryStartTime, fill.getQueryStartTime());
        maxQueryEndTime = Math.max(maxQueryEndTime, fill.getQueryEndTime());
      }
    } else {
      IFill fill = singleFill;
      fill.convertRange(startTime, endTime);
      minQueryStartTime = Math.min(minQueryStartTime, fill.getQueryStartTime());
      maxQueryEndTime = Math.max(maxQueryEndTime, fill.getQueryEndTime());
    }

    if (minQueryStartTime < startTime) {
      long queryRange = minQueryStartTime - startTime;
      long extraStartTime, intervalNum;
      if (isSlidingStepByMonth) {
        intervalNum = (long) Math.ceil(queryRange / (double) (slidingStep));
        extraStartTime = calcIntervalByMonth(startTime, intervalNum);
        while (extraStartTime < minQueryStartTime) {
          intervalNum += 1;
          extraStartTime = calcIntervalByMonth(startTime, intervalNum);
        }
      } else {
        intervalNum = (long) Math.ceil(queryRange / (double) slidingStep);
        extraStartTime = slidingStep * intervalNum + startTime;
      }
      minQueryStartTime = Math.min(extraStartTime, startTime);
    }

    maxQueryEndTime = Math.max(maxQueryEndTime, endTime);

    // save origin query range
    queryStartTime = startTime;
    queryEndTime = endTime;

    startTime = minQueryStartTime;
    endTime = maxQueryEndTime;
  }
}
