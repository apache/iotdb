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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

/** This class maintains information of {@code GROUP BY} clause. */
public class GroupByTimeComponent extends StatementNode {

  // [startTime, endTime)
  private long startTime;
  private long endTime;

  // time interval
  private long interval;

  // sliding step
  private long slidingStep;

  // if it is expressed by natural month. eg, 1mo
  private boolean isIntervalByMonth = false;
  private boolean isSlidingStepByMonth = false;

  // if it is left close and right open interval
  private boolean leftCRightO = true;

  public GroupByTimeComponent() {}

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public long getInterval() {
    return interval;
  }

  public void setInterval(long interval) {
    this.interval = interval;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(long slidingStep) {
    this.slidingStep = slidingStep;
  }

  public boolean isSlidingStepByMonth() {
    return isSlidingStepByMonth;
  }

  public void setSlidingStepByMonth(boolean isSlidingStepByMonth) {
    this.isSlidingStepByMonth = isSlidingStepByMonth;
  }

  public boolean isIntervalByMonth() {
    return isIntervalByMonth;
  }

  public void setIntervalByMonth(boolean isIntervalByMonth) {
    this.isIntervalByMonth = isIntervalByMonth;
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("GROUP BY TIME").append(' ');
    sqlBuilder.append('(');
    if (startTime != 0 || endTime != 0) {
      if (isLeftCRightO()) {
        sqlBuilder
            .append('[')
            .append(startTime)
            .append(',')
            .append(' ')
            .append(endTime)
            .append(')');
      } else {
        sqlBuilder
            .append('(')
            .append(startTime)
            .append(',')
            .append(' ')
            .append(endTime)
            .append(']');
      }
      sqlBuilder.append(',').append(' ');
    }
    String intervalStr = interval + (isIntervalByMonth ? "mo" : "ms");
    String slidingStepStr = slidingStep + (isSlidingStepByMonth ? "mo" : "ms");
    sqlBuilder.append(intervalStr);
    if (!slidingStepStr.equals(intervalStr)) {
      sqlBuilder.append(',').append(' ');
      sqlBuilder.append(slidingStepStr);
    }
    sqlBuilder.append(')');
    return sqlBuilder.toString();
  }
}
