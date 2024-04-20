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

package org.apache.iotdb.db.queryengine.plan.statement.component;

import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;

import org.apache.tsfile.utils.TimeDuration;

/** This class maintains information of {@code GROUP BY} clause. */
public class GroupByTimeComponent extends StatementNode {

  // [startTime, endTime)
  private long startTime;
  private long endTime;

  // time interval
  private TimeDuration interval;

  // sliding step
  private TimeDuration slidingStep;

  private String originalInterval;

  private String originalSlidingStep;

  // if it is left close and right open interval
  private boolean leftCRightO = true;

  public GroupByTimeComponent() {
    // do nothing
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public TimeDuration getInterval() {
    return interval;
  }

  public void setInterval(TimeDuration interval) {
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

  public TimeDuration getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(TimeDuration slidingStep) {
    this.slidingStep = slidingStep;
  }

  public String getOriginalInterval() {
    return originalInterval;
  }

  public void setOriginalInterval(String originalInterval) {
    this.originalInterval = originalInterval;
  }

  public void setOriginalSlidingStep(String originalSlidingStep) {
    this.originalSlidingStep = originalSlidingStep;
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
    sqlBuilder.append(originalInterval);
    if (!originalSlidingStep.equals(originalInterval)) {
      sqlBuilder.append(',').append(' ');
      sqlBuilder.append(originalSlidingStep);
    }
    sqlBuilder.append(')');
    return sqlBuilder.toString();
  }
}
