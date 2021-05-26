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

package org.apache.iotdb.db.qp.logical.crud;

public class GroupByClauseComponent extends SpecialClauseComponent {

  private long startTime;
  private long endTime;
  // time interval
  private long unit;
  // sliding step
  private long slidingStep;
  private boolean isIntervalByMonth = false;
  private boolean isSlidingStepByMonth = false;
  // if it is left close and right open interval
  private boolean leftCRightO;

  public GroupByClauseComponent() {}

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public long getUnit() {
    return unit;
  }

  public void setUnit(long unit) {
    this.unit = unit;
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
}
