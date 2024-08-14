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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.tsfile.utils.TimeDuration;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GroupByTime extends GroupingElement {

  // [startTime, endTime)
  private final long startTime;
  private final long endTime;
  // time interval
  private final TimeDuration interval;
  // sliding step
  private final TimeDuration slidingStep;
  // if it is left close and right open interval
  private final boolean leftCRightO;

  public GroupByTime(
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      boolean leftCRightO) {
    super(null);
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.leftCRightO = leftCRightO;
  }

  public GroupByTime(
      NodeLocation location,
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep,
      boolean leftCRightO) {
    super(location);
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
    this.leftCRightO = leftCRightO;
  }

  @Override
  public List<Expression> getExpressions() {
    return Collections.emptyList();
  }

  @Override
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGroupByTime(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupByTime that = (GroupByTime) o;
    return startTime == that.startTime
        && endTime == that.endTime
        && leftCRightO == that.leftCRightO
        && Objects.equals(interval, that.interval)
        && Objects.equals(slidingStep, that.slidingStep);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTime, endTime, interval, slidingStep, leftCRightO);
  }

  @Override
  public String toString() {
    return "GroupByTime{"
        + "startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", interval="
        + interval
        + ", slidingStep="
        + slidingStep
        + ", leftCRightO="
        + leftCRightO
        + '}';
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public TimeDuration getInterval() {
    return interval;
  }

  public TimeDuration getSlidingStep() {
    return slidingStep;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }
}
