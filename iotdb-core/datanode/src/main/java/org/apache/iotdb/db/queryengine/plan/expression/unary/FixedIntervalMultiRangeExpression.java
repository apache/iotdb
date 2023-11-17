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

package org.apache.iotdb.db.queryengine.plan.expression.unary;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.tsfile.utils.TimeDuration;

import static com.google.common.base.Preconditions.checkArgument;

public class FixedIntervalMultiRangeExpression extends UnaryExpression {

  // [startTime, endTime]
  private long startTime;
  private long endTime;

  // time interval
  private TimeDuration interval;

  // sliding step
  private TimeDuration slidingStep;

  public FixedIntervalMultiRangeExpression(
      Expression expression,
      long startTime,
      long endTime,
      TimeDuration interval,
      TimeDuration slidingStep) {
    super(expression);
    checkArgument(
        expression.getExpressionType().equals(ExpressionType.TIMESTAMP),
        "Only used for representing GROUP BY TIME filter.");
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
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

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.MULTI_RANGE;
  }

  @Override
  public String getOutputSymbolInternal() {
    return null;
  }

  @Override
  protected String getExpressionStringInternal() {
    return null;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitFixedIntervalMultiRangeExpression(this, context);
  }
}
