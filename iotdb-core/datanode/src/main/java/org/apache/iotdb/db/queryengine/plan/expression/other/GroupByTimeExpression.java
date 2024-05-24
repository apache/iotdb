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

package org.apache.iotdb.db.queryengine.plan.expression.other;

import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TimeDuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/** Only used for representing GROUP BY TIME filter. */
public class GroupByTimeExpression extends Expression {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupByTimeExpression.class);

  // [startTime, endTime]
  private final long startTime;
  private final long endTime;

  // time interval
  private final TimeDuration interval;

  // sliding step
  private final TimeDuration slidingStep;

  public GroupByTimeExpression(
      long startTime, long endTime, TimeDuration interval, TimeDuration slidingStep) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.interval = interval;
    this.slidingStep = slidingStep;
  }

  public GroupByTimeExpression(ByteBuffer byteBuffer) {
    this.startTime = ReadWriteIOUtils.readLong(byteBuffer);
    this.endTime = ReadWriteIOUtils.readLong(byteBuffer);
    this.interval = TimeDuration.deserialize(byteBuffer);
    this.slidingStep = TimeDuration.deserialize(byteBuffer);
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
    return ExpressionType.GROUP_BY_TIME;
  }

  @Override
  public boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean isConstantOperandInternal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Expression> getExpressions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOutputSymbolInternal() {
    return getExpressionStringInternal();
  }

  @Override
  protected String getExpressionStringInternal() {
    return "group by time(["
        + startTime
        + ", "
        + endTime
        + "], "
        + interval
        + ", "
        + slidingStep
        + ")";
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(startTime, byteBuffer);
    ReadWriteIOUtils.write(endTime, byteBuffer);
    interval.serialize(byteBuffer);
    slidingStep.serialize(byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(startTime, stream);
    ReadWriteIOUtils.write(endTime, stream);
    interval.serialize(stream);
    slidingStep.serialize(stream);
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitGroupByTimeExpression(this, context);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }
}
