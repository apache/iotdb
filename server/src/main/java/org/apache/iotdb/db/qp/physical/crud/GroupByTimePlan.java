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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class GroupByTimePlan extends AggregationPlan {

  // [startTime, endTime)
  protected long startTime;
  protected long endTime;
  // aggregation time interval
  protected long interval;
  // sliding step
  protected long slidingStep;
  // if group by query is by natural month
  protected boolean isIntervalByMonth;
  protected boolean isSlidingStepByMonth;

  // if it is left close and right open interval
  protected boolean leftCRightO = true;

  public GroupByTimePlan() {
    super();
    setOperatorType(Operator.OperatorType.GROUP_BY_TIME);
  }

  @Override
  public TSExecuteStatementResp getTSExecuteStatementResp(boolean isJdbcQuery)
      throws TException, MetadataException {
    TSExecuteStatementResp resp = super.getTSExecuteStatementResp(isJdbcQuery);
    resp.setIgnoreTimeStamp(false);
    return resp;
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

  public long getInterval() {
    return interval;
  }

  public void setInterval(long interval) {
    this.interval = interval;
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

  public long getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(long slidingStep) {
    this.slidingStep = slidingStep;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public static GlobalTimeExpression getTimeExpression(GroupByTimePlan plan)
      throws QueryProcessException {
    if (plan.isSlidingStepByMonth() || plan.isIntervalByMonth()) {
      if (!plan.isAscending()) {
        throw new QueryProcessException("Group by month doesn't support order by time desc now.");
      }
      return new GlobalTimeExpression(
          (new GroupByMonthFilter(
              plan.getInterval(),
              plan.getSlidingStep(),
              plan.getStartTime(),
              plan.getEndTime(),
              plan.isSlidingStepByMonth(),
              plan.isIntervalByMonth(),
              SessionManager.getInstance().getCurrSessionTimeZone())));
    } else {
      return new GlobalTimeExpression(
          new GroupByFilter(
              plan.getInterval(), plan.getSlidingStep(), plan.getStartTime(), plan.getEndTime()));
    }
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    super.serializeImpl(buffer);
    ReadWriteIOUtils.write(startTime, buffer);
    ReadWriteIOUtils.write(endTime, buffer);
    ReadWriteIOUtils.write(interval, buffer);
    ReadWriteIOUtils.write(slidingStep, buffer);
    ReadWriteIOUtils.write(isIntervalByMonth, buffer);
    ReadWriteIOUtils.write(isSlidingStepByMonth, buffer);
    ReadWriteIOUtils.write(leftCRightO, buffer);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException, IOException {
    super.deserialize(buffer);
    startTime = ReadWriteIOUtils.readLong(buffer);
    endTime = ReadWriteIOUtils.readLong(buffer);
    interval = ReadWriteIOUtils.readLong(buffer);
    slidingStep = ReadWriteIOUtils.readLong(buffer);
    isIntervalByMonth = ReadWriteIOUtils.readBool(buffer);
    isSlidingStepByMonth = ReadWriteIOUtils.readBool(buffer);
    leftCRightO = ReadWriteIOUtils.readBool(buffer);
  }
}
