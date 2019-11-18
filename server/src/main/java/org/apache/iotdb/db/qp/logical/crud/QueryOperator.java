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

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 */
public class QueryOperator extends SFWOperator {

  private long startTime;
  private long endTime;
  // time interval
  private long unit;
  // sliding step
  private long slidingStep;
  private boolean isGroupByTime = false;

  private Map<TSDataType, IFill> fillTypes;
  private boolean isFill = false;

  private int seriesLimit;
  private int seriesOffset;
  private boolean hasSlimit = false; // false if sql does not contain SLIMIT clause

  private boolean isGroupByDevice = false;

  public QueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = Operator.OperatorType.QUERY;
  }

  public boolean isFill() {
    return isFill;
  }

  public void setFill(boolean fill) {
    isFill = fill;
  }

  public Map<TSDataType, IFill> getFillTypes() {
    return fillTypes;
  }

  public void setFillTypes(Map<TSDataType, IFill> fillTypes) {
    this.fillTypes = fillTypes;
  }

  public boolean isGroupBy() {
    return isGroupByTime;
  }

  public void setGroupBy(boolean isGroupBy) {
    this.isGroupByTime = isGroupBy;
  }

  public int getSeriesLimit() {
    return seriesLimit;
  }

  public void setSeriesLimit(int seriesLimit) {
    this.seriesLimit = seriesLimit;
    this.hasSlimit = true;
  }

  public int getSeriesOffset() {
    return seriesOffset;
  }

  public void setSeriesOffset(int seriesOffset) {
    /*
     * Since soffset cannot be set alone without slimit, `hasSlimit` only need to be set true in the
     * `setSeriesLimit` function.
     */
    this.seriesOffset = seriesOffset;
  }

  public boolean hasSlimit() {
    return hasSlimit;
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

  public boolean isGroupByDevice() {
    return isGroupByDevice;
  }

  public void setGroupByDevice(boolean isGroupByDevice) {
    this.isGroupByDevice = isGroupByDevice;
  }
}
