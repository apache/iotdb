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

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 */
public class QueryOperator extends SFWOperator {

  private long unit;
  private long origin;
  private List<Pair<Long, Long>> intervals;
  private boolean isGroupByTime = false;

  private Map<TSDataType, IFill> fillTypes;
  private boolean isFill = false;

  private int rowLimit = 0;
  private int rowOffset = 0;
  private int seriesLimit = 0;
  private int seriesOffset = 0;

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

  public int getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public int getRowOffset() {
    return rowOffset;
  }

  public void setRowOffset(int rowOffset) {
    this.rowOffset = rowOffset;
  }

  public boolean hasLimit() {
    return rowLimit > 0;
  }

  public int getSeriesLimit() {
    return seriesLimit;
  }

  public void setSeriesLimit(int seriesLimit) {
    this.seriesLimit = seriesLimit;
  }

  public int getSeriesOffset() {
    return seriesOffset;
  }

  public void setSeriesOffset(int seriesOffset) {
    this.seriesOffset = seriesOffset;
  }

  public boolean hasSlimit() {
    return seriesLimit > 0;
  }

  public long getUnit() {
    return unit;
  }

  public void setUnit(long unit) {
    this.unit = unit;
  }

  public long getOrigin() {
    return origin;
  }

  public void setOrigin(long origin) {
    this.origin = origin;
  }

  public List<Pair<Long, Long>> getIntervals() {
    return intervals;
  }

  public void setIntervals(List<Pair<Long, Long>> intervals) {
    this.intervals = intervals;
  }

  public boolean isGroupByDevice() {
    return isGroupByDevice;
  }

  public void setGroupByDevice(boolean isGroupByDevice) {
    this.isGroupByDevice = isGroupByDevice;
  }
}
