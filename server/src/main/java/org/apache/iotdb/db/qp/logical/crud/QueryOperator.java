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

import java.util.Map;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

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
  // if it is left close and right open interval
  private boolean leftCRightO;

  private Map<TSDataType, IFill> fillTypes;
  private boolean isFill = false;

  private boolean isGroupByLevel = false;
  private int level = -1;

  private int rowLimit = 0;
  private int rowOffset = 0;
  private int seriesLimit = 0;
  private int seriesOffset = 0;

  private boolean isAlignByDevice = false;
  private boolean isAlignByTime = true;

  private String column;

  private boolean ascending = true;

  private Map<String, Object> props;

  private IndexType indexType;

  public QueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = Operator.OperatorType.QUERY;
  }

  public Map<String, Object> getProps() {
    return props;
  }

  public void setProps(Map<String, Object> props) {
    this.props = props;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
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

  public boolean isGroupByLevel() {
    return isGroupByLevel;
  }

  public void setGroupByLevel(boolean isGroupBy) {
    this.isGroupByLevel = isGroupBy;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
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

  public boolean isAlignByDevice() {
    return isAlignByDevice;
  }

  public void setAlignByDevice(boolean isAlignByDevice) {
    this.isAlignByDevice = isAlignByDevice;
  }

  public boolean isAlignByTime() {
    return isAlignByTime;
  }

  public void setAlignByTime(boolean isAlignByTime) {
    this.isAlignByTime = isAlignByTime;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public boolean isGroupByTime() {
    return isGroupByTime;
  }

  public void setGroupByTime(boolean groupByTime) {
    isGroupByTime = groupByTime;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }
}
