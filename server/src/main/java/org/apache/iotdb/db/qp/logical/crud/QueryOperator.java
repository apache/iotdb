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

import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;

import java.util.Map;

/** this class extends {@code RootOperator} and process getIndex statement */
public class QueryOperator extends SFWOperator {

  private int level = -1;

  private int rowLimit = 0;
  private int rowOffset = 0;
  private int seriesLimit = 0;
  private int seriesOffset = 0;

  private boolean isAlignByDevice = false;
  private boolean isAlignByTime = true;

  private boolean ascending = true;

  private Map<String, Object> props;

  private IndexType indexType;

  public QueryOperator() {
    super(SQLConstant.TOK_QUERY);
    operatorType = Operator.OperatorType.QUERY;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
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

  public boolean isAlignByDevice() {
    return isAlignByDevice;
  }

  public void setAlignByDevice(boolean alignByDevice) {
    this.isAlignByDevice = alignByDevice;
  }

  public boolean isAlignByTime() {
    return isAlignByTime;
  }

  public void setAlignByTime(boolean isAlignByTime) {
    this.isAlignByTime = isAlignByTime;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }
}
