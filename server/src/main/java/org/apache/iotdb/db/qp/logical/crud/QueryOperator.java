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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 */
public class QueryOperator extends SFWOperator {

  private long startTime;
  private long endTime;


  private int rowLimit = 0;
  private int rowOffset = 0;
  private int seriesLimit = 0;
  private int seriesOffset = 0;

  private boolean isAlignByDevice = false;
  private boolean isAlignByTime = true;

  private String column;


  private boolean isIntervalByMonth = false;
  private boolean isSlidingStepByMonth = false;
  private boolean ascending = true;

  private Map<String, Object> props;

  private IndexType indexType;

  public QueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = Operator.OperatorType.QUERY;
  }

  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize) throws QueryProcessException {
    if (hasUdf() && hasAggregation()) {
      throw new QueryProcessException(
          "User-defined and built-in hybrid aggregation is not supported.");
    }
    QueryPlan queryPlan;
    if (isLastQuery()) {
      queryPlan = new LastQueryPlan();
    } else if (getIndexType() != null) {
      queryPlan = new QueryIndexPlan();
    } else if (hasUdf()) {
      queryPlan = new UDTFPlan(getSelectOperator().getZoneId());
      ((UDTFPlan) queryPlan).constructUdfExecutors(getSelectOperator().getUdfList());
    } else if (hasAggregation()) {
      queryPlan = new AggregationPlan();
      convert((AggregationPlan) queryPlan);
    } else {
      queryPlan = new RawDataQueryPlan();
    }
    return queryPlan;
  }

  protected void convert(AggregationPlan plan) {
    plan.setAggregations(getSelectOperator().getAggregations());
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

  public boolean isAlignByDevice() {
    return isAlignByDevice;
  }

  public void setAlignByDevice(boolean alignByDevice) {
    isAlignByDevice = alignByDevice;
  }

  public boolean isAlignByTime() {
    return isAlignByTime;
  }

  public void setAlignByTime(boolean alignByTime) {
    isAlignByTime = alignByTime;
  }

  public void setSlidingStepByMonth(boolean slidingStepByMonth) {
    isSlidingStepByMonth = slidingStepByMonth;
  }

  public boolean isSlidingStepByMonth() {
    return isSlidingStepByMonth;
  }

  public boolean isIntervalByMonth() {
    return isIntervalByMonth;
  }

  public void setIntervalByMonth(boolean intervalByMonth) {
    isIntervalByMonth = intervalByMonth;
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
