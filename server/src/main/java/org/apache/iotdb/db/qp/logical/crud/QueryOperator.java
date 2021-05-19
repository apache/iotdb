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
import org.apache.iotdb.db.qp.logical.RootOperator;

import java.util.Map;

public class QueryOperator extends RootOperator {

  protected SelectComponent selectComponent;
  protected FromComponent fromComponent;
  protected FilterOperator filterOperator;
  protected SpecialClauseComponent specialClauseComponent;

  protected Map<String, Object> props;

  protected IndexType indexType;

  public QueryOperator() {
    super(SQLConstant.TOK_QUERY);
    operatorType = Operator.OperatorType.QUERY;
  }

  public SelectComponent getSelectComponent() {
    return selectComponent;
  }

  public void setSelectComponent(SelectComponent selectComponent) {
    this.selectComponent = selectComponent;
  }

  public FromComponent getFromComponent() {
    return fromComponent;
  }

  public void setFromComponent(FromComponent fromComponent) {
    this.fromComponent = fromComponent;
  }

  public FilterOperator getFilterOperator() {
    return filterOperator;
  }

  public void setFilterOperator(FilterOperator filterOperator) {
    this.filterOperator = filterOperator;
  }

  public void setSpecialClauseComponent(SpecialClauseComponent specialClauseComponent) {
    this.specialClauseComponent = specialClauseComponent;
  }

  public SpecialClauseComponent getSpecialClauseComponent() {
    return specialClauseComponent;
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

  public boolean hasAggregationFunction() {
    return selectComponent.hasAggregationFunction();
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return selectComponent.hasTimeSeriesGeneratingFunction();
  }

  public boolean isAlignByDevice() {
    return specialClauseComponent != null && specialClauseComponent.isAlignByDevice();
  }

  public boolean isAlignByTime() {
    return specialClauseComponent == null || specialClauseComponent.isAlignByTime();
  }

  public boolean isGroupByLevel() {
    return specialClauseComponent != null && specialClauseComponent.getLevel() != -1;
  }
}
