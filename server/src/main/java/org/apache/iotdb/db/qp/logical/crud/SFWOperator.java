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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.RootOperator;

/**
 * SFWOperator(select-from-where) includes four subclass: INSERT,DELETE,UPDATE,QUERY. All of these
 * four statements has three partition: select clause, from clause and filter clause(where clause).
 */
public abstract class SFWOperator extends RootOperator {

  private SelectOperator selectOperator;
  private FromOperator fromOperator;
  private FilterOperator filterOperator;
  private boolean hasAggregation = false;
  private boolean lastQuery = false;

  public SFWOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.SFW;
  }

  public FromOperator getFromOperator() {
    return fromOperator;
  }

  public void setFromOperator(FromOperator from) {
    this.fromOperator = from;
  }

  public SelectOperator getSelectOperator() {
    return selectOperator;
  }

  /**
   * set selectOperator, then init hasAggregation according to selectOperator.
   */
  public void setSelectOperator(SelectOperator sel) {
    this.selectOperator = sel;
    if (!sel.getAggregations().isEmpty()) {
      hasAggregation = true;
    }
    if (sel.isLastQuery()) {
      lastQuery = true;
    }
  }

  public FilterOperator getFilterOperator() {
    return filterOperator;
  }

  public void setFilterOperator(FilterOperator filter) {
    this.filterOperator = filter;
  }

  /**
   * get information from SelectOperator and FromOperator and generate all table paths.
   *
   * @return - a list of seriesPath
   */
  public List<PartialPath> getSelectedPaths() {
    List<PartialPath> suffixPaths = null;
    if (selectOperator != null) {
      suffixPaths = selectOperator.getSuffixPaths();
    }
    return suffixPaths;
  }

  public boolean hasAggregation() {
    return hasAggregation;
  }

  public boolean isLastQuery() {
    return lastQuery;
  }
}
