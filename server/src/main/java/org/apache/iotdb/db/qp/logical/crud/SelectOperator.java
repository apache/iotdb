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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;

/**
 * this class maintains information from select clause.
 */
public final class SelectOperator extends Operator {

  private List<PartialPath> suffixList;
  private List<String> aggregations;
  private boolean lastQuery;

  /**
   * init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>.
   */
  public SelectOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.SELECT;
    suffixList = new ArrayList<>();
    aggregations = new ArrayList<>();
    lastQuery = false;
  }

  public void addSelectPath(PartialPath suffixPath) {
    suffixList.add(suffixPath);
  }

  public void addClusterPath(PartialPath suffixPath, String aggregation) {
    suffixList.add(suffixPath);
    aggregations.add(aggregation);
  }

  public void setLastQuery() {
    lastQuery = true;
  }

  public List<String> getAggregations() {
    return this.aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public void setSuffixPathList(List<PartialPath> suffixPaths) {
    suffixList = suffixPaths;
  }

  public List<PartialPath> getSuffixPaths() {
    return suffixList;
  }

  public boolean isLastQuery() {return this.lastQuery; }
}
