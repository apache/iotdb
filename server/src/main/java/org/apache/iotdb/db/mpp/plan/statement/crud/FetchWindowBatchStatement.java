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

package org.apache.iotdb.db.mpp.plan.statement.crud;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.List;

public class FetchWindowBatchStatement extends Statement {

  private List<PartialPath> queryPaths;
  private GroupByTimeParameter groupByTimeParameter;
  private String queryFilter;

  public FetchWindowBatchStatement() {
    super();
    statementType = StatementType.FETCH_WINDOW_BATCH;
  }

  public void setQueryPaths(List<PartialPath> queryPaths) {
    this.queryPaths = queryPaths;
  }

  public void setGroupByTimeParameter(GroupByTimeParameter groupByTimeParameter) {
    this.groupByTimeParameter = groupByTimeParameter;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public void setQueryFilter(String queryFilter) {
    this.queryFilter = queryFilter;
  }

  public List<PartialPath> getQueryPaths() {
    return queryPaths;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return queryPaths;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitFetchWindowBatch(this, context);
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  public void semanticCheck() {
    if (queryFilter != null) {
      if (queryFilter.split(",").length != 2) {
        throw new IllegalArgumentException("queryFilter should be like \"startTime,endTime\"");
      }
    }
  }
}
