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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import java.util.HashMap;
import java.util.Map;

public class DistributionPlanContext {
  protected boolean isRoot;
  protected MPPQueryContext queryContext;
  protected boolean forceAddParent;
  // That the variable is true means there is some source series which is
  // distributed in multi DataRegions
  protected boolean oneSeriesInMultiRegion;
  // That the variable is true means this query will be distributed in multi
  // DataRegions
  protected boolean queryMultiRegion;

  // used by group by level
  private Map<String, Expression> columnNameToExpression;

  private Map<PlanNodeId, TRegionReplicaSet> preferDataRegion;

  protected DistributionPlanContext(MPPQueryContext queryContext) {
    this.isRoot = true;
    this.queryContext = queryContext;
  }

  protected DistributionPlanContext copy() {
    return new DistributionPlanContext(queryContext);
  }

  protected DistributionPlanContext setRoot(boolean isRoot) {
    this.isRoot = isRoot;
    return this;
  }

  public boolean isForceAddParent() {
    return this.forceAddParent;
  }

  public void setForceAddParent() {
    this.forceAddParent = true;
  }

  public void setOneSeriesInMultiRegion(boolean oneSeriesInMultiRegion) {
    this.oneSeriesInMultiRegion = oneSeriesInMultiRegion;
  }

  public boolean isQueryMultiRegion() {
    return queryMultiRegion;
  }

  public void setQueryMultiRegion(boolean queryMultiRegion) {
    this.queryMultiRegion = queryMultiRegion;
  }

  public Map<String, Expression> getColumnNameToExpression() {
    return columnNameToExpression;
  }

  public void setColumnNameToExpression(Map<String, Expression> columnNameToExpression) {
    this.columnNameToExpression = columnNameToExpression;
  }

  public void putNodePreferDataRegion(PlanNodeId id, TRegionReplicaSet region) {
    if (preferDataRegion == null) {
      preferDataRegion = new HashMap<>();
    }
    preferDataRegion.put(id, region);
  }

  public Map<PlanNodeId, TRegionReplicaSet> getPreferDataRegion() {
    return this.preferDataRegion;
  }

  public void setMainFragmentLocatedRegion(TRegionReplicaSet region) {
    this.queryContext.setMainFragmentLocatedRegion(region);
  }
}
