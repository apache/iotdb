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

package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregationQueryStatement extends QueryStatement {

  // GROUP BY LEVEL clause
  protected GroupByLevelComponent groupByLevelComponent;

  public AggregationQueryStatement() {
    super();
  }

  public AggregationQueryStatement(QueryStatement queryStatement) {
    super(queryStatement);
  }

  public GroupByLevelComponent getGroupByLevelComponent() {
    return groupByLevelComponent;
  }

  public void setGroupByLevelComponent(GroupByLevelComponent groupByLevelComponent) {
    this.groupByLevelComponent = groupByLevelComponent;
  }

  @Override
  public boolean isGroupByLevel() {
    return groupByLevelComponent != null && groupByLevelComponent.getLevels().length > 0;
  }

  public Map<String, Map<PartialPath, Set<AggregationType>>> getDeviceNameToAggregationsMap() {
    Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap =
        new HashMap<>();
    for (ResultColumn resultColumn : getSelectComponent().getResultColumns()) {
      FunctionExpression expression = (FunctionExpression) resultColumn.getExpression();
      PartialPath path = expression.getPaths().get(0);
      String functionName = expression.getFunctionName();
      deviceNameToAggregationsMap
          .computeIfAbsent(path.getDevice(), key -> new HashMap<>())
          .computeIfAbsent(path, key -> new HashSet<>())
          .add(AggregationType.valueOf(functionName.toUpperCase()));
    }
    return deviceNameToAggregationsMap;
  }

  public DatasetHeader constructDatasetHeader() {
    List<ColumnHeader> columnHeaders = new ArrayList<>();
    // TODO: consider Aggregation
    return new DatasetHeader(columnHeaders, true);
  }

  @Override
  public void selfCheck() {
    super.selfCheck();

    if (disableAlign()) {
      throw new SemanticException("AGGREGATION doesn't support disable align clause.");
    }
    checkSelectComponent(selectComponent);
    if (isGroupByLevel() && isAlignByDevice()) {
      throw new SemanticException("group by level does not support align by device now.");
    }
  }

  protected void checkSelectComponent(SelectComponent selectComponent) throws SemanticException {
    if (hasTimeSeriesGeneratingFunction()) {
      throw new SemanticException(
          "User-defined and built-in hybrid aggregation is not supported together.");
    }

    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof TimeSeriesOperand) {
        throw new SemanticException(
            "Common queries and aggregated queries are not allowed to appear at the same time.");
      }
      // Currently, the aggregation function expression can only contain a timeseries operand.
      if (expression instanceof FunctionExpression
          && (expression.getExpressions().size() != 1
              || !(expression.getExpressions().get(0) instanceof TimeSeriesOperand))) {
        throw new SemanticException(
            "The argument of the aggregation function must be a time series.");
      }
    }
  }

  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAggregationQuery(this, context);
  }
}
