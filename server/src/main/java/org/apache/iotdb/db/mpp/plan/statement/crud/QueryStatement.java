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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultSetFormat;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class of SELECT statement.
 *
 * <p>Here is the syntax definition of SELECT statement:
 *
 * <ul>
 *   SELECT
 *   <li>[LAST] resultColumn [, resultColumn] ...
 *   <li>FROM prefixPath [, prefixPath] ...
 *   <li>WHERE whereCondition
 *   <li>[GROUP BY ([startTime, endTime), interval, slidingStep)]
 *   <li>[GROUP BY LEVEL = levelNum [, levelNum] ...]
 *   <li>[FILL ({PREVIOUS, beforeRange | LINEAR, beforeRange, afterRange | constant})]
 *   <li>[LIMIT rowLimit] [OFFSET rowOffset]
 *   <li>[SLIMIT seriesLimit] [SOFFSET seriesOffset]
 *   <li>[WITHOUT NULL {ANY | ALL} [resultColumn [, resultColumn] ...]]
 *   <li>[ORDER BY TIME {ASC | DESC}]
 *   <li>[{ALIGN BY DEVICE | DISABLE ALIGN}]
 * </ul>
 */
public class QueryStatement extends Statement {

  protected SelectComponent selectComponent;
  protected FromComponent fromComponent;
  protected WhereCondition whereCondition;

  // row limit and offset for result set. The default value is 0, which means no limit
  protected int rowLimit = 0;
  // row offset for result set. The default value is 0
  protected int rowOffset = 0;

  // series limit and offset for result set. The default value is 0, which means no limit
  protected int seriesLimit = 0;
  // series offset for result set. The default value is 0
  protected int seriesOffset = 0;

  protected FillComponent fillComponent;

  protected FilterNullComponent filterNullComponent;

  protected OrderBy resultOrder = OrderBy.TIMESTAMP_ASC;

  protected ResultSetFormat resultSetFormat = ResultSetFormat.ALIGN_BY_TIME;

  // `GROUP BY TIME` clause
  protected GroupByTimeComponent groupByTimeComponent;

  // `GROUP BY LEVEL` clause
  protected GroupByLevelComponent groupByLevelComponent;

  public QueryStatement() {
    this.statementType = StatementType.QUERY;
  }

  @Override
  public List<PartialPath> getPaths() {
    return fromComponent.getPrefixPaths();
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

  public WhereCondition getWhereCondition() {
    return whereCondition;
  }

  public void setWhereCondition(WhereCondition whereCondition) {
    this.whereCondition = whereCondition;
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

  public FillComponent getFillComponent() {
    return fillComponent;
  }

  public void setFillComponent(FillComponent fillComponent) {
    this.fillComponent = fillComponent;
  }

  public FilterNullComponent getFilterNullComponent() {
    return filterNullComponent;
  }

  public void setFilterNullComponent(FilterNullComponent filterNullComponent) {
    this.filterNullComponent = filterNullComponent;
  }

  public OrderBy getResultOrder() {
    return resultOrder;
  }

  public void setResultOrder(OrderBy resultOrder) {
    this.resultOrder = resultOrder;
  }

  public ResultSetFormat getResultSetFormat() {
    return resultSetFormat;
  }

  public void setResultSetFormat(ResultSetFormat resultSetFormat) {
    this.resultSetFormat = resultSetFormat;
  }

  public GroupByTimeComponent getGroupByTimeComponent() {
    return groupByTimeComponent;
  }

  public void setGroupByTimeComponent(GroupByTimeComponent groupByTimeComponent) {
    this.groupByTimeComponent = groupByTimeComponent;
  }

  public GroupByLevelComponent getGroupByLevelComponent() {
    return groupByLevelComponent;
  }

  public void setGroupByLevelComponent(GroupByLevelComponent groupByLevelComponent) {
    this.groupByLevelComponent = groupByLevelComponent;
  }

  public boolean isLastQuery() {
    return selectComponent.isHasLast();
  }

  public boolean isAggregationQuery() {
    return selectComponent.isHasBuiltInAggregationFunction();
  }

  public boolean isGroupByLevel() {
    return groupByLevelComponent != null;
  }

  public boolean isGroupByTime() {
    return groupByTimeComponent != null;
  }

  public boolean isAlignByDevice() {
    return resultSetFormat == ResultSetFormat.ALIGN_BY_DEVICE;
  }

  public boolean disableAlign() {
    return resultSetFormat == ResultSetFormat.DISABLE_ALIGN;
  }

  public boolean HasBuiltInAggregationFunction() {
    return selectComponent.isHasBuiltInAggregationFunction();
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return selectComponent.isHasTimeSeriesGeneratingFunction();
  }

  public boolean hasUserDefinedAggregationFunction() {
    return selectComponent.isHasUserDefinedAggregationFunction();
  }

  public Map<String, Set<PartialPath>> getDeviceNameToDeduplicatedPathsMap() {
    return getSelectComponent().getDeviceNameToDeduplicatedPathsMap();
  }

  /** semantic check */
  public void selfCheck() {
    if (isAlignByDevice()) {
      if (hasTimeSeriesGeneratingFunction() || hasUserDefinedAggregationFunction()) {
        throw new SemanticException("ALIGN BY DEVICE: User-Defined Functions are not supported.");
      }

      for (PartialPath path : selectComponent.getPaths()) {
        if (path.getNodes().length > 1
            || path.getFullPath().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
          throw new SemanticException(
              "ALIGN BY DEVICE: The paths of the SELECT clause can only be measurement or one-level wildcard.");
        }
      }
    }

    if (isLastQuery()) {
      if (isAlignByDevice()) {
        throw new SemanticException("Last query doesn't support align by device.");
      }

      if (disableAlign()) {
        throw new SemanticException("Disable align cannot be applied to LAST query.");
      }

      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if (!(expression instanceof TimeSeriesOperand)) {
          throw new SemanticException("Last queries can only be applied on raw time series.");
        }
      }
    }

    if (isAggregationQuery()) {
      if (disableAlign()) {
        throw new SemanticException("AGGREGATION doesn't support disable align clause.");
      }
      checkSelectComponent(selectComponent);
      if (isGroupByLevel() && isAlignByDevice()) {
        throw new SemanticException("group by level does not support align by device now.");
      }
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

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
