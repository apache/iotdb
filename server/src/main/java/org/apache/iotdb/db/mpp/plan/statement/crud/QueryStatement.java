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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTagComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.HavingCondition;
import org.apache.iotdb.db.mpp.plan.statement.component.IntoComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultSetFormat;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
 *   <li>[FILL ({PREVIOUS | LINEAR | constant})]
 *   <li>[LIMIT rowLimit] [OFFSET rowOffset]
 *   <li>[SLIMIT seriesLimit] [SOFFSET seriesOffset]
 *   <li>[ORDER BY TIME {ASC | DESC}]
 *   <li>[{ALIGN BY DEVICE | DISABLE ALIGN}]
 * </ul>
 */
public class QueryStatement extends Statement {

  private SelectComponent selectComponent;
  private FromComponent fromComponent;
  private WhereCondition whereCondition;
  private HavingCondition havingCondition;

  // row limit and offset for result set. The default value is 0, which means no limit
  private int rowLimit = 0;
  // row offset for result set. The default value is 0
  private int rowOffset = 0;

  // series limit and offset for result set. The default value is 0, which means no limit
  private int seriesLimit = 0;
  // series offset for result set. The default value is 0
  private int seriesOffset = 0;

  private FillComponent fillComponent;

  private OrderByComponent orderByComponent;

  private ResultSetFormat resultSetFormat = ResultSetFormat.ALIGN_BY_TIME;

  // `GROUP BY TIME` clause
  private GroupByTimeComponent groupByTimeComponent;

  // `GROUP BY LEVEL` clause
  private GroupByLevelComponent groupByLevelComponent;

  // `GROUP BY TAG` clause
  private GroupByTagComponent groupByTagComponent;

  // `INTO` clause
  private IntoComponent intoComponent;

  private boolean isCqQueryBody;

  public QueryStatement() {
    this.statementType = StatementType.QUERY;
  }

  @Override
  public List<PartialPath> getPaths() {
    Set<PartialPath> authPaths = new HashSet<>();
    List<PartialPath> prefixPaths = fromComponent.getPrefixPaths();
    List<ResultColumn> resultColumns = selectComponent.getResultColumns();
    for (ResultColumn resultColumn : resultColumns) {
      Expression expression = resultColumn.getExpression();
      authPaths.addAll(ExpressionAnalyzer.concatExpressionWithSuffixPaths(expression, prefixPaths));
    }
    return new ArrayList<>(authPaths);
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

  public boolean hasWhere() {
    return whereCondition != null;
  }

  public WhereCondition getWhereCondition() {
    return whereCondition;
  }

  public void setWhereCondition(WhereCondition whereCondition) {
    this.whereCondition = whereCondition;
  }

  public boolean hasHaving() {
    return havingCondition != null;
  }

  public HavingCondition getHavingCondition() {
    return havingCondition;
  }

  public void setHavingCondition(HavingCondition havingCondition) {
    this.havingCondition = havingCondition;
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

  public OrderByComponent getOrderByComponent() {
    return orderByComponent;
  }

  public void setOrderByComponent(OrderByComponent orderByComponent) {
    this.orderByComponent = orderByComponent;
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

  public GroupByTagComponent getGroupByTagComponent() {
    return groupByTagComponent;
  }

  public void setGroupByTagComponent(GroupByTagComponent groupByTagComponent) {
    this.groupByTagComponent = groupByTagComponent;
  }

  public boolean isLastQuery() {
    return selectComponent.hasLast();
  }

  public boolean isAggregationQuery() {
    return selectComponent.isHasBuiltInAggregationFunction();
  }

  public boolean isGroupByLevel() {
    return groupByLevelComponent != null;
  }

  public boolean isGroupByTag() {
    return groupByTagComponent != null;
  }

  public boolean isGroupByTime() {
    return groupByTimeComponent != null;
  }

  public boolean isAlignByTime() {
    return resultSetFormat == ResultSetFormat.ALIGN_BY_TIME;
  }

  public boolean isAlignByDevice() {
    return resultSetFormat == ResultSetFormat.ALIGN_BY_DEVICE;
  }

  public boolean disableAlign() {
    return resultSetFormat == ResultSetFormat.DISABLE_ALIGN;
  }

  public boolean isOrderByTime() {
    return orderByComponent != null && orderByComponent.isOrderByTime();
  }

  public boolean isOrderByTimeseries() {
    return orderByComponent != null && orderByComponent.isOrderByTimeseries();
  }

  public boolean isOrderByDevice() {
    return orderByComponent != null && orderByComponent.isOrderByDevice();
  }

  public IntoComponent getIntoComponent() {
    return intoComponent;
  }

  public void setIntoComponent(IntoComponent intoComponent) {
    this.intoComponent = intoComponent;
  }

  public Ordering getResultTimeOrder() {
    if (orderByComponent == null || !orderByComponent.isOrderByTime()) {
      return Ordering.ASC;
    }
    return orderByComponent.getTimeOrder();
  }

  public List<SortItem> getSortItemList() {
    if (orderByComponent == null) {
      return Collections.emptyList();
    }
    return orderByComponent.getSortItemList();
  }

  public boolean hasFill() {
    return fillComponent != null;
  }

  public boolean hasOrderBy() {
    return orderByComponent != null;
  }

  public boolean isSelectInto() {
    return intoComponent != null;
  }

  public boolean isCqQueryBody() {
    return isCqQueryBody;
  }

  public void setCqQueryBody(boolean cqQueryBody) {
    isCqQueryBody = cqQueryBody;
  }

  public void semanticCheck() {
    if (isAggregationQuery()) {
      if (disableAlign()) {
        throw new SemanticException("AGGREGATION doesn't support disable align clause.");
      }
      if (isGroupByLevel() && isAlignByDevice()) {
        throw new SemanticException("GROUP BY LEVEL does not support align by device now.");
      }
      if (isGroupByTag() && isAlignByDevice()) {
        throw new SemanticException("GROUP BY TAGS does not support align by device now.");
      }
      Set<String> outputColumn = new HashSet<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        if (resultColumn.getColumnType() != ResultColumn.ColumnType.AGGREGATION) {
          throw new SemanticException("Raw data and aggregation hybrid query is not supported.");
        }
        outputColumn.add(
            resultColumn.getAlias() != null
                ? resultColumn.getAlias()
                : resultColumn.getExpression().getExpressionString());
      }
      if (isGroupByTag()) {
        for (String s : getGroupByTagComponent().getTagKeys()) {
          if (outputColumn.contains(s)) {
            throw new SemanticException("Output column is duplicated with the tag key: " + s);
          }
        }
      }
    } else {
      if (isGroupByTime() || isGroupByLevel()) {
        throw new SemanticException(
            "Common queries and aggregated queries are not allowed to appear at the same time");
      }
    }

    if (getHavingCondition() != null) {
      Expression havingExpression = getHavingCondition().getPredicate();
      if (ExpressionAnalyzer.identifyOutputColumnType(havingExpression, true)
          != ResultColumn.ColumnType.AGGREGATION) {
        throw new SemanticException("Expression of HAVING clause must to be an Aggregation");
      }
      try {
        if (isGroupByLevel()) { // check path in SELECT and HAVING only have one node
          for (ResultColumn resultColumn : getSelectComponent().getResultColumns()) {
            ExpressionAnalyzer.checkIsAllMeasurement(resultColumn.getExpression());
          }
          ExpressionAnalyzer.checkIsAllMeasurement(havingExpression);
        }
      } catch (SemanticException e) {
        throw new SemanticException("When Having used with GroupByLevel: " + e.getMessage());
      }
    }

    if (isAlignByDevice()) {
      // the paths can only be measurement or one-level wildcard in ALIGN BY DEVICE
      try {
        for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
          ExpressionAnalyzer.checkIsAllMeasurement(resultColumn.getExpression());
        }
        if (getWhereCondition() != null) {
          ExpressionAnalyzer.checkIsAllMeasurement(getWhereCondition().getPredicate());
        }
      } catch (SemanticException e) {
        throw new SemanticException("ALIGN BY DEVICE: " + e.getMessage());
      }

      if (isOrderByTimeseries()) {
        throw new SemanticException("Sorting by timeseries is only supported in last queries.");
      }
      //      if (isOrderByDevice()) {
      //        // TODO support sort by device
      //        throw new SemanticException("Sorting by device is not yet supported.");
      //      }
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
      if (isOrderByDevice()) {
        throw new SemanticException(
            "Sorting by device is only supported in ALIGN BY DEVICE queries.");
      }
      if (isOrderByTime()) {
        throw new SemanticException("Sorting by time is not yet supported in last queries.");
      }
    }

    if (!isAlignByDevice() && !isLastQuery()) {
      if (isOrderByTimeseries()) {
        throw new SemanticException("Sorting by timeseries is only supported in last queries.");
      }
      if (isOrderByDevice()) {
        throw new SemanticException(
            "Sorting by device is only supported in ALIGN BY DEVICE queries.");
      }
    }

    if (isSelectInto()) {
      if (getSeriesLimit() > 0) {
        throw new SemanticException("select into: slimit clauses are not supported.");
      }
      if (getSeriesOffset() > 0) {
        throw new SemanticException("select into: soffset clauses are not supported.");
      }
      if (disableAlign()) {
        throw new SemanticException("select into: disable align clauses are not supported.");
      }
      if (isLastQuery()) {
        throw new SemanticException("select into: last clauses are not supported.");
      }
      if (isGroupByTag()) {
        throw new SemanticException("select into: GROUP BY TAGS clause are not supported.");
      }
    }
  }

  public String constructFormattedSQL() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(selectComponent.toSQLString()).append("\n");
    if (isSelectInto()) {
      sqlBuilder.append("\t").append(intoComponent.toSQLString()).append("\n");
    }
    sqlBuilder.append("\t").append(fromComponent.toSQLString()).append("\n");
    if (hasWhere()) {
      sqlBuilder.append("\t").append(whereCondition.toSQLString()).append("\n");
    }
    if (isGroupByTime()) {
      sqlBuilder.append("\t").append(groupByTimeComponent.toSQLString()).append("\n");
    }
    if (isGroupByLevel()) {
      sqlBuilder
          .append("\t")
          .append(groupByLevelComponent.toSQLString(isGroupByTime()))
          .append("\n");
    }
    if (hasHaving()) {
      sqlBuilder.append("\t").append(havingCondition.toSQLString()).append("\n");
    }
    if (hasFill()) {
      sqlBuilder.append("\t").append(fillComponent.toSQLString()).append("\n");
    }
    if (hasOrderBy()) {
      sqlBuilder.append("\t").append(orderByComponent.toSQLString()).append("\n");
    }
    if (rowLimit != 0) {
      sqlBuilder.append("\t").append("LIMIT").append(' ').append(rowLimit).append("\n");
    }
    if (rowOffset != 0) {
      sqlBuilder.append("\t").append("OFFSET").append(' ').append(rowOffset).append("\n");
    }
    if (seriesLimit != 0) {
      sqlBuilder.append("\t").append("SLIMIT").append(' ').append(seriesLimit).append("\n");
    }
    if (seriesOffset != 0) {
      sqlBuilder.append("\t").append("SOFFSET").append(' ').append(seriesOffset).append("\n");
    }
    if (isAlignByDevice()) {
      sqlBuilder.append("\t").append("ALIGN BY DEVICE").append("\n");
    }
    sqlBuilder.append(';');
    return sqlBuilder.toString();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
