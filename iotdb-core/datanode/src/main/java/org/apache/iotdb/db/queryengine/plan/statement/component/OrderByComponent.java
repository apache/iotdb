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

package org.apache.iotdb.db.queryengine.plan.statement.component;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** The order of query result set */
public class OrderByComponent extends StatementNode {

  private final List<SortItem> sortItemList;
  private final List<Expression> sortItemExpressionList;

  private boolean orderByTime = false;
  private int timeOrderPriority = -1;

  private boolean orderByTimeseries = false;
  private int timeseriesOrderPriority = -1;

  private boolean orderByDevice = false;
  private int deviceOrderPriority = -1;

  public OrderByComponent() {
    this.sortItemList = new ArrayList<>();
    this.sortItemExpressionList = new ArrayList<>();
  }

  /**
   * Creates a structurally independent component for analysis-time copy-on-write updates.
   *
   * <p>Expression nodes are shared because analysis replaces them instead of mutating them. Lists
   * and SortItems are copied because both are changed in place while resolving ORDER BY
   * expressions.
   */
  public static OrderByComponent copyOf(OrderByComponent source) {
    OrderByComponent result = new OrderByComponent();
    int expressionIndex = 0;
    for (SortItem sortItem : source.sortItemList) {
      if (sortItem.isExpression()) {
        SortItem copiedSortItem =
            new SortItem(
                sortItem.getExpression(), sortItem.getOrdering(), sortItem.getNullOrdering());
        result.sortItemList.add(copiedSortItem);
        // SortItem intentionally keeps the parser-facing expression for SQL rendering, while the
        // parallel expression list contains the lower-case normalized AST consumed by analysis.
        // They are not interchangeable, so preserve both references independently in the COW
        // component.
        result.sortItemExpressionList.add(source.sortItemExpressionList.get(expressionIndex++));
      } else {
        result.addSortItem(
            new SortItem(
                sortItem.getSortKey(), sortItem.getOrdering(), sortItem.getNullOrdering()));
      }
    }
    return result;
  }

  public void addSortItem(SortItem sortItem) {
    this.sortItemList.add(sortItem);
    switch (sortItem.getSortKey()) {
      case OrderByKey.TIME:
        orderByTime = true;
        timeOrderPriority = sortItemList.size() - 1;
        break;
      case OrderByKey.TIMESERIES:
        orderByTimeseries = true;
        timeseriesOrderPriority = sortItemList.size() - 1;
        break;
      case OrderByKey.DEVICE:
        orderByDevice = true;
        deviceOrderPriority = sortItemList.size() - 1;
        break;
      case OrderByKey.QUERYID:
      case OrderByKey.DATANODEID:
      case OrderByKey.ELAPSEDTIME:
      case OrderByKey.STATEMENT:
      case OrderByKey.DATABASE:
      case OrderByKey.REGIONID:
      case OrderByKey.TIMEPARTITION:
      case OrderByKey.SIZEINBYTES:
        // show queries statement or show disk usage statement
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.QUERY_EXCEPTION_UNKNOWN_SORT_KEY_S_37965711,
                sortItem.getSortKey()));
    }
  }

  // if the sortItem can specify one unique time series
  public boolean isUnique() {
    return orderByDevice && orderByTime;
  }

  // if the first sortItem is device
  public boolean isBasedOnDevice() {
    return orderByDevice && deviceOrderPriority == 0;
  }

  public boolean isBasedOnTime() {
    return orderByTime && timeOrderPriority == 0;
  }

  public void addExpressionSortItem(SortItem sortItem) {
    this.sortItemList.add(sortItem);
    this.sortItemExpressionList.add(
        ExpressionAnalyzer.toLowerCaseExpression(sortItem.getExpression()));
  }

  public List<SortItem> getSortItemList() {
    return sortItemList;
  }

  public List<Expression> getExpressionSortItemList() {
    return sortItemExpressionList;
  }

  public boolean isOrderByTime() {
    return orderByTime;
  }

  public Ordering getTimeOrder() {
    checkState(
        timeOrderPriority != -1,
        DataNodeQueryMessages.EXCEPTION_THE_TIME_ORDER_IS_NOT_SPECIFIED_DOT_624A7526);
    return sortItemList.get(timeOrderPriority).getOrdering();
  }

  public boolean isOrderByTimeseries() {
    return orderByTimeseries;
  }

  public Ordering getTimeseriesOrder() {
    checkState(
        timeseriesOrderPriority != -1,
        DataNodeQueryMessages.EXCEPTION_THE_TIMESERIES_ORDER_IS_NOT_SPECIFIED_DOT_68EE3875);
    return sortItemList.get(timeseriesOrderPriority).getOrdering();
  }

  public boolean isOrderByDevice() {
    return orderByDevice;
  }

  public Ordering getDeviceOrder() {
    checkState(
        deviceOrderPriority != -1,
        DataNodeQueryMessages.EXCEPTION_THE_DEVICE_ORDER_IS_NOT_SPECIFIED_DOT_D3FB9559);
    return sortItemList.get(deviceOrderPriority).getOrdering();
  }

  public int getTimeOrderPriority() {
    return timeOrderPriority;
  }

  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("ORDER BY ");
    for (int i = 0; i < sortItemList.size(); i++) {
      sqlBuilder.append(sortItemList.get(i).toSQLString());
      if (i < sortItemList.size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    return sqlBuilder.toString();
  }
}
