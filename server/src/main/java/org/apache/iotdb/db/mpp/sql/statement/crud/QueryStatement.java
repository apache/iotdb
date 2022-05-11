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
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultSetFormat;
import org.apache.iotdb.db.mpp.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.tree.StatementVisitor;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class of SELECT statement.
 *
 * <p>Here is the syntax definition of SELECT statement:
 *
 * <ul>
 *   SELECT
 *   <li>[LAST] [TOP k]
 *   <li>resultColumn [, resultColumn] ...
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

  protected FilterNullComponent filterNullComponent;

  protected OrderBy resultOrder = OrderBy.TIMESTAMP_ASC;

  protected ResultSetFormat resultSetFormat = ResultSetFormat.ALIGN_BY_TIME;

  // used for TOP_N, LIKE, CONTAIN
  protected Map<String, Object> props;

  // TODO: add comments
  protected IndexType indexType;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  protected boolean isPrefixMatchPath = false;

  public QueryStatement() {
    this.statementType = StatementType.QUERY;
  }

  public QueryStatement(QueryStatement another) {
    this.statementType = StatementType.QUERY;
    this.selectComponent = another.getSelectComponent();
    this.fromComponent = another.getFromComponent();
    this.whereCondition = another.getWhereCondition();
    this.rowLimit = another.getRowLimit();
    this.rowOffset = another.getRowOffset();
    this.seriesLimit = another.getSeriesLimit();
    this.seriesOffset = another.getSeriesOffset();
    this.filterNullComponent = another.getFilterNullComponent();
    this.resultOrder = another.getResultOrder();
    this.resultSetFormat = another.getResultSetFormat();
    this.props = another.getProps();
    this.indexType = another.getIndexType();
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

  /** Reset sLimit and sOffset. */
  public void resetSLimitOffset() {
    this.seriesLimit = 0;
    this.seriesOffset = 0;
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

  public Map<String, Object> getProps() {
    return props;
  }

  public void addProp(String prop, Object value) {
    if (props == null) {
      props = new HashMap<>();
    }
    props.put(prop, value);
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

  public boolean isPrefixMatchPath() {
    return isPrefixMatchPath;
  }

  public void setPrefixMatchPath(boolean prefixMatchPath) {
    isPrefixMatchPath = prefixMatchPath;
  }

  public boolean isGroupByLevel() {
    return false;
  };

  public boolean isAlignByDevice() {
    return resultSetFormat == ResultSetFormat.ALIGN_BY_DEVICE;
  }

  public boolean DisableAlign() {
    return resultSetFormat != ResultSetFormat.DISABLE_ALIGN;
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return selectComponent.isHasTimeSeriesGeneratingFunction();
  }

  public boolean hasUserDefinedAggregationFunction() {
    return selectComponent.isHasUserDefinedAggregationFunction();
  }

  /** semantic check */
  public void selfCheck() {
    if (isAlignByDevice()) {
      if (hasTimeSeriesGeneratingFunction() || hasUserDefinedAggregationFunction()) {
        throw new SemanticException("The ALIGN BY DEVICE clause is not supported in UDF queries.");
      }

      for (PartialPath path : selectComponent.getPaths()) {
        if (path.getNodes().length > 1) {
          throw new SemanticException(
              "The paths of the SELECT clause can only be measurements or wildcard.");
        }
      }
    }
  }

  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
