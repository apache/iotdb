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

package org.apache.iotdb.db.sql.statement;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.sql.constant.StatementType;
import org.apache.iotdb.db.sql.statement.component.FromComponent;
import org.apache.iotdb.db.sql.statement.component.OrderBy;
import org.apache.iotdb.db.sql.statement.component.ResultSetFormat;
import org.apache.iotdb.db.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.sql.statement.component.WithoutPolicy;

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

  // row limit and offset for result set. The default value is -1, which means no limit
  protected int rowLimit = -1;
  // row offset for result set. The default value is 0
  protected int rowOffset = 0;

  // series limit and offset for result set. The default value is -1, which means no limit
  protected int seriesLimit = -1;
  // series offset for result set. The default value is 0
  protected int seriesOffset = 0;

  protected WithoutPolicy withoutPolicy;

  protected OrderBy resultOrder = OrderBy.TIMESTAMP_ASC;

  protected ResultSetFormat resultSetFormat = ResultSetFormat.ALIGN_BY_TIME;

  // used for TOP_N, LIKE, CONTAIN
  protected Map<String, Object> props;

  // TODO: add comments
  protected IndexType indexType;

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
    this.withoutPolicy = another.getWithoutPolicy();
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

  public WithoutPolicy getWithoutPolicy() {
    return withoutPolicy;
  }

  public void setWithoutPolicy(WithoutPolicy withoutPolicy) {
    this.withoutPolicy = withoutPolicy;
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

  public boolean isGroupByLevel() {
    return false;
  };

  public void check(){

  }
}
