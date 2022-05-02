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
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultSetFormat;
import org.apache.iotdb.db.mpp.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.MeasurementInfo;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

  public QueryStatement() {
    this.statementType = StatementType.QUERY;
  }

  @Override
  public List<PartialPath> getPaths() {
    return fromComponent.getPrefixPaths();
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

  public boolean isGroupByLevel() {
    return false;
  }

  public boolean isAlignByDevice() {
    return resultSetFormat == ResultSetFormat.ALIGN_BY_DEVICE;
  }

  public boolean disableAlign() {
    return resultSetFormat == ResultSetFormat.DISABLE_ALIGN;
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return selectComponent.isHasTimeSeriesGeneratingFunction();
  }

  public boolean hasUserDefinedAggregationFunction() {
    return selectComponent.isHasUserDefinedAggregationFunction();
  }

  public Map<String, Set<PartialPath>> getDeviceNameToDeduplicatedPathsMap() {
    Map<String, Set<PartialPath>> deviceNameToDeduplicatedPathsMap =
        new HashMap<>(getSelectComponent().getDeviceNameToDeduplicatedPathsMap());
    if (getWhereCondition() != null) {
      for (PartialPath path :
          getWhereCondition().getQueryFilter().getPathSet().stream()
              .filter(SQLConstant::isNotReservedPath)
              .collect(Collectors.toList())) {
        deviceNameToDeduplicatedPathsMap
            .computeIfAbsent(path.getDeviceIdString(), k -> new HashSet<>())
            .add(path);
      }
    }
    return deviceNameToDeduplicatedPathsMap;
  }

  public List<String> getSelectedPathNames() {
    Set<String> pathSet = new HashSet<>();
    for (ResultColumn resultColumn : getSelectComponent().getResultColumns()) {
      pathSet.addAll(
          resultColumn.collectPaths().stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toList()));
    }
    return new ArrayList<>(pathSet);
  }

  public DatasetHeader constructDatasetHeader() {
    List<ColumnHeader> columnHeaders = new ArrayList<>();
    if (this.isAlignByDevice()) {
      // add DEVICE column
      columnHeaders.add(new ColumnHeader(HeaderConstant.COLUMN_DEVICE, TSDataType.TEXT, null));

      // TODO: consider ALIGN BY DEVICE
    } else {
      columnHeaders.addAll(
          this.getSelectComponent().getResultColumns().stream()
              .map(ResultColumn::constructColumnHeader)
              .collect(Collectors.toList()));
    }
    return new DatasetHeader(columnHeaders, false);
  }

  /**
   * If path is a vectorPartialPath, we return its measurementId + subMeasurement as the final
   * measurement. e.g. path: root.sg.d1.vector1[s1], return "vector1.s1".
   */
  private String getMeasurementName(PartialPath path, String aggregation) {
    String initialMeasurement = path.getMeasurement();
    if (aggregation != null) {
      initialMeasurement = aggregation + "(" + initialMeasurement + ")";
    }
    return initialMeasurement;
  }

  private PartialPath getPathFromExpression(Expression expression) {
    return expression instanceof TimeSeriesOperand
        ? ((TimeSeriesOperand) expression).getPath()
        : (((FunctionExpression) expression).getPaths().get(0));
  }

  private String getAggregationFromExpression(Expression expression) {
    return expression.isBuiltInAggregationFunctionExpression()
        ? ((FunctionExpression) expression).getFunctionName()
        : null;
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

  /**
   * Check datatype consistency in ALIGN BY DEVICE.
   *
   * <p>an inconsistent example: select s0 from root.sg1.d1, root.sg1.d2 align by device, return
   * false while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
   */
  private boolean checkDataTypeConsistency(
      TSDataType checkedDataType, MeasurementInfo measurementInfo) {
    return measurementInfo == null || checkedDataType.equals(measurementInfo.getColumnDataType());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
