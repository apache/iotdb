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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class QueryPlan extends PhysicalPlan {

  protected List<ResultColumn> resultColumns = null;
  protected List<PartialPath> paths = null;
  protected List<TSDataType> dataTypes = null;
  private boolean alignByTime = true; // for disable align sql

  private int rowLimit = 0;
  private int rowOffset = 0;

  private boolean ascending = true;

  private Map<String, Integer> pathToIndex = new HashMap<>();

  private boolean enableRedirect = false;
  private boolean enableTracing = false;

  // if true, we don't need the row whose any column is null
  private boolean withoutAnyNull;

  // if true, we don't need the row whose all columns are null
  private boolean withoutAllNull;

  public QueryPlan() {
    super(true);
    setOperatorType(Operator.OperatorType.QUERY);
  }

  public QueryPlan(boolean isQuery, Operator.OperatorType operatorType) {
    super(isQuery, operatorType);
  }

  public abstract void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException;

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
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

  public boolean hasLimit() {
    return rowLimit > 0;
  }

  public boolean isAlignByTime() {
    return alignByTime;
  }

  public void setAlignByTime(boolean align) {
    alignByTime = align;
  }

  public void setColumnNameToDatasetOutputIndex(String columnName, Integer index) {
    pathToIndex.put(columnName, index);
  }

  public boolean isGroupByLevel() {
    return false;
  }

  public void setPathToIndex(Map<String, Integer> pathToIndex) {
    this.pathToIndex = pathToIndex;
  }

  public Map<String, Integer> getPathToIndex() {
    return pathToIndex;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public String getColumnForReaderFromPath(PartialPath path, int pathIndex) {
    ResultColumn resultColumn = resultColumns.get(pathIndex);
    return resultColumn.hasAlias() ? resultColumn.getAlias() : path.getExactFullPath();
  }

  public String getColumnForDisplay(String columnForReader, int pathIndex) {
    return resultColumns.get(pathIndex).getResultColumnName();
  }

  public boolean isEnableRedirect() {
    return enableRedirect;
  }

  public void setEnableRedirect(boolean enableRedirect) {
    this.enableRedirect = enableRedirect;
  }

  public boolean isEnableTracing() {
    return enableTracing;
  }

  public void setEnableTracing(boolean enableTracing) {
    this.enableTracing = enableTracing;
  }

  public List<ResultColumn> getResultColumns() {
    return resultColumns;
  }

  public void setResultColumns(List<ResultColumn> resultColumns) {
    this.resultColumns = resultColumns;
  }

  public boolean isWithoutAnyNull() {
    return withoutAnyNull;
  }

  public void setWithoutAnyNull(boolean withoutAnyNull) {
    this.withoutAnyNull = withoutAnyNull;
  }

  public boolean isWithoutAllNull() {
    return withoutAllNull;
  }

  public void setWithoutAllNull(boolean withoutAllNull) {
    this.withoutAllNull = withoutAllNull;
  }
}
