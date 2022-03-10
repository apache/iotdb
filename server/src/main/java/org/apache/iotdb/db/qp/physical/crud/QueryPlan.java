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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class QueryPlan extends PhysicalPlan {

  protected List<PartialPath> paths = null;
  protected List<PartialPath> fromPaths = null;
  protected List<TSDataType> dataTypes = null;
  private boolean alignByTime = true; // for disable align sql

  private int rowLimit = 0;
  private int rowOffset = 0;

  private boolean ascending = true;

  private Map<String, Integer> pathToIndex = new HashMap<>();

  private boolean enableRedirect = false;

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

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  @Override
  public List<PartialPath> getFromPaths() {
    return fromPaths;
  }

  public void setFromPaths(List<PartialPath> fromPaths) {
    this.fromPaths = fromPaths;
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

  public void setAlignByTime(boolean align) throws QueryProcessException {
    alignByTime = align;
  }

  public void addPathToIndex(String columnName, Integer index) {
    pathToIndex.put(columnName, index);
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
    String columnForReader = path.isTsAliasExists() ? path.getTsAlias() : null;
    if (columnForReader == null) {
      columnForReader =
          path.isMeasurementAliasExists() ? path.getFullPathWithAlias() : path.toString();
    }
    return columnForReader;
  }

  public String getColumnForDisplay(String columnForReader, int pathIndex)
      throws IllegalPathException {
    return columnForReader;
  }

  public boolean isEnableRedirect() {
    return enableRedirect;
  }

  public void setEnableRedirect(boolean enableRedirect) {
    this.enableRedirect = enableRedirect;
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
