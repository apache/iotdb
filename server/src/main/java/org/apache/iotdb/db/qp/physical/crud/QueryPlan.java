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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class QueryPlan extends PhysicalPlan {

  protected List<PartialPath> paths = null;
  private List<TSDataType> dataTypes = null;
  private boolean alignByTime = true; // for disable align sql

  private int rowLimit = 0;
  private int rowOffset = 0;

  private boolean ascending = true;

  private Map<String, Integer> pathToIndex = new HashMap<>();

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
}
