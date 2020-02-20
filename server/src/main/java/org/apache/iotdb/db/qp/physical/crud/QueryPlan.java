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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

public class QueryPlan extends PhysicalPlan {

  private List<Path> paths = null;
  private List<TSDataType> dataTypes = null;

  private List<Path> deduplicatedPaths = new ArrayList<>();
  private List<TSDataType> deduplicatedDataTypes = new ArrayList<>();


  private IExpression expression = null;

  private int rowLimit = 0;
  private int rowOffset = 0;

  private boolean isAlign = true; // for disable align sql
  private boolean isGroupByDevice = false; // for group by device sql
  private List<String> measurements; // for group by device sql, e.g. temperature
  private Map<String, Set<String>> measurementsGroupByDevice; // for group by device sql, e.g. root.ln.d1 -> temperature
  private Map<String, TSDataType> dataTypeConsistencyChecker; // for group by device sql, e.g. root.ln.d1.temperature -> Float
  private Map<String, IExpression> deviceToFilterMap; // for group by device sql
  private Map<Path, TSDataType> dataTypeMapping = new HashMap<>(); // for group by device sql

  public List<String> getNotExistMeasurements() {
    return notExistMeasurements;
  }

  public void setNotExistMeasurements(List<String> notExistMeasurements) {
    this.notExistMeasurements = notExistMeasurements;
  }

  public List<Integer> getPositionOfNotExistMeasurements() {
    return positionOfNotExistMeasurements;
  }

  public void setPositionOfNotExistMeasurements(
      List<Integer> positionOfNotExistMeasurements) {
    this.positionOfNotExistMeasurements = positionOfNotExistMeasurements;
  }

  public List<String> getConstMeasurements() {
    return constMeasurements;
  }

  public void setConstMeasurements(List<String> constMeasurements) {
    this.constMeasurements = constMeasurements;
  }

  public List<Integer> getPositionOfConstMeasurements() {
    return positionOfConstMeasurements;
  }

  public void setPositionOfConstMeasurements(List<Integer> positionOfConstMeasurements) {
    this.positionOfConstMeasurements = positionOfConstMeasurements;
  }

  //the measurements that do not exist in any device,
  // data type is considered as Boolean. The value is considered as null
  private List<String> notExistMeasurements = new ArrayList<>();; // for group by device sql
  private List<Integer> positionOfNotExistMeasurements = new ArrayList<>(); // for group by device sql
  //the measurements that have quotation mark. e.g., "abc",
  // '11', the data type is considered as String and the value  is considered is the same with measurement name
  private List<String> constMeasurements = new ArrayList<>(); // for group by device sql
  private List<Integer> positionOfConstMeasurements = new ArrayList<>(); // for group by device sql

  //we use the following algorithm to reproduce the order of measurements that user writes.
  //suppose user writes SELECT 'c1',a1,b1,b2,'c2',a2,a3,'c3',b3,a4,a5 FROM ... where for each a_i
  // column there is at least one device having it, and for each b_i column there is no device
  // having it, and 'c_i' is a const column.
  // Then, measurements = {a1, a2, a3, a4, a5};
  // notExistMeasurements = {b1, b2, b3}, and positionOfNotExistMeasurements = {2, 3, 8};
  // constMeasurements = {'c1', 'c2', 'c3'}, and positionOfConstMeasurements = {0, 4, 7}.
  // When to reproduce the order of measurements. The pseudocode is:
  //<pre>
  // current = 0;
  // if (min(notExist, const) <= current) {
  //  pull min_element(notExist, const);
  // } else {
  //  pull from measurements;
  // }
  // current ++;
  //</pre>

  public void addNotExistMeasurement(int position, String measurement) {
    notExistMeasurements.add(measurement);
    positionOfNotExistMeasurements.add(position);
  }

  public void addConstMeasurement(int position, String measurement) {
    constMeasurements.add(measurement);
    positionOfConstMeasurements.add(position);
  }

  public QueryPlan() {
    super(true);
    setOperatorType(Operator.OperatorType.QUERY);
  }

  public QueryPlan(boolean isQuery, Operator.OperatorType operatorType) {
    super(isQuery, operatorType);
  }

  /**
   * Check if all paths exist.
   */
  public void checkPaths(IQueryProcessExecutor executor) throws QueryProcessException {
    for (Path path : paths) {
      if (!executor.judgePathExists(path)) {
        throw new QueryProcessException("Path doesn't exist: " + path);
      }
    }
  }

  public IExpression getExpression() {
    return expression;
  }

  public void setExpression(IExpression expression) {
    this.expression = expression;
  }

  @Override
  public List<Path> getPaths() {
    return paths;
  }

  public void setPaths(List<Path> paths) {
    this.paths = paths;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<Path> getDeduplicatedPaths() {
    return deduplicatedPaths;
  }

  public void addDeduplicatedPaths(Path path) {
    this.deduplicatedPaths.add(path);
  }

  public List<TSDataType> getDeduplicatedDataTypes() {
    return deduplicatedDataTypes;
  }

  public void addDeduplicatedDataTypes(TSDataType dataType) {
    this.deduplicatedDataTypes.add(dataType);
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

  public boolean isGroupByDevice() {
    return isGroupByDevice;
  }

  public void setGroupByDevice(boolean groupByDevice) {
    isGroupByDevice = groupByDevice;
  }
  
  public boolean isAlign() {
    return isAlign;
  }
  
  public void setAlign(boolean align) {
    isAlign = align;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurementsGroupByDevice(
      Map<String, Set<String>> measurementsGroupByDevice) {
    this.measurementsGroupByDevice = measurementsGroupByDevice;
  }

  public Map<String, Set<String>> getMeasurementsGroupByDevice() {
    return measurementsGroupByDevice;
  }

  public void setDataTypeConsistencyChecker(
      Map<String, TSDataType> dataTypeConsistencyChecker) {
    this.dataTypeConsistencyChecker = dataTypeConsistencyChecker;
  }

  public Map<String, TSDataType> getDataTypeConsistencyChecker() {
    return dataTypeConsistencyChecker;
  }

  public Map<Path, TSDataType> getDataTypeMapping() {
    return dataTypeMapping;
  }

  public void addTypeMapping(Path path, TSDataType dataType) {
    dataTypeMapping.put(path, dataType);
  }

  public void setDeduplicatedPaths(
      List<Path> deduplicatedPaths) {
    this.deduplicatedPaths = deduplicatedPaths;
  }

  public void setDeduplicatedDataTypes(
      List<TSDataType> deduplicatedDataTypes) {
    this.deduplicatedDataTypes = deduplicatedDataTypes;
  }

  public Map<String, IExpression> getDeviceToFilterMap() {
    return deviceToFilterMap;
  }

  public void setDeviceToFilterMap(Map<String, IExpression> deviceToFilterMap) {
    this.deviceToFilterMap = deviceToFilterMap;
  }

}
