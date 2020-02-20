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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;

public class AlignByDevicePlan extends QueryPlan {

  private List<String> measurements; // e.g. temperature, status, speed
  private Map<String, Set<String>> deviceToMeasurementsMap; // e.g. root.ln.d1 -> temperature
  // to check data type consistency for the same name sensor of different devices
  private Map<String, TSDataType> dataTypeConsistencyChecker;
  private Map<String, IExpression> deviceToFilterMap;

  private GroupByPlan groupByPlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;

  // the measurements that do not exist in any device,
  // data type is considered as Boolean. The value is considered as null
  private List<String> notExistMeasurements = new ArrayList<>();
  private List<Integer> positionOfNotExistMeasurements = new ArrayList<>();
  // the measurements that have quotation mark. e.g. "abc",
  // '11', the data type is considered as String and the value is considered is the same with measurement name
  private List<String> constMeasurements = new ArrayList<>();
  private List<Integer> positionOfConstMeasurements = new ArrayList<>();

  public AlignByDevicePlan() {
    super();
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setDeviceToMeasurementsMap(
      Map<String, Set<String>> deviceToMeasurementsMap) {
    this.deviceToMeasurementsMap = deviceToMeasurementsMap;
  }

  public Map<String, Set<String>> getDeviceToMeasurementsMap() {
    return deviceToMeasurementsMap;
  }

  public void setDataTypeConsistencyChecker(
      Map<String, TSDataType> dataTypeConsistencyChecker) {
    this.dataTypeConsistencyChecker = dataTypeConsistencyChecker;
  }

  public Map<String, TSDataType> getDataTypeConsistencyChecker() {
    return dataTypeConsistencyChecker;
  }

  public Map<String, IExpression> getDeviceToFilterMap() {
    return deviceToFilterMap;
  }

  public void setDeviceToFilterMap(Map<String, IExpression> deviceToFilterMap) {
    this.deviceToFilterMap = deviceToFilterMap;
  }

  public GroupByPlan getGroupByPlan() {
    return groupByPlan;
  }

  public void setGroupByPlan(GroupByPlan groupByPlan) {
    this.groupByPlan = groupByPlan;
    this.setOperatorType(OperatorType.GROUPBY);
  }

  public FillQueryPlan getFillQueryPlan() {
    return fillQueryPlan;
  }

  public void setFillQueryPlan(FillQueryPlan fillQueryPlan) {
    this.fillQueryPlan = fillQueryPlan;
    this.setOperatorType(OperatorType.FILL);
  }

  public AggregationPlan getAggregationPlan() {
    return aggregationPlan;
  }

  public void setAggregationPlan(AggregationPlan aggregationPlan) {
    this.aggregationPlan = aggregationPlan;
    this.setOperatorType(Operator.OperatorType.AGGREGATION);
  }

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
}
