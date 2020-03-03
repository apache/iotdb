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
  private Map<String, TSDataType> measurementDataTypeMap;
  private Map<String, IExpression> deviceToFilterMap;
  private Map<String, measurementType> measurementTypeMap;

  private GroupByPlan groupByPlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;

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

  public void setMeasurementDataTypeMap(
      Map<String, TSDataType> measurementDataTypeMap) {
    this.measurementDataTypeMap = measurementDataTypeMap;
  }

  public Map<String, TSDataType> getMeasurementDataTypeMap() {
    return measurementDataTypeMap;
  }

  public Map<String, IExpression> getDeviceToFilterMap() {
    return deviceToFilterMap;
  }

  public void setDeviceToFilterMap(Map<String, IExpression> deviceToFilterMap) {
    this.deviceToFilterMap = deviceToFilterMap;
  }

  public Map<String, measurementType> getMeasurementTypeMap() {
    return measurementTypeMap;
  }

  public void setMeasurementTypeMap(
      Map<String, measurementType> measurementTypeMap) {
    this.measurementTypeMap = measurementTypeMap;
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
  // Then, measurements is {a1, a2, a3, a4, a5};
  // notExistMeasurements = {b1, b2, b3}, and positionOfNotExistMeasurements is {2, 3, 8};
  // constMeasurements is {'c1', 'c2', 'c3'}, and positionOfConstMeasurements is {0, 4, 7}.
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

  /**
   * NonExist: the measurements that do not exist in any device, data type is considered as Boolean.
   * The value is considered as null.
   * Constant: the measurements that have quotation mark. e.g. "abc",'11'.
   * The data type is considered as String and the value is considered is the same with measurement name.
   */
  public enum measurementType {
    Normal, NonExist, Constant;
  }
}
