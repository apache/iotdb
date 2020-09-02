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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;

public class AlignByDevicePlan extends QueryPlan {

  private List<String> measurements; // to record result measurement columns, e.g. temperature, status, speed
  // to check data type consistency for the same name sensor of different devices
  private List<PartialPath> devices;
  // to record the datatype of the column in the result set
  private Map<String, TSDataType> columnDataTypeMap;
  private Map<String, IExpression> deviceToFilterMap;
  // to record different kinds of measurement
  private Map<String, MeasurementType> measurementTypeMap;

  // to record the real type of the measurement
  private Map<String, TSDataType> measurementDataTypeMap;

  private GroupByTimePlan groupByTimePlan;

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

  public void setDevices(List<PartialPath> devices) {
    this.devices = devices;
  }

  public List<PartialPath> getDevices() {
    return devices;
  }

  public void setColumnDataTypeMap(
      Map<String, TSDataType> columnDataTypeMap) {
    this.columnDataTypeMap = columnDataTypeMap;
  }

  public Map<String, TSDataType> getColumnDataTypeMap() {
    return columnDataTypeMap;
  }

  public Map<String, IExpression> getDeviceToFilterMap() {
    return deviceToFilterMap;
  }

  public void setDeviceToFilterMap(Map<String, IExpression> deviceToFilterMap) {
    this.deviceToFilterMap = deviceToFilterMap;
  }

  public Map<String, MeasurementType> getMeasurementTypeMap() {
    return measurementTypeMap;
  }

  public void setMeasurementTypeMap(
      Map<String, MeasurementType> measurementTypeMap) {
    this.measurementTypeMap = measurementTypeMap;
  }

  public Map<String, TSDataType> getMeasurementDataTypeMap() {
    return measurementDataTypeMap;
  }

  public void setMeasurementDataTypeMap(Map<String, TSDataType> measurementDataTypeMap) {
    this.measurementDataTypeMap = measurementDataTypeMap;
  }


  public GroupByTimePlan getGroupByTimePlan() {
    return groupByTimePlan;
  }

  public void setGroupByTimePlan(GroupByTimePlan groupByTimePlan) {
    this.groupByTimePlan = groupByTimePlan;
    this.setOperatorType(OperatorType.GROUPBYTIME);
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

  /**
   * Exist: the measurements which don't belong to NonExist and Constant.
   * NonExist: the measurements that do not exist in any device, data type is considered as String.
   * The value is considered as null.
   * Constant: the measurements that have quotation mark. e.g. "abc",'11'.
   * The data type is considered as String and the value is the measurement name.
   */
  public enum MeasurementType {
    Exist, NonExist, Constant;
  }
}
