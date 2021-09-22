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

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.List;
import java.util.Map;

public class AlignByDevicePlan extends QueryPlan {

  public static final String MEASUREMENT_ERROR_MESSAGE =
      "The paths of the SELECT clause can only be measurements or STAR.";
  public static final String ALIAS_ERROR_MESSAGE =
      "alias %s can only be matched with one time series";

  // to record result measurement columns, e.g. temperature, status, speed
  private List<String> measurements;
  private Map<String, MeasurementInfo> measurementInfoMap;

  // to check data type consistency for the same name sensor of different devices
  private List<PartialPath> devices;
  private Map<String, IExpression> deviceToFilterMap;

  private GroupByTimePlan groupByTimePlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;

  public AlignByDevicePlan() {
    super();
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) {
    // do nothing
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

  public Map<String, IExpression> getDeviceToFilterMap() {
    return deviceToFilterMap;
  }

  public void setDeviceToFilterMap(Map<String, IExpression> deviceToFilterMap) {
    this.deviceToFilterMap = deviceToFilterMap;
  }

  public GroupByTimePlan getGroupByTimePlan() {
    return groupByTimePlan;
  }

  public void setGroupByTimePlan(GroupByTimePlan groupByTimePlan) {
    this.groupByTimePlan = groupByTimePlan;
    this.setOperatorType(OperatorType.GROUP_BY_TIME);
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

  public void setMeasurementInfoMap(Map<String, MeasurementInfo> measurementInfoMap) {
    this.measurementInfoMap = measurementInfoMap;
  }

  public Map<String, MeasurementInfo> getMeasurementInfoMap() {
    return measurementInfoMap;
  }

  /**
   * Exist: the measurements which don't belong to NonExist and Constant. NonExist: the measurements
   * that do not exist in any device, data type is considered as String. The value is considered as
   * null. Constant: the measurements that have quotation mark. e.g. "abc",'11'. The data type is
   * considered as String and the value is the measurement name.
   */
  public enum MeasurementType {
    Exist,
    NonExist,
    Constant
  }
}
