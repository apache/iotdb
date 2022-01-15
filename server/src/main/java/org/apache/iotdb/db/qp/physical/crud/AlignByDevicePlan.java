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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlignByDevicePlan extends QueryPlan {

  public static final String MEASUREMENT_ERROR_MESSAGE =
      "The paths of the SELECT clause can only be measurements or STAR.";
  public static final String ALIAS_ERROR_MESSAGE =
      "alias %s can only be matched with one time series";
  public static final String DATATYPE_ERROR_MESSAGE =
      "The data types of the same measurement column should be the same across devices.";

  // to record result measurement columns, e.g. temperature, status, speed
  private List<String> measurements;
  private Map<String, MeasurementInfo> measurementInfoMap;
  private List<PartialPath> deduplicatePaths = new ArrayList<>();
  private List<String> aggregations;

  // paths index of each device that need to execute
  private Map<String, List<Integer>> deviceToPathIndex = new LinkedHashMap<>();
  private Map<String, IExpression> deviceToFilterMap;

  private GroupByTimePlan groupByTimePlan;
  private GroupByTimeFillPlan groupByFillPlan;
  private FillQueryPlan fillQueryPlan;
  private AggregationPlan aggregationPlan;

  public AlignByDevicePlan() {
    super();
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) {
    Set<String> pathWithAggregationSet = new LinkedHashSet<>();
    List<String> deduplicatedAggregations = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      String aggregation = aggregations != null ? aggregations.get(i) : null;
      String pathStrWithAggregation = getPathStrWithAggregation(path, aggregation);
      if (!pathWithAggregationSet.contains(pathStrWithAggregation)) {
        pathWithAggregationSet.add(pathStrWithAggregation);
        deduplicatePaths.add(path);
        if (this.aggregations != null) {
          deduplicatedAggregations.add(this.aggregations.get(i));
        }
        deviceToPathIndex
            .computeIfAbsent(path.getDevice(), k -> new ArrayList<>())
            .add(deduplicatePaths.size() - 1);
      }
    }
    setAggregations(deduplicatedAggregations);
    this.paths = null;
  }

  public List<PartialPath> getDeduplicatePaths() {
    return deduplicatePaths;
  }

  public void removeDevice(String device) {
    deviceToPathIndex.remove(device);
  }

  public void setMeasurementInfoMap(Map<String, MeasurementInfo> measurementInfoMap) {
    this.measurementInfoMap = measurementInfoMap;
  }

  public Map<String, MeasurementInfo> getMeasurementInfoMap() {
    return measurementInfoMap;
  }

  @Override
  public TSExecuteStatementResp getTSExecuteStatementResp(boolean isJdbcQuery) {
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);

    List<String> respColumns = new ArrayList<>();
    List<String> columnTypes = new ArrayList<>();

    // the DEVICE column of ALIGN_BY_DEVICE result
    respColumns.add(SQLConstant.ALIGNBY_DEVICE_COLUMN_NAME);
    columnTypes.add(TSDataType.TEXT.toString());

    Set<String> deduplicatedMeasurements = new LinkedHashSet<>();
    // build column header with constant and non exist column and deduplication
    for (String measurement : measurements) {
      MeasurementInfo measurementInfo = measurementInfoMap.get(measurement);
      TSDataType type = TSDataType.TEXT;
      String measurementName = measurement;
      if (measurementInfo != null) {
        type = measurementInfo.getColumnDataType();
        measurementName = measurementInfo.getMeasurementAlias();
      }
      respColumns.add(measurementName != null ? measurementName : measurement);
      columnTypes.add(type.toString());

      deduplicatedMeasurements.add(measurement);
    }

    // save deduplicated measurements in AlignByDevicePlan for AlignByDeviceDataSet to use.
    measurements = new ArrayList<>(deduplicatedMeasurements);
    resp.setColumns(respColumns);
    resp.setDataTypeList(columnTypes);
    if (getOperatorType() == OperatorType.AGGREGATION) {
      resp.setIgnoreTimeStamp(true);
    }
    return resp;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  @Override
  public List<String> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations.isEmpty() ? null : aggregations;
  }

  public Map<String, List<Integer>> getDeviceToPathIndex() {
    return deviceToPathIndex;
  }

  public void setDeviceToPathIndex(Map<String, List<Integer>> deviceToPathIndex) {
    this.deviceToPathIndex = deviceToPathIndex;
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

  public GroupByTimeFillPlan getGroupByFillPlan() {
    return groupByFillPlan;
  }

  public void setGroupByFillPlan(GroupByTimeFillPlan groupByFillPlan) {
    this.groupByFillPlan = groupByFillPlan;
    this.setOperatorType(OperatorType.GROUP_BY_FILL);
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

  private String getPathStrWithAggregation(PartialPath path, String aggregation) {
    String initialPath = path.getFullPath();
    if (aggregation != null) {
      initialPath = aggregation + "(" + initialPath + ")";
    }
    return initialPath;
  }
}
