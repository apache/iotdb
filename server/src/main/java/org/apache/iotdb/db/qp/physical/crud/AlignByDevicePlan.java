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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.SpecialClauseComponent;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.ArrayList;
import java.util.HashSet;
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

  // measurements to record result measurement columns, e.g. temperature, status, speed
  // no contains alias
  private List<String> measurements;

  // stores the valid column name that without null can specified
  // want to see details, please see the method `addValidWithoutNullColumn` and
  // `isValidWithoutNullColumn`
  private Set<String> withoutNullValidSet = new HashSet<>();
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

  public void calcWithoutNullColumnIndex(List<Expression> withoutNullColumns)
      throws QueryProcessException {
    // record specified without null columns, include alias
    Set<String> withoutNullColumnSet = new HashSet<>();
    for (Expression expression : withoutNullColumns) {
      if (!isValidWithoutNullColumn(expression.getExpressionString())) {
        throw new QueryProcessException(QueryPlan.WITHOUT_NULL_FILTER_ERROR_MESSAGE);
      }
      withoutNullColumnSet.add(expression.getExpressionString());
    }

    if (!withoutNullColumnSet.isEmpty()) {
      withoutNullColumnsIndex = new HashSet<>();
    }

    int index = 1; // start 1, because first is device name
    for (String measurement : this.measurements) {
      String actualColumn = measurement; // may be alias
      if (measurementInfoMap.containsKey(measurement)) {
        String alias = measurementInfoMap.get(measurement).getMeasurementAlias();
        if (alias != null && !alias.equals("")) {
          actualColumn = alias;
        }
      }
      if (withoutNullColumnSet.contains(actualColumn)) {
        withoutNullColumnsIndex.add(index);
      }
      index++;
    }
  }

  /**
   * add columnName that appears in output name in result set so `withoutNullValidSet` stores the
   * valid column name that without null can specified
   *
   * @param columnName output name in result set, may be alias
   */
  public void addValidWithoutNullColumn(String columnName) {
    withoutNullValidSet.add(columnName);
  }

  /**
   * check the columnName specified without null is valid
   *
   * @param columnName the columnName specified without null
   * @return valid: return true; invalid: return false
   */
  public boolean isValidWithoutNullColumn(String columnName) {
    return withoutNullValidSet.contains(columnName);
  }

  /** make withoutNullValidSet is null, friendly for gc */
  public void closeWithoutNullValidSet() {
    withoutNullValidSet.clear();
    withoutNullValidSet = null;
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) {
    Set<String> pathWithAggregationSet = new LinkedHashSet<>();
    List<String> deduplicatedAggregations = new ArrayList<>();
    HashSet<String> measurements = new HashSet<>(getMeasurements());

    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      String aggregation = aggregations != null ? aggregations.get(i) : null;
      String measurementWithAggregation = getMeasurementStrWithAggregation(path, aggregation);
      if (!measurements.contains(measurementWithAggregation)) {
        continue;
      }

      String pathStrWithAggregation = getPathStrWithAggregation(path, aggregation);
      if (!pathWithAggregationSet.contains(pathStrWithAggregation)) {
        pathWithAggregationSet.add(pathStrWithAggregation);
        deduplicatePaths.add(path);
        if (measurementInfoMap.containsKey(measurementWithAggregation)) {
          MeasurementInfo measurementInfo = measurementInfoMap.get(measurementWithAggregation);
          if (measurementInfo.getMeasurementAlias() != null
              && !measurementInfo.getMeasurementAlias().equals("")) {
            addValidWithoutNullColumn(measurementInfo.getMeasurementAlias());
          } else {
            addValidWithoutNullColumn(measurementWithAggregation);
          }
        } else {
          addValidWithoutNullColumn(measurementWithAggregation);
        }

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

  @Override
  public void convertSpecialClauseValues(SpecialClauseComponent specialClauseComponent)
      throws QueryProcessException {
    if (specialClauseComponent != null) {
      setWithoutAllNull(specialClauseComponent.isWithoutAllNull());
      setWithoutAnyNull(specialClauseComponent.isWithoutAnyNull());
      setRowLimit(specialClauseComponent.getRowLimit());
      setRowOffset(specialClauseComponent.getRowOffset());
      setAscending(specialClauseComponent.isAscending());
      setAlignByTime(specialClauseComponent.isAlignByTime());
    }
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

  private String getMeasurementStrWithAggregation(PartialPath path, String aggregation) {
    String measurement = path.getMeasurement();
    if (aggregation != null) {
      measurement = aggregation + "(" + measurement + ")";
    }
    return measurement;
  }

  private String getPathStrWithAggregation(PartialPath path, String aggregation) {
    String initialPath = path.getFullPath();
    if (aggregation != null) {
      initialPath = aggregation + "(" + initialPath + ")";
    }
    return initialPath;
  }
}
