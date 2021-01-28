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
package org.apache.iotdb.db.qp.logical.crud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 */
public class QueryOperator extends SFWOperator {

  private long startTime;
  private long endTime;

  private int rowLimit = 0;
  private int rowOffset = 0;
  private int seriesLimit = 0;
  private int seriesOffset = 0;

  private boolean isAlignByDevice = false;
  private boolean isAlignByTime = true;

  private boolean isIntervalByMonth = false;
  private boolean isSlidingStepByMonth = false;
  private boolean ascending = true;

  private Map<String, Object> props;

  private IndexType indexType;

  public QueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = Operator.OperatorType.QUERY;
  }

  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize,
      PhysicalGenerator generator) throws QueryProcessException {
    if (hasUdf() && hasAggregation()) {
      throw new QueryProcessException(
          "User-defined and built-in hybrid aggregation is not supported.");
    }
    QueryPlan queryPlan;
    if (isLastQuery()) {
      // Last query result set will not be affected by alignment
      if (!isAlignByTime()) {
        throw new QueryProcessException("Disable align cannot be applied to LAST query.");
      }
      queryPlan = new LastQueryPlan();
    } else if (getIndexType() != null) {
      queryPlan = new QueryIndexPlan();
      ((QueryIndexPlan) queryPlan).setIndexType(getIndexType());
      ((QueryIndexPlan) queryPlan).setProps(getProps());
    } else if (hasUdf()) {
      queryPlan = new UDTFPlan(getSelectOperator().getZoneId());
      ((UDTFPlan) queryPlan).constructUdfExecutors(getSelectOperator().getUdfList());
    } else if (hasAggregation()) {
      queryPlan = new AggregationPlan();
      setPlanValues((AggregationPlan) queryPlan);
    } else {
      queryPlan = new RawDataQueryPlan();
    }

    return doOptimization(queryPlan, generator, fetchSize);
  }

  protected void setPlanValues(AggregationPlan plan) {
    plan.setAggregations(getSelectOperator().getAggregations());
  }

  protected PhysicalPlan doOptimization(QueryPlan queryPlan, PhysicalGenerator generator,
      int fetchSize) throws QueryProcessException {

    queryPlan.setRowLimit(getRowLimit());
    queryPlan.setRowOffset(getRowOffset());
    queryPlan.setAscending(isAscending());

    if (!(queryPlan instanceof RawDataQueryPlan)) {
      return queryPlan;
    }

    RawDataQueryPlan plan = (RawDataQueryPlan) queryPlan;

    if (!isAlignByDevice()) {

      plan.setPaths(getSelectedPaths());
      // Last query result set will not be affected by alignment
      if (plan instanceof LastQueryPlan && !isAlignByTime()) {
        throw new QueryProcessException("Disable align cannot be applied to LAST query.");
      }
      plan.setAlignByTime(isAlignByTime());

      // transform filter operator to expression
      FilterOperator filterOperator = getFilterOperator();

      if (filterOperator != null) {
        List<PartialPath> filterPaths = new ArrayList<>(filterOperator.getPathSet());
        try {
          List<TSDataType> seriesTypes = generator.getSeriesTypes(filterPaths);
          HashMap<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
          for (int i = 0; i < filterPaths.size(); i++) {
            plan.addFilterPathInDeviceToMeasurements(filterPaths.get(i));
            pathTSDataTypeHashMap.put(filterPaths.get(i), seriesTypes.get(i));
          }
          IExpression expression = filterOperator.transformToExpression(pathTSDataTypeHashMap);
          plan.setExpression(expression);
        } catch (MetadataException e) {
          throw new LogicalOptimizeException(e.getMessage());
        }
      }

      if (!(plan instanceof QueryIndexPlan)) {
        try {
          generator.deduplicate(plan, fetchSize);
        } catch (MetadataException e) {
          throw new QueryProcessException(e);
        }
      }
      return plan;
    }

    AlignByDevicePlan alignByDevicePlan = new AlignByDevicePlan();
    alignByDevicePlan.setPhysicPlan(plan);

    List<PartialPath> prefixPaths = getFromOperator().getPrefixPaths();
    // remove stars in fromPaths and get deviceId with deduplication
    List<PartialPath> devices = generator.removeStarsInDeviceWithUnique(prefixPaths);
    List<PartialPath> suffixPaths = getSelectOperator().getSuffixPaths();
    List<String> originAggregations = getSelectOperator().getAggregations();

    // to record result measurement columns
    List<String> measurements = new ArrayList<>();
    Map<String, String> measurementAliasMap = new HashMap<>();
    // to check the same measurement of different devices having the same datatype
    // record the data type of each column of result set
    Map<String, TSDataType> columnDataTypeMap = new HashMap<>();
    Map<String, MeasurementType> measurementTypeMap = new HashMap<>();

    // to record the real type of the corresponding measurement
    Map<String, TSDataType> measurementDataTypeMap = new HashMap<>();
    List<PartialPath> paths = new ArrayList<>();

    for (int i = 0; i < suffixPaths.size(); i++) { // per suffix in SELECT
      PartialPath suffixPath = suffixPaths.get(i);

      // to record measurements in the loop of a suffix path
      Set<String> measurementSetOfGivenSuffix = new LinkedHashSet<>();

      // if const measurement
      if (suffixPath.getMeasurement().startsWith("'")) {
        measurements.add(suffixPath.getMeasurement());
        measurementTypeMap.put(suffixPath.getMeasurement(), MeasurementType.Constant);
        continue;
      }

      for (PartialPath device : devices) { // per device in FROM after deduplication

        PartialPath fullPath = device.concatPath(suffixPath);
        try {
          // remove stars in SELECT to get actual paths
          List<PartialPath> actualPaths = generator.getMatchedTimeseries(fullPath);
          if (suffixPath.isTsAliasExists()) {
            if (actualPaths.size() == 1) {
              String columnName = actualPaths.get(0).getMeasurement();
              if (originAggregations != null && !originAggregations.isEmpty()) {
                measurementAliasMap.put(originAggregations.get(i) + "(" + columnName + ")",
                    suffixPath.getTsAlias());
              } else {
                measurementAliasMap.put(columnName, suffixPath.getTsAlias());
              }
            } else if (actualPaths.size() >= 2) {
              throw new QueryProcessException("alias '" + suffixPath.getTsAlias()
                  + "' can only be matched with one time series");
            }
          }

          // for actual non exist path
          if (originAggregations != null && actualPaths.isEmpty() && originAggregations
              .isEmpty()) {
            String nonExistMeasurement = fullPath.getMeasurement();
            if (measurementSetOfGivenSuffix.add(nonExistMeasurement)
                && measurementTypeMap.get(nonExistMeasurement) != MeasurementType.Exist) {
              measurementTypeMap
                  .put(fullPath.getMeasurement(), MeasurementType.NonExist);
            }
          }

          // Get data types with and without aggregate functions (actual time series) respectively
          // Data type with aggregation function `columnDataTypes` is used for:
          //  1. Data type consistency check 2. Header calculation, output result set
          // The actual data type of the time series `measurementDataTypes` is used for
          //  the actual query in the AlignByDeviceDataSet
          String aggregation =
              originAggregations != null && !originAggregations.isEmpty()
                  ? originAggregations.get(i) : null;

          Pair<List<TSDataType>, List<TSDataType>> pair = generator.getSeriesTypes(actualPaths,
              aggregation);
          List<TSDataType> columnDataTypes = pair.left;
          List<TSDataType> measurementDataTypes = pair.right;
          for (int pathIdx = 0; pathIdx < actualPaths.size(); pathIdx++) {
            PartialPath path = new PartialPath(actualPaths.get(pathIdx).getNodes());

            // check datatype consistency
            // a example of inconsistency: select s0 from root.sg1.d1, root.sg1.d2 align by device,
            // while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
            String measurementChecked;
            if (originAggregations != null && !originAggregations.isEmpty()) {
              measurementChecked = originAggregations.get(i) + "(" + path.getMeasurement() + ")";
            } else {
              measurementChecked = path.getMeasurement();
            }
            TSDataType columnDataType = columnDataTypes.get(pathIdx);
            if (columnDataTypeMap.containsKey(measurementChecked)) {
              if (!columnDataType.equals(columnDataTypeMap.get(measurementChecked))) {
                throw new QueryProcessException(
                    "The data types of the same measurement column should be the same across "
                        + "devices in ALIGN_BY_DEVICE sql. For more details please refer to the "
                        + "SQL document.");
              }
            } else {
              columnDataTypeMap.put(measurementChecked, columnDataType);
              measurementDataTypeMap.put(measurementChecked, measurementDataTypes.get(pathIdx));
            }

            // This step indicates that the measurement exists under the device and is correct,
            // First, update measurementSetOfGivenSuffix which is distinct
            // Then if this measurement is recognized as NonExist before，update it to Exist
            if (measurementSetOfGivenSuffix.add(measurementChecked)
                || measurementTypeMap.get(measurementChecked) != MeasurementType.Exist) {
              measurementTypeMap.put(measurementChecked, MeasurementType.Exist);
            }

            // update paths
            paths.add(path);
          }

        } catch (MetadataException | QueryProcessException e) {
          throw new LogicalOptimizeException(
              String.format(
                  "Error when getting all paths of a full path: %s", fullPath.getFullPath())
                  + e.getMessage());
        }
      }

      // update measurements
      // Note that in the loop of a suffix path, set is used.
      // And across the loops of suffix paths, list is used.
      // e.g. select *,s1 from root.sg.d0, root.sg.d1
      // for suffix *, measurementSetOfGivenSuffix = {s1,s2,s3}
      // for suffix s1, measurementSetOfGivenSuffix = {s1}
      // therefore the final measurements is [s1,s2,s3,s1].
      measurements.addAll(measurementSetOfGivenSuffix);
    }

    // slimit trim on the measurementColumnList
    if (hasSlimit()) {
      int seriesSlimit = getSeriesLimit();
      int seriesOffset = getSeriesOffset();
      measurements = generator.slimitTrimColumn(measurements, seriesSlimit, seriesOffset);
    }

    int maxDeduplicatedPathNum = QueryResourceManager.getInstance()
        .getMaxDeduplicatedPathNum(fetchSize);

    if (measurements.size() > maxDeduplicatedPathNum) {
      throw new PathNumOverLimitException(maxDeduplicatedPathNum, measurements.size());
    }

    // assigns to alignByDevicePlan
    alignByDevicePlan.setMeasurements(measurements);
    alignByDevicePlan.setMeasurementAliasMap(measurementAliasMap);
    alignByDevicePlan.setDevices(devices);
    alignByDevicePlan.setColumnDataTypeMap(columnDataTypeMap);
    alignByDevicePlan.setMeasurementTypeMap(measurementTypeMap);
    alignByDevicePlan.setMeasurementDataTypeMap(measurementDataTypeMap);
    alignByDevicePlan.setPaths(paths);

    // get deviceToFilterMap
    FilterOperator filterOperator = getFilterOperator();
    if (filterOperator != null) {
      alignByDevicePlan
          .setDeviceToFilterMap(generator.concatFilterByDevice(devices, filterOperator));
    }

    alignByDevicePlan.setRowLimit(getRowLimit());
    alignByDevicePlan.setRowOffset(getRowOffset());
    alignByDevicePlan.setAscending(isAscending());

    return alignByDevicePlan;
  }

  public Map<String, Object> getProps() {
    return props;
  }

  public void setProps(Map<String, Object> props) {
    this.props = props;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
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

  public int getSeriesLimit() {
    return seriesLimit;
  }

  public void setSeriesLimit(int seriesLimit) {
    this.seriesLimit = seriesLimit;
  }

  public int getSeriesOffset() {
    return seriesOffset;
  }

  public void setSeriesOffset(int seriesOffset) {
    this.seriesOffset = seriesOffset;
  }

  public boolean hasSlimit() {
    return seriesLimit > 0;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public boolean isAlignByDevice() {
    return isAlignByDevice;
  }

  public void setAlignByDevice(boolean alignByDevice) {
    isAlignByDevice = alignByDevice;
  }

  public boolean isAlignByTime() {
    return isAlignByTime;
  }

  public void setAlignByTime(boolean alignByTime) {
    isAlignByTime = alignByTime;
  }

  public void setSlidingStepByMonth(boolean slidingStepByMonth) {
    isSlidingStepByMonth = slidingStepByMonth;
  }

  public boolean isSlidingStepByMonth() {
    return isSlidingStepByMonth;
  }

  public boolean isIntervalByMonth() {
    return isIntervalByMonth;
  }

  public void setIntervalByMonth(boolean intervalByMonth) {
    isIntervalByMonth = intervalByMonth;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }
}
