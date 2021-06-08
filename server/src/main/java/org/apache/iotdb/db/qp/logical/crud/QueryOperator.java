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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryOperator extends Operator {

  protected SelectComponent selectComponent;
  protected FromComponent fromComponent;
  protected WhereComponent whereComponent;
  protected SpecialClauseComponent specialClauseComponent;

  protected Map<String, Object> props;
  protected IndexType indexType;

  public QueryOperator() {
    super(SQLConstant.TOK_QUERY);
    operatorType = Operator.OperatorType.QUERY;
  }

  public QueryOperator(QueryOperator queryOperator) {
    this();
    this.selectComponent = queryOperator.getSelectComponent();
    this.fromComponent = queryOperator.getFromComponent();
    this.whereComponent = queryOperator.getWhereComponent();
    this.specialClauseComponent = queryOperator.getSpecialClauseComponent();
    this.props = queryOperator.getProps();
    this.indexType = queryOperator.getIndexType();
  }

  public SelectComponent getSelectComponent() {
    return selectComponent;
  }

  public void setSelectComponent(SelectComponent selectComponent) {
    this.selectComponent = selectComponent;
  }

  public FromComponent getFromComponent() {
    return fromComponent;
  }

  public void setFromComponent(FromComponent fromComponent) {
    this.fromComponent = fromComponent;
  }

  public WhereComponent getWhereComponent() {
    return whereComponent;
  }

  public void setWhereComponent(WhereComponent whereComponent) {
    this.whereComponent = whereComponent;
  }

  public void setSpecialClauseComponent(SpecialClauseComponent specialClauseComponent) {
    this.specialClauseComponent = specialClauseComponent;
  }

  public SpecialClauseComponent getSpecialClauseComponent() {
    return specialClauseComponent;
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

  public boolean hasAggregationFunction() {
    return selectComponent.hasAggregationFunction();
  }

  public boolean hasTimeSeriesGeneratingFunction() {
    return selectComponent.hasTimeSeriesGeneratingFunction();
  }

  public boolean isAlignByDevice() {
    return specialClauseComponent != null && specialClauseComponent.isAlignByDevice();
  }

  public boolean isAlignByTime() {
    return specialClauseComponent == null || specialClauseComponent.isAlignByTime();
  }

  public boolean isGroupByLevel() {
    return specialClauseComponent != null && specialClauseComponent.getLevel() != -1;
  }

  public void check() throws LogicalOperatorException {
    if (isAlignByDevice()) {
      if (selectComponent.hasTimeSeriesGeneratingFunction()) {
        throw new LogicalOperatorException(
            "ALIGN BY DEVICE clause is not supported in UDF queries.");
      }

      for (PartialPath path : selectComponent.getPaths()) {
        String device = path.getDevice();
        if (!device.isEmpty()) {
          throw new LogicalOperatorException(
              "The paths of the SELECT clause can only be single level. In other words, "
                  + "the paths of the SELECT clause can only be measurements or STAR, without DOT."
                  + " For more details please refer to the SQL document.");
        }
      }
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    QueryPlan queryPlan = indexType == null ? new RawDataQueryPlan() : new QueryIndexPlan();
    return isAlignByDevice()
        ? this.generateAlignByDevicePlan(generator)
        : this.generateRawDataQueryPlan(generator, queryPlan);
  }

  protected QueryPlan generateRawDataQueryPlan(PhysicalGenerator generator, QueryPlan queryPlan)
      throws QueryProcessException {
    RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;
    rawDataQueryPlan.setPaths(selectComponent.getPaths());
    rawDataQueryPlan.setResultColumns(selectComponent.getResultColumns());

    // transform filter operator to expression
    if (whereComponent != null) {
      FilterOperator filterOperator = whereComponent.getFilterOperator();
      List<PartialPath> filterPaths = new ArrayList<>(filterOperator.getPathSet());
      try {
        List<TSDataType> seriesTypes = generator.getSeriesTypes(filterPaths);
        HashMap<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPaths.size(); i++) {
          rawDataQueryPlan.addFilterPathInDeviceToMeasurements(filterPaths.get(i));
          pathTSDataTypeHashMap.put(filterPaths.get(i), seriesTypes.get(i));
        }
        IExpression expression = filterOperator.transformToExpression(pathTSDataTypeHashMap);
        rawDataQueryPlan.setExpression(expression);
      } catch (MetadataException e) {
        throw new LogicalOptimizeException(e.getMessage());
      }
    }

    if (queryPlan instanceof QueryIndexPlan) {
      ((QueryIndexPlan) queryPlan).setIndexType(indexType);
      ((QueryIndexPlan) queryPlan).setProps(props);
      return queryPlan;
    }

    try {
      rawDataQueryPlan.setDataTypes(generator.getSeriesTypes(selectComponent.getPaths()));
      queryPlan.deduplicate(generator);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    convertSpecialClauseValues(rawDataQueryPlan);

    return rawDataQueryPlan;
  }

  protected AlignByDevicePlan generateAlignByDevicePlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AlignByDevicePlan alignByDevicePlan = new AlignByDevicePlan();

    List<PartialPath> prefixPaths = fromComponent.getPrefixPaths();
    // remove stars in fromPaths and get deviceId with deduplication
    List<PartialPath> devices = removeStarsInDeviceWithUnique(prefixPaths);
    List<ResultColumn> resultColumns = selectComponent.getResultColumns();
    List<String> originAggregations = selectComponent.getAggregationFunctions();

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

    for (int i = 0; i < resultColumns.size(); i++) { // per suffix in SELECT
      ResultColumn resultColumn = resultColumns.get(i);
      Expression suffixExpression = resultColumn.getExpression();
      PartialPath suffixPath =
          suffixExpression instanceof TimeSeriesOperand
              ? ((TimeSeriesOperand) suffixExpression).getPath()
              : (((FunctionExpression) suffixExpression).getPaths().get(0));

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
          List<PartialPath> actualPaths = getMatchedTimeseries(fullPath);
          if (resultColumn.hasAlias()) {
            if (actualPaths.size() == 1) {
              String columnName = actualPaths.get(0).getMeasurement();
              if (originAggregations != null && !originAggregations.isEmpty()) {
                measurementAliasMap.put(
                    originAggregations.get(i) + "(" + columnName + ")", resultColumn.getAlias());
              } else {
                measurementAliasMap.put(columnName, resultColumn.getAlias());
              }
            } else if (actualPaths.size() >= 2) {
              throw new QueryProcessException(
                  "alias '"
                      + resultColumn.getAlias()
                      + "' can only be matched with one time series");
            }
          }

          // for actual non exist path
          if (originAggregations != null && actualPaths.isEmpty() && originAggregations.isEmpty()) {
            String nonExistMeasurement = fullPath.getMeasurement();
            if (measurementSetOfGivenSuffix.add(nonExistMeasurement)
                && measurementTypeMap.get(nonExistMeasurement) != MeasurementType.Exist) {
              measurementTypeMap.put(fullPath.getMeasurement(), MeasurementType.NonExist);
            }
          }

          // Get data types with and without aggregate functions (actual time series) respectively
          // Data type with aggregation function `columnDataTypes` is used for:
          //  1. Data type consistency check 2. Header calculation, output result set
          // The actual data type of the time series `measurementDataTypes` is used for
          //  the actual query in the AlignByDeviceDataSet
          String aggregation =
              originAggregations != null && !originAggregations.isEmpty()
                  ? originAggregations.get(i)
                  : null;

          Pair<List<TSDataType>, List<TSDataType>> pair =
              generator.getSeriesTypes(actualPaths, aggregation);
          List<TSDataType> columnDataTypes = pair.left;
          List<TSDataType> measurementDataTypes = pair.right;
          for (int pathIdx = 0; pathIdx < actualPaths.size(); pathIdx++) {
            PartialPath path = new PartialPath(actualPaths.get(pathIdx).getNodes());

            // check datatype consistency
            // a example of inconsistency: select s0 from root.sg1.d1, root.sg1.d2 align by
            // device,
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

    convertSpecialClauseValues(alignByDevicePlan);
    // slimit trim on the measurementColumnList
    if (specialClauseComponent.hasSlimit()) {
      int seriesSlimit = specialClauseComponent.getSeriesLimit();
      int seriesOffset = specialClauseComponent.getSeriesOffset();
      measurements = slimitTrimColumn(measurements, seriesSlimit, seriesOffset);
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
    if (whereComponent != null) {
      alignByDevicePlan.setDeviceToFilterMap(
          concatFilterByDevice(generator, devices, whereComponent.getFilterOperator()));
    }

    return alignByDevicePlan;
  }

  private void convertSpecialClauseValues(QueryPlan queryPlan) {
    if (specialClauseComponent != null) {
      queryPlan.setWithoutAllNull(specialClauseComponent.isWithoutAllNull());
      queryPlan.setWithoutAnyNull(specialClauseComponent.isWithoutAnyNull());
      queryPlan.setRowLimit(specialClauseComponent.getRowLimit());
      queryPlan.setRowOffset(specialClauseComponent.getRowOffset());
      queryPlan.setAscending(specialClauseComponent.isAscending());
      queryPlan.setAlignByTime(specialClauseComponent.isAlignByTime());
    }
  }

  private List<PartialPath> removeStarsInDeviceWithUnique(List<PartialPath> paths)
      throws LogicalOptimizeException {
    List<PartialPath> retDevices;
    Set<PartialPath> deviceSet = new LinkedHashSet<>();
    try {
      for (PartialPath path : paths) {
        Set<PartialPath> tempDS = getMatchedDevices(path);
        deviceSet.addAll(tempDS);
      }
      retDevices = new ArrayList<>(deviceSet);
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return retDevices;
  }

  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit, int seriesOffset)
      throws QueryProcessException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      String errorMessage =
          "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.";
      throw new QueryProcessException(String.format(errorMessage, seriesOffset, size));
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }

  // e.g. translate "select * from root.ln.d1, root.ln.d2 where s1 < 20 AND s2 > 10" to
  // [root.ln.d1 -> root.ln.d1.s1 < 20 AND root.ln.d1.s2 > 10,
  //  root.ln.d2 -> root.ln.d2.s1 < 20 AND root.ln.d2.s2 > 10)]
  private Map<String, IExpression> concatFilterByDevice(
      PhysicalGenerator generator, List<PartialPath> devices, FilterOperator operator)
      throws QueryProcessException {
    Map<String, IExpression> deviceToFilterMap = new HashMap<>();
    Set<PartialPath> filterPaths = new HashSet<>();
    for (PartialPath device : devices) {
      FilterOperator newOperator = operator.copy();
      concatFilterPath(device, newOperator, filterPaths);
      // transform to a list so it can be indexed
      List<PartialPath> filterPathList = new ArrayList<>(filterPaths);
      try {
        List<TSDataType> seriesTypes = generator.getSeriesTypes(filterPathList);
        Map<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPathList.size(); i++) {
          pathTSDataTypeHashMap.put(filterPathList.get(i), seriesTypes.get(i));
        }
        deviceToFilterMap.put(
            device.getFullPath(), newOperator.transformToExpression(pathTSDataTypeHashMap));
        filterPaths.clear();
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }

    return deviceToFilterMap;
  }

  private void concatFilterPath(
      PartialPath prefix, FilterOperator operator, Set<PartialPath> filterPaths) {
    if (!operator.isLeaf()) {
      for (FilterOperator child : operator.getChildren()) {
        concatFilterPath(prefix, child, filterPaths);
      }
      return;
    }
    BasicFunctionOperator basicOperator = (BasicFunctionOperator) operator;
    PartialPath filterPath = basicOperator.getSinglePath();

    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath)
        || filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return;
    }

    PartialPath concatPath = prefix.concatPath(filterPath);
    filterPaths.add(concatPath);
    basicOperator.setSinglePath(concatPath);
  }

  protected Set<PartialPath> getMatchedDevices(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getDevices(path);
  }

  protected List<PartialPath> getMatchedTimeseries(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPath(path);
  }
}
