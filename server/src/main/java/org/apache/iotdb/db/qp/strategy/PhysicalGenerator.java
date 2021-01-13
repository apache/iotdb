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
package org.apache.iotdb.db.qp.strategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Used to convert logical operator to physical plan
 */
public class PhysicalGenerator {

  public PhysicalPlan transformToPhysicalPlan(Operator operator, int fetchSize)
      throws QueryProcessException {
    if (!(operator instanceof RootOperator)) {
      throw new LogicalOperatorException(operator.getType().toString(), "");
    }
    switch (operator.getType()) {
      case QUERY:
        QueryOperator query = (QueryOperator) operator;
        return transformQuery(query, fetchSize);
      case LOAD_CONFIGURATION:
        LoadConfigurationOperatorType type = ((LoadConfigurationOperator) operator)
            .getLoadConfigurationOperatorType();
        return generateLoadConfigurationPlan(type);
      default:
        return ((RootOperator) operator).convert(fetchSize);
    }
  }

  protected PhysicalPlan generateLoadConfigurationPlan(LoadConfigurationOperatorType type)
      throws QueryProcessException {
    switch (type) {
      case GLOBAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL);
      case LOCAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
      default:
        throw new QueryProcessException(
            String.format("Unrecognized load configuration operator type, %s", type.name()));
    }
  }

  /**
   * get types for path list
   *
   * @return pair.left is the type of column in result set, pair.right is the real type of the
   * measurement
   */
  protected Pair<List<TSDataType>, List<TSDataType>> getSeriesTypes(List<PartialPath> paths,
      String aggregation) throws MetadataException {
    List<TSDataType> measurementDataTypes = SchemaUtils.getSeriesTypesByPaths(paths, (String) null);
    // if the aggregation function is null, the type of column in result set
    // is equal to the real type of the measurement
    if (aggregation == null) {
      return new Pair<>(measurementDataTypes, measurementDataTypes);
    } else {
      // if the aggregation function is not null,
      // we should recalculate the type of column in result set
      List<TSDataType> columnDataTypes = SchemaUtils.getSeriesTypesByPaths(paths, aggregation);
      return new Pair<>(columnDataTypes, measurementDataTypes);
    }
  }

  protected List<TSDataType> getSeriesTypes(List<PartialPath> paths) throws MetadataException {
    return SchemaUtils.getSeriesTypesByPaths(paths);
  }


  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private PhysicalPlan transformQuery(QueryOperator queryOperator, int fetchSize)
      throws QueryProcessException {
    QueryPlan queryPlan;

    if (queryOperator.hasAggregation()) {
      if (queryOperator.hasUdf()) {
        throw new QueryProcessException(
            "User-defined and built-in hybrid aggregation is not supported.");
      }
    }
    queryPlan = (QueryPlan) queryOperator.convert(fetchSize);

    if (queryOperator.isAlignByDevice()) {
      // below is the core realization of ALIGN_BY_DEVICE sql logic
      AlignByDevicePlan alignByDevicePlan = new AlignByDevicePlan();
      if (queryPlan instanceof GroupByTimePlan) {
        alignByDevicePlan.setGroupByTimePlan((GroupByTimePlan) queryPlan);
      } else if (queryPlan instanceof FillQueryPlan) {
        alignByDevicePlan.setFillQueryPlan((FillQueryPlan) queryPlan);
      } else if (queryPlan instanceof AggregationPlan) {
        if (((AggregationPlan) queryPlan).getLevel() >= 0) {
          throw new QueryProcessException("group by level does not support align by device now.");
        }
        alignByDevicePlan.setAggregationPlan((AggregationPlan) queryPlan);
      }

      List<PartialPath> prefixPaths = queryOperator.getFromOperator().getPrefixPaths();
      // remove stars in fromPaths and get deviceId with deduplication
      List<PartialPath> devices = this.removeStarsInDeviceWithUnique(prefixPaths);
      List<PartialPath> suffixPaths = queryOperator.getSelectOperator().getSuffixPaths();
      List<String> originAggregations = queryOperator.getSelectOperator().getAggregations();

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
            List<PartialPath> actualPaths = getMatchedTimeseries(fullPath);
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
                throw new QueryProcessException(
                    "alias '" + suffixPath.getTsAlias()
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

            Pair<List<TSDataType>, List<TSDataType>> pair = getSeriesTypes(actualPaths,
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

          } catch (MetadataException e) {
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
      if (queryOperator.hasSlimit()) {
        int seriesSlimit = queryOperator.getSeriesLimit();
        int seriesOffset = queryOperator.getSeriesOffset();
        measurements = slimitTrimColumn(measurements, seriesSlimit, seriesOffset);
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
      FilterOperator filterOperator = queryOperator.getFilterOperator();
      if (filterOperator != null) {
        alignByDevicePlan.setDeviceToFilterMap(concatFilterByDevice(devices, filterOperator));
      }

      queryPlan = alignByDevicePlan;
    } else {
      queryPlan.setPaths(queryOperator.getSelectedPaths());
      // Last query result set will not be affected by alignment
      if (queryPlan instanceof LastQueryPlan && !queryOperator.isAlignByTime()) {
        throw new QueryProcessException("Disable align cannot be applied to LAST query.");
      }
      queryPlan.setAlignByTime(queryOperator.isAlignByTime());

      // transform filter operator to expression
      FilterOperator filterOperator = queryOperator.getFilterOperator();

      if (filterOperator != null) {
        List<PartialPath> filterPaths = new ArrayList<>(filterOperator.getPathSet());
        try {
          List<TSDataType> seriesTypes = getSeriesTypes(filterPaths);
          HashMap<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
          for (int i = 0; i < filterPaths.size(); i++) {
            ((RawDataQueryPlan) queryPlan).addFilterPathInDeviceToMeasurements(filterPaths.get(i));
            pathTSDataTypeHashMap.put(filterPaths.get(i), seriesTypes.get(i));
          }
          IExpression expression = filterOperator.transformToExpression(pathTSDataTypeHashMap);
          ((RawDataQueryPlan) queryPlan).setExpression(expression);
        } catch (MetadataException e) {
          throw new LogicalOptimizeException(e.getMessage());
        }
      }
    }

    if (queryOperator.getIndexType() != null) {
      if (queryPlan instanceof QueryIndexPlan) {
        ((QueryIndexPlan) queryPlan).setIndexType(queryOperator.getIndexType());
        ((QueryIndexPlan) queryPlan).setProps(queryOperator.getProps());
      }
      return queryPlan;
    }
    try {
      deduplicate(queryPlan, fetchSize);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    queryPlan.setRowLimit(queryOperator.getRowLimit());
    queryPlan.setRowOffset(queryOperator.getRowOffset());
    queryPlan.setAscending(queryOperator.isAscending());

    return queryPlan;
  }

  // e.g. translate "select * from root.ln.d1, root.ln.d2 where s1 < 20 AND s2 > 10" to
  // [root.ln.d1 -> root.ln.d1.s1 < 20 AND root.ln.d1.s2 > 10,
  //  root.ln.d2 -> root.ln.d2.s1 < 20 AND root.ln.d2.s2 > 10)]
  private Map<String, IExpression> concatFilterByDevice(
      List<PartialPath> devices, FilterOperator operator) throws QueryProcessException {
    Map<String, IExpression> deviceToFilterMap = new HashMap<>();
    Set<PartialPath> filterPaths = new HashSet<>();
    for (PartialPath device : devices) {
      FilterOperator newOperator = operator.copy();
      concatFilterPath(device, newOperator, filterPaths);
      // transform to a list so it can be indexed
      List<PartialPath> filterPathList = new ArrayList<>(filterPaths);
      try {
        List<TSDataType> seriesTypes = getSeriesTypes(filterPathList);
        Map<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPathList.size(); i++) {
          pathTSDataTypeHashMap.put(filterPathList.get(i), seriesTypes.get(i));
        }
        deviceToFilterMap
            .put(device.getFullPath(), newOperator.transformToExpression(pathTSDataTypeHashMap));
        filterPaths.clear();
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }

    return deviceToFilterMap;
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

  private void concatFilterPath(PartialPath prefix, FilterOperator operator,
      Set<PartialPath> filterPaths) {
    if (!operator.isLeaf()) {
      for (FilterOperator child : operator.getChildren()) {
        concatFilterPath(prefix, child, filterPaths);
      }
      return;
    }
    BasicFunctionOperator basicOperator = (BasicFunctionOperator) operator;
    PartialPath filterPath = basicOperator.getSinglePath();

    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath) || filterPath.getFirstNode()
        .startsWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return;
    }

    PartialPath concatPath = prefix.concatPath(filterPath);
    filterPaths.add(concatPath);
    basicOperator.setSinglePath(concatPath);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void deduplicate(QueryPlan queryPlan, int fetchSize)
      throws MetadataException, PathNumOverLimitException {
    // generate dataType first
    List<PartialPath> paths = queryPlan.getPaths();
    List<TSDataType> dataTypes = getSeriesTypes(paths);
    queryPlan.setDataTypes(dataTypes);

    // deduplicate from here
    if (queryPlan instanceof AlignByDevicePlan) {
      return;
    }

    if (queryPlan instanceof GroupByTimePlan) {
      GroupByTimePlan plan = (GroupByTimePlan) queryPlan;
      // the actual row number of group by query should be calculated from startTime, endTime and interval.
      long interval = (plan.getEndTime() - plan.getStartTime()) / plan.getInterval();
      if (interval > 0) {
        fetchSize = Math.min((int) (interval), fetchSize);
      }
    } else if (queryPlan instanceof AggregationPlan) {
      // the actual row number of aggregation query is 1
      fetchSize = 1;
    }

    RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;
    Set<String> columnForReaderSet = new HashSet<>();
    // if it's a last query, no need to sort by device
    if (queryPlan instanceof LastQueryPlan) {
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = paths.get(i);
        String column;
        if (path.isTsAliasExists()) {
          column = path.getTsAlias();
        } else {
          column = path.isMeasurementAliasExists() ? path.getFullPathWithAlias() : path.toString();
        }
        if (!columnForReaderSet.contains(column)) {
          TSDataType seriesType = dataTypes.get(i);
          rawDataQueryPlan.addDeduplicatedPaths(path);
          rawDataQueryPlan.addDeduplicatedDataTypes(seriesType);
          columnForReaderSet.add(column);
        }
      }
      return;
    }

    // sort path by device
    List<Pair<PartialPath, Integer>> indexedPaths = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      if (path != null) { // non-udf
        indexedPaths.add(new Pair<>(paths.get(i), i));
      } else { // udf
        UDFContext context = ((UDTFPlan) queryPlan).getExecutorByOriginalOutputColumnIndex(i)
            .getContext();
        for (PartialPath udfPath : context.getPaths()) {
          indexedPaths.add(new Pair<>(udfPath, i));
        }
      }
    }
    indexedPaths.sort(Comparator.comparing(pair -> pair.left));

    int maxDeduplicatedPathNum = QueryResourceManager.getInstance()
        .getMaxDeduplicatedPathNum(fetchSize);
    Map<String, Integer> pathNameToReaderIndex = new HashMap<>();
    Set<String> columnForDisplaySet = new HashSet<>();

    for (Pair<PartialPath, Integer> indexedPath : indexedPaths) {
      PartialPath originalPath = indexedPath.left;
      Integer originalIndex = indexedPath.right;

      String columnForReader = originalPath.isTsAliasExists() ? originalPath.getTsAlias() : null;
      if (columnForReader == null) {
        columnForReader = originalPath.isMeasurementAliasExists() ?
            originalPath.getFullPathWithAlias() : originalPath.toString();
        if (queryPlan instanceof AggregationPlan) {
          columnForReader =
              queryPlan.getAggregations().get(originalIndex) + "(" + columnForReader + ")";
        }
      }

      boolean isUdf = queryPlan instanceof UDTFPlan && paths.get(originalIndex) == null;

      if (!columnForReaderSet.contains(columnForReader)) {
        rawDataQueryPlan.addDeduplicatedPaths(originalPath);
        rawDataQueryPlan.addDeduplicatedDataTypes(isUdf
            ? IoTDB.metaManager.getSeriesType(originalPath) : dataTypes.get(originalIndex));
        pathNameToReaderIndex.put(columnForReader, pathNameToReaderIndex.size());
        if (queryPlan instanceof AggregationPlan) {
          ((AggregationPlan) queryPlan)
              .addDeduplicatedAggregations(queryPlan.getAggregations().get(originalIndex));
        }
        columnForReaderSet.add(columnForReader);
        if (maxDeduplicatedPathNum < columnForReaderSet.size()) {
          throw new PathNumOverLimitException(maxDeduplicatedPathNum, columnForReaderSet.size());
        }
      }

      String columnForDisplay = isUdf
          ? ((UDTFPlan) queryPlan).getExecutorByOriginalOutputColumnIndex(originalIndex)
          .getContext().getColumnName()
          : columnForReader;
      if (!columnForDisplaySet.contains(columnForDisplay)) {
        queryPlan.addPathToIndex(columnForDisplay, queryPlan.getPathToIndex().size());
        if (queryPlan instanceof UDTFPlan) {
          if (isUdf) {
            ((UDTFPlan) queryPlan).addUdfOutputColumn(columnForDisplay);
          } else {
            ((UDTFPlan) queryPlan).addRawQueryOutputColumn(columnForDisplay);
          }
        }
        columnForDisplaySet.add(columnForDisplay);
      }
    }

    if (queryPlan instanceof UDTFPlan) {
      ((UDTFPlan) queryPlan).setPathNameToReaderIndex(pathNameToReaderIndex);
    }
  }

  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit,
      int seriesOffset) throws QueryProcessException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      throw new QueryProcessException(String.format(
          "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.",
          seriesOffset, size));
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }

  private boolean verifyAllAggregationDataTypesEqual(QueryOperator queryOperator)
      throws MetadataException {
    List<String> aggregations = queryOperator.getSelectOperator().getAggregations();
    if (aggregations.isEmpty()) {
      return true;
    }

    List<PartialPath> paths = queryOperator.getSelectedPaths();
    List<TSDataType> dataTypes = getSeriesTypes(paths);
    String aggType = aggregations.get(0);
    switch (aggType) {
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return dataTypes.stream().allMatch(dataTypes.get(0)::equals);
      default:
        return true;
    }
  }

  protected List<PartialPath> getMatchedTimeseries(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPath(path);
  }

  protected Set<PartialPath> getMatchedDevices(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getDevices(path);
  }
}
