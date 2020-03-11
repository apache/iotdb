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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.CountOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.DataAuthOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadFilesOperator;
import org.apache.iotdb.db.qp.logical.sys.MoveFileOperator;
import org.apache.iotdb.db.qp.logical.sys.RemoveFileOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.SetTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowChildPathsOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowDevicesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTimeSeriesOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

/**
 * Used to convert logical operator to physical plan
 */
public class PhysicalGenerator {

  public PhysicalPlan transformToPhysicalPlan(Operator operator)
      throws QueryProcessException {
    List<Path> paths;
    switch (operator.getType()) {
      case AUTHOR:
        AuthorOperator author = (AuthorOperator) operator;
        try {
          return new AuthorPlan(author.getAuthorType(), author.getUserName(), author.getRoleName(),
              author.getPassWord(), author.getNewPassword(), author.getPrivilegeList(),
              author.getNodeName());
        } catch (AuthException e) {
          throw new QueryProcessException(e.getMessage());
        }
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
        DataAuthOperator dataAuthOperator = (DataAuthOperator) operator;
        return new DataAuthPlan(dataAuthOperator.getType(), dataAuthOperator.getUsers());
      case LOADDATA:
        LoadDataOperator loadData = (LoadDataOperator) operator;
        return new LoadDataPlan(loadData.getInputFilePath(), loadData.getMeasureType());
      case METADATA:
      case SET_STORAGE_GROUP:
        SetStorageGroupOperator setStorageGroup = (SetStorageGroupOperator) operator;
        return new SetStorageGroupPlan(setStorageGroup.getPath());
      case DELETE_STORAGE_GROUP:
        DeleteStorageGroupOperator deleteStorageGroup = (DeleteStorageGroupOperator) operator;
        return new DeleteStorageGroupPlan(deleteStorageGroup.getDeletePathList());
      case CREATE_TIMESERIES:
        CreateTimeSeriesOperator addPath = (CreateTimeSeriesOperator) operator;
        return new CreateTimeSeriesPlan(addPath.getPath(), addPath.getDataType(),
            addPath.getEncoding(), addPath.getCompressor(), addPath.getProps());
      case DELETE_TIMESERIES:
        DeleteTimeSeriesOperator deletePath = (DeleteTimeSeriesOperator) operator;
        return new DeleteTimeSeriesPlan(deletePath.getDeletePathList());
      case DELETE:
        DeleteDataOperator delete = (DeleteDataOperator) operator;
        paths = delete.getSelectedPaths();
        return new DeletePlan(delete.getTime(), paths);
      case INSERT:
        InsertOperator insert = (InsertOperator) operator;
        paths = insert.getSelectedPaths();
        if (paths.size() != 1) {
          throw new LogicalOperatorException(
              "For Insert command, cannot specified more than one seriesPath: " + paths);
        }
        return new InsertPlan(paths.get(0).getFullPath(), insert.getTime(),
            insert.getMeasurementList(),
            insert.getValueList());
      case QUERY:
        QueryOperator query = (QueryOperator) operator;
        return transformQuery(query);
      case TTL:
        switch (operator.getTokenIntType()) {
          case SQLConstant.TOK_SET:
            SetTTLOperator setTTLOperator = (SetTTLOperator) operator;
            return new SetTTLPlan(setTTLOperator.getStorageGroup(), setTTLOperator.getDataTTL());
          case SQLConstant.TOK_UNSET:
            SetTTLOperator unsetTTLOperator = (SetTTLOperator) operator;
            return new SetTTLPlan(unsetTTLOperator.getStorageGroup());
          case SQLConstant.TOK_SHOW:
            ShowTTLOperator showTTLOperator = (ShowTTLOperator) operator;
            return new ShowTTLPlan(showTTLOperator.getStorageGroups());
          default:
            throw new LogicalOperatorException(String
                .format("not supported operator type %s in ttl operation.", operator.getType()));
        }
      case LOAD_CONFIGURATION:
        return new LoadConfigurationPlan();
      case SHOW:
        switch (operator.getTokenIntType()) {
          case SQLConstant.TOK_DYNAMIC_PARAMETER:
            return new ShowPlan(ShowContentType.DYNAMIC_PARAMETER);
          case SQLConstant.TOK_FLUSH_TASK_INFO:
            return new ShowPlan(ShowContentType.FLUSH_TASK_INFO);
          case SQLConstant.TOK_VERSION:
            return new ShowPlan(ShowContentType.VERSION);
          case SQLConstant.TOK_TIMESERIES:
            return new ShowTimeSeriesPlan(ShowContentType.TIMESERIES,
                ((ShowTimeSeriesOperator) operator).getPath());
          case SQLConstant.TOK_STORAGE_GROUP:
            return new ShowPlan(ShowContentType.STORAGE_GROUP);
          case SQLConstant.TOK_DEVICES:
            return new ShowDevicesPlan(ShowContentType.DEVICES,
                ((ShowDevicesOperator) operator).getPath());
          case SQLConstant.TOK_COUNT_NODE_TIMESERIES:
            return new CountPlan(ShowContentType.COUNT_NODE_TIMESERIES,
                ((CountOperator) operator).getPath(), ((CountOperator) operator).getLevel());
          case SQLConstant.TOK_COUNT_NODES:
            return new CountPlan(ShowContentType.COUNT_NODES,
                ((CountOperator) operator).getPath(), ((CountOperator) operator).getLevel());
          case SQLConstant.TOK_COUNT_TIMESERIES:
            return new CountPlan(ShowContentType.COUNT_TIMESERIES,
                ((CountOperator) operator).getPath());
          case SQLConstant.TOK_CHILD_PATHS:
            return new ShowChildPathsPlan(ShowContentType.CHILD_PATH,
                ((ShowChildPathsOperator) operator).getPath());
          default:
            throw new LogicalOperatorException(String
                .format("not supported operator type %s in show operation.", operator.getType()));
        }
      case LOAD_FILES:
        if (((LoadFilesOperator) operator).isInvalid()) {
          throw new LogicalOperatorException(((LoadFilesOperator) operator).getErrMsg());
        }
        return new OperateFilePlan(((LoadFilesOperator) operator).getFile(),
            OperatorType.LOAD_FILES, ((LoadFilesOperator) operator).isAutoCreateSchema(),
            ((LoadFilesOperator) operator).getSgLevel());
      case REMOVE_FILE:
        return new OperateFilePlan(((RemoveFileOperator) operator).getFile(),
            OperatorType.REMOVE_FILE);
      case MOVE_FILE:
        return new OperateFilePlan(((MoveFileOperator) operator).getFile(),
            ((MoveFileOperator) operator).getTargetDir(), OperatorType.MOVE_FILE);
      default:
        throw new LogicalOperatorException(operator.getType().toString(), "");
    }
  }

  protected List<TSDataType> getSeriesTypes(List<String> paths,
      String aggregation) throws MetadataException {
    return SchemaUtils.getSeriesTypesByString(paths, aggregation);
  }

  protected List<TSDataType> getSeriesTypes(List<Path> paths) throws MetadataException {
    return SchemaUtils.getSeriesTypesByPath(paths);
  }

  private PhysicalPlan transformQuery(QueryOperator queryOperator)
      throws QueryProcessException {
    QueryPlan queryPlan;

    if (queryOperator.isGroupBy()) {
      queryPlan = new GroupByPlan();
      ((GroupByPlan) queryPlan).setInterval(queryOperator.getUnit());
      ((GroupByPlan) queryPlan).setSlidingStep(queryOperator.getSlidingStep());
      ((GroupByPlan) queryPlan).setStartTime(queryOperator.getStartTime());
      ((GroupByPlan) queryPlan).setEndTime(queryOperator.getEndTime());
      ((GroupByPlan) queryPlan)
          .setAggregations(queryOperator.getSelectOperator().getAggregations());
    } else if (queryOperator.isFill()) {
      queryPlan = new FillQueryPlan();
      FilterOperator timeFilter = queryOperator.getFilterOperator();
      if (!timeFilter.isSingle()) {
        throw new QueryProcessException("Slice query must select a single time point");
      }
      long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
      ((FillQueryPlan) queryPlan).setQueryTime(time);
      ((FillQueryPlan) queryPlan).setFillType(queryOperator.getFillTypes());
    } else if (queryOperator.hasAggregation()) {
      queryPlan = new AggregationPlan();
      ((AggregationPlan) queryPlan)
          .setAggregations(queryOperator.getSelectOperator().getAggregations());
    } else if (queryOperator.isLastQuery()) {
      queryPlan = new LastQueryPlan();
    } else {
      queryPlan = new RawDataQueryPlan();
    }
    if (queryPlan instanceof LastQueryPlan) {
      // Last query result set will not be affected by alignment
      queryPlan.setAlignByTime(true);
      List<Path> paths = queryOperator.getSelectedPaths();
      queryPlan.setPaths(paths);
    } else if (queryOperator.isAlignByDevice()) {
      // below is the core realization of ALIGN_BY_DEVICE sql logic
      AlignByDevicePlan alignByDevicePlan = new AlignByDevicePlan();
      if (queryPlan instanceof GroupByPlan) {
        alignByDevicePlan.setGroupByPlan((GroupByPlan) queryPlan);
      } else if (queryPlan instanceof FillQueryPlan) {
        alignByDevicePlan.setFillQueryPlan((FillQueryPlan) queryPlan);
      } else if (queryPlan instanceof AggregationPlan) {
        alignByDevicePlan.setAggregationPlan((AggregationPlan) queryPlan);
      }

      List<Path> prefixPaths = queryOperator.getFromOperator().getPrefixPaths();
      // remove stars in fromPaths and get deviceId with deduplication
      List<String> devices = this.removeStarsInDeviceWithUnique(prefixPaths);
      List<Path> suffixPaths = queryOperator.getSelectOperator().getSuffixPaths();
      List<String> originAggregations = queryOperator.getSelectOperator().getAggregations();

      // to record result measurement columns
      List<String> measurements = new ArrayList<>();
      Map<String, Set<String>> deviceToMeasurementsMap = new LinkedHashMap<>();
      // to check the same measurement of different devices having the same datatype
      Map<String, TSDataType> measurementDataTypeMap = new HashMap<>();
      Map<String, MeasurementType> measurementTypeMap = new HashMap<>();
      List<Path> paths = new ArrayList<>();

      for (int i = 0; i < suffixPaths.size(); i++) { // per suffix in SELECT
        Path suffixPath = suffixPaths.get(i);

        // to record measurements in the loop of a suffix path
        Set<String> measurementSetOfGivenSuffix = new LinkedHashSet<>();

        // if const measurement
        if (suffixPath.startWith("'") || suffixPath.startWith("\"")) {
          measurements.add(suffixPath.getMeasurement());
          measurementTypeMap.put(suffixPath.getMeasurement(), MeasurementType.Constant);
          continue;
        }

        for (String device : devices) { // per device in FROM after deduplication
          Path fullPath = Path.addPrefixPath(suffixPath, device);
          try {
            // remove stars in SELECT to get actual paths
            List<String> actualPaths = getMatchedTimeseries(fullPath.getFullPath());
            // for actual non exist path
            if (actualPaths.isEmpty() && originAggregations.isEmpty()) {
              String nonExistMeasurement = fullPath.getMeasurement();
              if (measurementSetOfGivenSuffix.add(nonExistMeasurement)
                  && measurementTypeMap.get(nonExistMeasurement) != MeasurementType.Exist) {
                measurementTypeMap.put(fullPath.getMeasurement(), MeasurementType.NonExist);
              }
            }

            String aggregation = originAggregations != null && !originAggregations.isEmpty() ?
                originAggregations.get(i) : null;
            List<TSDataType> dataTypes = getSeriesTypes(actualPaths, aggregation);
            for (int pathIdx = 0; pathIdx < actualPaths.size(); pathIdx++) {
              Path path = new Path(actualPaths.get(pathIdx));

              // check datatype consistency
              // a example of inconsistency: select s0 from root.sg1.d1, root.sg2.d3 align by device,
              // while root.sg1.d1.s0 is INT32 and root.sg2.d3.s0 is FLOAT.
              String measurementChecked;
              if (originAggregations != null && !originAggregations.isEmpty()) {
                measurementChecked = originAggregations.get(i) + "(" + path.getMeasurement() + ")";
              } else {
                measurementChecked = path.getMeasurement();
              }
              TSDataType dataType = dataTypes.get(pathIdx);
              if (measurementDataTypeMap.containsKey(measurementChecked)) {
                if (!dataType.equals(measurementDataTypeMap.get(measurementChecked))) {
                  throw new QueryProcessException(
                      "The data types of the same measurement column should be the same across "
                          + "devices in ALIGN_BY_DEVICE sql. For more details please refer to the "
                          + "SQL document.");
                }
              } else {
                measurementDataTypeMap.put(measurementChecked, dataType);
              }

              // update measurementSetOfGivenSuffix and Normal measurement
              if (measurementSetOfGivenSuffix.add(measurementChecked)
                  || measurementTypeMap.get(measurementChecked) != MeasurementType.Exist) {
                measurementTypeMap.put(measurementChecked, MeasurementType.Exist);
              }
              // update deviceToMeasurementsMap
              if (!deviceToMeasurementsMap.containsKey(device)) {
                deviceToMeasurementsMap.put(device, new HashSet<>());
              }
              deviceToMeasurementsMap.get(device).add(measurementChecked);
              // update paths
              paths.add(path);
            }

          } catch (MetadataException e) {
            throw new LogicalOptimizeException(
                String.format("Error when getting all paths of a full path: %s",
                    fullPath.getFullPath()) + e.getMessage());
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

      // assigns to alignByDevicePlan
      alignByDevicePlan.setMeasurements(measurements);
      alignByDevicePlan.setDeviceToMeasurementsMap(deviceToMeasurementsMap);
      alignByDevicePlan.setMeasurementDataTypeMap(measurementDataTypeMap);
      alignByDevicePlan.setMeasurementTypeMap(measurementTypeMap);
      alignByDevicePlan.setPaths(paths);

      // get deviceToFilterMap
      FilterOperator filterOperator = queryOperator.getFilterOperator();
      if (filterOperator != null) {
        alignByDevicePlan.setDeviceToFilterMap(concatFilterByDevice(devices, filterOperator));
      }

      queryPlan = alignByDevicePlan;
    } else {
      queryPlan.setAlignByTime(queryOperator.isAlignByTime());
      List<Path> paths = queryOperator.getSelectedPaths();
      queryPlan.setPaths(paths);

      // transform filter operator to expression
      FilterOperator filterOperator = queryOperator.getFilterOperator();

      if (filterOperator != null) {
        List<Path> filterPaths = new ArrayList<>(filterOperator.getPathSet());
        try {
          List<TSDataType> seriesTypes = getSeriesTypes(filterPaths);
          HashMap<Path, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
          for (int i = 0; i < filterPaths.size(); i++) {
            pathTSDataTypeHashMap.put(filterPaths.get(i), seriesTypes.get(i));
          }
          IExpression expression = filterOperator.transformToExpression(pathTSDataTypeHashMap);
          ((RawDataQueryPlan) queryPlan).setExpression(expression);
        } catch (MetadataException e) {
          throw new LogicalOptimizeException(e);
        }
      }
    }
    try {
      generateDataTypes(queryPlan);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    deduplicate(queryPlan);

    queryPlan.setRowLimit(queryOperator.getRowLimit());
    queryPlan.setRowOffset(queryOperator.getRowOffset());

    return queryPlan;
  }

  // e.g. translate "select * from root.ln.d1, root.ln.d2 where s1 < 20 AND s2 > 10" to
  // [root.ln.d1 -> root.ln.d1.s1 < 20 AND root.ln.d1.s2 > 10,
  //  root.ln.d2 -> root.ln.d2.s1 < 20 AND root.ln.d2.s2 > 10)]
  private Map<String, IExpression> concatFilterByDevice(List<String> devices,
      FilterOperator operator)
      throws QueryProcessException {
    Map<String, IExpression> deviceToFilterMap = new HashMap<>();
    Set<Path> filterPaths = new HashSet<>();
    for (String device : devices) {
      FilterOperator newOperator = operator.copy();
      concatFilterPath(device, newOperator, filterPaths);
      // transform to a list so it can be indexed
      List<Path> filterPathList = new ArrayList<>(filterPaths);
      try {
        List<TSDataType> seriesTypes = getSeriesTypes(filterPathList);
        Map<Path, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPathList.size(); i++) {
          pathTSDataTypeHashMap.put(filterPathList.get(i), seriesTypes.get(i));
        }
        deviceToFilterMap.put(device, newOperator.transformToExpression(pathTSDataTypeHashMap));
        filterPaths.clear();
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }

    return deviceToFilterMap;
  }

  private List<String> removeStarsInDeviceWithUnique(List<Path> paths)
      throws LogicalOptimizeException {
    List<String> retDevices;
    Set<String> deviceSet = new LinkedHashSet<>();
    try {
      for (Path path : paths) {
        Set<String> tempDS = getMatchedDevices(path.getFullPath());
        deviceSet.addAll(tempDS);
      }
      retDevices = new ArrayList<>(deviceSet);
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return retDevices;
  }

  private void concatFilterPath(String prefix, FilterOperator operator,
      Set<Path> filterPaths) {
    if (!operator.isLeaf()) {
      for (FilterOperator child : operator.getChildren()) {
        concatFilterPath(prefix, child, filterPaths);
      }
      return;
    }
    BasicFunctionOperator basicOperator = (BasicFunctionOperator) operator;
    Path filterPath = basicOperator.getSinglePath();

    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath) || filterPath.startWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return;
    }

    Path concatPath = Path.addPrefixPath(filterPath, prefix);
    filterPaths.add(concatPath);
    basicOperator.setSinglePath(concatPath);
  }

  private void generateDataTypes(QueryPlan queryPlan) throws MetadataException {
    List<Path> paths = queryPlan.getPaths();
    List<TSDataType> dataTypes = getSeriesTypes(paths);
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      TSDataType dataType = dataTypes.get(i);
      queryPlan.addTypeMapping(path, dataType);
    }
    queryPlan.setDataTypes(dataTypes);
  }

  private void deduplicate(QueryPlan queryPlan) {
    if (queryPlan instanceof AlignByDevicePlan) {
      return;
    }
    if (queryPlan instanceof AggregationPlan) {
      AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
      deduplicateAggregation(aggregationPlan);
      return;
    }
    RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;
    List<Path> paths = queryPlan.getPaths();

    Set<String> columnSet = new HashSet<>();
    Map<Path, TSDataType> dataTypeMapping = queryPlan.getDataTypeMapping();
    for (Path path : paths) {
      String column = path.toString();
      if (!columnSet.contains(column)) {
        TSDataType seriesType = dataTypeMapping.get(path);
        rawDataQueryPlan.addDeduplicatedPaths(path);
        rawDataQueryPlan.addDeduplicatedDataTypes(seriesType);
        columnSet.add(column);
      }
    }
  }

  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit, int seriesOffset)
      throws QueryProcessException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      throw new QueryProcessException("SOFFSET <SOFFSETValue>: SOFFSETValue exceeds the range.");
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }


  private void deduplicateAggregation(AggregationPlan queryPlan) {
    List<Path> paths = queryPlan.getPaths();
    List<String> aggregations = queryPlan.getAggregations();

    Set<String> columnSet = new HashSet<>();
    Map<Path, TSDataType> dataTypeMapping = queryPlan.getDataTypeMapping();
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      String column = aggregations.get(i) + "(" + path.toString() + ")";
      if (!columnSet.contains(column)) {
        queryPlan.addDeduplicatedPaths(path);
        TSDataType seriesType = dataTypeMapping.get(path);
        queryPlan.addDeduplicatedDataTypes(seriesType);
        queryPlan.addDeduplicatedAggregations(aggregations.get(i));
        columnSet.add(column);
      }
    }
  }

  protected List<String> getMatchedTimeseries(String path) throws MetadataException {
    return MManager.getInstance().getAllTimeseriesName(path);
  }

  protected Set<String> getMatchedDevices(String path) throws MetadataException {
    return MManager.getInstance().getDevices(path);
  }
}

