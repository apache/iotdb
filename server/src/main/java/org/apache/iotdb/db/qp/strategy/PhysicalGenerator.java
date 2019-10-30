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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.DataAuthOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.logical.sys.SetTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

/**
 * Used to convert logical operator to physical plan
 */
public class PhysicalGenerator {

  private IQueryProcessExecutor executor;

  public PhysicalGenerator(IQueryProcessExecutor executor) {
    this.executor = executor;
  }

  public PhysicalPlan transformToPhysicalPlan(Operator operator)
      throws QueryProcessorException {
    List<Path> paths;
    switch (operator.getType()) {
      case AUTHOR:
        AuthorOperator author = (AuthorOperator) operator;
        try {
          return new AuthorPlan(author.getAuthorType(), author.getUserName(), author.getRoleName(),
              author.getPassWord(), author.getNewPassword(), author.getPrivilegeList(),
              author.getNodeName());
        } catch (AuthException e) {
          throw new QueryProcessorException(e);
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
      case PROPERTY:
        PropertyOperator property = (PropertyOperator) operator;
        return new PropertyPlan(property.getPropertyType(), property.getPropertyPath(),
            property.getMetadataPath());
      case DELETE:
        DeleteDataOperator delete = (DeleteDataOperator) operator;
        paths = delete.getSelectedPaths();
        return new DeletePlan(delete.getTime(), paths);
      case INSERT:
        InsertOperator insert = (InsertOperator) operator;
        paths = insert.getSelectedPaths();
        if (paths.size() != 1) {
          throw new LogicalOperatorException(
              "For Insert command, cannot specified more than one seriesPath:" + paths);
        }
        return new InsertPlan(paths.get(0).getFullPath(), insert.getTime(),
            insert.getMeasurementList(),
            insert.getValueList());
      // case UPDATE:
      // UpdateOperator update = (UpdateOperator) operator;
      // UpdatePlan updatePlan = new UpdatePlan();
      // updatePlan.setValue(update.getValue());
      // paths = update.getSelectedPaths();
      // if (paths.size() > 1) {
      // throw new LogicalOperatorException("update command, must have and only have one seriesPath:" + paths);
      // }
      // updatePlan.setPath(paths.get(0));
      // parseUpdateTimeFilter(update, updatePlan);
      // return updatePlan;
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
        }

      default:
        throw new LogicalOperatorException("not supported operator type: " + operator.getType());
    }
  }

  // /**
  // * for update command, time should have start and end time range.
  // *
  // * @param updateOperator update logical plan
  // */
  // private void parseUpdateTimeFilter(UpdateOperator updateOperator, UpdatePlan plan) throws
  // LogicalOperatorException {
  // List<Pair<Long, Long>> intervals = extractTimeIntervals(updateOperator.getFilterOperator());
  // plan.addIntervals(intervals);
  // if (plan.getIntervals().isEmpty()) {
  // throw new LogicalOperatorException("For update command, time filter is invalid");
  // }
  // }

  // /**
  // * extract time intervals from filterOperator
  // *
  // * @return valid time intervals
  // * @throws LogicalOperatorException
  // */
  // private List<Pair<Long, Long>> extractTimeIntervals(FilterOperator filterOperator) throws
  // LogicalOperatorException {
  // List<Pair<Long, Long>> intervals = new ArrayList<>();
  // if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
  // throw new LogicalOperatorException("filter Operator must be a time filter");
  // }
  // // transfer the filter operator to FilterExpression
  // IExpression timeFilter;
  // try {
  // timeFilter = filterOperator.transformToExpression(executor);
  // } catch (QueryProcessorException e) {
  // e.printStackTrace();
  // throw new LogicalOperatorException(e.getMessage());
  // }
  // LongFilterVerifier filterVerifier = (LongFilterVerifier) FilterVerifier.create(TSDataType.INT64);
  // LongInterval longInterval = filterVerifier.getInterval((SingleSeriesFilterExpression) timeFilter);
  // long startTime;
  // long endTime;
  // for (int i = 0; i < longInterval.count; i = i + 2) {
  // if (longInterval.flag[i]) {
  // startTime = longInterval.v[i];
  // } else {
  // startTime = longInterval.v[i] + 1;
  // }
  // if (longInterval.flag[i + 1]) {
  // endTime = longInterval.v[i + 1];
  // } else {
  // endTime = longInterval.v[i + 1] - 1;
  // }
  // if ((startTime <= 0 && startTime != Long.MIN_VALUE) || endTime <= 0) {
  // throw new LogicalOperatorException("start and end time must be greater than 0.");
  // }
  // if (startTime == Long.MIN_VALUE) {
  // startTime = 1;
  // }
  //
  // if (endTime >= startTime)
  // intervals.add(new Pair<>(startTime, endTime));
  // }
  // return intervals;
  // }

  // private PhysicalPlan transformQuery(QueryOperator queryOperator) throws QueryProcessorException,
  // ProcessorException {
  //
  // List<Path> paths = queryOperator.getSelectedPaths();
  // List<String> aggregations = queryOperator.getSelectOperator().getAggregations();
  // ArrayList<SingleQueryPlan> subPlans = new ArrayList<>();
  //
  // FilterOperator filterOperator = queryOperator.getFilterOperator();
  // if (filterOperator == null) {
  // subPlans.add(new SingleQueryPlan(paths, null, null, null, executor, null));
  // } else {
  // List<FilterOperator> parts = splitFilter(filterOperator);
  // for (FilterOperator filter : parts) {
  // SingleQueryPlan plan = constructSelectPlan(filter, paths, executor);
  // subPlans.add(plan);
  // }
  // }
  // MultiQueryPlan multiQueryPlan = new MultiQueryPlan(subPlans, aggregations);
  //
  // if (queryOperator.isGroupBy()) { //old group by
  // multiQueryPlan.setType(MultiQueryPlan.QueryType.GROUPBY);
  // multiQueryPlan.setUnit(queryOperator.getUnit());
  // multiQueryPlan.setOrigin(queryOperator.getOrigin());
  // multiQueryPlan.setIntervals(queryOperator.getIntervals());
  // return multiQueryPlan;
  // } else if (queryOperator.isFill()) { // old deserialize query
  // multiQueryPlan.setType(MultiQueryPlan.QueryType.FILL);
  // FilterOperator timeFilter = queryOperator.getFilterOperator();
  // if (!timeFilter.isSingle())
  // throw new QueryProcessorException("Slice query must select a single time point");
  // long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
  // multiQueryPlan.setQueryTime(time);
  // multiQueryPlan.setFillType(queryOperator.getFillTypes());
  // return multiQueryPlan;
  // } else if (queryOperator.hasAggregation()) { //old aggregation
  // return multiQueryPlan;
  // } else { //ordinary query
  // return transformQuery(queryOperator);
  // }
  // }

  private PhysicalPlan transformQuery(QueryOperator queryOperator)
      throws QueryProcessorException {
    QueryPlan queryPlan;

    if (queryOperator.isGroupBy()) {
      queryPlan = new GroupByPlan();
      ((GroupByPlan) queryPlan).setUnit(queryOperator.getUnit());
      ((GroupByPlan) queryPlan).setOrigin(queryOperator.getOrigin());
      ((GroupByPlan) queryPlan).setIntervals(queryOperator.getIntervals());
      ((GroupByPlan) queryPlan)
          .setAggregations(queryOperator.getSelectOperator().getAggregations());
    } else if (queryOperator.isFill()) {
      queryPlan = new FillQueryPlan();
      FilterOperator timeFilter = queryOperator.getFilterOperator();
      if (!timeFilter.isSingle()) {
        throw new QueryProcessorException("Slice query must select a single time point");
      }
      long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
      ((FillQueryPlan) queryPlan).setQueryTime(time);
      ((FillQueryPlan) queryPlan).setFillType(queryOperator.getFillTypes());
    } else if (queryOperator.hasAggregation()) { // ordinary query
      queryPlan = new AggregationPlan();
      ((AggregationPlan) queryPlan)
          .setAggregations(queryOperator.getSelectOperator().getAggregations());
    } else {
      queryPlan = new QueryPlan();
    }

    if (!queryOperator.isGroupByDevice()) {
      List<Path> paths = queryOperator.getSelectedPaths();
      queryPlan.setPaths(paths);

    } else {
      // below is the core realization of GROUP_BY_DEVICE sql
      List<Path> prefixPaths = queryOperator.getFromOperator().getPrefixPaths();
      List<Path> suffixPaths = queryOperator.getSelectOperator().getSuffixPaths();
      List<String> originAggregations = queryOperator.getSelectOperator().getAggregations();

      List<String> measurementColumnList = new ArrayList<>();
      Map<String, Set<String>> measurementColumnsGroupByDevice = new LinkedHashMap<>();
      Map<String, TSDataType> dataTypeConsistencyChecker = new HashMap<>();
      Set<Path> allSelectPaths = new HashSet<>();

      for (int i = 0; i < suffixPaths.size(); i++) { // per suffix
        Path suffixPath = suffixPaths.get(i);
        Set<String> deviceSetOfGivenSuffix = new HashSet<>();
        Set<String> measurementSetOfGivenSuffix = new TreeSet<>();
        for (Path prefixPath : prefixPaths) { // per prefix
          Path fullPath = Path.addPrefixPath(suffixPath, prefixPath);
          Set<String> tmpDeviceSet = new HashSet<>();
          try {
            List<String> actualPaths = executor.getAllPaths(fullPath.getFullPath());
            for (String pathStr : actualPaths) {
              Path path = new Path(pathStr);
              String device = path.getDevice();
              // update tmpDeviceSet for a full path
              tmpDeviceSet.add(device);

              // ignore the duplicate prefix device for a given suffix path
              // e.g. select s0 from root.vehicle.d0, root.vehicle.d0 group by device,
              // for the given suffix path "s0", the second prefix device "root.vehicle.d0" is
              // duplicated and should be neglected.
              if (deviceSetOfGivenSuffix.contains(device)) {
                continue;
              }

              // get pathForDataType and measurementColumn
              String measurement = path.getMeasurement();
              String pathForDataType;
              String measurementColumn;
              if (originAggregations != null && !originAggregations.isEmpty()) {
                pathForDataType = originAggregations.get(i) + "(" + path.getFullPath() + ")";
                measurementColumn = originAggregations.get(i) + "(" + measurement + ")";
              } else {
                pathForDataType = path.getFullPath();
                measurementColumn = measurement;
              }
              // check the consistency of data types
              // a example of inconsistency: select s0 from root.sg1.d1, root.sg2.d3 group by device,
              // while root.sg1.d1.s0 is INT32 and root.sg2.d3.s0 is FLOAT.
              TSDataType dataType = TSServiceImpl.getSeriesType(pathForDataType);
              if (dataTypeConsistencyChecker.containsKey(measurementColumn)) {
                if (!dataType.equals(dataTypeConsistencyChecker.get(measurementColumn))) {
                  throw new QueryProcessorException(
                      "The data types of the same measurement column should be the same across "
                          + "devices in GROUP_BY_DEVICE sql. For more details please refer to the "
                          + "SQL document.");
                }
              } else {
                dataTypeConsistencyChecker.put(measurementColumn, dataType);
              }
              // update measurementSetOfGivenSuffix
              measurementSetOfGivenSuffix.add(measurementColumn);

              // update measurementColumnsGroupByDevice
              if (!measurementColumnsGroupByDevice.containsKey(device)) {
                measurementColumnsGroupByDevice.put(device, new HashSet<>());
              }
              measurementColumnsGroupByDevice.get(device).add(measurementColumn);

              // update allSelectedPaths
              allSelectPaths.add(path);
            }
            // update deviceSetOfGivenSuffix
            deviceSetOfGivenSuffix.addAll(tmpDeviceSet);

          } catch (MetadataErrorException e) {
            throw new LogicalOptimizeException(
                String.format("error when getting all paths of a full path: %s",
                    fullPath.getFullPath()), e);
          }
        }
        // update measurementColumnList
        // Note that in the loop of a suffix path, set is used.
        // And across the loops of suffix paths, list is used.
        // e.g. select *,s1 from root.sg.d0, root.sg.d1
        // for suffix *, measurementSetOfGivenSuffix = {s1,s2,s3}
        // for suffix s1, measurementSetOfGivenSuffix = {s1}
        // therefore the final measurementColumnList is [s1,s2,s3,s1].
        measurementColumnList.addAll(measurementSetOfGivenSuffix);
      }

      if (measurementColumnList.isEmpty()) {
        throw new QueryProcessorException("do not select any existing series");
      }

      // slimit trim on the measurementColumnList
      if (queryOperator.hasSlimit()) {
        int seriesSlimit = queryOperator.getSeriesLimit();
        int seriesOffset = queryOperator.getSeriesOffset();
        measurementColumnList = slimitTrimColumn(measurementColumnList, seriesSlimit, seriesOffset);
      }

      // assigns to queryPlan
      queryPlan.setGroupByDevice(true);
      queryPlan.setMeasurementColumnList(measurementColumnList);
      queryPlan.setMeasurementColumnsGroupByDevice(measurementColumnsGroupByDevice);
      queryPlan.setDataTypeConsistencyChecker(dataTypeConsistencyChecker);
      queryPlan.setPaths(new ArrayList<>(allSelectPaths));
    }

    queryPlan.checkPaths(executor);

    // transform filter operator to expression
    FilterOperator filterOperator = queryOperator.getFilterOperator();

    if (filterOperator != null) {
      IExpression expression = filterOperator.transformToExpression(executor);
      queryPlan.setExpression(expression);
    }

    return queryPlan;
  }

  // private SingleQueryPlan constructSelectPlan(FilterOperator filterOperator, List<Path> paths,
  // AbstractQueryProcessExecutor conf) throws QueryProcessorException {
  // FilterOperator timeFilter = null;
  // FilterOperator freqFilter = null;
  // FilterOperator valueFilter = null;
  // List<FilterOperator> singleFilterList;
  // if (filterOperator.isSingle()) {
  // singleFilterList = new ArrayList<>();
  // singleFilterList.add(filterOperator);
  // } else if (filterOperator.getTokenIntType() == KW_AND) {
  // // now it has been dealt with merge optimizer, thus all nodes with
  // // same seriesPath have been merged to one node
  // singleFilterList = filterOperator.getChildren();
  // } else {
  // throw new GeneratePhysicalPlanException("for one task, filter cannot be OR if it's not single");
  // }
  // List<FilterOperator> valueList = new ArrayList<>();
  // for (FilterOperator child : singleFilterList) {
  // if (!child.isSingle()) {
  // throw new GeneratePhysicalPlanException(
  // "in format:[(a) and () and ()] or [] or [], a is not single! a:" + child);
  // }
  // switch (child.getSinglePath().toString()) {
  // case RESERVED_TIME:
  // if (timeFilter != null) {
  // throw new GeneratePhysicalPlanException("time filter has been specified more than once");
  // }
  // timeFilter = child;
  // break;
  // case RESERVED_FREQ:
  // if (freqFilter != null) {
  // throw new GeneratePhysicalPlanException("freq filter has been specified more than once");
  // }
  // freqFilter = child;
  // break;
  // default:
  // valueList.add(child);
  // break;
  // }
  // }
  // if (valueList.size() == 1) {
  // valueFilter = valueList.get(0);
  // } else if (valueList.size() > 1) {
  // valueFilter = new FilterOperator(KW_AND, false);
  // valueFilter.setChildren(valueList);
  // }
  //
  // return new SingleQueryPlan(paths, timeFilter, freqFilter, valueFilter, conf, null);
  // }

  // /**
  // * split filter operator to a list of filter with relation of "or" each
  // * other.
  // */
  // private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
  // List<FilterOperator> ret = new ArrayList<>();
  // if (filterOperator.isSingle() || filterOperator.getTokenIntType() != KW_OR) {
  // // single or leaf(BasicFunction)
  // ret.add(filterOperator);
  // return ret;
  // }
  // // a list of partion linked with or
  // return filterOperator.getChildren();
  // }

  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit, int seriesOffset)
      throws QueryProcessorException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      throw new QueryProcessorException("SOFFSET <SOFFSETValue>: SOFFSETValue exceeds the range.");
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }

}

