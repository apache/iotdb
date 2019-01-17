/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.strategy;

import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to convert logical operator to physical plan
 */
public class PhysicalGenerator {

  private static final Logger logger = LoggerFactory.getLogger(PhysicalGenerator.class);
  private QueryProcessExecutor executor;

  public PhysicalGenerator(QueryProcessExecutor executor) {
    this.executor = executor;
  }

  public PhysicalPlan transformToPhysicalPlan(Operator operator)
      throws QueryProcessorException, ProcessorException {
    List<Path> paths;
    switch (operator.getType()) {
      case AUTHOR:
        AuthorOperator author = (AuthorOperator) operator;
        try {
          return new AuthorPlan(author.getAuthorType(), author.getUserName(), author.getRoleName(),
              author.getPassWord(), author.getNewPassword(), author.getPrivilegeList(),
              author.getNodeName());
        } catch (AuthException e) {
          throw new QueryProcessorException(e.getMessage());
        }
      case LOADDATA:
        LoadDataOperator loadData = (LoadDataOperator) operator;
        return new LoadDataPlan(loadData.getInputFilePath(), loadData.getMeasureType());
      case SET_STORAGE_GROUP:
      case DELETE_TIMESERIES:
      case METADATA:
        MetadataOperator metadata = (MetadataOperator) operator;
        return new MetadataPlan(metadata.getNamespaceType(), metadata.getPath(),
            metadata.getDataType(),
            metadata.getEncoding(), metadata.getEncodingArgs(), metadata.getDeletePathList());
      case PROPERTY:
        PropertyOperator property = (PropertyOperator) operator;
        return new PropertyPlan(property.getPropertyType(), property.getPropertyPath(),
            property.getMetadataPath());
      // case DELETE:
      // DeleteOperator delete = (DeleteOperator) operator;
      // paths = delete.getSelectedPaths();
      // if (delete.getTime() <= 0) {
      // throw new LogicalOperatorException("For Delete command, time must greater than 0.");
      // }
      // return new DeletePlan(delete.getTime(), paths);
      case INSERT:
        InsertOperator Insert = (InsertOperator) operator;
        paths = Insert.getSelectedPaths();
        if (paths.size() != 1) {
          throw new LogicalOperatorException(
              "For Insert command, cannot specified more than one seriesPath:" + paths);
        }
        if (Insert.getTime() <= 0) {
          throw new LogicalOperatorException("For Insert command, time must greater than 0.");
        }
        return new InsertPlan(paths.get(0).getFullPath(), Insert.getTime(),
            Insert.getMeasurementList(),
            Insert.getValueList());
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
  // } else if (queryOperator.isFill()) { // old fill query
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
  // return transformQueryV2(queryOperator);
  // }
  // }

  private PhysicalPlan transformQuery(QueryOperator queryOperator)
      throws QueryProcessorException, ProcessorException {

    QueryPlan queryPlan;

    if (queryOperator.isGroupBy()) {
      queryPlan = new GroupByPlan();
      ((GroupByPlan) queryPlan).setUnit(queryOperator.getUnit());
      ((GroupByPlan) queryPlan).setOrigin(queryOperator.getOrigin());
      ((GroupByPlan) queryPlan).setIntervals(queryOperator.getIntervals());
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

    // set selected paths
    List<Path> paths = queryOperator.getSelectedPaths();
    queryPlan.setPaths(paths);

    // transform filter operator to expression
    FilterOperator filterOperator = queryOperator.getFilterOperator();

    if (filterOperator != null) {
      IExpression expression = filterOperator.transformToExpression(executor);
      queryPlan.setExpression(expression);
    }

    queryPlan.checkPaths(executor);
    return queryPlan;
  }

  // private SingleQueryPlan constructSelectPlan(FilterOperator filterOperator, List<Path> paths,
  // QueryProcessExecutor conf) throws QueryProcessorException {
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

}
