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
package org.apache.iotdb.spark.tsfile.qp;

import org.apache.iotdb.spark.tsfile.qp.common.FilterOperator;
import org.apache.iotdb.spark.tsfile.qp.common.SQLConstant;
import org.apache.iotdb.spark.tsfile.qp.common.SingleQuery;
import org.apache.iotdb.spark.tsfile.qp.common.TSQueryPlan;
import org.apache.iotdb.spark.tsfile.qp.exception.QueryOperatorException;
import org.apache.iotdb.spark.tsfile.qp.exception.QueryProcessorException;
import org.apache.iotdb.spark.tsfile.qp.optimizer.DNFFilterOptimizer;
import org.apache.iotdb.spark.tsfile.qp.optimizer.MergeSingleFilterOptimizer;
import org.apache.iotdb.spark.tsfile.qp.optimizer.PhysicalOptimizer;
import org.apache.iotdb.spark.tsfile.qp.optimizer.RemoveNotOptimizer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to convert information given by sparkSQL to construct TSFile's query plans.
 * For TSFile's schema differ from SparkSQL's table schema e.g. TSFile's SQL: select s1,s2 from
 * root.car.d1 where s1 = 10 SparkSQL's SQL: select s1,s2 from XXX where delta_object = d1
 */
public class QueryProcessor {

  // construct logical query plans first, then convert them to physical ones
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public List<TSQueryPlan> generatePlans(
      FilterOperator filter,
      List<String> paths,
      List<String> columnNames,
      TsFileSequenceReader in,
      Long start,
      Long end)
      throws QueryProcessorException, IOException {

    List<TSQueryPlan> queryPlans = new ArrayList<>();

    if (filter != null) {
      RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
      filter = removeNot.optimize(filter);

      DNFFilterOptimizer dnf = new DNFFilterOptimizer();
      filter = dnf.optimize(filter);

      // merge different query path
      // e.g. or (sensor_1 > 20, sensor_1 <10, sensor_2 > 10)
      // => or (or (sensor_1 > 20, sensor_1 < 10), sensor_2 > 10)
      MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
      filter = merge.optimize(filter);

      List<FilterOperator> filterOperators = splitFilter(filter);

      for (FilterOperator filterOperator : filterOperators) {
        SingleQuery singleQuery = constructSelectPlan(filterOperator, columnNames);
        if (singleQuery != null) {
          queryPlans.addAll(
              new PhysicalOptimizer(columnNames).optimize(singleQuery, paths, in, start, end));
        }
      }
    } else {
      queryPlans.addAll(new PhysicalOptimizer(columnNames).optimize(null, paths, in, start, end));
    }
    // merge query plan
    Map<List<String>, List<TSQueryPlan>> pathMap = new HashMap<>();
    for (TSQueryPlan tsQueryPlan : queryPlans) {
      if (pathMap.containsKey(tsQueryPlan.getPaths())) {
        pathMap.get(tsQueryPlan.getPaths()).add(tsQueryPlan);
      } else {
        List<TSQueryPlan> plans = new ArrayList<>();
        plans.add(tsQueryPlan);
        pathMap.put(tsQueryPlan.getPaths(), plans);
      }
    }

    queryPlans.clear();

    for (List<TSQueryPlan> plans : pathMap.values()) {
      TSQueryPlan mergePlan = null;
      for (TSQueryPlan plan : plans) {
        if (mergePlan == null) {
          mergePlan = plan;
        } else {
          FilterOperator timeFilterOperator = new FilterOperator(SQLConstant.KW_OR);
          List<FilterOperator> timeFilterChildren = new ArrayList<>();
          timeFilterChildren.add(mergePlan.getTimeFilterOperator());
          timeFilterChildren.add(plan.getTimeFilterOperator());
          timeFilterOperator.setChildrenList(timeFilterChildren);
          mergePlan.setTimeFilterOperator(timeFilterOperator);

          FilterOperator valueFilterOperator = new FilterOperator(SQLConstant.KW_OR);
          List<FilterOperator> valueFilterChildren = new ArrayList<>();
          valueFilterChildren.add(mergePlan.getValueFilterOperator());
          valueFilterChildren.add(plan.getValueFilterOperator());
          valueFilterOperator.setChildrenList(valueFilterChildren);
          mergePlan.setValueFilterOperator(valueFilterOperator);
        }
      }
      queryPlans.add(mergePlan);
    }

    return queryPlans;
  }

  private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
    if (filterOperator.isSingle() || filterOperator.getTokenIntType() != SQLConstant.KW_OR) {
      List<FilterOperator> ret = new ArrayList<>();
      ret.add(filterOperator);
      return ret;
    }
    // a list of conjunctions linked by or
    return filterOperator.childOperators;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private SingleQuery constructSelectPlan(FilterOperator filterOperator, List<String> columnNames)
      throws QueryOperatorException {
    FilterOperator timeFilter = null;
    FilterOperator valueFilter = null;
    List<FilterOperator> columnFilterOperators = new ArrayList<>();

    List<FilterOperator> singleFilterList = null;

    if (filterOperator.isSingle()) {
      singleFilterList = new ArrayList<>();
      singleFilterList.add(filterOperator);

    } else if (filterOperator.getTokenIntType() == SQLConstant.KW_AND) {
      // original query plan has been dealt with merge optimizer, thus all nodes with same
      // path have been merged to one node
      singleFilterList = filterOperator.getChildren();
    }

    if (singleFilterList == null) {
      return null;
    }

    List<FilterOperator> valueList = new ArrayList<>();
    for (FilterOperator child : singleFilterList) {
      if (!child.isSingle()) {
        valueList.add(child);
      } else {
        String singlePath = child.getSinglePath();
        if (columnNames.contains(singlePath)) {
          if (!columnFilterOperators.contains(child)) {
            columnFilterOperators.add(child);
          } else {
            throw new QueryOperatorException(
                "The same key filter has been specified more than once: " + singlePath);
          }
        } else {
          switch (child.getSinglePath()) {
            case SQLConstant.RESERVED_TIME:
              if (timeFilter != null) {
                throw new QueryOperatorException("time filter has been specified more than once");
              }
              timeFilter = child;
              break;
            default:
              valueList.add(child);
              break;
          }
        }
      }
    }

    if (valueList.size() == 1) {
      valueFilter = valueList.get(0);

    } else if (valueList.size() > 1) {
      valueFilter = new FilterOperator(SQLConstant.KW_AND, false);
      valueFilter.childOperators = valueList;
    }

    return new SingleQuery(columnFilterOperators, timeFilter, valueFilter);
  }
}
