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
package org.apache.iotdb.db.qp.strategy.optimizer;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FunctionOperator;

import java.util.List;
import java.util.Set;

public class RemoveNotOptimizer implements IFilterOptimizer {

  /**
   * get DNF(disjunctive normal form) for this filter operator tree. Before getDNF, this op tree
   * must be binary, in another word, each non-leaf node has exactly two children.
   *
   * @return optimized operator
   * @throws LogicalOperatorException exception in RemoveNot optimizing
   */
  @Override
  public FilterOperator optimize(FilterOperator filter) throws LogicalOperatorException {
    Set<PartialPath> pathSet = filter.getPathSet();
    FilterOperator optimizedFilterOperator = removeNot(filter);
    optimizedFilterOperator.setPathSet(pathSet);
    return optimizedFilterOperator;
  }

  private FilterOperator removeNot(FilterOperator filter) throws LogicalOperatorException {
    if (filter.isLeaf()) {
      return filter;
    }
    FilterType filterType = filter.getFilterType();
    switch (filterType) {
      case KW_AND:
      case KW_OR:
        // replace children in-place for efficiency
        List<FilterOperator> children = filter.getChildren();
        if (children.size() < 2) {
          throw new LogicalOptimizeException(
              "Filter has some time series don't correspond to any known time series");
        }
        children.set(0, removeNot(children.get(0)));
        children.set(1, removeNot(children.get(1)));
        return filter;
      case KW_NOT:
        if (filter.getChildren().size() < 1) {
          throw new LogicalOptimizeException(
              "Filter has some time series don't correspond to any known time series");
        }
        return reverseFilter(filter.getChildren().get(0));
      default:
        throw new LogicalOptimizeException("removeNot", filterType);
    }
  }

  /**
   * reverse given filter to reversed expression.
   *
   * @param filter BasicFunctionOperator
   * @return FilterOperator reversed BasicFunctionOperator
   * @throws LogicalOptimizeException exception in reverse filter
   */
  private FilterOperator reverseFilter(FilterOperator filter) throws LogicalOperatorException {
    FilterType filterType = filter.getFilterType();
    if (filter.isLeaf()) {
      ((FunctionOperator) filter).reverseFunc();
      return filter;
    }
    switch (filterType) {
      case KW_AND:
      case KW_OR:
        List<FilterOperator> children = filter.getChildren();
        children.set(0, reverseFilter(children.get(0)));
        children.set(1, reverseFilter(children.get(1)));
        filter.setFilterType(FilterConstant.filterReverseWords.get(filterType));
        return filter;
      case KW_NOT:
        return removeNot(filter.getChildren().get(0));
      default:
        throw new LogicalOptimizeException("reverseFilter", filterType);
    }
  }
}
