/**
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

import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_AND;
import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_NOT;
import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_OR;

import java.util.List;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

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
    return removeNot(filter);
  }

  private FilterOperator removeNot(FilterOperator filter) throws LogicalOperatorException {
    if (filter.isLeaf()) {
      return filter;
    }
    int tokenInt = filter.getTokenIntType();
    switch (tokenInt) {
      case KW_AND:
      case KW_OR:
        // replace children in-place for efficiency
        List<FilterOperator> children = filter.getChildren();
        children.set(0, removeNot(children.get(0)));
        children.set(1, removeNot(children.get(1)));
        return filter;
      case KW_NOT:
        return reverseFilter(filter.getChildren().get(0));
      default:
        throw new LogicalOptimizeException(
            "Unknown token in removeNot: " + tokenInt + "," + SQLConstant.tokenNames.get(tokenInt));
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
    int tokenInt = filter.getTokenIntType();
    if (filter.isLeaf()) {
      try {
        ((BasicFunctionOperator) filter).setReversedTokenIntType();
      } catch (LogicalOperatorException e) {
        throw new LogicalOperatorException(
            "convert BasicFuntion to reserved meet failed: previous token:"
                + tokenInt + "tokenName:" + SQLConstant.tokenNames.get(tokenInt));
      }
      return filter;
    }
    switch (tokenInt) {
      case KW_AND:
      case KW_OR:
        List<FilterOperator> children = filter.getChildren();
        children.set(0, reverseFilter(children.get(0)));
        children.set(1, reverseFilter(children.get(1)));
        filter.setTokenIntType(SQLConstant.reverseWords.get(tokenInt));
        return filter;
      case KW_NOT:
        return removeNot(filter.getChildren().get(0));
      default:
        throw new LogicalOptimizeException(
            "Unknown token in reverseFilter: " + tokenInt + "," + SQLConstant.tokenNames
                .get(tokenInt));
    }
  }

}
