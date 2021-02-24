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
package org.apache.iotdb.spark.tsfile.qp.optimizer;

import org.apache.iotdb.spark.tsfile.qp.common.BasicOperator;
import org.apache.iotdb.spark.tsfile.qp.common.FilterOperator;
import org.apache.iotdb.spark.tsfile.qp.common.SQLConstant;
import org.apache.iotdb.spark.tsfile.qp.exception.RemoveNotException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.iotdb.spark.tsfile.qp.common.SQLConstant.KW_AND;
import static org.apache.iotdb.spark.tsfile.qp.common.SQLConstant.KW_NOT;
import static org.apache.iotdb.spark.tsfile.qp.common.SQLConstant.KW_OR;

public class RemoveNotOptimizer implements IFilterOptimizer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveNotOptimizer.class);

  /**
   * get DNF(disjunctive normal form) for this filter operator tree. Before getDNF, this op tree
   * must be binary, in another word, each non-leaf node has exactly two children.
   *
   * @param filter filter operator to be optimized
   * @return optimized filter operator
   * @throws RemoveNotException excepiton in remove not optimizing
   */
  @Override
  public FilterOperator optimize(FilterOperator filter) throws RemoveNotException {
    return removeNot(filter);
  }

  private FilterOperator removeNot(FilterOperator filter) throws RemoveNotException {
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
        throw new RemoveNotException(
            "Unknown token in removeNot: " + tokenInt + "," + SQLConstant.tokenNames.get(tokenInt));
    }
  }

  private FilterOperator reverseFilter(FilterOperator filter) throws RemoveNotException {
    int tokenInt = filter.getTokenIntType();
    if (filter.isLeaf()) {
      ((BasicOperator) filter).setReversedTokenIntType();
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
        throw new RemoveNotException(
            "Unknown token in reverseFilter: "
                + tokenInt
                + ","
                + SQLConstant.tokenNames.get(tokenInt));
    }
  }
}
