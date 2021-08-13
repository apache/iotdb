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

import org.apache.iotdb.spark.tsfile.qp.common.FilterOperator;
import org.apache.iotdb.spark.tsfile.qp.exception.DNFOptimizeException;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.spark.tsfile.qp.common.SQLConstant.KW_AND;
import static org.apache.iotdb.spark.tsfile.qp.common.SQLConstant.KW_OR;

public class DNFFilterOptimizer implements IFilterOptimizer {

  /**
   * get DNF(disjunctive normal form) for this filter operator tree. Before invoking getDNF
   * function, make sure that operator tree must be binary tree. In other words, each non-leaf node
   * has exactly two children.
   *
   * @param filter filter operator to be optimized
   * @return FilterOperator
   * @throws DNFOptimizeException exception in DNF optimizing
   */
  @Override
  public FilterOperator optimize(FilterOperator filter) throws DNFOptimizeException {
    return getDNF(filter);
  }

  private FilterOperator getDNF(FilterOperator filter) throws DNFOptimizeException {
    if (filter.isLeaf()) {
      return filter;
    }
    List<FilterOperator> children = filter.getChildren();
    if (children.size() != 2) {
      throw new DNFOptimizeException(
          "node :" + filter.getTokenSymbol() + " has " + children.size() + " children");
    }
    FilterOperator left = getDNF(children.get(0));
    FilterOperator right = getDNF(children.get(1));
    List<FilterOperator> newChildrenList = new ArrayList<>();
    switch (filter.getTokenIntType()) {
      case KW_OR:
        addChildOpInOr(left, newChildrenList);
        addChildOpInOr(right, newChildrenList);
        break;
      case KW_AND:
        if (left.getTokenIntType() != KW_OR && right.getTokenIntType() != KW_OR) {
          addChildOpInAnd(left, newChildrenList);
          addChildOpInAnd(right, newChildrenList);
        } else {
          List<FilterOperator> leftAndChildren = getAndChild(left);
          List<FilterOperator> rightAndChildren = getAndChild(right);
          for (FilterOperator laChild : leftAndChildren) {
            for (FilterOperator raChild : rightAndChildren) {
              FilterOperator r = mergeToConjunction(laChild.clone(), raChild.clone());
              newChildrenList.add(r);
            }
          }
          filter.setTokenIntType(KW_OR);
        }
        break;
      default:
        throw new DNFOptimizeException(
            "get DNF failed, this tokenType is:" + filter.getTokenIntType());
    }
    filter.setChildrenList(newChildrenList);
    return filter;
  }

  /**
   * Merge two conjunction filter operators into one.<br>
   * conjunction operator consists of {@code FilterOperator} and inner operator which token is
   * KW_AND.<br>
   * e.g. (a and b) merge (c) is (a and b and c)
   *
   * @param a
   * @param b
   * @return FilterOperator
   * @throws DNFOptimizeException
   */
  private FilterOperator mergeToConjunction(FilterOperator a, FilterOperator b)
      throws DNFOptimizeException {
    List<FilterOperator> retChildrenList = new ArrayList<>();
    addChildOpInAnd(a, retChildrenList);
    addChildOpInAnd(b, retChildrenList);
    FilterOperator ret = new FilterOperator(KW_AND, false);
    ret.setChildrenList(retChildrenList);
    return ret;
  }

  /**
   * Obtain conjunction node according to input filter operator's token type. If token type ==
   * KW_OR, return its children. Otherwise, return a list contains input filter operator
   *
   * @param child
   * @return List<FilterOperator>
   */
  private List<FilterOperator> getAndChild(FilterOperator child) {
    switch (child.getTokenIntType()) {
      case KW_OR:
        return child.getChildren();
      default:
        // other token type means leaf node or "and" operator
        List<FilterOperator> ret = new ArrayList<>();
        ret.add(child);
        return ret;
    }
  }

  /**
   * @param child
   * @param newChildrenList
   * @throws DNFOptimizeException
   */
  private void addChildOpInAnd(FilterOperator child, List<FilterOperator> newChildrenList)
      throws DNFOptimizeException {
    if (child.isLeaf()) {
      newChildrenList.add(child);
    } else if (child.getTokenIntType() == KW_AND) {
      newChildrenList.addAll(child.getChildren());
    } else {
      throw new DNFOptimizeException(
          "add all children of an OR operator to newChildrenList in AND");
    }
  }

  /**
   * @param child
   * @param newChildrenList
   */
  private void addChildOpInOr(FilterOperator child, List<FilterOperator> newChildrenList) {
    if (child.isLeaf() || child.getTokenIntType() == KW_AND) {
      newChildrenList.add(child);
    } else {
      newChildrenList.addAll(child.getChildren());
    }
  }
}
