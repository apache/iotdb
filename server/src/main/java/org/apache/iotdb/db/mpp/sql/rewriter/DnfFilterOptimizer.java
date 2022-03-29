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
package org.apache.iotdb.db.mpp.sql.rewriter;

import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant.FilterType;

import java.util.ArrayList;
import java.util.List;

public class DnfFilterOptimizer implements IFilterOptimizer {

  /**
   * get DNF(disjunctive normal form) for this filter operator tree. Before getDnf, this op tree
   * must be binary, in another word, each non-leaf node has exactly two children.
   *
   * @return QueryFilter optimized filter
   * @throws StatementAnalyzeException exception in DNF optimize
   */
  @Override
  public QueryFilter optimize(QueryFilter filter) throws StatementAnalyzeException {
    return getDnf(filter);
  }

  private void dealWithLeftAndRightAndChildren(
      List<QueryFilter> leftAndChildren,
      List<QueryFilter> rightAndChildren,
      List<QueryFilter> newChildrenList)
      throws StatementAnalyzeException {
    for (QueryFilter leftAndChild : leftAndChildren) {
      for (QueryFilter rightAndChild : rightAndChildren) {
        QueryFilter r = mergeToConjunction(leftAndChild.copy(), rightAndChild.copy());
        newChildrenList.add(r);
      }
    }
  }

  private QueryFilter getDnf(QueryFilter filter) throws StatementAnalyzeException {
    if (filter.isLeaf()) {
      return filter;
    }
    List<QueryFilter> childOperators = filter.getChildren();
    if (childOperators.size() != 2) {
      throw new StatementAnalyzeException(
          "node :" + filter.getFilterName() + " has " + childOperators.size() + " children");
    }
    QueryFilter left = getDnf(childOperators.get(0));
    QueryFilter right = getDnf(childOperators.get(1));
    List<QueryFilter> newChildrenList = new ArrayList<>();
    switch (filter.getFilterType()) {
      case KW_OR:
        addChildOpInOr(left, newChildrenList);
        addChildOpInOr(right, newChildrenList);
        break;
      case KW_AND:
        if (left.getFilterType() != FilterType.KW_OR && right.getFilterType() != FilterType.KW_OR) {
          addChildOpInAnd(left, newChildrenList);
          addChildOpInAnd(right, newChildrenList);
        } else {
          dealWithLeftAndRightAndChildren(getAndChild(left), getAndChild(right), newChildrenList);
          filter.setFilterType(FilterType.KW_OR);
        }
        break;
      default:
        throw new StatementAnalyzeException(
            "get DNF failed, this tokenType is:" + filter.getFilterType());
    }
    filter.setChildren(newChildrenList);
    return filter;
  }

  /**
   * used by getDnf. merge two conjunction filter operators into a conjunction.<br>
   * conjunction operator consists of {@code FunctionOperator} and inner operator which token is
   * KW_AND.<br>
   * e.g. (a and b) merge (c) is (a and b and c)
   *
   * @param operator1 To be merged operator
   * @param operator2 To be merged operator
   * @return merged operator
   * @throws StatementAnalyzeException exception in DNF optimizing
   */
  private QueryFilter mergeToConjunction(QueryFilter operator1, QueryFilter operator2)
      throws StatementAnalyzeException {
    List<QueryFilter> retChildrenList = new ArrayList<>();
    addChildOpInAnd(operator1, retChildrenList);
    addChildOpInAnd(operator2, retChildrenList);
    QueryFilter ret = new QueryFilter(FilterType.KW_AND, false);
    ret.setChildren(retChildrenList);
    return ret;
  }

  /**
   * used by getDnf. get conjunction node. <br>
   * If child is basic function or AND node, return a list just containing this. <br>
   * If this child is OR, return children of OR.
   *
   * @param child operator
   * @return children operator
   */
  private List<QueryFilter> getAndChild(QueryFilter child) {
    if (child.getFilterType() == FilterType.KW_OR) {
      return child.getChildren();
    } else {
      // other token type means leaf node or and
      List<QueryFilter> ret = new ArrayList<>();
      ret.add(child);
      return ret;
    }
  }

  /**
   * If operator is leaf, add it in newChildrenList. If operator is And, add its children to
   * newChildrenList.
   *
   * @param operator which children should be added in new children list
   * @param newChildrenList new children list
   * @throws StatementAnalyzeException exception in DNF optimizing
   */
  private void addChildOpInAnd(QueryFilter operator, List<QueryFilter> newChildrenList)
      throws StatementAnalyzeException {
    if (operator.isLeaf()) {
      newChildrenList.add(operator);
    } else if (operator.getFilterType() == FilterType.KW_AND) {
      newChildrenList.addAll(operator.getChildren());
    } else {
      throw new StatementAnalyzeException(
          "add all children of an OR operator to newChildrenList in AND");
    }
  }

  /**
   * used by getDnf. If operator is leaf or And, add operator to newChildrenList. Else add
   * operator's children to newChildrenList
   *
   * @param operator to be added in new children list
   * @param newChildrenList new children list
   */
  private void addChildOpInOr(QueryFilter operator, List<QueryFilter> newChildrenList) {
    if (operator.isLeaf() || operator.getFilterType() == FilterType.KW_AND) {
      newChildrenList.add(operator);
    } else {
      newChildrenList.addAll(operator.getChildren());
    }
  }
}
