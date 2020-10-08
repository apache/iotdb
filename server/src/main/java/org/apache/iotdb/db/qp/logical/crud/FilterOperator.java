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
package org.apache.iotdb.db.qp.logical.crud;

import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_AND;
import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_OR;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * This class is for filter operator and implements {@link Operator} . It may consist of more than
 * two child FilterOperators, but if it's not a leaf operator, the relation is the same among all of
 * its children (AND or OR), which is identified by tokenType.
 */
public class FilterOperator extends Operator implements Comparable<FilterOperator> {

  // it is the symbol of token. e.g. AND is & and OR is |
  String tokenSymbol;

  private List<FilterOperator> childOperators;
  // leaf filter operator means it doesn't have left and right child filterOperator. Leaf filter
  // should set FunctionOperator.
  protected boolean isLeaf;
  // isSingle being true means all recursive children of this filter belong to one seriesPath.
  boolean isSingle = false;
  // if isSingle = false, singlePath must be null
  PartialPath singlePath = null;
  // all paths involved in this filter
  Set<PartialPath> pathSet;

  public FilterOperator(int tokenType) {
    super(tokenType);
    operatorType = OperatorType.FILTER;
    childOperators = new ArrayList<>();
    this.tokenIntType = tokenType;
    isLeaf = false;
    tokenSymbol = SQLConstant.tokenSymbol.get(tokenType);
  }

  public FilterOperator(int tokenType, boolean isSingle) {
    this(tokenType);
    this.isSingle = isSingle;
  }

  @Override
  public int getTokenIntType() {
    return tokenIntType;
  }

  public void setTokenIntType(int intType) {
    this.tokenIntType = intType;
    this.tokenName = SQLConstant.tokenNames.get(tokenIntType);
    this.tokenSymbol = SQLConstant.tokenSymbol.get(tokenIntType);
  }

  public List<FilterOperator> getChildren() {
    return childOperators;
  }

  public void setChildren(List<FilterOperator> children) {
    this.childOperators = children;
  }

  public void setIsSingle(boolean b) {
    this.isSingle = b;
  }

  public PartialPath getSinglePath() {
    return singlePath;
  }

  public void setSinglePath(PartialPath singlePath) {
    this.singlePath = singlePath;
  }

  public boolean addChildOperator(FilterOperator op) {
    childOperators.add(op);
    return true;
  }

  public void setPathSet(Set<PartialPath> pathSet) {
    this.pathSet = pathSet;
  }

  public Set<PartialPath> getPathSet() {
    return pathSet;
  }

  /**
   * For a filter operator, if isSingle, call transformToSingleQueryFilter.<br> FilterOperator
   * cannot be leaf.
   *
   * @return QueryFilter in TsFile
   * @param pathTSDataTypeHashMap
   */
  public IExpression transformToExpression(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap) throws QueryProcessException {
    if (isSingle) {
      Pair<IUnaryExpression, String> ret;
      try {
        ret = transformToSingleQueryFilter(pathTSDataTypeHashMap);
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
      return ret.left;
    } else {
      if (childOperators.isEmpty()) {
        throw new LogicalOperatorException(String.valueOf(tokenIntType),
            "this filter is not leaf, but it's empty");
      }
      IExpression retFilter = childOperators.get(0).transformToExpression(pathTSDataTypeHashMap);
      IExpression currentFilter;
      for (int i = 1; i < childOperators.size(); i++) {
        currentFilter = childOperators.get(i).transformToExpression(pathTSDataTypeHashMap);
        switch (tokenIntType) {
          case KW_AND:
            retFilter = BinaryExpression.and(retFilter, currentFilter);
            break;
          case KW_OR:
            retFilter = BinaryExpression.or(retFilter, currentFilter);
            break;
          default:
            throw new LogicalOperatorException(String.valueOf(tokenIntType),
                "Maybe it means " + SQLConstant.tokenNames.get(tokenIntType));
        }
      }
      return retFilter;
    }
  }

  /**
   * it will be used in BasicFunction Operator.
   *
   * @return - pair.left: UnaryQueryFilter constructed by its one child; pair.right: Path
   * represented by this child.
   * @throws MetadataException exception in filter transforming
   * @param pathTSDataTypeHashMap
   */
  protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap)
      throws LogicalOperatorException, MetadataException {
    if (childOperators.isEmpty()) {
      throw new LogicalOperatorException(String.valueOf(tokenIntType),
          "TransformToSingleFilter: this filter is not a leaf, but it's empty.");
    }
    Pair<IUnaryExpression, String> currentPair = childOperators.get(0)
        .transformToSingleQueryFilter(pathTSDataTypeHashMap);

    IUnaryExpression retFilter = currentPair.left;
    String path = currentPair.right;

    for (int i = 1; i < childOperators.size(); i++) {
      currentPair = childOperators.get(i).transformToSingleQueryFilter(pathTSDataTypeHashMap);
      if (!path.equals(currentPair.right)) {
        throw new LogicalOperatorException(
            "TransformToSingleFilter: paths among children are not inconsistent: one is: "
                + path + ", another is: " + currentPair.right);
      }
      switch (tokenIntType) {
        case KW_AND:
          retFilter.setFilter(FilterFactory.and(retFilter.getFilter(),
              currentPair.left.getFilter()));
          break;
        case KW_OR:
          retFilter.setFilter(FilterFactory.or(retFilter.getFilter(),
              currentPair.left.getFilter()));
          break;
        default:
          throw new LogicalOperatorException(String.valueOf(tokenIntType),
              "Maybe it means " + SQLConstant.tokenNames.get(tokenIntType));
      }
    }
    return new Pair<>(retFilter, path);
  }

  /**
   * a filter with null path is no smaller than any other filter.
   */
  @Override
  public int compareTo(FilterOperator fil) {
    if (singlePath == null && fil.singlePath == null) {
      return 0;
    }
    if (singlePath == null) {
      return 1;
    }
    if (fil.singlePath == null) {
      return -1;
    }
    return fil.singlePath.getFullPath().compareTo(singlePath.getFullPath());
  }

  @Override
  public boolean equals(Object fil) {
    if (!(fil instanceof FilterOperator)) {
      return false;
    }
    // if child is leaf, will execute BasicFunctionOperator.equals()
    FilterOperator operator = (FilterOperator) fil;
    return this.tokenIntType == operator.tokenIntType
        && this.getChildren().equals(operator.getChildren());
  }

  @Override
  public int hashCode() {
    return tokenSymbol.hashCode();
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public boolean isSingle() {
    return isSingle;
  }

  public String showTree() {
    return showTree(0);
  }

  public String showTree(int spaceNum) {
    StringContainer sc = new StringContainer();
    for (int i = 0; i < spaceNum; i++) {
      sc.addTail("  ");
    }
    sc.addTail(this.tokenName);
    if (isSingle) {
      sc.addTail("[single:", getSinglePath().getFullPath(), "]");
    }
    sc.addTail("\n");
    for (FilterOperator filter : childOperators) {
      sc.addTail(filter.showTree(spaceNum + 1));
    }
    return sc.toString();
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer();
    sc.addTail("[", this.tokenName);
    if (isSingle) {
      sc.addTail("[single:", getSinglePath().getFullPath(), "]");
    }
    sc.addTail(" ");
    for (FilterOperator filter : childOperators) {
      sc.addTail(filter.toString());
    }
    sc.addTail("]");
    return sc.toString();
  }

  public FilterOperator copy() {
    FilterOperator ret = new FilterOperator(this.tokenIntType);
    ret.tokenSymbol = tokenSymbol;
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    if (singlePath != null) {
      ret.singlePath = new PartialPath(singlePath.getNodes().clone());
    }
    for (FilterOperator filterOperator : this.childOperators) {
      ret.addChildOperator(filterOperator.copy());
    }
    return ret;
  }
}
