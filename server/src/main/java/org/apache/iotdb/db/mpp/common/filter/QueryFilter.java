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
package org.apache.iotdb.db.mpp.common.filter;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.plan.constant.FilterConstant;
import org.apache.iotdb.db.mpp.plan.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is for filter in where clause. It may consist of more than two child filters, but if
 * it's not a leaf filter, the relation is the same among all of its children (AND or OR), which is
 * identified by tokenType.
 */
public class QueryFilter implements Comparable<QueryFilter> {

  protected FilterType filterType;

  private List<QueryFilter> childOperators = new ArrayList<>();
  // leaf filter operator means it doesn't have left and right child filterOperator. Leaf filter
  // should set FunctionOperator.
  protected boolean isLeaf = false;
  // isSingle being true means all recursive children of this filter belong to one seriesPath.
  boolean isSingle = false;
  // if isSingle = false, singlePath must be null
  PartialPath singlePath = null;
  // all paths involved in this filter
  Set<PartialPath> pathSet;

  public QueryFilter() {}

  public QueryFilter(FilterType filterType) {
    this.filterType = filterType;
  }

  public QueryFilter(FilterType filterType, boolean isSingle) {
    this.filterType = filterType;
    this.isSingle = isSingle;
  }

  public FilterType getFilterType() {
    return filterType;
  }

  public void setFilterType(FilterType filterType) {
    this.filterType = filterType;
  }

  public String getFilterName() {
    return FilterConstant.filterNames.get(filterType);
  }

  public String getFilterSymbol() {
    return FilterConstant.filterSymbol.get(filterType);
  }

  public List<QueryFilter> getChildren() {
    return childOperators;
  }

  public void setChildren(List<QueryFilter> children) {
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

  public void addChildOperator(QueryFilter op) {
    childOperators.add(op);
  }

  public void setPathSet(Set<PartialPath> pathSet) {
    this.pathSet = pathSet;
  }

  public Set<PartialPath> getPathSet() {
    return pathSet;
  }

  /**
   * For a filter operator, if isSingle, call transformToSingleQueryFilter.<br>
   * FilterOperator cannot be leaf.
   *
   * @return QueryFilter in TsFile
   * @param pathTSDataTypeHashMap
   */
  public IExpression transformToExpression(Map<PartialPath, TSDataType> pathTSDataTypeHashMap)
      throws StatementAnalyzeException {
    if (isSingle) {
      Pair<IUnaryExpression, String> ret;
      try {
        ret = transformToSingleQueryFilter(pathTSDataTypeHashMap);
      } catch (MetadataException e) {
        throw new StatementAnalyzeException("Meet error when transformToSingleQueryFilter");
      }
      return ret.left;
    } else {
      if (childOperators.isEmpty()) {
        throw new StatementAnalyzeException(
            String.valueOf(filterType), "this filter is not leaf, but it's empty");
      }
      IExpression retFilter = childOperators.get(0).transformToExpression(pathTSDataTypeHashMap);
      IExpression currentFilter;
      for (int i = 1; i < childOperators.size(); i++) {
        currentFilter = childOperators.get(i).transformToExpression(pathTSDataTypeHashMap);
        switch (filterType) {
          case KW_AND:
            retFilter = BinaryExpression.and(retFilter, currentFilter);
            break;
          case KW_OR:
            retFilter = BinaryExpression.or(retFilter, currentFilter);
            break;
          default:
            throw new StatementAnalyzeException(
                String.valueOf(filterType), "Maybe it means " + getFilterName());
        }
      }
      return retFilter;
    }
  }

  /**
   * it will be used in BasicFunction Operator.
   *
   * @return - pair.left: UnaryQueryFilter constructed by its one child; pair.right: Path
   *     represented by this child.
   * @throws MetadataException exception in filter transforming
   * @param pathTSDataTypeHashMap
   */
  protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap)
      throws StatementAnalyzeException, MetadataException {
    if (childOperators.isEmpty()) {
      throw new StatementAnalyzeException(
          String.valueOf(filterType),
          "TransformToSingleFilter: this filter is not a leaf, but it's empty.");
    }
    Pair<IUnaryExpression, String> currentPair =
        childOperators.get(0).transformToSingleQueryFilter(pathTSDataTypeHashMap);

    IUnaryExpression retFilter = currentPair.left;
    String path = currentPair.right;

    for (int i = 1; i < childOperators.size(); i++) {
      currentPair = childOperators.get(i).transformToSingleQueryFilter(pathTSDataTypeHashMap);
      if (!path.equals(currentPair.right)) {
        throw new StatementAnalyzeException(
            "TransformToSingleFilter: paths among children are not inconsistent: one is: "
                + path
                + ", another is: "
                + currentPair.right);
      }
      switch (filterType) {
        case KW_AND:
          retFilter.setFilter(
              FilterFactory.and(retFilter.getFilter(), currentPair.left.getFilter()));
          break;
        case KW_OR:
          retFilter.setFilter(
              FilterFactory.or(retFilter.getFilter(), currentPair.left.getFilter()));
          break;
        default:
          throw new StatementAnalyzeException(
              String.valueOf(filterType), "Maybe it means " + getFilterName());
      }
    }
    return new Pair<>(retFilter, path);
  }

  /** a filter with null path is no smaller than any other filter. */
  @Override
  public int compareTo(QueryFilter fil) {
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
    if (!(fil instanceof QueryFilter)) {
      return false;
    }
    // if child is leaf, will execute BasicFunctionOperator.equals()
    QueryFilter operator = (QueryFilter) fil;
    return this.filterType == operator.filterType
        && this.getChildren().equals(operator.getChildren());
  }

  @Override
  public int hashCode() {
    return getFilterSymbol().hashCode();
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
    sc.addTail(getFilterName());
    if (isSingle) {
      sc.addTail("[single:", getSinglePath().getFullPath(), "]");
    }
    sc.addTail("\n");
    for (QueryFilter filter : childOperators) {
      sc.addTail(filter.showTree(spaceNum + 1));
    }
    return sc.toString();
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer();
    sc.addTail("[", FilterConstant.filterNames.get(filterType));
    if (isSingle) {
      sc.addTail("[single:", getSinglePath().getFullPath(), "]");
    }
    sc.addTail(" ");
    for (QueryFilter filter : childOperators) {
      sc.addTail(filter.toString());
    }
    sc.addTail("]");
    return sc.toString();
  }

  public QueryFilter copy() {
    QueryFilter ret = new QueryFilter(this.filterType);
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    if (singlePath != null) {
      ret.singlePath = singlePath.clone();
    }
    for (QueryFilter filterOperator : this.childOperators) {
      ret.addChildOperator(filterOperator.copy());
    }
    return ret;
  }

  public void serialize(ByteBuffer byteBuffer) {
    FilterTypes.Query.serialize(byteBuffer);
    serializeWithoutType(byteBuffer);
  }

  protected void serializeWithoutType(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(filterType.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(childOperators.size(), byteBuffer);
    for (QueryFilter queryFilter : childOperators) {
      queryFilter.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(isLeaf, byteBuffer);
    ReadWriteIOUtils.write(isSingle, byteBuffer);
    if (isSingle) {
      singlePath.serialize(byteBuffer);
    }
    if (pathSet == null) {
      ReadWriteIOUtils.write(-1, byteBuffer);
    } else {
      ReadWriteIOUtils.write(pathSet.size(), byteBuffer);
      for (PartialPath partialPath : pathSet) {
        partialPath.serialize(byteBuffer);
      }
    }
  }

  public static QueryFilter deserialize(ByteBuffer byteBuffer) {
    int filterTypeIndex = ReadWriteIOUtils.readInt(byteBuffer);
    int childSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<QueryFilter> queryFilters = new ArrayList<>();
    for (int i = 0; i < childSize; i++) {
      queryFilters.add(FilterDeserializeUtil.deserialize(byteBuffer));
    }
    boolean isLeaf = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isSingle = ReadWriteIOUtils.readBool(byteBuffer);
    PartialPath singlePath = null;
    if (isSingle) {
      singlePath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    }
    int pathSetSize = ReadWriteIOUtils.readInt(byteBuffer);
    Set<PartialPath> pathSet = null;
    if (pathSetSize != -1) {
      pathSet = new HashSet<>();
    }
    for (int i = 0; i < pathSetSize; i++) {
      pathSet.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }

    QueryFilter queryFilter = new QueryFilter(FilterType.values()[filterTypeIndex], isSingle);
    queryFilter.setChildren(queryFilters);
    queryFilter.setPathSet(pathSet);
    queryFilter.setSinglePath(singlePath);
    queryFilter.isLeaf = isLeaf;
    return queryFilter;
  }
}
