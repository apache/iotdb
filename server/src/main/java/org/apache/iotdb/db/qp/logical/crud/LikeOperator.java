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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

/** fuzzy query structure LikeOperator. */
public class LikeOperator extends FunctionOperator {

  private boolean not;
  protected String value;

  public LikeOperator(FilterType filterType, PartialPath path, String value, boolean not) {
    super(filterType);
    this.singlePath = path;
    this.value = value;
    this.not = not;
    isLeaf = true;
    isSingle = true;
  }

  @Override
  protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap)
      throws LogicalOperatorException, MetadataException {
    TSDataType type = pathTSDataTypeHashMap.get(singlePath);
    if (type == null) {
      throw new MetadataException(
          "given seriesPath:{" + singlePath.getFullPath() + "} don't exist in metadata");
    }
    IUnaryExpression ret;
    if (type != TEXT) {
      throw new LogicalOperatorException(type.toString(), "Only TEXT is supported in 'Like'");
    } else if (value.startsWith("\"") && value.endsWith("\"")) {
      throw new LogicalOperatorException(value, "Please use single quotation marks");
    } else {
      ret =
          Like.getUnaryExpression(
              singlePath,
              (value.startsWith("'") && value.endsWith("'"))
                  ? value.substring(1, value.length() - 1)
                  : value,
              not);
    }
    return new Pair<>(ret, singlePath.getFullPath());
  }

  private static class Like {
    public static <T extends Comparable<T>> IUnaryExpression getUnaryExpression(
        PartialPath path, String value, boolean not) {
      return new SingleSeriesExpression(path, ValueFilter.like(value, not));
    }

    public <T extends Comparable<T>> Filter getValueFilter(String value, boolean not) {
      return ValueFilter.like(value, not);
    }
  }

  @Override
  public String showTree(int spaceNum) {
    StringContainer sc = new StringContainer();
    for (int i = 0; i < spaceNum; i++) {
      sc.addTail("  ");
    }
    sc.addTail(singlePath.getFullPath(), getFilterSymbol(), not, value, ", single\n");
    return sc.toString();
  }

  @Override
  public LikeOperator copy() {
    LikeOperator ret = new LikeOperator(this.filterType, singlePath.clone(), value, not);
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    ret.pathSet = pathSet;
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LikeOperator that = (LikeOperator) o;
    return Objects.equals(value, that.value) && not == that.not;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), singlePath, value, not);
  }

  @Override
  public String toString() {
    return "["
        + singlePath.getFullPath()
        + (not ? " NOT " : " ")
        + getFilterSymbol()
        + " "
        + value
        + "]";
  }

  @Override
  public void reverseFunc() {
    not = !not;
  }

  public String getValue() {
    return value;
  }

  public boolean isNot() {
    return not;
  }
}
