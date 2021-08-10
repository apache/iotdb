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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** operator 'in' 'not in' */
public class InOperator extends FunctionOperator {

  private boolean not;
  protected Set<String> values;

  /**
   * In Operator Constructor.
   *
   * @param filterType filter Type
   * @param path path
   * @param values values
   */
  public InOperator(FilterType filterType, PartialPath path, boolean not, Set<String> values) {
    super(filterType);
    this.singlePath = path;
    this.values = values;
    this.not = not;
    isLeaf = true;
    isSingle = true;
  }

  public Set<String> getValues() {
    return values;
  }

  public boolean getNot() {
    return not;
  }

  @Override
  public void reverseFunc() {
    not = !not;
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap)
      throws LogicalOperatorException, MetadataException {
    TSDataType type = pathTSDataTypeHashMap.get(singlePath);
    if (type == null) {
      throw new MetadataException(
          "given seriesPath:{" + singlePath.getFullPath() + "} don't exist in metadata");
    }
    IUnaryExpression ret;

    switch (type) {
      case INT32:
        Set<Integer> integerValues = new HashSet<>();
        for (String val : values) {
          integerValues.add(Integer.valueOf(val));
        }
        ret = In.getUnaryExpression(singlePath, integerValues, not);
        break;
      case INT64:
        Set<Long> longValues = new HashSet<>();
        for (String val : values) {
          longValues.add(Long.valueOf(val));
        }
        ret = In.getUnaryExpression(singlePath, longValues, not);
        break;
      case BOOLEAN:
        Set<Boolean> booleanValues = new HashSet<>();
        for (String val : values) {
          booleanValues.add(Boolean.valueOf(val));
        }
        ret = In.getUnaryExpression(singlePath, booleanValues, not);
        break;
      case FLOAT:
        Set<Float> floatValues = new HashSet<>();
        for (String val : values) {
          floatValues.add(Float.parseFloat(val));
        }
        ret = In.getUnaryExpression(singlePath, floatValues, not);
        break;
      case DOUBLE:
        Set<Double> doubleValues = new HashSet<>();
        for (String val : values) {
          doubleValues.add(Double.parseDouble(val));
        }
        ret = In.getUnaryExpression(singlePath, doubleValues, not);
        break;
      case TEXT:
        Set<Binary> binaryValues = new HashSet<>();
        for (String val : values) {
          binaryValues.add(
              (val.startsWith("'") && val.endsWith("'"))
                      || (val.startsWith("\"") && val.endsWith("\""))
                  ? new Binary(val.substring(1, val.length() - 1))
                  : new Binary(val));
        }
        ret = In.getUnaryExpression(singlePath, binaryValues, not);
        break;
      default:
        throw new LogicalOperatorException(type.toString(), "");
    }

    return new Pair<>(ret, singlePath.getFullPath());
  }

  @Override
  public String showTree(int spaceNum) {
    StringContainer sc = new StringContainer();
    for (int i = 0; i < spaceNum; i++) {
      sc.addTail("  ");
    }
    sc.addTail(singlePath.getFullPath(), getFilterSymbol(), not, values, ", single\n");
    return sc.toString();
  }

  @Override
  public InOperator copy() {
    InOperator ret =
        new InOperator(
            this.filterType,
            new PartialPath(singlePath.getNodes().clone()),
            not,
            new HashSet<>(values));
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    ret.pathSet = pathSet;
    return ret;
  }

  @Override
  public String toString() {
    List<String> valuesList = new ArrayList<>(values);
    Collections.sort(valuesList);
    return "[" + singlePath.getFullPath() + getFilterSymbol() + not + valuesList + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InOperator that = (InOperator) o;
    return Objects.equals(singlePath, that.singlePath)
        && values.containsAll(that.values)
        && values.size() == that.values.size()
        && not == that.not;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), singlePath, not, values);
  }

  private static class In {

    public static <T extends Comparable<T>> IUnaryExpression getUnaryExpression(
        Path path, Set<T> values, boolean not) {
      if (path.equals("time")) {
        return new GlobalTimeExpression(TimeFilter.in((Set<Long>) values, not));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.in(values, not));
      }
    }

    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.notEq(value);
    }
  }
}
