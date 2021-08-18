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
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/** basic operator includes = < > >= <= !=. */
public class BasicFunctionOperator extends FunctionOperator {

  protected String value;
  private Logger logger = LoggerFactory.getLogger(BasicFunctionOperator.class);
  private BasicOperatorType funcToken;

  /**
   * BasicFunctionOperator Constructor.
   *
   * @param filterType filter Type
   * @param path path
   * @param value value
   * @throws LogicalOperatorException Logical Operator Exception
   */
  public BasicFunctionOperator(FilterType filterType, PartialPath path, String value)
      throws SQLParserException {
    super(filterType);
    funcToken = BasicOperatorType.getBasicOpBySymbol(filterType);
    this.singlePath = path;
    this.value = value;
    isLeaf = true;
    isSingle = true;
  }

  public String getValue() {
    return value;
  }

  @Override
  public void reverseFunc() {
    FilterType filterType = FilterConstant.filterReverseWords.get(this.filterType);
    setFilterType(filterType);
    funcToken = BasicOperatorType.getBasicOpBySymbol(filterType);
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

    switch (type) {
      case INT32:
        ret = funcToken.getUnaryExpression(singlePath, Integer.valueOf(value));
        break;
      case INT64:
        ret = funcToken.getUnaryExpression(singlePath, Long.valueOf(value));
        break;
      case BOOLEAN:
        ret = funcToken.getUnaryExpression(singlePath, Boolean.valueOf(value));
        break;
      case FLOAT:
        ret = funcToken.getUnaryExpression(singlePath, Float.valueOf(value));
        break;
      case DOUBLE:
        ret = funcToken.getUnaryExpression(singlePath, Double.valueOf(value));
        break;
      case TEXT:
        if (funcToken.equals(BasicOperatorType.EQ)
            || funcToken.equals(BasicOperatorType.NOTEQUAL)) {
          ret =
              funcToken.getUnaryExpression(
                  singlePath,
                  (value.startsWith("'") && value.endsWith("'"))
                          || (value.startsWith("\"") && value.endsWith("\""))
                      ? new Binary(value.substring(1, value.length() - 1))
                      : new Binary(value));
        } else {
          throw new LogicalOperatorException(
              "For Basic operator,TEXT type only support EQUAL or NOTEQUAL operator");
        }
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
    sc.addTail(singlePath.getFullPath(), getFilterSymbol(), value, ", single\n");
    return sc.toString();
  }

  @Override
  public BasicFunctionOperator copy() {
    BasicFunctionOperator ret;
    try {
      ret =
          new BasicFunctionOperator(
              this.filterType, new PartialPath(singlePath.getNodes().clone()), value);
    } catch (SQLParserException e) {
      logger.error("error copy:", e);
      return null;
    }
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    ret.pathSet = pathSet;
    return ret;
  }

  @Override
  public String toString() {
    return "[" + singlePath.getFullPath() + getFilterSymbol() + value + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BasicFunctionOperator that = (BasicFunctionOperator) o;
    return Objects.equals(singlePath, that.singlePath)
        && Objects.equals(value, that.value)
        && funcToken == that.funcToken;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), singlePath, value, funcToken);
  }
}
