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
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.plan.constant.FilterConstant;
import org.apache.iotdb.db.mpp.plan.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/** basic operator includes = < > >= <= !=. */
public class BasicFunctionFilter extends FunctionFilter {

  protected String value;
  private final Logger logger = LoggerFactory.getLogger(BasicFunctionFilter.class);
  private BasicFilterType funcToken;

  /**
   * BasicFunctionFilter Constructor.
   *
   * @param filterType filter Type
   * @param path path
   * @param value value
   * @throws SQLParserException SQL Parser Exception
   */
  public BasicFunctionFilter(FilterType filterType, PartialPath path, String value)
      throws SQLParserException {
    super(filterType);
    funcToken = BasicFilterType.getBasicOpBySymbol(filterType);
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
    funcToken = BasicFilterType.getBasicOpBySymbol(filterType);
  }

  @Override
  protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap)
      throws StatementAnalyzeException, MetadataException {
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
        if (funcToken.equals(BasicFilterType.EQ) || funcToken.equals(BasicFilterType.NOTEQUAL)) {
          ret =
              funcToken.getUnaryExpression(
                  singlePath,
                  (value.startsWith("'") && value.endsWith("'"))
                          || (value.startsWith("\"") && value.endsWith("\""))
                      ? new Binary(value.substring(1, value.length() - 1))
                      : new Binary(value));
        } else {
          throw new StatementAnalyzeException(
              "For Basic operator,TEXT type only support EQUAL or NOTEQUAL operator");
        }
        break;
      default:
        throw new StatementAnalyzeException(type.toString(), "");
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
  public BasicFunctionFilter copy() {
    BasicFunctionFilter ret;
    try {
      ret = new BasicFunctionFilter(this.filterType, singlePath.clone(), value);
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
    BasicFunctionFilter that = (BasicFunctionFilter) o;
    return Objects.equals(singlePath, that.singlePath)
        && Objects.equals(value, that.value)
        && funcToken == that.funcToken;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), singlePath, value, funcToken);
  }

  public void serialize(ByteBuffer byteBuffer) {
    FilterTypes.BasicFunction.serialize(byteBuffer);
    super.serializeWithoutType(byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
    ReadWriteIOUtils.write(funcToken.ordinal(), byteBuffer);
  }

  public static BasicFunctionFilter deserialize(ByteBuffer byteBuffer) {
    QueryFilter queryFilter = QueryFilter.deserialize(byteBuffer);
    BasicFunctionFilter basicFunctionFilter =
        new BasicFunctionFilter(
            queryFilter.filterType,
            queryFilter.singlePath,
            ReadWriteIOUtils.readString(byteBuffer));
    basicFunctionFilter.funcToken = BasicFilterType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    return basicFunctionFilter;
  }
}
