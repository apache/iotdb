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
package org.apache.iotdb.spark.tsfile.qp.common;

/** basic operators include < > >= <= !=. */
public class BasicOperator extends FilterOperator {

  private String seriesPath;
  private String seriesValue;

  public BasicOperator(int tokenIntType, String path, String value) {
    super(tokenIntType);
    this.seriesPath = this.singlePath = path;
    this.seriesValue = value;
    this.isLeaf = true;
    this.isSingle = true;
  }

  public String getSeriesPath() {
    return seriesPath;
  }

  public String getSeriesValue() {
    return seriesValue;
  }

  public void setReversedTokenIntType() {
    int intType = SQLConstant.reverseWords.get(tokenIntType);
    setTokenIntType(intType);
  }

  @Override
  public String getSinglePath() {
    return singlePath;
  }

  @Override
  public BasicOperator clone() {
    BasicOperator ret = new BasicOperator(this.tokenIntType, seriesPath, seriesValue);
    ret.tokenSymbol = tokenSymbol;
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    return ret;
  }

  @Override
  public String toString() {
    return "[" + seriesPath + tokenSymbol + seriesValue + "]";
  }
}
