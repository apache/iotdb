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

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** this class maintains information from select clause. */
public class SelectOperator extends Operator {

  protected final ZoneId zoneId;
  protected List<PartialPath> suffixList = new ArrayList<>();
  private List<UDFContext> udfList;

  private boolean udfQuery = false;

  /** init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>. */
  public SelectOperator(ZoneId zoneId) {
    super(SQLConstant.TOK_SELECT);
    operatorType = OperatorType.SELECT;
    this.zoneId = zoneId;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void addSelectPath(PartialPath suffixPath) {
    suffixList.add(suffixPath);
  }

  public void setSuffixPathList(List<PartialPath> suffixPaths) {
    suffixList = suffixPaths;
  }

  public List<PartialPath> getSuffixPaths() {
    return suffixList;
  }

  public void addUdf(UDFContext udf) {
    if (udf != null) {
      udfQuery = true;
    }
    if (udfList == null) {
      udfList = new ArrayList<>();
    }
    udfList.add(udf);
  }

  public List<UDFContext> getUdfList() {
    return udfList;
  }

  public boolean isUdfQuery() {
    return udfQuery;
  }

  public void setUdfList(List<UDFContext> udfList) {
    this.udfList = udfList;
  }

  public boolean isLastQuery() {
    return false;
  }

  public boolean hasAggregation() {
    return false; // todo: hasBuiltinAggregation || hasUDAF
  }

  // TODO: package it
  public List<String> getAggregations() {
    return new ArrayList<>();
  }

  public void setAggregations(List<String> aggregations) {}
}
