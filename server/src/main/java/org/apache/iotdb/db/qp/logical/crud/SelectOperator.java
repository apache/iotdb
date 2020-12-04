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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;

/**
 * this class maintains information from select clause.
 */
public final class SelectOperator extends Operator {

  private final ZoneId zoneId;
  private List<PartialPath> suffixList;
  private List<String> aggregations;
  private List<UDFContext> udfList;

  private boolean lastQuery;
  private boolean udfQuery;
  private boolean hasBuiltinAggregation;

  /**
   * init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>.
   */
  public SelectOperator(int tokenIntType, ZoneId zoneId) {
    super(tokenIntType);
    this.zoneId = zoneId;
    operatorType = OperatorType.SELECT;
    suffixList = new ArrayList<>();
    aggregations = new ArrayList<>();
    udfList = new ArrayList<>();
    lastQuery = false;
    udfQuery = false;
    hasBuiltinAggregation = false;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void addSelectPath(PartialPath suffixPath) {
    suffixList.add(suffixPath);
  }

  public void addClusterPath(PartialPath suffixPath, String aggregation) {
    suffixList.add(suffixPath);
    aggregations.add(aggregation);
    if (aggregation != null) {
      hasBuiltinAggregation = true;
    }
  }

  public boolean isLastQuery() {
    return this.lastQuery;
  }

  public void setLastQuery() {
    lastQuery = true;
  }

  public List<String> getAggregations() {
    return this.aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public boolean hasAggregation() {
    return hasBuiltinAggregation; // todo: hasBuiltinAggregation || hasUDAF
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
}
