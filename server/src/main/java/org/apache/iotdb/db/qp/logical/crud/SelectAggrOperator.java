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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class SelectAggrOperator extends SelectOperator {

  private List<String> aggregations = new ArrayList<>();

  /** init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>. */
  public SelectAggrOperator(ZoneId zoneId) {
    super(zoneId);
  }

  public void addClusterPath(PartialPath suffixPath, String aggregation) {
    suffixList.add(suffixPath);
    if (aggregations == null) {
      aggregations = new ArrayList<>();
    }
    aggregations.add(aggregation);
  }

  public List<String> getAggregations() {
    return this.aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public boolean hasAggregation() {
    return true; // todo: hasBuiltinAggregation || hasUDAF
  }
}
