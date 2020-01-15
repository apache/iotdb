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

package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SumAggrResult extends AvgAggrResult {

  private static final String SUM_AGGR_NAME = "SUM";

  public SumAggrResult(TSDataType seriesDataType) {
    super(seriesDataType);
    reset();
  }

  @Override
  public AggregateResult getResult() {
    setDoubleRet(sum);
    return this;
  }

  /**
   * Return type name of aggregation
   */
  @Override
  public String getAggreTypeName() {
    return SUM_AGGR_NAME;
  }
}
