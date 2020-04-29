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

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * this class extends {@code RootOperator} and process delete statement.
 */
public class DeleteDataOperator extends SFWOperator {

  private Filter timeFilter;
  private long minTime = Long.MIN_VALUE;
  private long maxTime = Long.MAX_VALUE;

  public DeleteDataOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = Operator.OperatorType.DELETE;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public long getMinTime() {
    return minTime;
  }

  public void setMinTime(long minTime) {
    this.minTime = minTime;
  }

  public long getMaxTime() {
    return maxTime;
  }

  public void setMaxTime(long maxTime) {
    this.maxTime = maxTime;
  }
}
