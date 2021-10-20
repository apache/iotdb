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
package org.apache.iotdb.db.cost.statistic;

public enum Operation {
  EXECUTE_JDBC_BATCH("EXECUTE_JDBC_BATCH"),
  EXECUTE_ONE_SQL_IN_BATCH("EXECUTE_ONE_SQL_IN_BATCH"),
  EXECUTE_ROWS_PLAN_IN_BATCH("EXECUTE_ROWS_PLAN_IN_BATCH"),
  EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH("EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH"),
  EXECUTE_RPC_BATCH_INSERT("EXECUTE_RPC_BATCH_INSERT"),
  EXECUTE_QUERY("EXECUTE_QUERY"),
  EXECUTE_SELECT_INTO("EXECUTE_SELECT_INTO");

  public String getName() {
    return name;
  }

  String name;

  Operation(String name) {
    this.name = name;
  }
}
