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
package org.apache.iotdb.db.conf;

public enum OperationType {
  CLOSE_OPERATION("closeOperation"),
  FETCH_METADATA("fetchMetadata"),
  EXECUTE_STATEMENT("executeStatement"),
  EXECUTE_BATCH_STATEMENT("executeBatchStatement"),
  EXECUTE_QUERY_STATEMENT("executeQueryStatement"),
  EXECUTE_RAW_DATA_QUERY("executeRawDataQuery"),
  EXECUTE_LAST_DATA_QUERY("lastDataQueryReqToPhysicalPlan"),
  FETCH_RESULTS("fetchResults"),
  EXECUTE_UPDATE_STATEMENT("executeUpdateStatement"),
  GET_TIME_ZONE("getTimeZone"),
  SET_TIME_ZONE("setTimeZone"),
  INSERT_RECORDS("insertRecords"),
  INSERT_RECORDS_OF_ONE_DEVICE("insertRecordsOfOneDevice"),
  INSERT_STRING_RECORDS("insertStringRecords"),
  INSERT_RECORD("insertRecord"),
  INSERT_STRING_RECORD("insertStringRecord"),
  DELETE_DATA("deleteData"),
  INSERT_TABLET("insertTablet"),
  INSERT_TABLETS("insertTablets"),
  SET_STORAGE_GROUP("setStorageGroup"),
  DELETE_STORAGE_GROUPS("deleteStorageGroup"),
  CREATE_TIMESERIES("createTimeseries"),
  CREATE_ALIGNED_TIMESERIES("createAlignedTimeseries"),
  CREATE_MULTI_TIMESERIES("createMultiTimeseries"),
  DELETE_TIMESERIES("deleteTimeseries"),
  CREATE_SCHEMA_TEMPLATE("createSchemaTemplate"),
  CHECK_AUTHORITY("checkAuthority"),
  EXECUTE_NON_QUERY_PLAN("executeNonQueryPlan"),
  ;
  private final String name;

  OperationType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
