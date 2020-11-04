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

package org.apache.iotdb.db.service;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CANCELLED;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CHILD_PATHS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COLUMN;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COUNT;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CREATED_TIME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DEVICES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DONE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ITEM;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PRIVILEGE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PROGRESS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ROLE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TASK_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TTL;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_USER;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VERSION;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Static responses that won't change for all requests.
 */
class StaticResps {

  private StaticResps() {
    // enum-like class
  }

  static final TSExecuteStatementResp TTL_RESP = getNoTimeExecuteResp(
      Arrays.asList(COLUMN_STORAGE_GROUP, COLUMN_TTL),
      Arrays.asList(TSDataType.TEXT.toString(), TSDataType.INT64.toString()));

  static final TSExecuteStatementResp FLUSH_INFO_RESP = getNoTimeExecuteResp(
      Arrays.asList(COLUMN_ITEM, COLUMN_VALUE),
      Arrays.asList(TSDataType.TEXT.toString(), TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp SHOW_VERSION_RESP = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_VERSION),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp SHOW_TIMESERIES_RESP = getNoTimeExecuteResp(
      Arrays.asList(COLUMN_TIMESERIES, COLUMN_STORAGE_GROUP, COLUMN_TIMESERIES_DATATYPE,
          COLUMN_TIMESERIES_ENCODING, COLUMN_TIMESERIES_COMPRESSION),
      Arrays.asList(TSDataType.TEXT.toString(),
          TSDataType.TEXT.toString(),
          TSDataType.TEXT.toString(),
          TSDataType.TEXT.toString(),
          TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp SHOW_DEVICES = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_DEVICES),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp SHOW_STORAGE_GROUP = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_STORAGE_GROUP),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp SHOW_CHILD_PATHS = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_CHILD_PATHS),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp COUNT_TIMESERIES = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_COUNT),
      Collections.singletonList(TSDataType.INT32.toString()));

  static final TSExecuteStatementResp COUNT_NODE_TIMESERIES = getNoTimeExecuteResp(
      Arrays.asList(COLUMN_COLUMN, COLUMN_COUNT),
      Arrays.asList(TSDataType.TEXT.toString(), TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp COUNT_DEVICES = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_COUNT),
      Collections.singletonList(TSDataType.INT32.toString()));

  static final TSExecuteStatementResp COUNT_STORAGE_GROUP = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_COUNT),
      Collections.singletonList(TSDataType.INT32.toString()));

  static final TSExecuteStatementResp COUNT_NODES = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_COUNT),
      Collections.singletonList(TSDataType.INT32.toString()));

  static final TSExecuteStatementResp LIST_ROLE_RESP = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_ROLE),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp LIST_USER_RESP = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_USER),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp LIST_USER_PRIVILEGE_RESP = getNoTimeExecuteResp(
      Arrays.asList(COLUMN_ROLE, COLUMN_PRIVILEGE),
      Arrays.asList(TSDataType.TEXT.toString(), TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp LIST_ROLE_PRIVILEGE_RESP = getNoTimeExecuteResp(
      Collections.singletonList(COLUMN_PRIVILEGE),
      Collections.singletonList(TSDataType.TEXT.toString()));

  static final TSExecuteStatementResp LAST_RESP = getExecuteResp(
      Arrays.asList(COLUMN_TIMESERIES, COLUMN_VALUE),
      Arrays.asList(TSDataType.TEXT.toString(), TSDataType.TEXT.toString()), false
  );

  static final TSExecuteStatementResp MERGE_STATUS_RESP = getNoTimeExecuteResp(
      Arrays.asList(COLUMN_STORAGE_GROUP, COLUMN_TASK_NAME, COLUMN_CREATED_TIME, COLUMN_PROGRESS,
          COLUMN_CANCELLED, COLUMN_DONE),
      Arrays.asList(TSDataType.TEXT.toString(), TSDataType.TEXT.toString(),
          TSDataType.TEXT.toString(),
          TSDataType.TEXT.toString(), TSDataType.BOOLEAN.toString(), TSDataType.BOOLEAN.toString()));

  private static TSExecuteStatementResp getNoTimeExecuteResp(List<String> columns,
      List<String> dataTypes) {
    return getExecuteResp(columns, dataTypes, true);
  }

  private static TSExecuteStatementResp getExecuteResp(List<String> columns,
      List<String> dataTypes, boolean ignoreTimeStamp) {
    TSExecuteStatementResp resp =
        RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);
    resp.setIgnoreTimeStamp(ignoreTimeStamp);
    resp.setColumns(columns);
    resp.setDataTypeList(dataTypes);
    return resp;
  }
}
