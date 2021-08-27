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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

/** Static responses that won't change for all requests. */
class StaticResps {

  private StaticResps() {
    // enum-like class
  }

  static final TSExecuteStatementResp LAST_RESP =
      getExecuteResp(
          Arrays.asList(COLUMN_TIMESERIES, COLUMN_VALUE, COLUMN_TIMESERIES_DATATYPE),
          Arrays.asList(
              TSDataType.TEXT.toString(), TSDataType.TEXT.toString(), TSDataType.TEXT.toString()),
          false);

  public static TSExecuteStatementResp getNoTimeExecuteResp(
      List<String> columns, List<String> dataTypes) {
    return getExecuteResp(columns, dataTypes, true);
  }

  private static TSExecuteStatementResp getExecuteResp(
      List<String> columns, List<String> dataTypes, boolean ignoreTimeStamp) {
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);
    resp.setIgnoreTimeStamp(ignoreTimeStamp);
    resp.setColumns(columns);
    resp.setDataTypeList(dataTypes);
    return resp;
  }
}
