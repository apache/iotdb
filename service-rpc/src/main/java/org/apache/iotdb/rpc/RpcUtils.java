/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.rpc;

import java.lang.reflect.Proxy;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

public class RpcUtils {

  private RpcUtils() {
    // util class
  }

  public static final TSStatus SUCCESS_STATUS = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

  public static TSIService.Iface newSynchronizedClient(TSIService.Iface client) {
    return (TSIService.Iface) Proxy.newProxyInstance(RpcUtils.class.getClassLoader(),
        new Class[]{TSIService.Iface.class}, new SynchronizedHandler(client));
  }

  /**
   * verify success.
   *
   * @param status -status
   */
  public static void verifySuccess(TSStatus status) throws StatementExecutionException {
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      verifySuccess(status.getSubStatus());
      return;
    }
    if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new StatementExecutionException(status);
    }
  }

  public static void verifySuccess(List<TSStatus> statuses) throws BatchExecutionException {
    for (TSStatus status : statuses) {
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new BatchExecutionException(statuses, status.message);
      }
    }
  }

  /**
   * convert from TSStatusCode to TSStatus according to status code and status message
   */
  public static TSStatus getStatus(TSStatusCode tsStatusCode) {
    return new TSStatus(tsStatusCode.getStatusCode());
  }

  public static TSStatus getStatus(List<TSStatus> statusList) {
    TSStatus status = new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode());
    status.setSubStatus(statusList);
    return status;
  }

  /**
   * convert from TSStatusCode to TSStatus, which has message appending with existed status message
   *
   * @param tsStatusCode    status type
   * @param message appending message
   */
  public static TSStatus getStatus(TSStatusCode tsStatusCode, String message) {
    TSStatus status = new TSStatus(tsStatusCode.getStatusCode());
    status.setMessage(message);
    return status;
  }

  public static TSStatus getStatus(int code, String message) {
    TSStatus status = new TSStatus(code);
    status.setMessage(message);
    return status;
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSExecuteStatementResp(status);
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatusCode tsStatusCode, String message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSExecuteStatementResp(status);
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatus status) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSFetchResultsResp(status);
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatusCode tsStatusCode, String appendMessage) {
    TSStatus status = getStatus(tsStatusCode, appendMessage);
    return getTSFetchResultsResp(status);
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatus status) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

}
