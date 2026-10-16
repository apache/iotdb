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

package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataNodeTSStatusRPCHandlerTest {

  @Test
  public void testRegionOperationTerminalStatuses() {
    assertTrue(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.CREATE_DATA_REGION, status(TSStatusCode.SUCCESS_STATUS)));
    assertTrue(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.CREATE_DATA_REGION, status(TSStatusCode.REGION_ALREADY_EXISTS)));
    assertTrue(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.CREATE_SCHEMA_REGION,
            status(TSStatusCode.REGION_ALREADY_EXISTS)));
    assertTrue(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.DELETE_REGION, status(TSStatusCode.REGION_NOT_EXIST)));

    assertFalse(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.CREATE_SCHEMA_REGION, status(TSStatusCode.CREATE_REGION_ERROR)));
    assertFalse(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.DELETE_REGION, status(TSStatusCode.DELETE_REGION_ERROR)));
    assertFalse(
        DataNodeTSStatusRPCHandler.isRequestCompleted(
            CnToDnAsyncRequestType.SET_TTL, status(TSStatusCode.REGION_ALREADY_EXISTS)));
  }

  private static TSStatus status(TSStatusCode statusCode) {
    return new TSStatus(statusCode.getStatusCode());
  }
}
