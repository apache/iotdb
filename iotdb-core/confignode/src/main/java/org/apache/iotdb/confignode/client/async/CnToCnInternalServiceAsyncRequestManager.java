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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeInternalServiceClient;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.client.request.ConfigNodeInternalServiceAsyncRequestManager;
import org.apache.iotdb.commons.client.request.TestConnectionUtils;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.ConfigNodeAsyncRequestRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.ConfigNodeTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SubmitTestConnectionTaskToConfigNodeRPCHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CnToCnInternalServiceAsyncRequestManager
    extends ConfigNodeInternalServiceAsyncRequestManager<CnToCnNodeRequestType> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CnToCnInternalServiceAsyncRequestManager.class);

  @Override
  protected void initActionMapBuilder() {
    actionMapBuilder.put(
        CnToCnNodeRequestType.SUBMIT_TEST_CONNECTION_TASK,
        (req, client, handler) ->
            client.submitTestConnectionTask(
                (TNodeLocations) req, (SubmitTestConnectionTaskToConfigNodeRPCHandler) handler));
    actionMapBuilder.put(
        CnToCnNodeRequestType.TEST_CONNECTION,
        (req, client, handler) ->
            client.testConnectionEmptyRPC((ConfigNodeTSStatusRPCHandler) handler));
  }

  @Override
  protected AsyncRequestRPCHandler<?, CnToCnNodeRequestType, TConfigNodeLocation> buildHandler(
      AsyncRequestContext<?, ?, CnToCnNodeRequestType, TConfigNodeLocation> requestContext,
      int requestId,
      TConfigNodeLocation targetNode) {
    return ConfigNodeAsyncRequestRPCHandler.buildHandler(requestContext, requestId, targetNode);
  }

  @Override
  protected void adjustClientTimeoutIfNecessary(
      CnToCnNodeRequestType cnToCnNodeRequestType, AsyncConfigNodeInternalServiceClient client) {
    if (CnToCnNodeRequestType.SUBMIT_TEST_CONNECTION_TASK.equals(cnToCnNodeRequestType)) {
      client.setTimeoutTemporarily(TestConnectionUtils.calculateCnLeaderToAllCnMaxTime());
    }
  }

  private static class ClientPoolHolder {
    private static final CnToCnInternalServiceAsyncRequestManager INSTANCE =
        new CnToCnInternalServiceAsyncRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static CnToCnInternalServiceAsyncRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
