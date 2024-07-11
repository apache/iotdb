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

package org.apache.iotdb.db.protocol.client.cn;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeInternalServiceClient;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.client.request.ConfigNodeInternalServiceAsyncRequestManager;
import org.apache.iotdb.commons.client.request.TestConnectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DnToCnInternalServiceAsyncRequestManager
    extends ConfigNodeInternalServiceAsyncRequestManager<DnToCnRequestType> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DnToCnInternalServiceAsyncRequestManager.class);

  @Override
  protected void initActionMapBuilder() {
    actionMapBuilder.put(
        DnToCnRequestType.TEST_CONNECTION,
        (req, client, handler) ->
            client.testConnectionEmptyRPC((AsyncConfigNodeTSStatusRPCHandler) handler));
  }

  @Override
  protected AsyncRequestRPCHandler<?, DnToCnRequestType, TConfigNodeLocation> buildHandler(
      AsyncRequestContext<?, ?, DnToCnRequestType, TConfigNodeLocation> requestContext,
      int requestId,
      TConfigNodeLocation targetNode) {
    return ConfigNodeAsyncRequestRPCHandler.buildHandler(requestContext, requestId, targetNode);
  }

  @Override
  protected void adjustClientTimeoutIfNecessary(
      DnToCnRequestType dnToCnRequestType, AsyncConfigNodeInternalServiceClient client) {
    if (DnToCnRequestType.SUBMIT_TEST_CONNECTION_TASK.equals(dnToCnRequestType)) {
      client.setTimeoutTemporarily(TestConnectionUtils.calculateCnLeaderToAllNodeMaxTime());
    }
  }

  private static class ClientPoolHolder {
    private static final DnToCnInternalServiceAsyncRequestManager INSTANCE =
        new DnToCnInternalServiceAsyncRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static DnToCnInternalServiceAsyncRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
