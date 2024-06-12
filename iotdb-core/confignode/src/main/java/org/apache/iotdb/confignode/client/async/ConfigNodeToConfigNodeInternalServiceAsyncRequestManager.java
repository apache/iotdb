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
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.client.request.ConfigNodeInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.ConfigNodeToConfigNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.rpc.ConfigNodeAsyncRequestRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.ConfigNodeTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.SubmitTestConnectionTaskToConfigNodeRPCHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigNodeToConfigNodeInternalServiceAsyncRequestManager
    extends ConfigNodeInternalServiceAsyncRequestManager<ConfigNodeToConfigNodeRequestType> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConfigNodeToConfigNodeInternalServiceAsyncRequestManager.class);

  @Override
  protected void initActionMapBuilder() {
    actionMapBuilder.put(
        ConfigNodeToConfigNodeRequestType.SUBMIT_TEST_CONNECTION_TASK,
        (req, client, handler) ->
            client.submitTestConnectionTask(
                (TNodeLocations) req, (SubmitTestConnectionTaskToConfigNodeRPCHandler) handler));
    actionMapBuilder.put(
        ConfigNodeToConfigNodeRequestType.TEST_CONNECTION,
        (req, client, handler) ->
            client.testConnectionEmptyRPC((ConfigNodeTSStatusRPCHandler) handler));
  }

  @Override
  protected AsyncRequestRPCHandler<?, ConfigNodeToConfigNodeRequestType, TConfigNodeLocation>
      buildHandler(
          AsyncRequestContext<?, ?, ConfigNodeToConfigNodeRequestType, TConfigNodeLocation>
              requestContext,
          int requestId,
          TConfigNodeLocation targetNode) {
    return ConfigNodeAsyncRequestRPCHandler.buildHandler(requestContext, requestId, targetNode);
  }

  private static class ClientPoolHolder {
    private static final ConfigNodeToConfigNodeInternalServiceAsyncRequestManager INSTANCE =
        new ConfigNodeToConfigNodeInternalServiceAsyncRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeToConfigNodeInternalServiceAsyncRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
