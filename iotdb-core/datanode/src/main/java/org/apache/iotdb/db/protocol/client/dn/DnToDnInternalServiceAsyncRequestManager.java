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

package org.apache.iotdb.db.protocol.client.dn;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.client.request.DataNodeInternalServiceRequestManager;
import org.apache.iotdb.mpp.rpc.thrift.TAttributeUpdateReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DnToDnInternalServiceAsyncRequestManager
    extends DataNodeInternalServiceRequestManager<DnToDnRequestType> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DnToDnInternalServiceAsyncRequestManager.class);

  @Override
  protected void initActionMapBuilder() {
    actionMapBuilder.put(
        DnToDnRequestType.TEST_CONNECTION,
        (req, client, handler) -> client.testConnectionEmptyRPC((AsyncTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        DnToDnRequestType.UPDATE_ATTRIBUTE,
        (req, client, handler) ->
            client.updateAttribute((TAttributeUpdateReq) req, (AsyncTSStatusRPCHandler) handler));
  }

  @Override
  protected AsyncRequestRPCHandler<?, DnToDnRequestType, TDataNodeLocation> buildHandler(
      final AsyncRequestContext<?, ?, DnToDnRequestType, TDataNodeLocation> requestContext,
      final int requestId,
      final TDataNodeLocation targetNode) {
    return DataNodeAsyncRequestRPCHandler.createAsyncRPCHandler(
        requestContext, requestId, targetNode);
  }

  private static class ClientPoolHolder {

    private static final DnToDnInternalServiceAsyncRequestManager INSTANCE =
        new DnToDnInternalServiceAsyncRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static DnToDnInternalServiceAsyncRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
