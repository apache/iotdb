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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TopologyPushRPCHandler extends DataNodeAsyncRequestRPCHandler<TDataNodeHeartbeatResp> {

  public TopologyPushRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TDataNodeHeartbeatResp> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TDataNodeHeartbeatResp resp) {
    responseMap.put(requestId, resp);
    nodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    nodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }
}
