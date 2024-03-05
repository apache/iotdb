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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class SubscriptionAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionAgent.class);

  public final TPipeSubscribeResp handle(TPipeSubscribeReq req) {
    // TODO: handle request version
    final byte reqVersion = req.getVersion();
    final short rawRequestType = req.getType();
    if (PipeSubscribeRequestType.isValidatedRequestType(rawRequestType)) {
      switch (PipeSubscribeRequestType.valueOf(rawRequestType)) {
        case HANDSHAKE:
          return handlePipeSubscribeHandshake(PipeSubscribeHandshakeReq.fromTPipeSubscribeReq(req));
        case HEARTBEAT:
          return handlePipeSubscribeHeartbeat(PipeSubscribeHeartbeatReq.fromTPipeSubscribeReq(req));
        case SUBSCRIBE:
        case UNSUBSCRIBE:
        case POLL:
        case COMMIT:
        case CLOSE:
        default:
          break;
      }
    }

    final TSStatus status =
        RpcUtils.getStatus(
            TSStatusCode.PIPE_TYPE_ERROR,
            String.format("Unknown PipeSubscribeRequestType %s.", rawRequestType));
    LOGGER.warn("Unknown PipeSubscribeRequestType, response status = {}.", status);
    return new TPipeSubscribeResp(
        status,
        PipeSubscribeResponseVersion.VERSION_1.getVersion(),
        PipeSubscribeResponseType.ACK.getType());
  }

  private PipeSubscribeHandshakeResp handlePipeSubscribeHandshake(PipeSubscribeHandshakeReq req) {
    // set thread local id info

    // getDataNodeConfiguration
    try {
      return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
          RpcUtils.SUCCESS_STATUS, new ArrayList<>());
    } catch (Exception e) {
      return null;
    }
  }

  private PipeSubscribeHeartbeatResp handlePipeSubscribeHeartbeat(PipeSubscribeHeartbeatReq req) {
    // set thread local id info

    // getDataNodeConfiguration
    try {
      return PipeSubscribeHeartbeatResp.toTPipeSubscribeResp(RpcUtils.SUCCESS_STATUS);
    } catch (Exception e) {
      return null;
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionAgentHolder {

    private static final SubscriptionAgent INSTANCE = new SubscriptionAgent();

    private SubscriptionAgentHolder() {
      // empty constructor
    }
  }

  public static SubscriptionAgent getInstance() {
    return SubscriptionAgent.SubscriptionAgentHolder.INSTANCE;
  }

  private SubscriptionAgent() {
    // empty constructor
  }
}
