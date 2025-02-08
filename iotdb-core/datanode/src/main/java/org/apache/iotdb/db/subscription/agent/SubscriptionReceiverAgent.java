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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.subscription.receiver.SubscriptionReceiver;
import org.apache.iotdb.db.subscription.receiver.SubscriptionReceiverV1;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class SubscriptionReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReceiverAgent.class);

  private final ThreadLocal<SubscriptionReceiver> receiverThreadLocal = new ThreadLocal<>();

  private static final Map<Byte, Supplier<SubscriptionReceiver>> RECEIVER_CONSTRUCTORS =
      new HashMap<>();

  SubscriptionReceiverAgent() {
    RECEIVER_CONSTRUCTORS.put(
        PipeSubscribeRequestVersion.VERSION_1.getVersion(), SubscriptionReceiverV1::new);
  }

  public TPipeSubscribeResp handle(final TPipeSubscribeReq req) {
    final byte reqVersion = req.getVersion();
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      return getReceiver(reqVersion).handle(req);
    } else {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_VERSION_ERROR,
              String.format("Unknown PipeSubscribeRequestVersion %s.", reqVersion));
      LOGGER.warn(
          "Subscription: Unknown PipeSubscribeRequestVersion, response status = {}.", status);
      return new TPipeSubscribeResp(
          status,
          PipeSubscribeResponseVersion.VERSION_1.getVersion(),
          PipeSubscribeResponseType.ACK.getType());
    }
  }

  public long remainingMs() {
    return remainingMs(PipeSubscribeRequestVersion.VERSION_1.getVersion()); // default to VERSION_1
  }

  public long remainingMs(final byte reqVersion) {
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      return getReceiver(reqVersion).remainingMs();
    } else {
      return SubscriptionConfig.getInstance().getSubscriptionDefaultTimeoutInMs();
    }
  }

  private SubscriptionReceiver getReceiver(final byte reqVersion) {
    if (receiverThreadLocal.get() == null) {
      return setAndGetReceiver(reqVersion);
    }

    final byte receiverThreadLocalVersion = receiverThreadLocal.get().getVersion().getVersion();
    if (receiverThreadLocalVersion != reqVersion) {
      LOGGER.warn(
          "The subscription request version {} is different from the client request version {},"
              + " the receiver will be reset to the client request version.",
          receiverThreadLocalVersion,
          reqVersion);
      receiverThreadLocal.remove();
      return setAndGetReceiver(reqVersion);
    }

    return receiverThreadLocal.get();
  }

  private SubscriptionReceiver setAndGetReceiver(final byte reqVersion) {
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      receiverThreadLocal.set(RECEIVER_CONSTRUCTORS.get(reqVersion).get());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported subscription request version %d", reqVersion));
    }
    return receiverThreadLocal.get();
  }

  public final void handleClientExit() {
    final SubscriptionReceiver receiver = receiverThreadLocal.get();
    if (receiver != null) {
      receiver.handleExit();
      receiverThreadLocal.remove();
    }
  }
}
