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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.subscription.receiver.SubscriptionReceiver;
import org.apache.iotdb.db.subscription.receiver.SubscriptionReceiverV1;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class SubscriptionReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionReceiverAgent.class);

  private static final TPipeSubscribeResp SUBSCRIPTION_NOT_ENABLED_ERROR_RESP =
      new TPipeSubscribeResp(
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_NOT_ENABLED_ERROR,
              DataNodeQueryMessages.QUERY_EXCEPTION_SUBSCRIPTION_IS_NOT_ENABLED_7F43DCBB),
          PipeSubscribeResponseVersion.VERSION_1.getVersion(),
          PipeSubscribeResponseType.ACK.getType());

  private final Map<Byte, Supplier<SubscriptionReceiver>> receiverConstructors = new HashMap<>();
  private final ThreadLocal<SubscriptionReceiver> receiverThreadLocal = new ThreadLocal<>();

  /**
   * The receiver currently serving each consumer identity. A disconnected receiver deliberately
   * remains in this map until its inactivity timeout closes the consumer, while a reconnecting
   * receiver replaces it atomically through {@link ConcurrentHashMap#compute(Object,
   * java.util.function.BiFunction)}.
   */
  private final ConcurrentHashMap<ConsumerIdentity, SubscriptionReceiver> consumerReceivers =
      new ConcurrentHashMap<>();

  private final BooleanSupplier subscriptionEnabledSupplier;
  private final ScheduledExecutorService receiverTimeoutChecker;

  SubscriptionReceiverAgent() {
    this(
        SubscriptionReceiverV1::new,
        SubscriptionConfig.getInstance().getSubscriptionEnabled(),
        () -> SubscriptionConfig.getInstance().getSubscriptionEnabled());
  }

  SubscriptionReceiverAgent(
      final Supplier<SubscriptionReceiver> receiverConstructor,
      final boolean scheduleTimeoutChecker,
      final BooleanSupplier subscriptionEnabledSupplier) {
    this.subscriptionEnabledSupplier = subscriptionEnabledSupplier;
    receiverConstructors.put(
        PipeSubscribeRequestVersion.VERSION_1.getVersion(), receiverConstructor);
    if (scheduleTimeoutChecker) {
      receiverTimeoutChecker =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              SubscriptionReceiverAgent.class.getSimpleName() + "-Timeout-Checker");
      ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
          receiverTimeoutChecker,
          this::checkReceiverTimeouts,
          Math.max(
              1_000L, SubscriptionConfig.getInstance().getSubscriptionDefaultTimeoutInMs() / 2L),
          Math.max(
              1_000L, SubscriptionConfig.getInstance().getSubscriptionDefaultTimeoutInMs() / 2L),
          TimeUnit.MILLISECONDS);
    } else {
      receiverTimeoutChecker = null;
    }
  }

  public TPipeSubscribeResp handle(final TPipeSubscribeReq req) {
    return handle(req, null);
  }

  public TPipeSubscribeResp handle(final TPipeSubscribeReq req, final String username) {
    if (username == null) {
      return new TPipeSubscribeResp(
          RpcUtils.getStatus(TSStatusCode.NO_PERMISSION),
          PipeSubscribeResponseVersion.VERSION_1.getVersion(),
          PipeSubscribeResponseType.ACK.getType());
    }
    if (!subscriptionEnabledSupplier.getAsBoolean()) {
      return SUBSCRIPTION_NOT_ENABLED_ERROR_RESP;
    }

    final byte reqVersion = req.getVersion();
    if (receiverConstructors.containsKey(reqVersion)) {
      final SubscriptionReceiver receiver = getReceiver(reqVersion);
      receiver.setAuthenticatedUsername(username);
      final ConsumerIdentity consumerIdentity = getConsumerIdentity(req, receiver);
      final RequestResult requestResult = new RequestResult();

      if (Objects.isNull(consumerIdentity)) {
        requestResult.response = handleRequest(receiver, req, null);
      } else {
        consumerReceivers.compute(
            consumerIdentity,
            (identity, currentReceiver) -> {
              requestResult.response = handleRequest(receiver, req, currentReceiver);

              if (isHandshake(req)) {
                if (isSuccessful(requestResult.response)) {
                  if (currentReceiver != null && currentReceiver != receiver) {
                    currentReceiver.invalidateConsumer();
                  }
                  return receiver;
                }
                return currentReceiver;
              }

              if (currentReceiver != null && currentReceiver != receiver) {
                return currentReceiver;
              }
              return receiver.hasActiveConsumer() ? receiver : null;
            });
      }

      if (isHandshake(req) && isSuccessful(requestResult.response)) {
        final ConsumerIdentity activeIdentity = getConsumerIdentity(receiver);
        if (!Objects.equals(consumerIdentity, activeIdentity)) {
          registerReceiver(receiver, activeIdentity);
        } else {
          removeReceiverMappingsExcept(receiver, activeIdentity);
        }
      } else if (isClose(req) && isSuccessful(requestResult.response)) {
        removeReceiverMappings(receiver);
      }
      return requestResult.response;
    } else {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.SUBSCRIPTION_VERSION_ERROR,
              String.format("Unknown PipeSubscribeRequestVersion %s.", reqVersion));
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_UNKNOWN_PIPESUBSCRIBEREQUESTVERSION_RESPONSE_56E5D93F,
          status);
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
    if (receiverConstructors.containsKey(reqVersion)) {
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
          DataNodePipeMessages
              .PIPE_LOG_THE_SUBSCRIPTION_REQUEST_VERSION_IS_DIFFERENT_FROM_THE_CLIENT_324A125F,
          receiverThreadLocalVersion,
          reqVersion);
      receiverThreadLocal.remove();
      return setAndGetReceiver(reqVersion);
    }

    return receiverThreadLocal.get();
  }

  private SubscriptionReceiver setAndGetReceiver(final byte reqVersion) {
    if (receiverConstructors.containsKey(reqVersion)) {
      receiverThreadLocal.set(receiverConstructors.get(reqVersion).get());
    } else {
      throw new UnsupportedOperationException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_UNSUPPORTED_SUBSCRIPTION_REQUEST_VERSION_D_1E7C211A,
              reqVersion));
    }
    return receiverThreadLocal.get();
  }

  public final void handleClientExit() {
    final SubscriptionReceiver receiver = receiverThreadLocal.get();
    if (receiver != null) {
      try {
        final ConsumerIdentity consumerIdentity = getConsumerIdentity(receiver);
        if (Objects.isNull(consumerIdentity)) {
          receiver.handleExit();
        } else {
          consumerReceivers.compute(
              consumerIdentity,
              (identity, currentReceiver) -> {
                if (currentReceiver != null && currentReceiver != receiver) {
                  // A newer connection has already taken over this consumer. Do not let the old
                  // connection's exit cleanup touch the new owner's subscription state.
                  receiver.invalidateConsumer();
                  receiver.handleExit();
                  return currentReceiver;
                }
                receiver.handleExit();
                return receiver.hasActiveConsumer() ? receiver : null;
              });
        }
      } finally {
        receiverThreadLocal.remove();
      }
    }
  }

  void checkReceiverTimeouts() {
    consumerReceivers.forEach(
        (identity, receiver) ->
            consumerReceivers.computeIfPresent(
                identity,
                (currentIdentity, currentReceiver) -> {
                  if (currentReceiver != receiver) {
                    return currentReceiver;
                  }
                  if (!identity.equals(getConsumerIdentity(receiver))) {
                    return null;
                  }
                  receiver.handleTimeout();
                  return receiver.hasActiveConsumer() ? receiver : null;
                }));
  }

  private TPipeSubscribeResp handleRequest(
      final SubscriptionReceiver receiver,
      final TPipeSubscribeReq req,
      final SubscriptionReceiver currentReceiver) {
    if (!isHandshake(req) && currentReceiver != null && currentReceiver != receiver) {
      receiver.invalidateConsumer();
    }
    return receiver.handle(req);
  }

  private void registerReceiver(
      final SubscriptionReceiver receiver, final ConsumerIdentity identity) {
    if (Objects.isNull(identity)) {
      removeReceiverMappings(receiver);
      return;
    }
    consumerReceivers.compute(
        identity,
        (key, currentReceiver) -> {
          if (currentReceiver != null && currentReceiver != receiver) {
            currentReceiver.invalidateConsumer();
          }
          return receiver;
        });
    removeReceiverMappingsExcept(receiver, identity);
  }

  private void removeReceiverMappingsExcept(
      final SubscriptionReceiver receiver, final ConsumerIdentity retainedIdentity) {
    consumerReceivers.forEach(
        (registeredIdentity, currentReceiver) -> {
          if (currentReceiver == receiver
              && !Objects.equals(retainedIdentity, registeredIdentity)) {
            consumerReceivers.remove(registeredIdentity, receiver);
          }
        });
  }

  private void removeReceiverMappings(final SubscriptionReceiver receiver) {
    consumerReceivers.forEach(
        (identity, currentReceiver) -> consumerReceivers.remove(identity, receiver));
  }

  private static ConsumerIdentity getConsumerIdentity(
      final TPipeSubscribeReq req, final SubscriptionReceiver receiver) {
    if (isHandshake(req) && req.isSetBody()) {
      try {
        final ByteBuffer body = req.bufferForBody();
        if (body.hasRemaining()) {
          final ConsumerConfig consumerConfig = ConsumerConfig.deserialize(body);
          final ConsumerIdentity identity =
              ConsumerIdentity.of(
                  consumerConfig.getConsumerGroupId(), consumerConfig.getConsumerId());
          if (Objects.nonNull(identity)) {
            return identity;
          }
        }
      } catch (final RuntimeException ignored) {
        // Let the receiver report the malformed handshake request. It still needs to see the
        // original buffer, so parsing is intentionally done on a duplicate above.
      }
    }
    return getConsumerIdentity(receiver);
  }

  private static ConsumerIdentity getConsumerIdentity(final SubscriptionReceiver receiver) {
    return ConsumerIdentity.of(receiver.getConsumerGroupId(), receiver.getConsumerId());
  }

  private static boolean isHandshake(final TPipeSubscribeReq req) {
    return req.getType() == PipeSubscribeRequestType.HANDSHAKE.getType();
  }

  private static boolean isClose(final TPipeSubscribeReq req) {
    return req.getType() == PipeSubscribeRequestType.CLOSE.getType();
  }

  private static boolean isSuccessful(final TPipeSubscribeResp response) {
    return response != null
        && response.getStatus() != null
        && response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static final class RequestResult {
    private TPipeSubscribeResp response;
  }

  private record ConsumerIdentity(String consumerGroupId, String consumerId) {
    private static ConsumerIdentity of(final String consumerGroupId, final String consumerId) {
      return Objects.isNull(consumerGroupId) || Objects.isNull(consumerId)
          ? null
          : new ConsumerIdentity(consumerGroupId, consumerId);
    }
  }
}
