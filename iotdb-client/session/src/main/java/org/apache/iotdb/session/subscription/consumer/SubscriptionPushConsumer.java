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

package org.apache.iotdb.session.subscription.consumer;

import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionPushConsumer extends SubscriptionConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPushConsumer.class);

  private final AckStrategy ackStrategy;
  private final ConsumeListener consumeListener;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  protected SubscriptionPushConsumer(final Builder builder) {
    super(builder);

    this.ackStrategy = builder.ackStrategy;
    this.consumeListener = builder.consumeListener;
  }

  public SubscriptionPushConsumer(final Properties config) {
    this(
        config,
        (AckStrategy)
            config.getOrDefault(ConsumerConstant.ACK_STRATEGY_KEY, AckStrategy.defaultValue()),
        (ConsumeListener)
            config.getOrDefault(
                ConsumerConstant.CONSUME_LISTENER_KEY,
                (ConsumeListener) message -> ConsumeResult.SUCCESS));
  }

  private SubscriptionPushConsumer(
      final Properties config,
      final AckStrategy ackStrategy,
      final ConsumeListener consumeListener) {
    super(new Builder().ackStrategy(ackStrategy), config);

    this.ackStrategy = ackStrategy;
    this.consumeListener = consumeListener;
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open() throws SubscriptionException {
    if (!isClosed.get()) {
      return;
    }

    super.open();
    launchAutoPollWorker();
    isClosed.set(false);
  }

  @Override
  public synchronized void close() {
    if (isClosed.get()) {
      return;
    }

    super.close();
    isClosed.set(true);
  }

  @Override
  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// auto poll ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchAutoPollWorker() {
    final ScheduledFuture<?>[] future = new ScheduledFuture<?>[1];
    future[0] =
        SubscriptionExecutorService.submitAutoPollWorker(
            () -> {
              if (isClosed()) {
                if (Objects.nonNull(future[0])) {
                  future[0].cancel(false);
                }
                return;
              }
              new AutoPollWorker().run();
            },
            ConsumerConstant.PUSH_CONSUMER_AUTO_POLL_INTERVAL_MS);
  }

  class AutoPollWorker implements Runnable {
    @Override
    public void run() {
      if (isClosed()) {
        return;
      }

      try {
        // Poll all subscribed topics by passing an empty set
        final List<SubscriptionMessage> messages =
            poll(Collections.emptySet(), ConsumerConstant.PUSH_CONSUMER_AUTO_POLL_TIME_OUT_MS);

        if (ackStrategy.equals(AckStrategy.BEFORE_CONSUME)) {
          ack(messages);
        }

        final List<SubscriptionMessage> messagesToAck = new ArrayList<>();
        final List<SubscriptionMessage> messagesToNack = new ArrayList<>();
        for (final SubscriptionMessage message : messages) {
          final ConsumeResult consumeResult;
          try {
            consumeResult = consumeListener.onReceive(message);
            if (consumeResult.equals(ConsumeResult.SUCCESS)) {
              messagesToAck.add(message);
            } else {
              LOGGER.warn("Consumer listener result failure when consuming message: {}", message);
              messagesToNack.add(message);
            }
          } catch (final Exception e) {
            LOGGER.warn(
                "Consumer listener raised an exception while consuming message: {}", message, e);
            messagesToNack.add(message);
          }
        }

        if (ackStrategy.equals(AckStrategy.AFTER_CONSUME)) {
          ack(messagesToAck);
          nack(messagesToNack);
        }
      } catch (final Exception e) {
        LOGGER.warn("something unexpected happened when auto poll messages...", e);
      }
    }
  }

  /////////////////////////////// builder ///////////////////////////////

  public static class Builder extends SubscriptionConsumer.Builder {

    private AckStrategy ackStrategy = AckStrategy.defaultValue();
    private ConsumeListener consumeListener = message -> ConsumeResult.SUCCESS;

    public SubscriptionPushConsumer.Builder host(final String host) {
      super.host(host);
      return this;
    }

    public SubscriptionPushConsumer.Builder port(final int port) {
      super.port(port);
      return this;
    }

    public SubscriptionPushConsumer.Builder username(final String username) {
      super.username(username);
      return this;
    }

    public SubscriptionPushConsumer.Builder password(final String password) {
      super.password(password);
      return this;
    }

    public SubscriptionPushConsumer.Builder consumerId(final String consumerId) {
      super.consumerId(consumerId);
      return this;
    }

    public SubscriptionPushConsumer.Builder consumerGroupId(final String consumerGroupId) {
      super.consumerGroupId(consumerGroupId);
      return this;
    }

    public SubscriptionPushConsumer.Builder heartbeatIntervalMs(final long heartbeatIntervalMs) {
      super.heartbeatIntervalMs(heartbeatIntervalMs);
      return this;
    }

    public SubscriptionPushConsumer.Builder endpointsSyncIntervalMs(
        final long endpointsSyncIntervalMs) {
      super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
      return this;
    }

    public SubscriptionPushConsumer.Builder fileSaveDir(final String fileSaveDir) {
      this.fileSaveDir = fileSaveDir;
      return this;
    }

    public SubscriptionPushConsumer.Builder ackStrategy(final AckStrategy ackStrategy) {
      this.ackStrategy = ackStrategy;
      return this;
    }

    public SubscriptionPushConsumer.Builder consumeListener(final ConsumeListener consumeListener) {
      this.consumeListener = consumeListener;
      return this;
    }

    @Override
    public SubscriptionPullConsumer buildPullConsumer() {
      throw new SubscriptionException(
          "SubscriptionPushConsumer.Builder do not support build pull consumer.");
    }

    @Override
    public SubscriptionPushConsumer buildPushConsumer() {
      return new SubscriptionPushConsumer(this);
    }
  }
}
