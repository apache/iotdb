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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionPushConsumer extends SubscriptionConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPushConsumer.class);

  private final AckStrategy ackStrategy;
  private final ConsumeListener consumeListener;

  private ScheduledExecutorService workerExecutor;

  private final AtomicBoolean isClosed = new AtomicBoolean(true);

  protected SubscriptionPushConsumer(Builder builder) {
    super(builder);

    this.ackStrategy = builder.ackStrategy;
    this.consumeListener = builder.consumeListener;
  }

  public SubscriptionPushConsumer(Properties config) {
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
      Properties config, AckStrategy ackStrategy, ConsumeListener consumeListener) {
    super(new Builder().ackStrategy(ackStrategy), config);

    this.ackStrategy = ackStrategy;
    this.consumeListener = consumeListener;
  }

  /////////////////////////////// open & close ///////////////////////////////

  public synchronized void open()
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    if (!isClosed.get()) {
      return;
    }

    super.open();

    launchAutoPollWorker();

    isClosed.set(false);
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed.get()) {
      return;
    }

    try {
      shutdownWorker();
      super.close();
    } finally {
      isClosed.set(true);
    }
  }

  @Override
  boolean isClosed() {
    return isClosed.get();
  }

  /////////////////////////////// auto poll worker ///////////////////////////////

  @SuppressWarnings("unsafeThreadSchedule")
  private void launchAutoPollWorker() {
    workerExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t =
                  new Thread(Thread.currentThread().getThreadGroup(), r, "PushConsumerWorker", 0);
              if (!t.isDaemon()) {
                t.setDaemon(true);
              }
              if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
              }
              return t;
            });
    workerExecutor.scheduleAtFixedRate(
        new PushConsumerWorker(),
        0,
        ConsumerConstant.PUSH_CONSUMER_AUTO_POLL_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
  }

  private void shutdownWorker() {
    workerExecutor.shutdown();
    workerExecutor = null;
  }

  /////////////////////////////// builder ///////////////////////////////

  public static class Builder extends SubscriptionConsumer.Builder {

    private AckStrategy ackStrategy = AckStrategy.defaultValue();
    private ConsumeListener consumeListener = message -> ConsumeResult.SUCCESS;

    public SubscriptionPushConsumer.Builder host(String host) {
      super.host(host);
      return this;
    }

    public SubscriptionPushConsumer.Builder port(int port) {
      super.port(port);
      return this;
    }

    public SubscriptionPushConsumer.Builder username(String username) {
      super.username(username);
      return this;
    }

    public SubscriptionPushConsumer.Builder password(String password) {
      super.password(password);
      return this;
    }

    public SubscriptionPushConsumer.Builder consumerId(String consumerId) {
      super.consumerId(consumerId);
      return this;
    }

    public SubscriptionPushConsumer.Builder consumerGroupId(String consumerGroupId) {
      super.consumerGroupId(consumerGroupId);
      return this;
    }

    public SubscriptionPushConsumer.Builder ackStrategy(AckStrategy ackStrategy) {
      this.ackStrategy = ackStrategy;
      return this;
    }

    public SubscriptionPushConsumer.Builder consumeListener(ConsumeListener consumeListener) {
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

  class PushConsumerWorker implements Runnable {
    @Override
    public void run() {
      if (isClosed()) {
        return;
      }

      try {
        // Poll all subscribed topics by passing an empty set
        List<SubscriptionMessage> pollResults =
            poll(Collections.emptySet(), ConsumerConstant.PUSH_CONSUMER_AUTO_POLL_TIME_OUT_MS);

        if (ackStrategy.equals(AckStrategy.BEFORE_CONSUME)) {
          commitSync(pollResults);
        }

        for (SubscriptionMessage pollResult : pollResults) {
          ConsumeResult consumeResult = consumeListener.onReceive(pollResult);
          if (consumeResult.equals(ConsumeResult.FAILURE)) {
            LOGGER.warn("consumeListener failed when processing message: {}", pollResult);
          }
        }

        if (ackStrategy.equals(AckStrategy.AFTER_CONSUME)) {
          commitSync(pollResults);
        }

      } catch (TException
          | IOException
          | StatementExecutionException
          | IoTDBConnectionException e) {
        LOGGER.warn("Exception occurred when auto polling: ", e);
      }
    }
  }
}
