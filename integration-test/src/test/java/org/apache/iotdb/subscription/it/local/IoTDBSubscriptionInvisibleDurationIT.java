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

package org.apache.iotdb.subscription.it.local;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionInvisibleDurationIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionInvisibleDurationIT.class);

  private static final Duration INVISIBLE_DURATION = Duration.ofSeconds(5L);
  private static final int RECONSUME_COUNT = 3;

  @Test
  public void testReconsumeByPoll() throws Exception {
    testInvisibleDurationInternal(
        (consumer, rowCount, reconsumeCount) -> {
          LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
          final List<SubscriptionMessage> messages;
          if (reconsumeCount.incrementAndGet() < RECONSUME_COUNT) {
            messages =
                consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS, INVISIBLE_DURATION);
          } else {
            messages = consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
          }
          for (final SubscriptionMessage message : messages) {
            for (final SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
              while (dataSet.hasNext()) {
                dataSet.next();
                rowCount.addAndGet(1);
              }
            }
          }
          if (reconsumeCount.get() >= RECONSUME_COUNT) {
            consumer.commitSync(messages);
          }
        });
  }

  @Test
  public void testReconsumeByChangeInvisibleDuration() throws Exception {
    testInvisibleDurationInternal(
        (consumer, rowCount, reconsumeCount) -> {
          LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
          final List<SubscriptionMessage> messages =
              consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
          for (final SubscriptionMessage message : messages) {
            for (final SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
              while (dataSet.hasNext()) {
                dataSet.next();
                rowCount.addAndGet(1);
              }
            }
          }
          if (reconsumeCount.incrementAndGet() < RECONSUME_COUNT) {
            consumer.changeInvisibleDuration(messages, INVISIBLE_DURATION);
          } else {
            consumer.commitSync(messages);
          }
        });
  }

  private void testInvisibleDurationInternal(final ReconsumeAction action) throws Exception {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.createDatabase("root.db");
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName = "topic1";
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.PATTERN_KEY, "root.db.d1.s1");
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final AtomicInteger rowCount = new AtomicInteger();
    final AtomicInteger reconsumeCount = new AtomicInteger(0);
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumer =
                  new SubscriptionPullConsumer.Builder()
                      .host(host)
                      .port(port)
                      .consumerId("c1")
                      .consumerGroupId("cg1")
                      .autoCommit(false)
                      .buildPullConsumer()) {
                consumer.open();
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  action.apply(consumer, rowCount, reconsumeCount);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getDisplayName()));
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(100 * RECONSUME_COUNT, rowCount.get()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  @FunctionalInterface
  private interface ReconsumeAction {
    void apply(
        SubscriptionPullConsumer consumer, AtomicInteger rowCount, AtomicInteger reconsumeCount);
  }
}
