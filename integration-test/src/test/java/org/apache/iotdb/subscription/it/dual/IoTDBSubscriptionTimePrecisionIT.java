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

package org.apache.iotdb.subscription.it.dual;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionArchVerification;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionArchVerification.class})
public class IoTDBSubscriptionTimePrecisionIT extends AbstractSubscriptionDualIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionTimePrecisionIT.class);

  @Override
  protected void setUpConfig() {
    super.setUpConfig();

    // Set timestamp precision to nanosecond
    senderEnv.getConfig().getCommonConfig().setTimestampPrecision("ns");
    receiverEnv.getConfig().getCommonConfig().setTimestampPrecision("ns");
  }

  @Test
  public void testTopicTimePrecision() throws Exception {
    final String host = senderEnv.getIP();
    final int port = Integer.parseInt(senderEnv.getPort());

    // Insert some historical data on sender
    final long currentTime1 = System.currentTimeMillis() * 1000_000L; // in nanosecond
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s2) values (%s, 1)", currentTime1 - i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic on sender
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      {
        final Properties config = new Properties();
        config.put(TopicConstant.START_TIME_KEY, currentTime1 - 99);
        config.put(
            TopicConstant.END_TIME_KEY,
            TopicConstant.NOW_TIME_VALUE); // now should be strictly larger than current time 1
        session.createTopic(topic1, config);
      }
      {
        final Properties config = new Properties();
        config.put(
            TopicConstant.START_TIME_KEY,
            TopicConstant.NOW_TIME_VALUE); // now should be strictly smaller than current time 2
        session.createTopic(topic2, config);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Insert some historical data on sender again
    final long currentTime2 = System.currentTimeMillis() * 1000_000L; // in nanosecond
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s1) values (%s, 1)", currentTime2 + i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscribe on sender and insert on receiver
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
                          .buildPullConsumer();
                  final ISession session = receiverEnv.getSessionConnection()) {
                consumer.open();
                consumer.subscribe(topic1, topic2);
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    for (final Iterator<Tablet> it =
                            message.getSessionDataSetsHandler().tabletIterator();
                        it.hasNext(); ) {
                      final Tablet tablet = it.next();
                      session.insertTablet(tablet);
                    }
                  }
                  consumer.commitSync(messages);
                }
                // Auto unsubscribe topics
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getDisplayName()));
    thread.start();

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        AWAIT.untilAsserted(
            () ->
                TestUtils.assertSingleResultSetEqual(
                    TestUtils.executeQueryWithRetry(statement, "select count(*) from root.**"),
                    new HashMap<String, String>() {
                      {
                        put("count(root.db.d1.s2)", "100");
                        put("count(root.db.d2.s1)", "100");
                      }
                    }));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }
}
