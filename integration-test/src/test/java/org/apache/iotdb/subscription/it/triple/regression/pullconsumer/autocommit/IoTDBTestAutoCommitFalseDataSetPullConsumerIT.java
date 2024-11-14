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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.autocommit;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/***
 * If autoCommit is set to false, not using commit to submit consumption progress will lead to repeated consumption;
 * If set to true, it will not cause duplicate consumption (but considering the underlying implementation of the data subscription pipe is at-least-once semantics, there may also be cases of duplicate data)
 * autoCommit itself is just an auxiliary feature for at least once semantics. The commit itself is only related to restart recovery. In normal situations without bugs (pure log/batch), whether to commit or not, it will still be sent once.
 * Now the data subscription so-called progress information and the Pipe's progress information is a concept.
 * For the semantics related to data subscription commit, here are some supplements: *
 * In the implementation of data subscription, after a batch of messages is polled by the consumer, if they are not committed within a certain period of time (hardcoded value), the consumers under this subscription can repoll this batch of messages. This mechanism is designed to prevent messages from accumulating on the server side when the consumer does not explicitly commit with autoCommit set to false. However, this mechanism is not part of the functional definition (not exposed to the user), but only to handle some exceptional cases on the client side, such as when a consumer crashes after polling a batch of messages, or forgets to commit.
 * Tests can be more focused on situations where autoCommit is true. In this case, if not explicitly committed, it is expected that all messages successfully polled by the consumer should be committed before the consumer is closed (reflected in the Pipe as no accumulated resources).
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBTestAutoCommitFalseDataSetPullConsumerIT
    extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.TestAutoCommitFalseDataSetPullConsumer";
  private static final String device = database + ".d_0";
  private static final String topicName = "Topic_auto_commit_false";
  private String pattern = device + ".**";
  private static SubscriptionPullConsumer consumer;
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);

    createTopic_s(topicName, pattern, null, null, false);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest2.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest2.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "create show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      consumer.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
    dropDB(database);
    super.tearDown();
  }

  private void insert_data(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
      timestamp += row * 2000;
    }
    session_src.insertTablet(tablet);
  }

  private void consume_data_noCommit(SubscriptionPullConsumer consumer, Session session)
      throws InterruptedException,
          TException,
          IOException,
          StatementExecutionException,
          IoTDBConnectionException {
    while (true) {
      Thread.sleep(1000);
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
      if (messages.isEmpty()) {
        break;
      }
      for (final SubscriptionMessage message : messages) {
        for (final Iterator<Tablet> it = message.getSessionDataSetsHandler().tabletIterator();
            it.hasNext(); ) {
          final Tablet tablet = it.next();
          session.insertTablet(tablet);
          System.out.println(
              FORMAT.format(new Date()) + " consume data no commit:" + tablet.rowSize);
        }
      }
    }
  }

  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    consumer = create_pull_consumer("pull_commit", "Auto_commit_FALSE", false, null);
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    // Subscribe
    assertEquals(subs.getSubscriptions().size(), 0, "Before subscription show subscriptions");
    consumer.subscribe(topicName);
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");
    String sql1 = "select count(s_0) from " + device;
    String sql2 = "select count(s_1) from " + device;
    consume_data_noCommit(consumer, session_dest);
    System.out.println(FORMAT.format(new Date()) + " src sql1: " + getCount(session_src, sql1));
    System.out.println("dest sql1: " + getCount(session_dest, sql1));
    System.out.println("dest2 sql1: " + getCount(session_dest2, sql1));
    check_count(4, sql1, "dest consume subscription before data:s_0");
    check_count(4, sql2, "dest consume subscription before data:s_1");
    check_count2(0, sql2, "dest2 consumption subscription previous data: s_1");

    // Subscribe and then write data
    insert_data(System.currentTimeMillis());
    consume_data_noCommit(consumer, session_dest2);
    System.out.println("src sql1: " + getCount(session_src, sql1));
    System.out.println("dest sql1: " + getCount(session_dest, sql1));
    System.out.println("dest2 sql1: " + getCount(session_dest2, sql1));
    check_count(4, sql1, "dest consume subscription data 2:s_0");
    check_count2(4, sql1, "dest2 consumption subscription data 2:s_0");
    check_count2(4, sql2, "dest2 consumption subscription data 2:s_1");

    //        insert_data(1706659300000L); //2024-01-31 08:00:00+08:00
    // Will consume again
    consume_data_noCommit(consumer, session_dest);
    System.out.println("src sql1: " + getCount(session_src, sql1));
    System.out.println("dest sql1: " + getCount(session_dest, sql1));
    System.out.println("dest2 sql1: " + getCount(session_dest2, sql1));
    check_count(4, sql1, "dest consumption subscription before data3:s_0");
    check_count(4, sql2, "dest consume subscription before data3:s_1");
    check_count2(4, sql2, "dest2 consumption subscription before count 3 data:s_1");
  }
}
