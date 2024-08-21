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

package org.apache.iotdb.subscription.it.triple.regression.param;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionIdentifierSemanticException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBTestParamPullConsumerIT extends AbstractSubscriptionRegressionIT {
  private static SubscriptionPullConsumer consumer;
  private static final String topicName = "TestParamPullConsumerTopic1";
  private static final String database = "root.TestParamPullConsumer";
  private static final String device = database + ".d_0";
  private final String pattern = "root.**";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  static {
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("TestParamPullConsumer_1")
            .buildPullConsumer();
    consumer.open();
    createDB(database);
    createTopic_s(topicName, null, null, null, false);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    subs.getTopics().forEach(System.out::println);
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
    session_src.executeNonQueryStatement("create user user02 'user02';");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      consumer.close();
    } catch (Exception e) {
    }
    try {
      session_src.executeNonQueryStatement("drop user user02");
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

  private void run_single(SubscriptionPullConsumer consumer, int index)
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    consumer.open();
    // Subscribe
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "subscribe then show subscriptions:" + index);
    Long timestamp = 1706659200000L + 8000 * index;
    // Write data
    insert_data(timestamp);
    // consume
    consume_data(consumer, session_dest);
    // Unsubscribe
    consumer.unsubscribe(topicName);
    consumer.close();
    check_count(
        4,
        "select count(s_0) from " + device + " where time >= " + timestamp,
        "Consumption data:" + pattern);
  }

  @Test
  public void testUnsetGroup()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .autoCommit(true)
            .autoCommitIntervalMs(1000L)
            .buildPullConsumer();
    run_single(consumer1, 1);
  }

  @Test
  public void testUnsetConsumer()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerGroupId("TestParamPullConsumer_group_id_1")
            .autoCommit(true)
            .autoCommitIntervalMs(1000L)
            .buildPullConsumer();
    run_single(consumer1, 2);
  }

  @Test
  public void testUnsetConsumerGroup()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .autoCommit(true)
            .autoCommitIntervalMs(1000L)
            .buildPullConsumer();
    run_single(consumer1, 3);
  }

  @Test
  public void testAutoCommitIntervalNegative()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("TestParamPullConsumer")
            .autoCommitIntervalMs(-1)
            .buildPullConsumer();
    run_single(consumer1, 5);
  }

  @Test
  public void testDuplicateConsumerId() {
    SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testDuplicateConsumerId")
            .consumerGroupId("TestParamPullConsumer_1")
            .autoCommitIntervalMs(-1)
            .buildPullConsumer();
    SubscriptionPullConsumer consumer2 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testDuplicateConsumerId")
            .consumerGroupId("TestParamPullConsumer_2")
            .autoCommitIntervalMs(-1)
            .buildPullConsumer();
    SubscriptionPullConsumer consumer3 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testDuplicateConsumerId")
            .consumerGroupId("TestParamPullConsumer_1")
            .autoCommitIntervalMs(-1)
            .buildPullConsumer();
    consumer.open();
    consumer2.open();
    consumer3.open();
    consumer.close();
    consumer2.close();
    consumer3.close();
  }

  @Test
  public void testNodeUrls()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .nodeUrls(Collections.singletonList(SRC_HOST + ":" + SRC_PORT))
            .consumerId("testNodeUrls")
            .consumerGroupId("TestParamPullConsumer")
            .autoCommitIntervalMs(500L)
            .buildPullConsumer();
    run_single(consumer1, 5);
  }

  @Test(expected = NullPointerException.class)
  public void testCreateConsumer_null() {
    new SubscriptionPullConsumer(null).open();
  }

  @Test(expected = NullPointerException.class)
  public void testCreateConsumer_empty() {
    new SubscriptionPullConsumer(new Properties()).open();
  }

  @Test(expected = SubscriptionConnectionException.class)
  public void testCreateConsumer_empty2() {
    new SubscriptionPullConsumer.Builder().buildPullConsumer().open();
  }

  @Test(expected = NullPointerException.class)
  public void testSubscribe_null() {
    consumer.subscribe((String) null);
  }

  @Test(expected = SubscriptionIdentifierSemanticException.class)
  public void testSubscribe_empty() {
    consumer.subscribe("");
  }

  @Test(expected = SubscriptionRuntimeCriticalException.class)
  public void testSubscribe_notTopic() {
    consumer.subscribe("topic_notCreate");
  }

  @Test
  public void testSubscribe_dup() {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testSubscribe_dup")
            .consumerGroupId("TestParamPullConsumer")
            .buildPullConsumer();
    consumer1.open();
    consumer1.subscribe(topicName);
    consumer1.subscribe(topicName);
    consumer1.close();
  }

  @Test(expected = NullPointerException.class)
  public void testUnSubscribe_null() {
    consumer.unsubscribe((String) null);
  }

  @Test(expected = SubscriptionIdentifierSemanticException.class)
  public void testUnSubscribe_empty() {
    consumer.unsubscribe("");
  }

  @Test(expected = SubscriptionRuntimeCriticalException.class)
  public void testUnSubscribe_notTopic() {
    consumer.unsubscribe("topic_notCreate");
  }

  @Test
  public void testUnSubscribe_dup() {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testUnSubscribe_dup")
            .consumerGroupId("TestParamPullConsumer")
            .buildPullConsumer();
    consumer1.open();
    consumer1.subscribe(topicName);
    consumer1.unsubscribe(topicName);
    consumer1.unsubscribe(topicName);
    consumer1.close();
  }

  @Test
  public void testUnSubscribe_notSubs()
      throws StatementExecutionException, IoTDBConnectionException {
    subs.createTopic("t");
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testUnSubscribe_notSubs")
            .consumerGroupId("TestParamPullConsumer")
            .buildPullConsumer();
    consumer1.open();
    // No subscription, unsubscribe directly
    consumer1.unsubscribe("t");
    consumer1.close();
    subs.dropTopic("t");
  }

  @Test(expected = SubscriptionException.class)
  public void testSubscribe_AfterClose() {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testSubscribe_AfterClose")
            .consumerGroupId("TestParamPullConsumer")
            .buildPullConsumer();
    consumer1.open();
    consumer1.subscribe(topicName);
    consumer1.close();
    consumer1.subscribe(topicName);
  }

  @Test(expected = SubscriptionException.class)
  public void testUnSubscribe_AfterClose() {
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("testUnSubscribe_AfterClose")
            .consumerGroupId("TestParamPullConsumer")
            .buildPullConsumer();
    consumer1.open();
    consumer1.close();
    consumer1.unsubscribe(topicName);
  }

  @Test(expected = SubscriptionConnectionException.class)
  public void testNoUser() {
    String userName = "user01";
    new SubscriptionPullConsumer.Builder()
        .host(SRC_HOST)
        .username(userName)
        .buildPullConsumer()
        .open();
  }

  @Test(expected = SubscriptionConnectionException.class)
  public void testErrorPasswd() {
    String userName = "user02";
    SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .username(userName)
            .buildPullConsumer();
    consumer1.open();
    consumer1.close();
  }

  @Test
  public void testTsfile_ts()
      throws IoTDBConnectionException,
          StatementExecutionException,
          InterruptedException,
          IOException {
    String t1 = "tsTopic";
    createTopic_s(t1, database + ".d_0.s_0", null, null, true);
    try (SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder().host(SRC_HOST).port(SRC_PORT).buildPullConsumer()) {
      consumer1.open();
      consumer1.subscribe(t1);
      session_src.executeNonQueryStatement(
          "insert into " + database + ".d_0(time,s_0,s_1) values (1,10,20),(1000,30,60);");
      session_src.executeNonQueryStatement("flush;");
      Thread.sleep(3000L);
      AtomicInteger rowCount = new AtomicInteger(0);
      while (true) {
        Thread.sleep(1000L);
        List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(1000));
        if (messages.isEmpty()) {
          break;
        }
      }
    }
    subs.dropTopic(t1);
  }

  @Test
  public void testTsfile_ts_normal()
      throws IoTDBConnectionException,
          StatementExecutionException,
          InterruptedException,
          IOException {
    String t1 = "tsTopicNormal";
    String device = database + ".d_1";
    createTopic_s(t1, device + ".s_0", null, null, true);
    try (SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder().host(SRC_HOST).port(SRC_PORT).buildPullConsumer()) {
      consumer1.open();
      consumer1.subscribe(t1);
      session_src.executeNonQueryStatement(
          "insert into " + device + "(time,s_0) values (1,10),(1000,30);");
      session_src.executeNonQueryStatement("flush;");
      Thread.sleep(3000L);
      AtomicInteger rowCount = new AtomicInteger(0);
      while (true) {
        Thread.sleep(1000L);
        List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(1000));
        if (messages.isEmpty()) {
          break;
        }
        for (final SubscriptionMessage message : messages) {
          TsFileReader reader = message.getTsFileHandler().openReader();
          QueryDataSet dataset =
              reader.query(
                  QueryExpression.create(
                      Collections.singletonList(new Path(device, "s_0", true)), null));
          while (dataset.hasNext()) {
            rowCount.addAndGet(1);
            RowRecord next = dataset.next();
            System.out.println(device + ":" + next.getTimestamp() + "," + next.getFields());
          }
        }
        consumer1.commitSync(messages);
      }
    }
    subs.dropTopic(t1);
  }

  @Test
  public void testTsfile_device()
      throws IoTDBConnectionException,
          StatementExecutionException,
          InterruptedException,
          IOException {
    String t1 = "DeviceTopic";
    String device = database + ".d2";
    createTopic_s(t1, device + ".**", null, null, true);
    try (SubscriptionPullConsumer consumer1 =
        new SubscriptionPullConsumer.Builder().host(SRC_HOST).port(SRC_PORT).buildPullConsumer()) {
      consumer1.open();
      consumer1.subscribe(t1);
      session_src.executeNonQueryStatement(
          "insert into " + device + "(time,s_0,s_1) values (1,10,20),(1000,30,60);");
      session_src.executeNonQueryStatement("flush;");
      Thread.sleep(3000L);
      AtomicInteger rowCount = new AtomicInteger(0);
      while (true) {
        Thread.sleep(1000L);
        List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(1000));
        if (messages.isEmpty()) {
          break;
        }
        for (final SubscriptionMessage message : messages) {
          TsFileReader reader = message.getTsFileHandler().openReader();
          QueryDataSet dataset =
              reader.query(
                  QueryExpression.create(
                      Collections.singletonList(new Path(device, "s_0", true)), null));
          while (dataset.hasNext()) {
            rowCount.addAndGet(1);
            RowRecord next = dataset.next();
            System.out.println(device + ":" + next.getTimestamp() + "," + next.getFields());
          }
        }
        consumer1.commitSync(messages);
      }
    }
    subs.dropTopic(t1);
  }
}
