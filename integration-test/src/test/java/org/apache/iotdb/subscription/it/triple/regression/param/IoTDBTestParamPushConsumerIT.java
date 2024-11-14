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
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBTestParamPushConsumerIT extends AbstractSubscriptionRegressionIT {
  private static SubscriptionPushConsumer consumer;
  private static final String topicName = "TestParamPushConsumerTopic1";
  private static final String database = "root.TestParamPushConsumer";
  private static final String device = database + ".d_0";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private String sql = "select count(s_0) from " + device;

  static {
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
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
    session_src.executeNonQueryStatement("create user user02 'user02';");
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_default")
            .consumerGroupId("TestParamPushConsumer")
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .consumeListener(
                message -> {
                  System.out.println(message.getMessageType());
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer();
    consumer.open();
    consumer.subscribe(topicName);
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
      timestamp += 2000;
    }
    session_src.insertTablet(tablet);
    session_src.executeNonQueryStatement("flush");
  }

  @Test
  public void testUnsetGroupConsumer()
      throws IoTDBConnectionException, StatementExecutionException {
    long count = getCount(session_src, sql);
    insert_data(1706659200000L);
    try (final SubscriptionPushConsumer consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      // System.out.println("#### " + dataSet.getTablet().rowSize);
                      session_dest.insertTablet(dataSet.getTablet());
                    } catch (StatementExecutionException e) {
                      throw new RuntimeException(e);
                    } catch (IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer()) {
      consumer.open();
      consumer.subscribe(topicName);
      AWAIT.untilAsserted(
          () -> {
            check_count(5, sql, "before count=" + count);
          });
    }
  }

  @Test
  public void testAlterPollTime() throws IoTDBConnectionException, StatementExecutionException {
    String sql = "select count(s_0) from " + device;
    long count = getCount(session_src, sql);
    insert_data(1706669800000L);
    try (final SubscriptionPushConsumer consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .autoPollTimeoutMs(1000)
            .autoPollIntervalMs(10)
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      session_dest.insertTablet(dataSet.getTablet());
                    } catch (StatementExecutionException e) {
                      throw new RuntimeException(e);
                    } catch (IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer()) {
      consumer.open();
      consumer.subscribe(topicName);
      AWAIT.untilAsserted(
          () -> {
            System.out.println(getCount(session_dest, sql));
            check_count(5, sql, " would sync all data including deleted, before count=" + count);
          });
    }
  }

  @Test(expected = NullPointerException.class)
  public void testCreateConsumer_null() {
    new SubscriptionPushConsumer(null).open();
  }

  @Test(expected = NullPointerException.class)
  public void testCreateConsumer_empty() {
    new SubscriptionPushConsumer(new Properties()).open();
  }

  @Test(expected = SubscriptionConnectionException.class)
  public void testCreateConsumer_empty2() {
    new SubscriptionPushConsumer.Builder().buildPushConsumer().open();
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
    SubscriptionPushConsumer consumer1 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_default")
            .consumerGroupId("TestParamPushConsumer")
            .buildPushConsumer();
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
    SubscriptionPushConsumer consumer1 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("push_param_group_id_1")
            .buildPushConsumer();
    consumer1.open();
    consumer1.subscribe(topicName);
    consumer1.unsubscribe(topicName);
    consumer1.unsubscribe(topicName);
    consumer1.close();
  }

  @Test
  public void testUnSubscribe_notSubs()
      throws IoTDBConnectionException, StatementExecutionException {
    subs.createTopic("t");
    SubscriptionPushConsumer consumer1 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("push_param_group_id_1")
            .buildPushConsumer();
    consumer1.open();
    // No subscription, unsubscribe directly
    consumer1.unsubscribe("t");
    consumer1.close();
    subs.dropTopic("t");
  }

  @Test(expected = SubscriptionException.class)
  public void testSubscribe_AfterClose() {
    SubscriptionPushConsumer consumer1 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("push_param_group_id_1")
            .buildPushConsumer();
    consumer1.open();
    consumer1.subscribe(topicName);
    consumer1.close();
    consumer1.subscribe(topicName);
  }

  @Test(expected = SubscriptionException.class)
  public void testUnSubscribe_AfterClose() {
    SubscriptionPushConsumer consumer1 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("push_param_group_id_1")
            .buildPushConsumer();
    consumer1.open();
    consumer1.close();
    consumer1.unsubscribe(topicName);
  }

  @Test(expected = SubscriptionConnectionException.class)
  public void testNoUser() {
    String userName = "user01";
    new SubscriptionPushConsumer.Builder()
        .host(SRC_HOST)
        .username(userName)
        .buildPushConsumer()
        .open();
  }

  @Test(expected = SubscriptionConnectionException.class)
  public void testErrorPasswd() {
    String userName = "user02";
    SubscriptionPushConsumer consumer1 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .username(userName)
            .buildPushConsumer();
    consumer1.open();
    consumer1.close();
  }
}
