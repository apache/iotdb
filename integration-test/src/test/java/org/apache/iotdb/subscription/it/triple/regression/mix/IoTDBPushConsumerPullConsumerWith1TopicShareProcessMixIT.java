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

package org.apache.iotdb.subscription.it.triple.regression.mix;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
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
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * PushConsumer
 * pattern: db
 * Dataset
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBPushConsumerPullConsumerWith1TopicShareProcessMixIT
    extends AbstractSubscriptionRegressionIT {
  private static String topicName = "`1-group.1-consumer.db`";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private final String database = "root.PushConsumerPullConsumerWith1TopicShareProcessMix";
  private final String device = database + ".d_0";
  private final String pattern = database + ".**";
  private SubscriptionPushConsumer consumer;
  private SubscriptionPullConsumer consumer2;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createTopic_s(topicName, pattern, null, null, false);
    createDB(database);
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
    assertTrue(subs.getTopic(topicName).isPresent(), "show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    consumer.close();
    consumer2.close();
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
  }

  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    // Write data before subscribing
    Thread thread =
        new Thread(
            () -> {
              long timestamp = 1706659200000L; // 2024-01-31 08:00:00+08:00
              for (int i = 0; i < 100; i++) {
                try {
                  insert_data(timestamp);
                } catch (IoTDBConnectionException e) {
                  throw new RuntimeException(e);
                } catch (StatementExecutionException e) {
                  throw new RuntimeException(e);
                }
                timestamp += 20000;
              }
            });
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("dataset_push_consumer_1")
            .consumerGroupId("db_pull_push_mix")
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
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
            .buildPushConsumer();
    consumer.open();
    consumer.subscribe(topicName);

    consumer2 =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("dataset_pull_consumer_2")
            .consumerGroupId("db_pull_push_mix")
            .buildPullConsumer();
    consumer2.open();
    consumer2.subscribe(topicName);

    thread.start();
    thread.join();
    consume_data(consumer2, session_dest2);
    System.out.println("After subscribing:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions");

    Thread.sleep(3000);
    // The first 5 entries may have duplicate data
    String sql = "select count(s_0) from " + device;
    System.out.println("src push consumer: " + getCount(session_src, sql));
    System.out.println("dest push consumer: " + getCount(session_dest, sql));
    System.out.println("dest2 pull consumer: " + getCount(session_dest2, sql));
    AWAIT.untilAsserted(
        () -> {
          assertEquals(
              getCount(session_dest, sql) + getCount(session_dest2, sql),
              getCount(session_src, sql),
              "share process");
        });
  }
}
