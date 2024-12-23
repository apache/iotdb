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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.multi;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
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
 * 1 consumer subscribes to 2 topics: fixed time range
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBOneConsumerMultiTopicsDatasetIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.OneConsumerMultiTopicsDataset";
  private static final String device = database + ".d_0";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  private String pattern = device + ".s_0";
  private String pattern2 = device + ".s_1";
  private String topicName = "topic1_OneConsumerMultiTopicsDataset";
  private String topicName2 = "topic2_OneConsumerMultiTopicsDataset";
  private static SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(
        topicName, pattern, "2024-01-01T00:00:00+08:00", "2024-03-01T00:00:00+08:00", false);
    createTopic_s(
        topicName2, pattern2, "2024-01-01T00:00:00+08:00", "2024-03-13T00:00:00+08:00", false);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "create show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    consumer.close();
    subs.dropTopic(topicName);
    subs.dropTopic(topicName2);
    dropDB(database);
    super.tearDown();
  }

  private void insert_data(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
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
    Thread.sleep(1000);
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1710288000000,313,6.78);"); // 2024-03-13 08:00:00+08:00
    // Subscribe
    consumer = create_pull_consumer("multi_1consumer_multiTopics", "c1", false, null);
    assertEquals(subs.getSubscriptions().size(), 0, "Before subscription show subscriptions");

    consumer.subscribe(topicName, topicName2);
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 2, "show subscriptions after subscription");

    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  insert_data(System.currentTimeMillis());
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    thread.start();
    thread.join();
    String sql1 = "select count(s_0) from " + device;
    String sql2 = "select count(s_1) from " + device;
    System.out.println("src s_0:" + getCount(session_src, sql1));
    System.out.println("src s_1:" + getCount(session_src, sql2));
    // Consumption data
    consume_data(consumer, session_dest);
    System.out.println("dest s_0:" + getCount(session_dest, sql1));
    System.out.println("dest s_1:" + getCount(session_dest, sql2));
    AWAIT.untilAsserted(
        () -> {
          check_count(5, sql1, "Consumption data:" + pattern);
          check_count(5, sql2, "Consumption data:" + pattern2);
        });
    // Unsubscribe
    consumer.unsubscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    // Subscribe and then write data
    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1703980800000,1231,321.45);"); // 2023-12-31 08:00:00+08:00

    // Consumption data
    consume_data(consumer, session_dest);
    AWAIT.untilAsserted(
        () -> {
          check_count(5, sql1, "consume data again:" + pattern);
          check_count(10, sql2, "consume data again:" + pattern2);
        });
    // Unsubscribe
    consumer.unsubscribe(topicName, topicName2);
    System.out.println("###### Query after unsubscription again:");
    subs.getSubscriptions().forEach((System.out::println));
  }
}
