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

package org.apache.iotdb.subscription.it.triple.treemodel.regression.pullconsumer.multi;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionTreeRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;
import org.apache.iotdb.subscription.it.Retry;
import org.apache.iotdb.subscription.it.RetryRule;
import org.apache.iotdb.subscription.it.triple.treemodel.regression.AbstractSubscriptionTreeRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/***
 * 1 consumer subscribes to 2 topics: historical data
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionTreeRegressionConsumer.class})
public class IoTDBOneConsumerMultiTopicsTsfileIT extends AbstractSubscriptionTreeRegressionIT {

  @Rule public RetryRule retryRule = new RetryRule();

  private static final String database = "root.test.OneConsumerMultiTopicsTsfile";
  private static final String device = database + ".d_0";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private String pattern = database + ".**";
  private String database2 = "root.OneConsumerMultiTopicsTsfile";
  private String pattern2 = database2 + ".**";
  private String device2 = database2 + ".d_0";
  private String topicName = "topic1_OneConsumerMultiTopicsTsfile";
  private String topicName2 = "topic2_OneConsumerMultiTopicsTsfile";
  private SubscriptionTreePullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createDB(database2);
    createTopic_s(topicName, pattern, "now", null, true);
    createTopic_s(topicName2, pattern2, "now", null, true);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.LZMA2);
    session_src.createTimeseries(
        device2 + ".s_0", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device2 + ".s_1", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.TEXT));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
    assertTrue(subs.getTopic(topicName2).isPresent(), "Create show topics 2");
  }

  @Override
  protected void setUpConfig() {
    super.setUpConfig();

    IoTDBSubscriptionITConstant.FORCE_SCALABLE_SINGLE_NODE_MODE.accept(sender);
    IoTDBSubscriptionITConstant.FORCE_SCALABLE_SINGLE_NODE_MODE.accept(receiver1);
    IoTDBSubscriptionITConstant.FORCE_SCALABLE_SINGLE_NODE_MODE.accept(receiver2);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      consumer.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
    subs.dropTopic(topicName2);
    dropDB(database);
    dropDB(database2);
    schemaList.clear();
    super.tearDown();
  }

  private void insert_data(long timestamp, String device)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row + 2.45f);
      tablet.addValue("s_1", rowIndex, "rowIndex" + rowIndex);
      timestamp += 2000;
    }
    session_src.insertTablet(tablet);
    session_src.executeNonQueryStatement("flush");
  }

  @Retry
  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    Thread.sleep(1000);
    // Write data before subscribing
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1710288000000,313,'2024-03-13 08:00:00+08:00');"); // 2024-03-13
    // 08:00:00+08:00
    insert_data(1706659200000L, device); // 2024-01-31 08:00:00+08:00
    insert_data(1706659200000L, device2); // 2024-01-31 08:00:00+08:00
    // Subscribe
    consumer = create_pull_consumer("multi_tsfile_2topic", "1_consumer", false, null);
    System.out.println("###### Subscription Query Before:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 0, "Before subscription show subscriptions");
    consumer.subscribe(topicName, topicName2);
    System.out.println("###### Subscribe and query:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 2, "subscribe then show subscriptions");

    // Subscribe and then write data
    Thread thread =
        new Thread(
            () -> {
              long timestamp = System.currentTimeMillis();
              for (int i = 0; i < 20; i++) {
                try {
                  insert_data(timestamp, device);
                  insert_data(timestamp, device2);
                  timestamp += 30000;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    thread.start();
    thread.join();

    System.out.println(
        "src insert " + device + " :" + getCount(session_src, "select count(s_0) from " + device));
    System.out.println(
        "src insert "
            + device2
            + " :"
            + getCount(session_src, "select count(s_0) from " + device2));
    // After first consumption
    List<String> devices = new ArrayList<>(2);
    devices.add(device);
    devices.add(device2);
    consume_tsfile_await(consumer, devices, Arrays.asList(100, 100));
    // Unsubscribe
    consumer.unsubscribe(topicName);
    System.out.println("###### After cancellation query:");
    subs.getSubscriptions(topicName).forEach((System.out::println));
    assertEquals(
        subs.getSubscriptions(topicName).size(), 0, "Unsubscribe 1 and then show subscriptions");

    // Unsubscribe and then write data
    insert_data(System.currentTimeMillis(), device2);
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1703980800000,3.45,'2023-12-31 08:00:00+08:00');"); // 2023-12-31 08:00:00+08:00
    consume_tsfile_await(
        consumer, Collections.singletonList(device2), Collections.singletonList(5));

    // close
    consumer.close();
    try {
      consumer.subscribe(topicName, topicName2);
    } catch (Exception e) {
      System.out.println("subscribe again after close, expecting an exception");
    }
    assertEquals(subs.getSubscriptions(topicName).size(), 0, "show subscriptions after close");
    subs.getSubscriptions(topicName).forEach((System.out::println));
  }
}
