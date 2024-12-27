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
import java.util.concurrent.atomic.AtomicInteger;

/***
 * 1 consumer subscribes to 2 topics: historical data
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBOneConsumerMultiTopicsTsfileIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.OneConsumerMultiTopicsTsfile";
  private static final String device = database + ".d_0";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private String pattern = database + ".**";
  private String database2 = "root.OneConsumerMultiTopicsTsfile";
  private String pattern2 = database2 + ".**";
  private String device2 = database2 + ".d_0";
  private String topicName = "topic1_OneConsumerMultiTopicsTsfile";
  private String topicName2 = "topic2_OneConsumerMultiTopicsTsfile";
  private SubscriptionPullConsumer consumer;

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

    final AtomicInteger rowCount = new AtomicInteger();
    Thread thread1 =
        new Thread(
            () -> {
              List<String> devices = new ArrayList<>(2);
              devices.add(device);
              devices.add(device2);
              try {
                List<Integer> results = consume_tsfile(consumer, devices);
                System.out.println(results);
                rowCount.addAndGet(results.get(0));
                rowCount.addAndGet(results.get(1));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
    thread1.start();
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
    thread1.join();

    System.out.println(
        "src insert " + device + " :" + getCount(session_src, "select count(s_0) from " + device));
    System.out.println(
        "src insert "
            + device2
            + " :"
            + getCount(session_src, "select count(s_0) from " + device2));
    assertEquals(rowCount.get(), 200, "After first consumption");
    // Unsubscribe
    consumer.unsubscribe(topicName);
    System.out.println("###### After cancellation query:");
    subs.getSubscriptions(topicName).forEach((System.out::println));
    assertEquals(
        subs.getSubscriptions(topicName).size(), 0, "Unsubscribe 1 and then show subscriptions");

    // Unsubscribe and then write data
    insert_data(System.currentTimeMillis(), device2);
    int result = consume_tsfile(consumer, device2);
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1703980800000,3.45,'2023-12-31 08:00:00+08:00');"); // 2023-12-31 08:00:00+08:00
    assertEquals(result, 5, "After the second consumption");

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
