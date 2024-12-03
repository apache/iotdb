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

package org.apache.iotdb.subscription.it.triple.regression.auto_create_db;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
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

/***
 * PullConsumer
 * pattern: db
 * Tsfile
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBRootPullConsumeTsfileIT extends AbstractSubscriptionRegressionIT {
  private static final String pattern = "root.**";
  private static final String device = "root.auto_create_db.RootPullConsumeTsfile.d_0";
  private static final String device2 = "root.RootPullConsumeTsfile.d_1";
  public static SubscriptionPullConsumer consumer;
  private static String topicName = "topicAutoCreateDB_RootPullConsumeTsfile";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createTopic_s(topicName, pattern, null, null, true);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
    session_src.executeNonQueryStatement("create database root.auto_create_db");
    session_src.executeNonQueryStatement("create database root.RootPullConsumeTsfile");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      consumer.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
    session_src.executeNonQueryStatement("drop database root.auto_create_db");
    session_src.executeNonQueryStatement("drop database root.RootPullConsumeTsfile");
    super.tearDown();
  }

  private void insert_data(long timestamp, String device)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    Tablet tablet = new Tablet(device, schemaList, 5);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, (row + 1) * 20 + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
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
    // Write data before subscribing
    insert_data(1706659200000L, device); // 2024-01-31 08:00:00+08:00
    insert_data(1706659200000L, device2); // 2024-01-31 08:00:00+08:00
    consumer =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("root_tsfile")
            .consumerGroupId("pull_auto_create_db")
            .autoCommit(false)
            .fileSaveDir("target/pull-subscription") // hack for license check
            .buildPullConsumer();
    consumer.open();
    consumer.subscribe(topicName);
    subs.getSubscriptions(topicName).forEach(System.out::println);
    assertEquals(subs.getSubscriptions(topicName).size(), 1, "subscribe:show subscriptions");
    insert_data(System.currentTimeMillis(), device);
    insert_data(System.currentTimeMillis(), device2);
    List<String> devices = new ArrayList<>(2);
    devices.add(device);
    devices.add(device2);
    List<Integer> results = consume_tsfile(consumer, devices);
    assertEquals(results.get(0), 10);
    assertEquals(results.get(1), 10);
    consumer.unsubscribe(topicName);
    assertEquals(subs.getSubscriptions(topicName).size(), 0, "unsubscribe:show subscriptions");
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "subscribe again:show subscriptions");
    insert_data(1707782400000L, device); // 2024-02-13 08:00:00+08:00
    insert_data(1707782400000L, device2); // 2024-02-13 08:00:00+08:00
    results = consume_tsfile(consumer, devices);
    assertEquals(
        results.get(0),
        15,
        "Unsubscribing and then re-subscribing will not retain progress. Full synchronization.");
    assertEquals(
        results.get(1),
        15,
        "Unsubscribing and then re-subscribing will not retain progress. Full synchronization.");
  }
}
