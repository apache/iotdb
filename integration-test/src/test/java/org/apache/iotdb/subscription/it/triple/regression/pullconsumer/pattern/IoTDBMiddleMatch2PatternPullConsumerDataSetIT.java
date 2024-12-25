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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.pattern;

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

/***
 * pattern: root.*.d_*.*
 * format: dataset
 * time-range: history
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBMiddleMatch2PatternPullConsumerDataSetIT
    extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.MiddleMatch2PatternPullConsumerDataSet";
  private static final String database2 = "root.MiddleMatch2PatternPullConsumerDataSet";
  private static List<String> devices = new ArrayList<>(3);
  private static final String device = database + ".d_0";
  private static final String device2 = database2 + ".sd_1";
  private static final String device3 = database2 + ".d_2";
  private static final String topicName = "topicMiddleMatch2PatternPullConsumerDataSet";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  private String pattern = "root.*.d_*.*";
  private static SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createDB(database2);
    createTopic_s(topicName, pattern, null, "now", false);
    devices.add(device);
    devices.add(device2);
    devices.add(device3);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_src.executeNonQueryStatement(
        "create aligned timeseries " + device2 + "(s_0 int64,s_1 double);");
    session_dest.executeNonQueryStatement(
        "create aligned timeseries " + device2 + "(s_0 int64,s_1 double);");
    session_src.executeNonQueryStatement("create timeseries " + device3 + ".s_0 int64;");
    session_dest.executeNonQueryStatement("create timeseries " + device3 + ".s_0 int64;");
    session_src.executeNonQueryStatement("create timeseries " + device3 + ".s_1 double;");
    session_dest.executeNonQueryStatement("create timeseries " + device3 + ".s_1 double;");
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    consumer.close();
    subs.dropTopic(topicName);
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
      tablet.addValue("s_0", rowIndex, (row + 1) * 20L + row);
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
    consumer =
        create_pull_consumer("pull_pattern", "MiddleMatchPatternHistory_DataSet", false, null);
    // Write data before subscribing
    for (int i = 0; i < 3; i++) {
      insert_data(1706659200000L, devices.get(i)); // 2024-01-31 08:00:00+08:00
    }
    // Subscribe
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");
    for (int i = 0; i < 3; i++) {
      insert_data(System.currentTimeMillis() - 30000L, devices.get(i));
    }
    // Consumption data
    consume_data(consumer, session_dest);
    String sql = "select count(s_0) from ";
    for (int i = 0; i < 3; i++) {
      System.out.println(
          "src " + devices.get(i) + ": " + getCount(session_src, sql + devices.get(i)));
    }
    check_count(0, sql + device, "Consumption data: s_0 " + device);
    check_count(0, "select count(s_1) from " + device, "Consumption data: s_1 " + device);
    check_count(0, "select count(s_0) from " + device2, "Consumption data: s_0 " + device2);
    check_count(0, "select count(s_1) from " + device2, "Consumption data: s_1 " + device2);
    check_count(10, "select count(s_0) from " + device3, "Consumption data: s_0 " + device3);
    check_count(10, "select count(s_1) from " + device3, "Consumption data: s_1 " + device3);
    for (int i = 0; i < 3; i++) {
      insert_data(System.currentTimeMillis(), devices.get(i));
    }
    // Unsubscribe
    consumer.unsubscribe(topicName);
    // Subscribe and then write data
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after re-subscribing");
    for (int i = 0; i < 3; i++) {
      System.out.println(
          "src " + devices.get(i) + ": " + getCount(session_src, sql + devices.get(i)));
    }
    // Consumption data: Progress is not retained when re-subscribing after cancellation. Full
    // synchronization.
    consume_data(consumer, session_dest);
    check_count(0, "select count(s_0) from " + device, "consume data again:" + device);
    check_count(0, "select count(s_0) from " + device2, "consume data again:" + device2);
    check_count(10, "select count(s_1) from " + device3, "Consumption data:" + device3);
  }
}
