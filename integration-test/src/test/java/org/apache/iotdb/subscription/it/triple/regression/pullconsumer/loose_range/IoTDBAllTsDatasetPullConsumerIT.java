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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.loose_range;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
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
 * pull consumer
 * pattern: ts
 * accurate time
 * format: dataset
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBAllTsDatasetPullConsumerIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.AllTsDatasetPullConsumer";
  private static final String database2 = "root.test.AllTsDatasetPullConsumer";
  private static final String device = database + ".d_0";
  private String device2 = database + ".d_1";
  private static final String pattern = device + ".s_0";
  private static final String topicName = "topic_loose_range_all_pull_dataset";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createDB(database2);
    createTopic_s(
        topicName,
        pattern,
        "2024-01-01T00:00:00+08:00",
        "2024-03-31T23:59:59+08:00",
        false,
        TopicConstant.MODE_LIVE_VALUE,
        TopicConstant.LOOSE_RANGE_ALL_VALUE);
    session_src.createTimeseries(
        pattern, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        pattern, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_src.executeNonQueryStatement(
        "create aligned timeseries " + device2 + "(s_0 int64,s_1 double);");
    session_dest.executeNonQueryStatement(
        "create aligned timeseries " + device2 + "(s_0 int64,s_1 double);");
    session_src.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_0 int32;");
    session_dest.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_0 int32;");
    session_src.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_1 double;");
    session_dest.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_1 double;");
    session_src.executeNonQueryStatement(
        "insert into " + database2 + ".d_2(time,s_0,s_1)values(1000,132,4567.89);");
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "create topics");
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
      tablet.addValue("s_0", rowIndex, (1 + row) * 20L + row);
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
    String sql =
        "select count(s_0) from "
            + device
            + " where time >= 2024-01-01T00:00:00+08:00 and time <= 2024-03-31T23:59:59+08:00";
    consumer = create_pull_consumer("ts_accurate_dataset_pull", "loose_range_all", false, null);
    // Write data before subscribing
    insert_data(1704038396000L, device); // 2023-12-31 23:59:56+08:00
    insert_data(1704038396000L, device2); // 2023-12-31 23:59:56+08:00
    System.out.println("src filter:" + getCount(session_src, sql));
    // Subscribe
    consumer.subscribe(topicName);
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    // Before consumption subscription data
    consume_data(consumer, session_dest);
    check_count_non_strict(
        3, "select count(s_0) from " + device, "Start time boundary data: s_0 " + device);
    check_count(0, "select count(s_1) from " + device, "Start time boundary data: s_1 " + device);
    check_count(0, "select count(s_0) from " + device2, "Start time boundary data: s_0 " + device2);
    check_count(0, "select count(s_1) from " + device2, "Start time boundary data: s_1 " + device2);

    insert_data(System.currentTimeMillis(), device); // now
    insert_data(System.currentTimeMillis(), device2); // now
    System.out.println("src filter:" + getCount(session_src, sql));
    consume_data(consumer, session_dest);
    check_count_non_strict(
        3, "select count(s_0) from " + device, "Write some real-time data later:s_0 " + device);
    check_count(
        0, "select count(s_1) from " + device, "Write some real-time data later: s_1 " + device);
    check_count(0, "select count(s_0) from " + device2, "not in range: s_0 " + device2);
    check_count(0, "select count(s_1) from " + device2, "not in range: s_1 " + device2);

    insert_data(1707782400000L, device); // 2024-02-13 08:00:00+08:00
    insert_data(1707782400000L, device2); // 2024-02-13 08:00:00+08:00
    consume_data(consumer, session_dest);
    System.out.println("src filter:" + getCount(session_src, sql));
    check_count_non_strict(
        8, "select count(s_0) from " + device, "data within the time range: s_0 " + device);
    check_count(0, "select count(s_1) from " + device, "data within the time range: s_1 " + device);
    check_count(
        0, "select count(s_0) from " + device2, "data within the time range: s_0 " + device2);
    check_count(
        0, "select count(s_1) from " + device2, "data within the time range: s_1 " + device2);

    insert_data(1711814398000L, device); // 2024-03-30 23:59:58+08:00
    insert_data(1711814398000L, device2); // 2024-03-30 23:59:58+08:00
    System.out.println("src filter:" + getCount(session_src, sql));
    consume_data(consumer, session_dest);
    check_count_non_strict(
        13, "select count(s_0) from " + device, "End time limit data: s_0 " + device);
    check_count(0, "select count(s_1) from " + device, "End time limit data: s_1" + device);
    check_count(0, "select count(s_0) from " + device2, "End time limit data: s_0" + device2);
    check_count(0, "select count(s_1) from " + device2, "End time limit data: s_1" + device2);

    insert_data(1711900798000L, device); // 2024-03-31 23:59:58+08:00
    insert_data(1711900798000L, device2); // 2024-03-31 23:59:58+08:00
    System.out.println("src filter:" + getCount(session_src, sql));
    consume_data(consumer, session_dest);
    check_count_non_strict(
        14, "select count(s_0) from " + device, "End time limit data 2:s_0 " + device);
    check_count(0, "select count(s_1) from " + device, "End time limit data 2:s_1" + device);
    check_count(0, "select count(s_0) from " + device2, "End time limit data 2: s_0" + device2);
    check_count(0, "select count(s_1) from " + device2, "End time limit data 2: s_1" + device2);

    consumer.unsubscribe(topicName);
    consumer.subscribe(topicName);
    consume_data(consumer, session_dest);
    check_count_non_strict(
        14, "select count(s_0) from " + device, "End time limit data 2:s_0 " + device);
    check_count(0, "select count(s_1) from " + device, "End time limit data 2:s_1" + device);
    check_count(0, "select count(s_0) from " + device2, "End time limit data 2: s_0" + device2);
    check_count(0, "select count(s_1) from " + device2, "End time limit data 2: s_1" + device2);
  }
}
