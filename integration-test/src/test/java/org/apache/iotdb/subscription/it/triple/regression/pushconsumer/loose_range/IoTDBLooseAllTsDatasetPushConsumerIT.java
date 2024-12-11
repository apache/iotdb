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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.loose_range;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
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
 * DataSet
 * pattern: ts
 * loose-range: all
 * mode: live
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBLooseAllTsDatasetPushConsumerIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.LooseAllTsDatasetPushConsumer";
  private static final String database2 = "root.LooseAllTsDatasetPushConsumer";
  private static final String device = database + ".d_0";
  private static final String device2 = database + ".d_1";
  private static final String topicName = "topic_LooseAllTsDatasetPushConsumer";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private String pattern = device + ".**";
  private static SubscriptionPushConsumer consumer;

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
        "2024-02-13T08:00:02+08:00",
        false,
        TopicConstant.MODE_LIVE_VALUE,
        TopicConstant.LOOSE_RANGE_ALL_VALUE);
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
    session_src.executeNonQueryStatement(
        "create aligned timeseries " + database2 + ".d_2(s_0 int32,s_1 float);");
    session_dest.executeNonQueryStatement(
        "create aligned timeseries " + database2 + ".d_2(s_0 int32,s_1 float);");
    session_src.executeNonQueryStatement(
        "insert into " + database2 + ".d_2(time,s_0,s_1)values(1000,132,4567.89);");
    session_src.executeNonQueryStatement(
        "insert into " + database + ".d_1(time,s_0,s_1)values(2000,232,567.891);");
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
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
    dropDB(database2);
    super.tearDown();
  }

  private void insert_data(long timestamp, String device)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 5);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, (row + 1) * 20L + row);
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
    String sql =
        "select count(s_0) from "
            + device
            + " where time >= 2024-01-01T00:00:00+08:00 and time <= 2024-02-13T08:00:02+08:00";

    // Write data before subscribing
    insert_data(1704038399000L, device); // 2023-12-31 23:59:59+08:00
    insert_data(1704038399000L, device2); // 2023-12-31 23:59:59+08:00
    insert_data(1706659200000L, device); // 2024-01-31 08:00:00+08:00
    insert_data(1706659200000L, device2); // 2024-01-31 08:00:00+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println("LooseAllTsDatasetPushConsumer src1: " + getCount(session_src, sql));

    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("device_accurate_dataset_push_snapshot")
            .consumerGroupId("loose_range_all")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveDir("target")
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

    // Subscribe
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    insert_data(1706745600000L, device); // 2024-02-01 08:00:00+08:00
    insert_data(1706745600000L, device2); // 2024-02-01 08:00:00+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println("LooseAllTsDatasetPushConsumer src2: " + getCount(session_src, sql));

    AWAIT.untilAsserted(
        () -> {
          check_count_non_strict(
              14, "select count(s_0) from " + device, "Consumption data: s_0 " + device);
          check_count_non_strict(
              14, "select count(s_1) from " + device, "Consumption data: s_1 " + device);
          check_count(0, "select count(s_0) from " + device2, "Consumption data: s_0 " + device2);
          check_count(0, "select count(s_1) from " + device2, "Consumption data: s_1 " + device2);
          check_count(0, "select count(s_0) from " + database2 + ".d_2", "Consumption data:d_2");
        });

    // Unsubscribe
    consumer.unsubscribe(topicName);
    // Subscribe and then write data
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after re-subscribing");

    insert_data(1707782400000L, device); // 2024-02-13 08:00:00+08:00
    insert_data(1707782400000L, device2); // 2024-02-13 08:00:00+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println("LooseAllTsDatasetPushConsumer src3: " + getCount(session_src, sql));

    // Consumption data: Progress is not retained when re-subscribing after cancellation. Full
    // synchronization.
    AWAIT.untilAsserted(
        () -> {
          check_count_non_strict(
              16, "select count(s_0) from " + device, "consume data again: s_0 " + device);
          check_count_non_strict(
              16, "select count(s_1) from " + device, "Consumption data: s_1 " + device);
          check_count(0, "select count(s_0) from " + device2, "Consumption data: s_0 " + device2);
          check_count(0, "select count(s_1) from " + device2, "Consumption data: s_1 " + device2);
          check_count(0, "select count(s_0) from " + database2 + ".d_2", "Consumption data:d_2");
        });
  }
}
