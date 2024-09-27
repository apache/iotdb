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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.format;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * PushConsumer: BEFORE_CONSUME
 * DataSet
 * pattern: root.**
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBTestPushConsumeDataSetIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.TestPushConsumeDataSet";
  private static final String topicName = "topic_TestPushConsumeDataSet";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private static final String pattern = "root.**";
  private static SubscriptionPushConsumer consumer;
  private static final String device = database + ".d_push_dataset";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(topicName, pattern, null, null, false);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "create show topics");
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
    super.tearDown();
  }

  private void insert_data(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    Tablet tablet = new Tablet(device, schemaList, 5);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
      timestamp += row * 2000;
    }
    session_src.insertTablet(tablet);
    session_src.executeNonQueryStatement("flush;");
  }

  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00

    final AtomicInteger rowCount = new AtomicInteger(0);
    consumer =
        new SubscriptionPushConsumer.Builder()
            .nodeUrls(Collections.singletonList(SRC_HOST + ":" + SRC_PORT))
            .consumerId("root_dataset_consumer")
            .consumerGroupId("push_format")
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .fileSaveDir("target/iotdb-subscription")
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
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");
    insert_data(System.currentTimeMillis());
    String sql0 = "select count(s_0) from " + device;
    String sql1 = "select count(s_1) from " + device;
    long expectCount = getCount(session_src, sql0);
    System.out.println("###### src " + expectCount);
    long finalExpectCount = expectCount;
    AWAIT.untilAsserted(
        () -> {
          assertEquals(getCount(session_dest, sql0), finalExpectCount, "First result:s_0");
          assertEquals(getCount(session_dest, sql1), finalExpectCount, "First result:s_1");
        });

    // Unsubscribe
    consumer.unsubscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 0, "After cancellation, show subscriptions");
    // Subscribe and then write data
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after re-subscribing");
    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    expectCount = getCount(session_src, sql0);
    System.out.println("###### src2 " + expectCount);
    // Consumption data: Progress is not retained when re-subscribing after cancellation. Full
    // synchronization.
    long finalExpectCount1 = expectCount;
    AWAIT.untilAsserted(
        () -> {
          assertEquals(getCount(session_dest, sql0), finalExpectCount1, "Second result: s_0");
          assertEquals(getCount(session_dest, sql1), finalExpectCount1, "Second result: s_1");
        });
  }
}
