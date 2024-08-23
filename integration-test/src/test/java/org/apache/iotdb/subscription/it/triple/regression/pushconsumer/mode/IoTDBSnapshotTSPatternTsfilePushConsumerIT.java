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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.mode;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * PushConsumer
 * TsFile
 * pattern: ts
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBSnapshotTSPatternTsfilePushConsumerIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.SnapshotTSPatternTsfilePushConsumer";
  private static final String database2 = "root.SnapshotTSPatternTsfilePushConsumer";
  private static final String device = database + ".d_0";
  private static final String device2 = database + ".d_1";
  private static final String topicName = "topic_SnapshotTSPatternTsfilePushConsumer";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private static final String pattern = device + ".s_0";
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
        "2024-03-31T00:00:00+08:00",
        true,
        TopicConstant.MODE_SNAPSHOT_VALUE,
        null);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_src.executeNonQueryStatement(
        "create aligned timeseries " + database + ".d_1(s_0 int64,s_1 double);");
    session_dest.executeNonQueryStatement(
        "create aligned timeseries " + database + ".d_1(s_0 int64,s_1 double);");
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
    session_src.executeNonQueryStatement("flush;");
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
            + " where time >= 2024-01-01T00:00:00+08:00 and time <= 2024-03-31T00:00:00+08:00";
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    final AtomicInteger onReceiveCount = new AtomicInteger(0);
    List<AtomicInteger> rowCounts = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      rowCounts.add(new AtomicInteger(0));
    }

    Path path_d0s0 = new Path(device, "s_0", true);
    Path path_d0s1 = new Path(device, "s_1", true);
    Path path_d1s0 = new Path(database + ".d_1", "s_0", true);
    Path path_other_d2 = new Path(database2 + ".d_2", "s_0", true);
    List<Path> paths = new ArrayList<>(4);
    paths.add(path_d0s0);
    paths.add(path_d0s1);
    paths.add(path_d1s0);
    paths.add(path_other_d2);

    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("ts_tsfile_snapshot")
            .consumerGroupId("push_mode")
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  onReceiveCount.incrementAndGet();
                  System.out.println("onReceiveCount=" + onReceiveCount.get());
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    for (int i = 0; i < 4; i++) {
                      QueryDataSet dataset =
                          reader.query(
                              QueryExpression.create(
                                  Collections.singletonList(paths.get(i)), null));
                      while (dataset.hasNext()) {
                        rowCounts.get(i).addAndGet(1);
                        RowRecord next = dataset.next();
                        System.out.println(next.getTimestamp() + "," + next.getFields());
                      }
                      System.out.println(
                          FORMAT.format(new Date())
                              + " rowCounts_"
                              + i
                              + ":"
                              + rowCounts.get(i).get());
                    }
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer();
    consumer.open();
    // Subscribe
    consumer.subscribe(topicName);
    subs.getSubscriptions(topicName).forEach(System.out::println);
    assertEquals(
        subs.getSubscriptions(topicName).size(), 1, "show subscriptions after subscription");

    insert_data(1707609600000L); // 2024-02-11 08:00:00+08:00
    insert_data(System.currentTimeMillis());
    System.out.println(FORMAT.format(new Date()) + " src:" + getCount(session_src, sql));

    AWAIT.untilAsserted(
        () -> {
          assertEquals(onReceiveCount.get(), 1, "receive files");
          assertEquals(rowCounts.get(0).get(), 5, device + ".s_0");
          assertEquals(rowCounts.get(1).get(), 0, device + ".s_1");
          assertEquals(rowCounts.get(2).get(), 0, database + ".d_1.s_0");
          assertEquals(rowCounts.get(3).get(), 0, database2 + ".d_2.s_0");
        });

    // Unsubscribe
    consumer.unsubscribe(topicName);
    // Subscribe and then write data
    consumer.subscribe(topicName);
    assertEquals(
        subs.getSubscriptions(topicName).size(), 1, "show subscriptions after re-subscribing");

    AWAIT.untilAsserted(
        () -> {
          assertGte(onReceiveCount.get(), 2, "receive files over 2");
          assertEquals(rowCounts.get(0).get(), 15, device + ".s_0");
          assertEquals(rowCounts.get(1).get(), 0, device + ".s_1");
          assertEquals(rowCounts.get(2).get(), 0, database + ".d_1.s_0");
          assertEquals(rowCounts.get(3).get(), 0, database2 + ".d_2.s_0");
        });
  }
}
