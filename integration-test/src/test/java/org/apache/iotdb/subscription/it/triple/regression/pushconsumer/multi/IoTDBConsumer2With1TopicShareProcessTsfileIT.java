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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.multi;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * PushConsumer
 * pattern: db
 * tsfile
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBConsumer2With1TopicShareProcessTsfileIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.Consumer2With1TopicShareProcessTsfile";
  private static final String device = database + ".d_0";
  private static final String topicName = "topic_Consumer2With1TopicShareProcessTsfile";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  private static final String pattern = database + ".**";
  private static SubscriptionPushConsumer consumer;
  private static SubscriptionPushConsumer consumer2;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(topicName, pattern, null, null, true);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
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
      consumer2.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
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
    session_src.executeNonQueryStatement("flush");
  }

  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    Thread thread =
        new Thread(
            () -> {
              long timestamp = 1706659200000L; // 2024-01-31 08:00:00+08:00
              for (int i = 0; i < 20; i++) {
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
    AtomicInteger rowCount1 = new AtomicInteger(0);
    AtomicInteger rowCount2 = new AtomicInteger(0);
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("db_tsfile_consumer_1")
            .consumerGroupId("push_multi_Consumer2With1TopicShareProcessTsfile")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    List<Path> paths = new ArrayList<>(2);
                    for (int i = 0; i < 2; i++) {
                      paths.add(new Path(device, "s_" + i, true));
                    }
                    QueryDataSet dataset = reader.query(QueryExpression.create(paths, null));
                    while (dataset.hasNext()) {
                      rowCount1.addAndGet(1);
                      RowRecord next = dataset.next();
                      //
                      // System.out.println(next.getTimestamp()+","+next.getFields());
                    }
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer();
    consumer.open();
    consumer.subscribe(topicName);
    consumer2 =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("db_tsfile_consumer_2")
            .consumerGroupId("push_multi_Consumer2With1TopicShareProcessTsfile")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    List<Path> paths = new ArrayList<>(2);
                    for (int i = 0; i < 2; i++) {
                      paths.add(new Path(device, "s_" + i, true));
                    }
                    QueryDataSet dataset = reader.query(QueryExpression.create(paths, null));
                    while (dataset.hasNext()) {
                      rowCount2.addAndGet(1);
                      RowRecord next = dataset.next();
                      //
                      // System.out.println(next.getTimestamp()+","+next.getFields());
                    }
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer();
    consumer2.open();
    consumer2.subscribe(topicName);
    thread.start();

    thread.join();
    System.out.println("After subscription:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    // The first 5 entries may have duplicate data
    String sql = "select count(s_0) from " + device;
    System.out.println("src " + getCount(session_src, sql));
    System.out.println("rowCount1.get()=" + rowCount1.get());
    System.out.println("rowCount2.get()=" + rowCount2.get());

    AWAIT.untilAsserted(
        () -> {
          assertTrue(rowCount1.get() >= 0, "first consumer");
          assertTrue(rowCount2.get() >= 0, "second Consumer");
          assertGte(rowCount1.get() + rowCount2.get(), getCount(session_src, sql), "share process");
        });
  }
}
