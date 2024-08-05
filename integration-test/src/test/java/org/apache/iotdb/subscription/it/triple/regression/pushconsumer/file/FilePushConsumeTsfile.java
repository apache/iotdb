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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.file;

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
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * PushConsumer:AFTER_CONSUME
 * TsFileHandler
 * pattern: db
 */
public class FilePushConsumeTsfile extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.TestAutoCommitFalseDataSetPullConsumer";
  private static final String device = database + ".d_0";
  private static final String topicName = "Topic_auto_commit_false";
  private static final String pattern = database + ".**";
  private static SubscriptionPushConsumer consumer;
  private static List<MeasurementSchema> schemaList = new ArrayList<>();

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
      tablet.addValue("s_0", rowIndex, (row + 1) * 20L + row);
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
    // Subscribe before writing data
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00

    final AtomicInteger onReceiveCount = new AtomicInteger(0);
    final AtomicInteger rowCount = new AtomicInteger(0);
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("TsFile_target_consumer")
            .consumerGroupId("push_file_process")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveFsync(true)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  onReceiveCount.incrementAndGet();
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    Path path = new Path(device, "s_0", true);
                    QueryDataSet dataset =
                        reader.query(QueryExpression.create(Collections.singletonList(path), null));
                    while (dataset.hasNext()) {
                      rowCount.addAndGet(1);
                      RowRecord next = dataset.next();
                      System.out.println(next.getTimestamp() + "," + next.getFields());
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
    insert_data(System.currentTimeMillis());

    AWAIT.untilAsserted(
        () -> {
          assertEquals(rowCount.get(), 8, "should process 8 rows data");
        });
    // Unsubscribe
    consumer.unsubscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 0, "Show subscriptions after cancellation");
    // Subscribe and then write data
    consumer.subscribe(topicName);
    assertEquals(
        subs.getSubscriptions(topicName).size(), 1, "show subscriptions after re-subscribing");
    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    AWAIT.untilAsserted(
        () -> {
          // Consumption data: Progress is not retained when re-subscribing after cancellation. Full
          // synchronization.
          assertEquals(rowCount.get(), 20, "should process 20 rows data, include ");
          assertEquals(
              consumer.allSnapshotTopicMessagesHaveBeenConsumed(),
              true,
              "All messages have been consumed");
        });
  }
}
