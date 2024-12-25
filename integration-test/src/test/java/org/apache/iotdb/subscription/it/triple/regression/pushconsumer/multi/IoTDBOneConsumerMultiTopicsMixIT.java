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
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * 1 consumer subscribes to 2 topics: historical data
 * The timing of flush is very critical. If the data inside the filter and the data outside the filter are within one tsfile, they will all be extracted.
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBOneConsumerMultiTopicsMixIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.OneConsumerMultiTopicsMix";
  private static final String database2 = "root.OneConsumerMultiTopicsMix";
  private static final String device = database + ".d_0";
  private static final String topicName = "topic_OneConsumerMultiTopicsMix_1";
  private String pattern = database + ".**";
  private String pattern2 = database2 + ".**";
  private String device2 = database2 + ".d_0";
  private String topicName2 = "topic_OneConsumerMultiTopicsMix_2";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPushConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createDB(database2);
    createTopic_s(topicName, pattern, null, null, false);
    createTopic_s(topicName2, pattern2, null, null, true);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.LZMA2);
    session_src.createTimeseries(
        device2 + ".s_0", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device2 + ".s_1", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.TEXT));
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
    // Write data before subscribing
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1710288000000,313,'2024-03-13 08:00:00+08:00');"); // 2024-03-13
    // 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1703980800000,133.45,'2023-12-31 08:00:00+08:00');"); // 2023-12-31 08:00:00+08:00
    insert_data(1706659200000L, device); // 2024-01-31 08:00:00+08:00
    insert_data(1706659200000L, device2); // 2024-01-31 08:00:00+08:00
    AtomicInteger rowCount1 = new AtomicInteger(0);
    AtomicInteger rowCount2 = new AtomicInteger(0);
    // Subscribe
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("db_mix_consumer_2_topic")
            .consumerGroupId("OneConsumerMultiTopicsMix")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  final short messageType = message.getMessageType();
                  if (SubscriptionMessageType.isValidatedMessageType(messageType)) {
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
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
                        break;
                      case TS_FILE_HANDLER:
                        try {
                          TsFileReader reader = message.getTsFileHandler().openReader();
                          QueryDataSet dataset =
                              reader.query(
                                  QueryExpression.create(
                                      Collections.singletonList(new Path(device, "s_0", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount1.addAndGet(1);
                            RowRecord next = dataset.next();
                            System.out.println(
                                device + ":" + next.getTimestamp() + "," + next.getFields());
                          }
                          dataset =
                              reader.query(
                                  QueryExpression.create(
                                      Collections.singletonList(new Path(device2, "s_0", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount2.addAndGet(1);
                            RowRecord next = dataset.next();
                            System.out.println(
                                device2 + ":" + next.getTimestamp() + "," + next.getFields());
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        break;
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer();
    consumer.open();
    consumer.subscribe(topicName, topicName2);

    System.out.println("###### Subscription Query:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 2, "subscribe and show subscriptions");
    // Subscribe and then write data
    Thread thread =
        new Thread(
            () -> {
              try {
                insert_data(System.currentTimeMillis(), device);
                insert_data(System.currentTimeMillis(), device2);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    thread.start();
    thread.join();

    AWAIT.untilAsserted(
        () -> {
          assertEquals(rowCount1.get(), 0, "pattern1");
          check_count(12, "select count(s_0) from " + device, "dataset pattern1");
          assertEquals(rowCount2.get(), 10, "pattern2");
        });
  }
}
