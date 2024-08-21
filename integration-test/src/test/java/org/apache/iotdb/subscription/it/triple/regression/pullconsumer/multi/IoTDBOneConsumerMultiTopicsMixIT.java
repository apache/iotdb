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
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * 1 consumer subscribes to 2 topics: Historical data
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBOneConsumerMultiTopicsMixIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.OneConsumerMultiTopicsMix";
  private static final String device = database + ".d_0";
  private String pattern = device + ".s_0";
  private String pattern2 = "root.**";
  private String topicName = "topic1_OneConsumerMultiTopicsMix";
  private String topicName2 = "topic2_OneConsumerMultiTopicsMix";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(topicName, pattern, null, "now", false);
    createTopic_s(topicName2, pattern2, null, "now", true);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.FLOAT));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.TEXT));
    assertTrue(subs.getTopic(topicName).isPresent(), "create show topics");
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
    super.tearDown();
  }

  private void insert_data(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row + 2.45f);
      tablet.addValue("s_1", rowIndex, "rowIndex" + rowIndex);
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
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1710288000000,313,'2024-03-13 08:00:00+08:00');"); // 2024-03-13
    // 08:00:00+08:00
    session_src.executeNonQueryStatement("flush;");
    // Subscribe
    consumer = create_pull_consumer("multi_1consumer_mix", "tsfile_dataset", false, null);
    System.out.println("###### Before Subscription Query:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 0, "Before subscription show subscriptions");
    consumer.subscribe(topicName, topicName2);
    long timestamp = System.currentTimeMillis();
    System.out.println("###### Subscription Query:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 2, "show subscriptions after subscription");
    // Subscribe and then write data
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  insert_data(System.currentTimeMillis());
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    thread.start();

    AtomicInteger rowCount = new AtomicInteger(0);
    Thread thread2 =
        new Thread(
            () -> {
              while (true) {
                List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                if (messages.isEmpty()) {
                  break;
                }

                for (final SubscriptionMessage message : messages) {
                  final short messageType = message.getMessageType();
                  if (SubscriptionMessageType.isValidatedMessageType(messageType)) {
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
                        for (final Iterator<Tablet> it =
                                message.getSessionDataSetsHandler().tabletIterator();
                            it.hasNext(); ) {
                          final Tablet tablet = it.next();
                          try {
                            session_dest.insertTablet(tablet);
                            //
                            // System.out.println(format.format(new Date())+" "+tablet.rowSize);
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
                                      Collections.singletonList(new Path(device, "s_1", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount.addAndGet(1);
                            RowRecord next = dataset.next();
                            System.out.println(
                                device + ".s_1:" + next.getTimestamp() + "," + next.getFields());
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        consumer.commitSync(messages);
                        break;
                    }
                  }
                }
              }
            });
    Thread thread3 =
        new Thread(
            () -> {
              while (true) {
                List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                if (messages.isEmpty()) {
                  break;
                }

                for (final SubscriptionMessage message : messages) {
                  final short messageType = message.getMessageType();
                  if (SubscriptionMessageType.isValidatedMessageType(messageType)) {
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
                        for (final Iterator<Tablet> it =
                                message.getSessionDataSetsHandler().tabletIterator();
                            it.hasNext(); ) {
                          final Tablet tablet = it.next();
                          try {
                            session_dest.insertTablet(tablet);
                            System.out.println(FORMAT.format(new Date()) + " " + tablet.rowSize);
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
                                      Collections.singletonList(new Path(device, "s_1", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount.addAndGet(1);
                            RowRecord next = dataset.next();
                            System.out.println(
                                device + ":" + next.getTimestamp() + "," + next.getFields());
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        consumer.commitSync(messages);
                        break;
                    }
                  }
                }
              }
            });
    thread2.start();
    thread.join();
    thread2.join(5000);
    String sql1 = "select count(s_0) from " + device + " where time <= " + timestamp;
    String sql2 = "select count(s_1) from " + device + " where time <= " + timestamp;

    System.out.println(FORMAT.format(new Date()) + " src:" + getCount(session_src, sql1));
    AWAIT.untilAsserted(
        () -> {
          // Consumption data
          check_count(6, sql1, "Consumption data:" + pattern);
          check_count(0, sql2, "Consumption data:" + pattern2);
          assertEquals(rowCount.get(), 6, "tsfile consumer");
        });
    // Unsubscribe
    consumer.unsubscribe(topicName);
    System.out.println("###### After unsubscribing query:");
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 1, "Unsubscribe 1 and then show subscriptions");

    // Unsubscribe and then write data
    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,s_0,s_1)values(1703980800000,3.45,'2023-12-31 08:00:00+08:00');"); // 2023-12-31 08:00:00+08:00
    insert_data(System.currentTimeMillis());
    session_src.executeNonQueryStatement("flush;");
    System.out.println(
        FORMAT.format(new Date())
            + " Unsubscribe after writing data src:"
            + getCount(session_src, sql1));

    thread3.start();
    thread3.join(5000);
    System.out.println(FORMAT.format(new Date()));
    AWAIT.untilAsserted(
        () -> {
          assertEquals(
              rowCount.get(), 12, "Re-consume data: tsfile consumer " + FORMAT.format(new Date()));
          check_count(6, sql1, "consume data again:" + pattern);
          check_count(0, sql2, "Reconsume data:" + pattern2);
        });
    // close
    consumer.close();
    try {
      consumer.subscribe(topicName, topicName2);
    } catch (Exception e) {
      System.out.println("subscribe again after close, expecting an exception");
    }
    assertEquals(subs.getSubscriptions().size(), 0, "show subscriptions after close");
    subs.getSubscriptions().forEach((System.out::println));
  }
}
