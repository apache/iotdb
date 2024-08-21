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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.autocommit;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/***
 * PullConsumer DataSet
 * pattern: device
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBTestAutoCommitTrueDataSetPullConsumerIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.TestAutoCommitTrueDataSetPullConsumer";
  private static final String device = database + ".d_0";
  private static final String topicName = "topic_auto_commit_true";
  private String pattern = device + ".**";
  private static SubscriptionPullConsumer consumer;
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

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
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest2.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest2.createTimeseries(
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
    } finally {
      subs.dropTopic(topicName);
      dropDB(database);
    }
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
      timestamp += row * 2000;
    }
    session_src.insertTablet(tablet);
  }

  private void consume_data_noCommit(SubscriptionPullConsumer consumer, Session session)
      throws InterruptedException,
          TException,
          IOException,
          StatementExecutionException,
          IoTDBConnectionException {
    while (true) {
      Thread.sleep(1000);
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
      if (messages.isEmpty()) {
        break;
      }
      for (final SubscriptionMessage message : messages) {
        for (final Iterator<Tablet> it = message.getSessionDataSetsHandler().tabletIterator();
            it.hasNext(); ) {
          final Tablet tablet = it.next();
          session.insertTablet(tablet);
          System.out.println(
              FORMAT.format(new Date()) + " consume data no commit:" + tablet.rowSize);
        }
      }
    }
  }

  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    String sql = "select count(s_0) from " + device;
    consumer = create_pull_consumer("pull_commit", "Auto_commit_true", true, 1000L);
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    // Subscribe
    assertEquals(subs.getSubscriptions().size(), 0, "Before subscription show subscriptions");
    consumer.subscribe(topicName);
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    System.out.println("First consumption");
    consume_data_noCommit(consumer, session_dest);
    System.out.println(FORMAT.format(new Date()) + ", " + getCount(session_src, sql));
    check_count(4, sql, "Consumption subscription previous data: s_0");
    check_count(4, "select count(s_1) from " + device, "Consumption subscription before data: s_1");
    // Subscribe and then write data
    insert_data(System.currentTimeMillis());
    consume_data_noCommit(consumer, session_dest2);
    System.out.println("second consumption");
    check_count2(4, "select count(s_0) from " + device, "Consumption subscription data 2: s_0");
    check_count2(4, "select count(s_1) from " + device, "Consumption subscription data 2:s_1");
    // Consumed data will not be consumed again
    consume_data_noCommit(consumer, session_dest);
    System.out.println("Third consumption");
    check_count(4, "select count(s_0) from " + device, "Consumption subscription before data: s_0");
    check_count(
        4, "select count(s_1) from " + device, "Consumption subscription previous data: s_1");
  }
}
