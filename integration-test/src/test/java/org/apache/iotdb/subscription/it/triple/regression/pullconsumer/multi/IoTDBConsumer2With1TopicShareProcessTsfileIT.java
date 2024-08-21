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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * format: tsfile
 * pattern:device
 * Same group pull consumer share progress
 * About 1/5 chance, over 1000
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBConsumer2With1TopicShareProcessTsfileIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.Consumer2With1TopicShareProcessTsfile";
  private static final String device = database + ".d_0";
  private static final String topicName = "topicConsumer2With1TopicShareProcessTsfile";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private String pattern = device + ".**";
  private SubscriptionPullConsumer consumer2;
  private static SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createTopic_s(topicName, pattern, null, null, true);
    createDB(database);
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
    assertTrue(subs.getTopic(topicName).isPresent(), "create show topics");
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
    consumer = create_pull_consumer("tsfile_group_share_process", "c1", false, null);
    consumer2 = create_pull_consumer("tsfile_group_share_process", "c2", false, 10L);
    // Subscribe
    assertEquals(
        subs.getSubscriptions(topicName).size(), 0, "Show subscriptions before subscribing");
    consumer.subscribe(topicName);
    consumer2.subscribe(topicName);
    subs.getSubscriptions(topicName).forEach((System.out::println));
    assertEquals(
        subs.getSubscriptions(topicName).size(), 1, "show subscriptions after subscription");
    // insert 1000 records
    Thread thread =
        new Thread(
            () -> {
              for (int i = 0; i < 200; i++) {
                try {
                  insert_data(1706659200000L);
                  Thread.sleep(1000);
                } catch (IoTDBConnectionException e) {
                  throw new RuntimeException(e);
                } catch (StatementExecutionException e) {
                  throw new RuntimeException(e);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });
    AtomicInteger rowCount1 = new AtomicInteger(0);
    AtomicInteger rowCount2 = new AtomicInteger(0);
    Thread thread1 =
        new Thread(
            () -> {
              try {
                rowCount1.addAndGet(consume_tsfile(consumer, device));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
    Thread thread2 =
        new Thread(
            () -> {
              try {
                rowCount2.addAndGet(consume_tsfile(consumer2, device));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    thread1.start();
    thread2.start();
    thread.start();
    thread1.join();
    thread2.join();
    thread.join();

    System.out.println("src :" + getCount(session_src, "select count(s_0) from " + device));
    System.out.println("rowCount1=" + rowCount1.get());
    System.out.println("rowCount2=" + rowCount2.get());
    AWAIT.untilAsserted(
        () -> {
          assertGte(
              rowCount1.get() + rowCount2.get(),
              1000,
              "consumer share process rowCount1="
                  + rowCount1.get()
                  + " rowCount2="
                  + rowCount2.get()
                  + " src="
                  + getCount(session_src, "select count(s_0) from " + device));
          assertTrue(rowCount1.get() > 0);
          assertTrue(rowCount2.get() > 0);
        });
  }
}
