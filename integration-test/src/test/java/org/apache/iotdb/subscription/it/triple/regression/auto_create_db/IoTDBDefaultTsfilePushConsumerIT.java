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

package org.apache.iotdb.subscription.it.triple.regression.auto_create_db;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
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
 * pattern: root.**
 * TsFile
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBDefaultTsfilePushConsumerIT extends AbstractSubscriptionRegressionIT {
  private SubscriptionPushConsumer consumer;
  private int deviceCount = 3;
  private static final String databasePrefix = "root.DefaultTsfilePushConsumer";
  private static String topicName = "topicDefaultTsfilePushConsumer";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createTopic_s(topicName, null, null, null, true);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
    for (int i = 0; i < deviceCount; i++) {
      session_src.executeNonQueryStatement("create database " + databasePrefix + i);
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      consumer.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
    for (int i = 0; i < deviceCount; i++) {
      session_src.executeNonQueryStatement("drop database " + databasePrefix + i);
    }
    super.tearDown();
  }

  private void insert_data(long timestamp, String device)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, (1 + row) * 20 + row);
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
    List<String> devices = new ArrayList<>(deviceCount);
    List<Path> paths = new ArrayList<>(deviceCount);
    for (int i = 0; i < deviceCount; i++) {
      devices.add(databasePrefix + i + ".d_0");
      paths.add(new Path(devices.get(i), "s_0", true));
    }
    System.out.println("### Before Subscription Write Data ###");
    for (int i = 0; i < deviceCount; i++) {
      insert_data(1706659200000L, devices.get(i)); // 2024-01-31 08:00:00+08:00
    }
    session_src.executeNonQueryStatement("flush");
    final AtomicInteger onReceiveCount = new AtomicInteger(0);
    List<AtomicInteger> rowCounts = new ArrayList<>(deviceCount);
    for (int i = 0; i < deviceCount; i++) {
      rowCounts.add(new AtomicInteger(0));
    }
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("default_pattern_TsFile_consumer")
            .consumerGroupId("push_auto_create_db")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  onReceiveCount.incrementAndGet();
                  System.out.println(
                      FORMAT.format(new Date()) + " ######## onReceived: " + onReceiveCount.get());
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    for (int i = 0; i < deviceCount; i++) {
                      QueryDataSet dataset =
                          reader.query(
                              QueryExpression.create(
                                  Collections.singletonList(paths.get(i)), null));
                      while (dataset.hasNext()) {
                        rowCounts.get(i).addAndGet(1);
                        RowRecord next = dataset.next();
                        //                                System.out.println(format.format(new
                        // Date())+" "+next.getTimestamp()+","+next.getFields());
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
        subs.getSubscriptions(topicName).size(), 1, "After subscribing: show subscriptions");
    for (int i = 0; i < deviceCount; i++) {
      insert_data(System.currentTimeMillis(), devices.get(i));
      System.out.println(
          FORMAT.format(new Date())
              + " src "
              + i
              + ":"
              + getCount(session_src, "select count(s_0) from " + devices.get(i)));
    }
    session_src.executeNonQueryStatement("flush");
    AWAIT.untilAsserted(
        () -> {
          for (int i = 0; i < deviceCount; i++) {
            assertEquals(rowCounts.get(i).get(), 10, devices.get(i) + ".s_0");
          }
        });
    // Unsubscribe
    consumer.unsubscribe(topicName);
    // Subscribe and then write data
    consumer.subscribe(topicName);
    System.out.println("### Subscribe and write data ###");
    assertEquals(
        subs.getSubscriptions(topicName).size(), 1, "After subscribing again: show subscriptions");
    for (int i = 0; i < deviceCount; i++) {
      insert_data(1707782400000L, devices.get(i)); // 2024-02-13 08:00:00+08:00
      System.out.println(
          FORMAT.format(new Date())
              + " src "
              + i
              + ":"
              + getCount(session_src, "select count(s_0) from " + devices.get(i)));
    }
    session_src.executeNonQueryStatement("flush");

    // Unsubscribe, then it will consume all again.
    AWAIT.untilAsserted(
        () -> {
          for (int i = 0; i < deviceCount; i++) {
            assertEquals(rowCounts.get(i).get(), 25, devices.get(i) + ".s_0");
          }
        });
    System.out.println(FORMAT.format(new Date()) + " onReceived: " + onReceiveCount.get());
  }
}
