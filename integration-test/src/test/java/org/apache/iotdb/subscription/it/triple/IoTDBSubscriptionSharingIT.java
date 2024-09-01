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

package org.apache.iotdb.subscription.it.triple;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionArchVerification;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

/**
 * refer to {@link
 * org.apache.iotdb.subscription.it.triple.regression.pushconsumer.multi.IoTDBMultiGroupVsMultiConsumerIT}
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionArchVerification.class})
public class IoTDBSubscriptionSharingIT extends AbstractSubscriptionTripleIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionSharingIT.class);

  private final String topicNamePrefix = "topic_";
  private final String databasePrefix = "root.test.g_";

  private final AtomicLong rowCount00 = new AtomicLong(0);
  private final AtomicLong rowCount10 = new AtomicLong(0);
  private final AtomicLong rowCount70 = new AtomicLong(0);
  private final AtomicLong rowCount90 = new AtomicLong(0);
  private final AtomicLong rowCount6 = new AtomicLong(0);

  private final String sql1 = "select count(s_0) from " + databasePrefix + "1.d_0";
  private final String sql2 = "select count(s_0) from " + databasePrefix + "2.d_0";
  private final String sql3 = "select count(s_0) from " + databasePrefix + "3.d_0";
  private final String sql4 = "select count(s_0) from " + databasePrefix + "4.d_0";

  private final List<IMeasurementSchema> schemaList = new ArrayList<>(2);
  private final List<SubscriptionPushConsumer> consumers = new ArrayList<>(10);

  private void createTopic(
      final String topicName,
      final String path,
      final String startTime,
      final String endTime,
      final boolean isTsFile) {
    final String host = sender.getIP();
    final int port = Integer.parseInt(sender.getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      if (path != null) {
        properties.setProperty(TopicConstant.PATH_KEY, path);
      }
      if (startTime != null) {
        properties.setProperty(TopicConstant.START_TIME_KEY, startTime);
      }
      if (endTime != null) {
        properties.setProperty(TopicConstant.END_TIME_KEY, endTime);
      }
      if (isTsFile) {
        properties.setProperty(
            TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      } else {
        properties.setProperty(
            TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
      }
      session.createTopic(topicName, properties);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void insertData(long timestamp, final String device, final int rows) {
    // Insert some data on sender
    try (final ISession session = sender.getSessionConnection()) {
      final Tablet tablet = new Tablet(device, schemaList, rows);
      int rowIndex;
      for (int row = 0; row < rows; row++) {
        rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, (row + 1) * 1400 + row);
        tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (row + 1) * 100 + 0.5);
        timestamp += 2000;
      }
      session.insertTablet(tablet);
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createTopics() {
    createTopic(
        topicNamePrefix + 0,
        databasePrefix + "0.**",
        "2024-01-01T00:00:00+08:00",
        "2024-03-31T23:59:59+08:00",
        true);
    createTopic(topicNamePrefix + 1, databasePrefix + "1.**", null, null, false);

    createTopic(topicNamePrefix + 2, databasePrefix + "2.**", "now", null, false);
    createTopic(topicNamePrefix + 3, databasePrefix + "3.**", null, "now", false);
    createTopic(
        topicNamePrefix + 4, databasePrefix + "4.**", null, "2024-03-31T23:59:59+08:00", false);

    createTopic(topicNamePrefix + 6, databasePrefix + "6.**", "now", null, true);

    createTopic(
        topicNamePrefix + 5, databasePrefix + "5.**", "2024-01-01T00:00:00+08:00", null, false);
    createTopic(
        topicNamePrefix + 7, databasePrefix + "7.**", null, "2024-03-31T23:59:59+08:00", true);
    createTopic(topicNamePrefix + 8, databasePrefix + "8.**", null, "now", true);
    createTopic(
        topicNamePrefix + 9, databasePrefix + "9.**", "2024-01-01T00:00:00+08:00", null, true);
  }

  private void insertDatum() {
    long timestamp = 1706659200000L; // 2024-01-31 08:00:00+08:00

    for (int i = 0; i < 20; i++) {
      for (int k = 0; k < 10; k++) {
        final String device = databasePrefix + k + ".d_0";
        insertData(timestamp, device, 20);
      }
      timestamp += 40000;
    }

    for (int i = 0; i < 10; i++) {
      final String device = databasePrefix + i + ".d_0";
      insertData(System.currentTimeMillis(), device, 5);
    }

    final String device = databasePrefix + 2 + ".d_0";
    for (int i = 0; i < 20; i++) {
      insertData(System.currentTimeMillis(), device, 5);
    }
  }

  private void preparePushConsumers() {
    // create consumers
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_0")
            .consumerGroupId("push_group_id_1")
            .consumeListener(
                message -> {
                  try {
                    try (final TsFileReader reader = message.getTsFileHandler().openReader()) {
                      final QueryDataSet dataset =
                          reader.query(
                              QueryExpression.create(
                                  Collections.singletonList(
                                      new Path(databasePrefix + "0.d_0", "s_0", true)),
                                  null));
                      while (dataset.hasNext()) {
                        rowCount00.addAndGet(1);
                        dataset.next();
                      }
                    }
                  } catch (final IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_1")
            .consumerGroupId("push_group_id_1")
            .consumeListener(
                message -> {
                  try {
                    try (final TsFileReader reader = message.getTsFileHandler().openReader()) {
                      final QueryDataSet dataset =
                          reader.query(
                              QueryExpression.create(
                                  Collections.singletonList(
                                      new Path(databasePrefix + "0.d_0", "s_0", true)),
                                  null));
                      while (dataset.hasNext()) {
                        rowCount10.addAndGet(1);
                        dataset.next();
                      }
                    }
                  } catch (final IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_2")
            .consumerGroupId("push_group_id_1")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      receiver1.getSessionConnection().insertTablet(dataSet.getTablet());
                    } catch (final StatementExecutionException | IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_3")
            .consumerGroupId("push_group_id_1")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      receiver2.getSessionConnection().insertTablet(dataSet.getTablet());
                    } catch (final StatementExecutionException | IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_4")
            .consumerGroupId("push_group_id_2")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      receiver1.getSessionConnection().insertTablet(dataSet.getTablet());
                    } catch (final StatementExecutionException | IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_5")
            .consumerGroupId("push_group_id_2")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      receiver2.getSessionConnection().insertTablet(dataSet.getTablet());
                    } catch (final StatementExecutionException | IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_6")
            .consumerGroupId("push_group_id_2")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      receiver1.getSessionConnection().insertTablet(dataSet.getTablet());
                    } catch (final StatementExecutionException | IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_7")
            .consumerGroupId("push_group_id_3")
            .consumeListener(
                message -> {
                  final short messageType = message.getMessageType();
                  if (SubscriptionMessageType.isValidatedMessageType(messageType)) {
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
                        for (final SubscriptionSessionDataSet dataSet :
                            message.getSessionDataSetsHandler()) {
                          try {
                            receiver1.getSessionConnection().insertTablet(dataSet.getTablet());
                          } catch (final StatementExecutionException | IoTDBConnectionException e) {
                            throw new RuntimeException(e);
                          }
                        }
                        break;
                      case TS_FILE_HANDLER:
                        try (final TsFileReader reader = message.getTsFileHandler().openReader()) {
                          final QueryDataSet dataset =
                              reader.query(
                                  QueryExpression.create(
                                      Collections.singletonList(
                                          new Path(databasePrefix + "0.d_0", "s_0", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount70.addAndGet(1);
                            dataset.next();
                          }
                        } catch (final IOException e) {
                          throw new RuntimeException(e);
                        }
                        break;
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_8")
            .consumerGroupId("push_group_id_3")
            .consumeListener(
                message -> {
                  try (final TsFileReader reader = message.getTsFileHandler().openReader()) {
                    final QueryDataSet dataset =
                        reader.query(
                            QueryExpression.create(
                                Collections.singletonList(
                                    new Path(databasePrefix + "6.d_0", "s_0", true)),
                                null));
                    while (dataset.hasNext()) {
                      rowCount6.addAndGet(1);
                      dataset.next();
                    }
                  } catch (final IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(sender.getIP())
            .port(Integer.parseInt(sender.getPort()))
            .consumerId("consumer_id_9")
            .consumerGroupId("push_group_id_3")
            .consumeListener(
                message -> {
                  final short messageType = message.getMessageType();
                  if (SubscriptionMessageType.isValidatedMessageType(messageType)) {
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
                        for (final SubscriptionSessionDataSet dataSet :
                            message.getSessionDataSetsHandler()) {
                          try {
                            receiver2.getSessionConnection().insertTablet(dataSet.getTablet());
                          } catch (final StatementExecutionException | IoTDBConnectionException e) {
                            throw new RuntimeException(e);
                          }
                        }
                        break;
                      case TS_FILE_HANDLER:
                        try (final TsFileReader reader = message.getTsFileHandler().openReader()) {
                          final QueryDataSet dataset =
                              reader.query(
                                  QueryExpression.create(
                                      Collections.singletonList(
                                          new Path(databasePrefix + "0.d_0", "s_0", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount90.addAndGet(1);
                            dataset.next();
                          }
                        } catch (final IOException e) {
                          throw new RuntimeException(e);
                        }
                        break;
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());

    // open consumers
    for (final SubscriptionPushConsumer consumer : consumers) {
      consumer.open();
    }

    // subscribe topics
    consumers.get(0).subscribe(topicNamePrefix + 0);
    consumers.get(1).subscribe(topicNamePrefix + 0);
    consumers.get(2).subscribe(topicNamePrefix + 1);
    consumers.get(3).subscribe(topicNamePrefix + 1);
    consumers.get(4).subscribe(topicNamePrefix + 2, topicNamePrefix + 3);
    consumers.get(5).subscribe(topicNamePrefix + 3, topicNamePrefix + 4);
    consumers.get(6).subscribe(topicNamePrefix + 2, topicNamePrefix + 4);
    consumers.get(7).subscribe(topicNamePrefix + 0, topicNamePrefix + 3);
    consumers.get(8).subscribe(topicNamePrefix + 6);
    consumers.get(9).subscribe(topicNamePrefix + 0, topicNamePrefix + 3);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    // prepare schemaList
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    // log some info
    try {
      LOGGER.info("[src] {} = {}", sql1, getCount(sender, sql1));
      LOGGER.info("[dest1] {} = {}", sql1, getCount(receiver1, sql1));
      LOGGER.info("[dest2] {} = {}", sql1, getCount(receiver2, sql1));

      LOGGER.info("[src] {} = {}", sql2, getCount(sender, sql2));
      LOGGER.info("[dest1] {} = {}", sql2, getCount(receiver1, sql2));
      LOGGER.info("[dest2] {} = {}", sql2, getCount(receiver2, sql2));

      LOGGER.info("[src] {} = {}", sql3, getCount(sender, sql3));
      LOGGER.info("[dest1] {} = {}", sql3, getCount(receiver1, sql3));
      LOGGER.info("[dest2] {} = {}", sql3, getCount(receiver2, sql3));

      LOGGER.info("[src] {} = {}", sql4, getCount(sender, sql4));
      LOGGER.info("[dest1] {} = {}", sql4, getCount(receiver1, sql4));
      LOGGER.info("[dest2] {} = {}", sql4, getCount(receiver2, sql4));
    } catch (final Exception ignored) {
    }

    LOGGER.info("rowCount00 = {}", rowCount00.get());
    LOGGER.info("rowCount10 = {}", rowCount10.get());
    LOGGER.info("rowCount70 = {}", rowCount70.get());
    LOGGER.info("rowCount90 = {}", rowCount90.get());
    LOGGER.info("rowCount6 = {}", rowCount6.get());

    // close consumers
    for (final SubscriptionPushConsumer consumer : consumers) {
      try {
        consumer.close();
      } catch (final Exception ignored) {
      }
    }

    super.tearDown();
  }

  @Test
  public void testSubscriptionSharing() {
    createTopics();
    preparePushConsumers();
    insertDatum();

    AWAIT.untilAsserted(
        () -> {
          // "c0,c1|topic0"
          final long topic0Group1Total = rowCount00.get() + rowCount10.get();
          // TODO: ensure that the total consumption of tsfile format data equals the written data
          Assert.assertTrue(400 <= topic0Group1Total && topic0Group1Total <= 800);

          // "c2,c3|topic1"
          Assert.assertEquals(
              getCount(sender, sql1), getCount(receiver1, sql1) + getCount(receiver2, sql1));

          // "c4,c6|topic2"
          Assert.assertEquals(
              getCount(sender, sql2) - 400, getCount(receiver1, sql2) + getCount(receiver2, sql2));

          // "c4,c5|c7,c9|topic3"
          final long topic3Total = getCount(receiver1, sql3) + getCount(receiver2, sql3);
          Assert.assertTrue(400 <= topic3Total && topic3Total <= 800);

          // "c5,c6|topic4"
          Assert.assertEquals(400, getCount(receiver1, sql4) + getCount(receiver2, sql4));

          // "c7,c9|topic0"
          final long topic0Group3Total = rowCount70.get() + rowCount90.get();
          // TODO: ensure that the total consumption of tsfile format data equals the written data
          Assert.assertTrue(400 <= topic0Group3Total && topic0Group3Total <= 800);

          // "c8|topic6"
          Assert.assertEquals(5, rowCount6.get());
        });
  }

  private static long getCount(final BaseEnv env, final String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    try (final ISession session = env.getSessionConnection()) {
      final SessionDataSet dataSet = session.executeQueryStatement(sql);
      return dataSet.hasNext() ? dataSet.next().getFields().get(0).getLongV() : 0;
    }
  }
}
