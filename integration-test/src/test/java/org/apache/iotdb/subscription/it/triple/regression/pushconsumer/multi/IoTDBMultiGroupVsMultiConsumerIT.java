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
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/***
 * pattern: device, different db
 * |c0|t0|g1|  tsfile databasePrefix+"0.**", "2024-01-01T00:00:00+08:00", "2024-03-31T23:59:59+08:00"
 * |c1|t0|g1|  tsfile
 * |c2|t1|g1|  dataset(dest) databasePrefix+"1.**"
 * |c3|t1|g1|  dataset(dest2)
 * |c4|t2,t3|g2| dataset(dest) databasePrefix+"2.**", "now", null;  databasePrefix+"3.**", null, "now"
 * |c5|t3,t4|g2| dataset(dest2) databasePrefix+"4.**",  null, "2024-03-31T23:59:59+08:00"
 * |c6|t2,t4|g2| dataset(dest)
 * |c7|t0,t3|g3| dataset(dest)/tsfile
 * |c8|t6|g3|  tsfile databasePrefix+"6.**", "now", null,
 * |c9|t0,t3|g3| dataset(dest2)/tsfile
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBMultiGroupVsMultiConsumerIT extends AbstractSubscriptionRegressionIT {

  private String topicNamePrefix = "topic_pushMultiGroupVsMultiConsumer_";
  private String databasePrefix = "root.test.pushMultiGroupVsMultiConsumer_";
  private int tsCount = 10;
  private int consumertCount = 10;
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();

  private List<SubscriptionPushConsumer> consumers = new ArrayList<>(consumertCount);
  private AtomicInteger rowCount00 = new AtomicInteger(0);
  private AtomicInteger rowCount10 = new AtomicInteger(0);
  private AtomicInteger rowCount70 = new AtomicInteger(0);
  private AtomicInteger rowCount90 = new AtomicInteger(0);
  private AtomicInteger rowCount6 = new AtomicInteger(0);
  private String sql1 = "select count(s_0) from " + databasePrefix + "1.d_0";
  private String sql2 = "select count(s_0) from " + databasePrefix + "2.d_0";
  private String sql3 = "select count(s_0) from " + databasePrefix + "3.d_0";
  private String sql4 = "select count(s_0) from " + databasePrefix + "4.d_0";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    for (int i = 0; i < tsCount; i++) {
      createDB(databasePrefix + i);
    }
    createTopic_s(
        topicNamePrefix + 0,
        databasePrefix + "0.**",
        "2024-01-01T00:00:00+08:00",
        "2024-03-31T23:59:59+08:00",
        true);
    createTopic_s(topicNamePrefix + 1, databasePrefix + "1.**", null, null, false);

    createTopic_s(topicNamePrefix + 2, databasePrefix + "2.**", "now", null, false);
    createTopic_s(topicNamePrefix + 3, databasePrefix + "3.**", null, "now", false);
    createTopic_s(
        topicNamePrefix + 4, databasePrefix + "4.**", null, "2024-03-31T23:59:59+08:00", false);

    createTopic_s(topicNamePrefix + 6, databasePrefix + "6.**", "now", null, true);

    createTopic_s(
        topicNamePrefix + 5, databasePrefix + "5.**", "2024-01-01T00:00:00+08:00", null, false);
    createTopic_s(
        topicNamePrefix + 7, databasePrefix + "7.**", null, "2024-03-31T23:59:59+08:00", true);
    createTopic_s(topicNamePrefix + 8, databasePrefix + "8.**", null, "now", true);
    createTopic_s(
        topicNamePrefix + 9, databasePrefix + "9.**", "2024-01-01T00:00:00+08:00", null, true);

    subs.getTopics().forEach(System.out::println);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    System.out.println(databasePrefix + "1.d_0:[src]" + getCount(session_src, sql1));
    System.out.println(databasePrefix + "1.d_0:[dest]" + getCount(session_dest, sql1));
    System.out.println(databasePrefix + "1.d_0:[dest2]" + getCount(session_dest2, sql1));
    System.out.println(databasePrefix + "2.d_0:[src]" + getCount(session_src, sql2));
    System.out.println(databasePrefix + "2.d_0:[dest]" + getCount(session_dest, sql2));
    System.out.println(databasePrefix + "2.d_0:[dest2]" + getCount(session_dest2, sql2));
    System.out.println(databasePrefix + "3.d_0:[src]" + getCount(session_src, sql3));
    System.out.println(databasePrefix + "3.d_0:[dest]" + getCount(session_dest, sql3));
    System.out.println(databasePrefix + "3.d_0:[dest2]" + getCount(session_dest2, sql3));
    System.out.println(databasePrefix + "4.d_0:[src]" + getCount(session_src, sql4));
    System.out.println(databasePrefix + "4.d_0:[dest]" + getCount(session_dest, sql4));
    System.out.println(databasePrefix + "4.d_0:[dest2]" + getCount(session_dest2, sql4));
    System.out.println("rowCount00.get()=" + rowCount00.get());
    System.out.println("rowCount10.get()=" + rowCount10.get());
    System.out.println("rowCount70.get()=" + rowCount70.get());
    System.out.println("rowCount90.get()=" + rowCount90.get());
    System.out.println("rowCount6.get()=" + rowCount6.get());
    for (SubscriptionPushConsumer c : consumers) {
      try {
        c.close();
      } catch (Exception e) {
      }
    }
    for (int i = 0; i < tsCount; i++) {
      subs.dropTopic(topicNamePrefix + i);
    }
    dropDB(databasePrefix);
    super.tearDown();
  }

  private void insert_data(long timestamp, String device, int rows)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, rows);
    int rowIndex = 0;
    for (int row = 0; row < rows; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementName(), rowIndex, (row + 1) * 1400 + row);
      tablet.addValue(schemaList.get(1).getMeasurementName(), rowIndex, (row + 1) * 100 + 0.5);
      timestamp += 2000;
    }
    session_src.insertTablet(tablet);
    session_src.executeNonQueryStatement("flush");
  }

  /***
   * |c0|t0|g1|  tsfile databasePrefix+"0.**", "2024-01-01T00:00:00+08:00", "2024-03-31T23:59:59+08:00"
   * |c1|t0|g1|  tsfile
   * |c2|t1|g1|  dataset(dest) databasePrefix+"1.**"
   * |c3|t1|g1|  dataset(dest2)
   * |c4|t2,t3|g2| dataset(dest) databasePrefix+"2.**", "now", null;  databasePrefix+"3.**", null, "now"
   * |c5|t3,t4|g2| dataset(dest2) databasePrefix+"4.**",  null, "2024-03-31T23:59:59+08:00"
   * |c6|t2,t4|g2| dataset(dest)
   * |c7|t0,t3|g3| dataset(dest)/tsfile
   * |c8|t6|g3|  tsfile databasePrefix+"6.**", "now", null,
   * |c9|t0,t3|g3| dataset(dest2)/tsfile
   */
  @Test
  public void do_test()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_0")
            .consumerGroupId("push_group_id_1")
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    QueryDataSet dataset =
                        reader.query(
                            QueryExpression.create(
                                Collections.singletonList(
                                    new Path(databasePrefix + "0.d_0", "s_0", true)),
                                null));
                    while (dataset.hasNext()) {
                      rowCount00.addAndGet(1);
                      RowRecord next = dataset.next();
                      //                                System.out.println("c0,g1:[rowCount00]" +
                      // next.getTimestamp() + "," + next.getFields());
                    }
                    //
                    // System.out.println("c0,g1,rowCount00="+rowCount00.get());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_1")
            .consumerGroupId("push_group_id_1")
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    QueryDataSet dataset =
                        reader.query(
                            QueryExpression.create(
                                Collections.singletonList(
                                    new Path(databasePrefix + "0.d_0", "s_0", true)),
                                null));
                    while (dataset.hasNext()) {
                      rowCount10.addAndGet(1);
                      RowRecord next = dataset.next();
                      //
                      // System.out.println(databasePrefix+0+".d_0:[rowCount10]" +
                      // next.getTimestamp() + "," + next.getFields());
                    }
                    //
                    // System.out.println("c1,g1,rowCount10="+rowCount00.get());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_2")
            .consumerGroupId("push_group_id_1")
            .fileSaveDir("target/push-subscription")
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
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_3")
            .consumerGroupId("push_group_id_1")
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      session_dest2.insertTablet(dataSet.getTablet());
                    } catch (StatementExecutionException e) {
                      throw new RuntimeException(e);
                    } catch (IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_4")
            .consumerGroupId("push_group_id_2")
            .fileSaveDir("target/push-subscription")
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
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_5")
            .consumerGroupId("push_group_id_2")
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  for (final SubscriptionSessionDataSet dataSet :
                      message.getSessionDataSetsHandler()) {
                    try {
                      session_dest2.insertTablet(dataSet.getTablet());
                    } catch (StatementExecutionException e) {
                      throw new RuntimeException(e);
                    } catch (IoTDBConnectionException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_6")
            .consumerGroupId("push_group_id_2")
            .fileSaveDir("target/push-subscription")
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
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_7")
            .consumerGroupId("push_group_id_3")
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
                                      Collections.singletonList(
                                          new Path(databasePrefix + "0.d_0", "s_0", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount70.addAndGet(1);
                            RowRecord next = dataset.next();
                            //
                            // System.out.println(databasePrefix+"0.d_0:[rowCount70]" +
                            // next.getTimestamp() + "," + next.getFields());
                          }
                        } catch (IOException e) {
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
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_8")
            .consumerGroupId("push_group_id_3")
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    QueryDataSet dataset =
                        reader.query(
                            QueryExpression.create(
                                Collections.singletonList(
                                    new Path(databasePrefix + "6.d_0", "s_0", true)),
                                null));
                    while (dataset.hasNext()) {
                      rowCount6.addAndGet(1);
                      RowRecord next = dataset.next();
                      //                            System.out.println(databasePrefix+6+".d_0:" +
                      // next.getTimestamp() + "," + next.getFields());
                    }
                    //
                    // System.out.println("c8,g3,rowCount00="+rowCount6.get());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());
    consumers.add(
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("consumer_id_9")
            .consumerGroupId("push_group_id_3")
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
                            session_dest2.insertTablet(dataSet.getTablet());
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
                                      Collections.singletonList(
                                          new Path(databasePrefix + "0.d_0", "s_0", true)),
                                      null));
                          while (dataset.hasNext()) {
                            rowCount90.addAndGet(1);
                            RowRecord next = dataset.next();
                            //
                            // System.out.println(databasePrefix+"0.d_0:[rowCount90]" +
                            // next.getTimestamp() + "," + next.getFields());
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        break;
                    }
                  }
                  return ConsumeResult.SUCCESS;
                })
            .buildPushConsumer());

    for (int j = 0; j < consumers.size(); j++) {
      consumers.get(j).open();
    }

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
    subs.getSubscriptions().forEach((System.out::println));

    // Write data
    Thread thread =
        new Thread(
            () -> {
              long timestamp = 1706659200000L; // 2024-01-31 08:00:00+08:00
              for (int i = 0; i < 20; i++) {
                for (int k = 0; k < 10; k++) {
                  String device = databasePrefix + k + ".d_0";
                  try {
                    insert_data(timestamp, device, 20);
                  } catch (IoTDBConnectionException e) {
                    throw new RuntimeException(e);
                  } catch (StatementExecutionException e) {
                    throw new RuntimeException(e);
                  }
                }
                timestamp += 40000;
              }
              for (int i = 0; i < 10; i++) {
                String device = databasePrefix + i + ".d_0";
                try {
                  insert_data(System.currentTimeMillis(), device, 5);
                } catch (IoTDBConnectionException e) {
                  throw new RuntimeException(e);
                } catch (StatementExecutionException e) {
                  throw new RuntimeException(e);
                }
              }
              String device = databasePrefix + 2 + ".d_0";
              for (int i = 0; i < 20; i++) {
                try {
                  insert_data(System.currentTimeMillis(), device, 5);
                } catch (IoTDBConnectionException e) {
                  throw new RuntimeException(e);
                } catch (StatementExecutionException e) {
                  throw new RuntimeException(e);
                }
              }
            });

    thread.start();
    thread.join();

    System.out.println(databasePrefix + "1.d_0:[src]" + getCount(session_src, sql1));
    System.out.println(databasePrefix + "2.d_0:[src]" + getCount(session_src, sql2));
    System.out.println(databasePrefix + "3.d_0:[src]" + getCount(session_src, sql3));
    System.out.println(databasePrefix + "4.d_0:[src]" + getCount(session_src, sql4));

    AWAIT.untilAsserted(
        () -> {
          assertGte(rowCount00.get() + rowCount10.get(), 400, "c0,c1,topic0,tsfile");
          assertEquals(
              getCount(session_dest, sql1) + getCount(session_dest2, sql1),
              getCount(session_src, sql1),
              "c2,c3,topic1,group1");
          assertEquals(
              getCount(session_dest, sql2) + getCount(session_dest2, sql2),
              getCount(session_src, sql2) - 400,
              "c4,c6,topic2,group2");
          final long topic3Total = getCount(session_dest, sql3) + getCount(session_dest2, sql3);
          assertTrue(400 <= topic3Total && topic3Total <= 800, "c4,c5|c7,c9|topic3");
          assertEquals(
              getCount(session_dest, sql4) + getCount(session_dest2, sql4),
              400,
              "c5,c6,topic4,group3");
          assertGte(rowCount70.get() + rowCount90.get(), 400, "c7,c9,topic0,tsfile");
          assertEquals(rowCount6.get(), 5, "c8,topic6,tsfile");
          //            assertTrue(rowCount00.get()>0);
          //            assertTrue(rowCount10.get()>0);
          //            assertTrue(rowCount70.get()>0);
          //            assertTrue(rowCount90.get()>0);
        });
  }
}
/***
 * Expected result:
 * root.test.MultiGroupVsMultiConsumer_1.d_0:[src]405
 * root.test.MultiGroupVsMultiConsumer_1.d_0:[dest]305
 * root.test.MultiGroupVsMultiConsumer_1.d_0:[dest2]100
 * root.test.MultiGroupVsMultiConsumer_2.d_0:[src]505
 * root.test.MultiGroupVsMultiConsumer_2.d_0:[dest]105
 * root.test.MultiGroupVsMultiConsumer_2.d_0:[dest2]0
 * root.test.MultiGroupVsMultiConsumer_3.d_0:[src]405
 * root.test.MultiGroupVsMultiConsumer_3.d_0:[dest]220
 * root.test.MultiGroupVsMultiConsumer_3.d_0:[dest2]300
 * root.test.MultiGroupVsMultiConsumer_4.d_0:[src]405
 * root.test.MultiGroupVsMultiConsumer_4.d_0:[dest]300
 * root.test.MultiGroupVsMultiConsumer_4.d_0:[dest2]100
 * rowCount00.get()=200
 * rowCount10.get()=200
 * rowCount70.get()=240
 * rowCount90.get()=160
 * rowCount6.get()=5
 **/
