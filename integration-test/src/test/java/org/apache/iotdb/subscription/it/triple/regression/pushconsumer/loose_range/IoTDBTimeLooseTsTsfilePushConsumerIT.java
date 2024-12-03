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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.loose_range;

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
 * mode: live
 * loose-range:path
 * format: tsfile
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBTimeLooseTsTsfilePushConsumerIT extends AbstractSubscriptionRegressionIT {
  private String database = "root.TimeLooseTsTsfilePushConsumer";
  private String device = database + ".d_0";
  private String device2 = database + ".d_1";
  private String pattern = device + ".s_0";
  private String topicName = "topic_TimeLooseTsTsfilePushConsumer";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPushConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(
        topicName,
        pattern,
        "2024-01-01T00:00:00+08:00",
        "2024-03-31T00:00:00+08:00",
        true,
        TopicConstant.MODE_LIVE_VALUE,
        TopicConstant.LOOSE_RANGE_TIME_VALUE);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_src.createTimeseries(
        device2 + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device2 + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device2 + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device2 + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
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

  private void insert_data(long timestamp, String device)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, (1 + row) * 20L + row);
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
    final AtomicInteger onReceive = new AtomicInteger(0);
    List<AtomicInteger> rowCounts = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      rowCounts.add(new AtomicInteger(0));
    }

    List<Path> paths = new ArrayList<>(4);
    paths.add(new Path(device, "s_0", true));
    paths.add(new Path(device, "s_1", true));
    paths.add(new Path(device2, "s_0", true));
    paths.add(new Path(device2, "s_1", true));

    String sql =
        "select count(s_0) from "
            + device
            + " where time >= 2024-01-01T00:00:00+08:00 and time <= 2024-03-31T00:00:00+08:00";

    // Write data before subscribing
    insert_data(1704038396000L, device); // 2023-12-31 23:59:56+08:00
    insert_data(1704038396000L, device2); // 2023-12-31 23:59:56+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println(FORMAT.format(new Date()) + " src: " + getCount(session_src, sql));

    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("time_range_accurate_ts_tsfile_push")
            .consumerGroupId("loose_range_time")
            .ackStrategy(AckStrategy.AFTER_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    onReceive.addAndGet(1);
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
                          FORMAT.format(new Date()) + " " + i + " " + rowCounts.get(i).get());
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
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    AWAIT.untilAsserted(
        () -> {
          assertGte(
              rowCounts.get(0).get(),
              3,
              device + ".s_0, subscribe before writing data start boundary");
          assertEquals(
              rowCounts.get(1).get(),
              0,
              device + ".s_1, Subscription before writing data start boundary");
          assertEquals(
              rowCounts.get(2).get(),
              0,
              device2 + ".s_0, Subscribe before writing data start boundary");
          assertEquals(
              rowCounts.get(3).get(),
              0,
              device2 + ".s_1, Subscription before writing data start boundary");
        });

    insert_data(System.currentTimeMillis(), device); // now, not in range
    insert_data(System.currentTimeMillis(), device2); // now, not in range
    session_src.executeNonQueryStatement("flush");
    System.out.println(FORMAT.format(new Date()) + " src: " + getCount(session_src, sql));

    AWAIT.untilAsserted(
        () -> {
          assertGte(rowCounts.get(0).get(), 3, device + ".s_0, Write out-of-range data");
          assertEquals(rowCounts.get(1).get(), 0, device + ".s_1, Write out-of-range data");
          assertEquals(rowCounts.get(2).get(), 0, device2 + ".s_0, Write out-of-range data");
          assertEquals(rowCounts.get(3).get(), 0, device2 + ".s_1, Write out-of-range data");
        });

    insert_data(1707782400000L, device); // 2024-02-13 08:00:00+08:00
    insert_data(1707782400000L, device2); // 2024-02-13 08:00:00+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println(FORMAT.format(new Date()) + " src: " + getCount(session_src, sql));

    AWAIT.untilAsserted(
        () -> {
          assertGte(rowCounts.get(0).get(), 8, device + ".s_0, write normal data");
          assertEquals(rowCounts.get(1).get(), 0, device + ".s_1, write normal data");
          assertEquals(rowCounts.get(2).get(), 0, device2 + ".s_0, Write normal data");
          assertEquals(rowCounts.get(3).get(), 0, device2 + ".s_1, Write normal data");
        });

    insert_data(1711814398000L, device); // 2024-03-30 23:59:58+08:00
    insert_data(1711814398000L, device2); // 2024-03-30 23:59:58+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println(FORMAT.format(new Date()) + " src: " + getCount(session_src, sql));

    AWAIT.untilAsserted(
        () -> {
          assertGte(rowCounts.get(0).get(), 10, device + ".s_0, write end boundary data");
          assertEquals(rowCounts.get(1).get(), 0, device + ".s_1, write end boundary data");
          assertEquals(rowCounts.get(2).get(), 0, device2 + ".s_0, write end boundary data");
          assertEquals(rowCounts.get(3).get(), 0, device2 + ".s_1, write end boundary data");
        });

    insert_data(1711900798000L, device); // 2024-03-31 23:59:58+08:00
    insert_data(1711900798000L, device2); // 2024-03-31 23:59:58+08:00
    session_src.executeNonQueryStatement("flush");
    System.out.println(FORMAT.format(new Date()) + " src: " + getCount(session_src, sql));

    AWAIT.untilAsserted(
        () -> {
          assertGte(rowCounts.get(0).get(), 10, device + ".s_0, Write data outside end range");
          assertEquals(rowCounts.get(1).get(), 0, device + ".s_0, Write data outside of end range");
          assertEquals(rowCounts.get(2).get(), 0, device + ".s_0, Write data outside of end range");
          assertEquals(rowCounts.get(3).get(), 0, device + ".s_0, Write data outside of end range");
        });
  }
}
