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

package org.apache.iotdb.subscription.it.triple.regression.pushconsumer.time;

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

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBRealTimeDBTsfilePushConsumerIT extends AbstractSubscriptionRegressionIT {
  private String database = "root.RealTimeDBTsfilePushConsumer";
  private String device = database + ".d_0";
  private String pattern = database + ".**";
  private String topicName = "topic_RealTimeDBTsfilePushConsumer";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPushConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(topicName, pattern, "2024-01-31T08:02:00+08:00", null, true);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
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
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 10);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
      timestamp += row * 2000;
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
    final AtomicInteger rowCount = new AtomicInteger(0);
    final AtomicInteger onReceive = new AtomicInteger(0);
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    consumer =
        new SubscriptionPushConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("DB_realtime_tsfile")
            .consumerGroupId("push_time")
            .ackStrategy(AckStrategy.BEFORE_CONSUME)
            .fileSaveDir("target/push-subscription")
            .consumeListener(
                message -> {
                  try {
                    onReceive.addAndGet(1);
                    TsFileReader reader = message.getTsFileHandler().openReader();
                    List<Path> paths = new ArrayList<>(2);
                    for (int i = 0; i < 2; i++) {
                      paths.add(new Path(device, "s_" + i, true));
                    }
                    QueryDataSet dataset = reader.query(QueryExpression.create(paths, null));
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
    subs.getSubscriptions().forEach((System.out::println));
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    AWAIT.untilAsserted(
        () -> {
          assertEquals(onReceive.get(), 0);
          assertEquals(rowCount.get(), 0);
        });
    insert_data(System.currentTimeMillis());
    AWAIT.untilAsserted(
        () -> {
          assertEquals(onReceive.get(), 1, "should process 1 file");
          assertEquals(rowCount.get(), 4, "4 records");
        });

    // Subscribe and then write data
    insert_data(System.currentTimeMillis() + 200000); // now

    AWAIT.untilAsserted(
        () -> {
          assertEquals(onReceive.get(), 2, "should process 2 file");
          assertEquals(rowCount.get(), 8, "8 records");
        });
  }
}
