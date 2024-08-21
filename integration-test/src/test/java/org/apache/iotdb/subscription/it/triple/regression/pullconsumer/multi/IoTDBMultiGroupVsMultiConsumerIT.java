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

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionConsumer.class})
public class IoTDBMultiGroupVsMultiConsumerIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.pullMultiGroupVsMultiConsumer";
  private static final String device = database + ".d_0";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private String topicNamePrefix = "TopicPullMultiGroupVsMultiConsumer_";
  private int tsCount = 10;
  private int consumertCount = 10;
  private List<SubscriptionPullConsumer> consumers = new ArrayList<>(consumertCount);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    for (int i = 0; i < tsCount; i++) {
      createTopic_s(topicNamePrefix + i, device + ".s_" + i, null, null, false);
      session_src.createTimeseries(
          device + ".s_" + i, TSDataType.INT32, TSEncoding.RLE, CompressionType.LZMA2);
      session_dest.createTimeseries(
          device + ".s_" + i, TSDataType.INT32, TSEncoding.RLE, CompressionType.LZMA2);
      session_dest2.createTimeseries(
          device + ".s_" + i, TSDataType.INT32, TSEncoding.RLE, CompressionType.LZMA2);
      schemaList.add(new MeasurementSchema("s_" + i, TSDataType.INT32));
    }
    System.out.println("topics:" + subs.getTopics());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    for (SubscriptionPullConsumer c : consumers) {
      try {
        c.close();
      } catch (Exception e) {
      }
    }
    for (int i = 0; i < tsCount; i++) {
      subs.dropTopic(topicNamePrefix + i);
    }
    dropDB(database);
    super.tearDown();
  }

  private void insert_data(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(device, schemaList, 5);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int i = 0; i < tsCount; i++) {
        tablet.addValue(
            schemaList.get(i).getMeasurementId(), rowIndex, (row + 1) * 20 + i * 1000 + row);
      }
      timestamp += 2000;
    }
    session_src.insertTablet(tablet);
    session_src.executeNonQueryStatement("flush;");
  }

  /***
   * |c0|t0|g1|
   * |c1|t0|g1|
   * |c2|t1|g1|
   * |c3|t1|g1|
   * |c4|t2,t3|g2|
   * |c5|t3,t4|g2|
   * |c6|t2,t4|g2|
   * |c7|t0,t3|g3|
   * |c8|t6|g3|
   * |c9|t0,t3|g3|
   */
  @Test
  public void do_test()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    int i = 0;
    for (i = 0; i < 4; i++) {
      consumers.add(
          new SubscriptionPullConsumer.Builder()
              .host(SRC_HOST)
              .port(SRC_PORT)
              .consumerId("consumer_id_" + i)
              .consumerGroupId("pull_group_id_1")
              .buildPullConsumer());
    }
    for (; i < 7; i++) {
      consumers.add(
          new SubscriptionPullConsumer.Builder()
              .host(SRC_HOST)
              .port(SRC_PORT)
              .consumerId("consumer_id_" + i)
              .consumerGroupId("pull_group_id_2")
              .buildPullConsumer());
    }
    for (; i < consumertCount; i++) {
      consumers.add(
          new SubscriptionPullConsumer.Builder()
              .host(SRC_HOST)
              .port(SRC_PORT)
              .consumerId("consumer_id_" + i)
              .consumerGroupId("pull_group_id_3")
              .buildPullConsumer());
    }
    for (int j = 0; j < consumertCount; j++) {
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

    subs.getSubscriptions().forEach(System.out::println);
    // Write data
    System.out.println("Write data 1");
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    consume_data(consumers.get(0), session_dest);
    System.out.println("src:" + getCount(session_src, "select count(s_0) from " + device));
    check_count(5, "select count(s_0) from " + device, "1 post-consumption check: s_0");
    for (i = 1; i < tsCount; i++) {
      check_count(0, "select count(s_" + i + ") from " + device, "1 pre-consumption check: s_" + i);
    }
    consume_data(consumers.get(2), session_dest);
    check_count(5, "select count(s_1) from " + device, "1 post-consumption check: s_1");
    consume_data(consumers.get(4), session_dest);
    check_count(5, "select count(s_2) from " + device, "1 post-consumption check: s_2");
    check_count(5, "select count(s_3) from " + device, "1 post-consumption check: s_3");
    for (i = 4; i < tsCount; i++) {
      check_count(0, "select count(s_" + i + ") from " + device, "1 pre-consumption check: s_" + i);
    }

    System.out.println("Write data 2");
    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    System.out.println("src:" + getCount(session_src, "select count(s_0) from " + device));
    consume_data(consumers.get(1), session_dest2);
    consume_data(consumers.get(3), session_dest2);
    consume_data(consumers.get(5), session_dest2);
    consume_data(consumers.get(8), session_dest2);

    for (i = 0; i < 4; i++) {
      if (i == 2) continue;
      check_count2(
          5, "select count(s_" + i + ") from " + device, "2 pre-consumption check: s_" + i);
    }
    check_count2(10, "select count(s_4) from " + device, "2 check dest2:s_4 after consumption");
    check_count2(10, "select count(s_6) from " + device, "2 check dest2:s_6 after consumption");

    consume_data(consumers.get(7), session_dest);
    check_count(10, "select count(s_0) from " + device, "2 post-consumption check: s_0");
    check_count(10, "select count(s_3) from " + device, "2 post-consumption check: s_3");

    System.out.println("Write data 3");
    insert_data(System.currentTimeMillis());
    consume_data(consumers.get(7), session_dest2);
    check_count2(10, "select count(s_0) from " + device, "3 check dest2:s_0 after consumption");
    check_count2(10, "select count(s_3) from " + device, "3 consume check dest2:s_3");
  }
}
