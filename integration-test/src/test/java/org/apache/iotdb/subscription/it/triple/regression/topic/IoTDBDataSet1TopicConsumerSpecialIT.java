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

package org.apache.iotdb.subscription.it.triple.regression.topic;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
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

/***
 * Sequence-level topic, with start, end, tsfile
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBDataSet1TopicConsumerSpecialIT extends AbstractSubscriptionRegressionIT {
  private String database = "root.test.ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz1";
  private String device = database + ".`#01`";
  private String pattern = device + ".`ABH#01`";
  private String topicName = "topic_DataSet1TopicConsumerSpecial";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(
        topicName, pattern, "2024-01-01T00:00:00+08:00", "2024-03-01T00:00:00+08:00", false);
    session_src.createTimeseries(
        pattern, TSDataType.INT32, TSEncoding.GORILLA, CompressionType.SNAPPY);
    session_src.createTimeseries(
        device + ".`BJ-ABH#01`", TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("`ABH#01`", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("`BJ-ABH#01`", TSDataType.BOOLEAN));
    System.out.println("topics:" + subs.getTopics());
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
      tablet.addValue("`ABH#01`", rowIndex, row * 20 + row);
      tablet.addValue("`BJ-ABH#01`", rowIndex, true);
      timestamp += row * 2000;
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
    // Write data before subscribing
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,`ABH#01`,`BJ-ABH#01`)values(1710288000000,313,false);"); // 2024-03-13
    // 08:00:00+08:00
    // Subscribe
    consumer = create_pull_consumer("`Group-ABH#01`", "`ABH#01`", false, 0L);
    assertEquals(subs.getSubscriptions().size(), 0, "Before subscribing, show subscriptions");
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");
    // Consumption data
    consume_data(consumer);
    check_count(4, "select count(`ABH#01`) from " + device, "Consumption data:" + pattern);
    // Unsubscribe
    consumer.unsubscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 0, "After cancellation, show subscriptions");
    // Subscribe and then write data
    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    session_src.executeNonQueryStatement(
        "insert into "
            + device
            + "(time,`ABH#01`,`BJ-ABH#01`)values(1703980800000,1231,false);"); // 2023-12-31
    // 08:00:00+08:00
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after re-subscribing");
    // Consumption data
    consume_data(consumer);
    check_count(8, "select count(`ABH#01`) from " + device, "consume data again:" + pattern);
    // Unsubscribe
    consumer.unsubscribe(topicName);
  }
}
