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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.time;

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
public class IoTDBTimeRangeAccuratePullConsumerDataSetIT extends AbstractSubscriptionRegressionIT {
  private String database = "root.TimeRangeAccuratePullConsumerDataSet";
  private String device = database + ".d_0";
  private String pattern = device + ".s_0";
  private String topicName = "topic_TimeRangeAccuratePullConsumerDataSet";
  private List<IMeasurementSchema> schemaList = new ArrayList<>();
  private SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(
        topicName, pattern, "2024-01-01T00:00:00+08:00", "2024-03-31T23:59:59+08:00", false);
    session_src.createTimeseries(
        pattern, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        pattern, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "create show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    consumer.close();
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
      tablet.addValue("s_0", rowIndex, row * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
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
    consumer = create_pull_consumer("pull_time", "ts_accurate_dataset", false, null);
    // Write data before subscribing
    insert_data(1704038396000L); // 2023-12-31 23:59:56+08:00
    // Subscribe
    consumer.subscribe(topicName);
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    // Before consumption subscription data
    consume_data(consumer, session_dest);
    check_count(2, "select count(s_0) from " + device, "Start time boundary data:" + pattern);

    insert_data(System.currentTimeMillis()); // now
    consume_data(consumer, session_dest);
    check_count(
        2, "select count(s_0) from " + device, "Write some real-time data after:" + pattern);

    insert_data(1707782400000L); // 2024-02-13 08:00:00+08:00
    consume_data(consumer, session_dest);
    check_count(6, "select count(s_0) from " + device, "Data within the time range:" + pattern);

    insert_data(1711814398000L); // 2024-03-30 23:59:58+08:00
    consume_data(consumer, session_dest);
    check_count(10, "select count(s_0) from " + device, "End time boundary data:" + pattern);

    insert_data(1711900798000L); // 2024-03-31 23:59:58+08:00
    consume_data(consumer, session_dest);
    check_count(11, "select count(s_0) from " + device, "End time limit data 2:" + pattern);
  }
}
