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

package org.apache.iotdb.subscription.it.triple.regression.user;

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
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/***
 * Permission Test: Username currently only serves for connection, no permissions defined.
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBOtherUserConsumerIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.test.OtherUserConsumer";
  private static final String device = database + ".d_0";
  private static final String topicName = "topic_OtherUserConsumer";
  private static List<IMeasurementSchema> schemaList = new ArrayList<>();
  private static final String pattern = "root.**";
  private static final String userName = "other_user";
  private static final String passwd = "other_user";
  private static SubscriptionPullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    createTopic_s(topicName, pattern, null, null, false);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZ4);
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    session_src.executeNonQueryStatement("drop user " + userName);
    try {
      consumer.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
    dropDB(database);
    super.tearDown();
  }

  private void insert_data(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    Tablet tablet = new Tablet(device, schemaList, 5);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
      timestamp += row * 2000;
    }
    session_src.insertTablet(tablet);
    Thread.sleep(1000);
  }

  // @Test
  public void testPrivilege() throws IoTDBConnectionException, StatementExecutionException {
    session_src.executeNonQueryStatement("create user " + userName + " '" + passwd + "';");
    session_src.executeNonQueryStatement("grant read,write on root.** to user " + userName);
  }

  // @Test
  // TODO: Failed to fetch all endpoints, only the admin user can perform this operation...
  public void testNormal()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    session_src.executeNonQueryStatement("create user " + userName + " '" + passwd + "';");
    session_src.executeNonQueryStatement("grant read,write on root.** to user " + userName);
    consumer =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .username(userName)
            .password(passwd)
            .buildPullConsumer();
    consumer.open();
    insert_data(1706659200000L); // 2024-01-31 08:00:00+08:00
    // Subscribe
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");
    Thread.sleep(1000);
    subs.getSubscriptions().forEach(System.out::println);
    // Consumption data
    consume_data(consumer, session_dest);
    check_count(4, "select count(s_0) from " + device, "Consumption Data: s_0");
    check_count(4, "select count(s_1) from " + device, "Consumption Data: s_1");
  }
}
