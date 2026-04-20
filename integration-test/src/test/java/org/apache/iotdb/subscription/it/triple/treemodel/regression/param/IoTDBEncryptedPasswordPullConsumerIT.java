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

package org.apache.iotdb.subscription.it.triple.treemodel.regression.param;

import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionTreeRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.subscription.it.triple.treemodel.regression.AbstractSubscriptionTreeRegressionIT;

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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionTreeRegressionMisc.class})
public class IoTDBEncryptedPasswordPullConsumerIT extends AbstractSubscriptionTreeRegressionIT {

  private static final String DATABASE = "root.TestEncryptedPasswordPullConsumer";
  private static final String DEVICE = DATABASE + ".d_0";
  private static final String TOPIC_NAME = "TestEncryptedPasswordPullConsumerTopic";
  private static final String USERNAME = "encrypted_user";
  private static final String PASSWORD = "EncryptedUser@123";
  private static final String ENCRYPTED_PASSWORD = AuthUtils.encryptPassword(PASSWORD);
  private static final String WRONG_ENCRYPTED_PASSWORD =
      AuthUtils.encryptPassword("WrongEncryptedUser@123");

  private static final List<IMeasurementSchema> SCHEMA_LIST = new ArrayList<>();

  static {
    SCHEMA_LIST.add(new MeasurementSchema("s_0", TSDataType.INT64));
    SCHEMA_LIST.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
  }

  private SubscriptionTreePullConsumer consumer;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(DATABASE);
    createTopic_s(TOPIC_NAME, "root.**", null, null, false);
    session_src.createTimeseries(
        DEVICE + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        DEVICE + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZ4);
    session_dest.createTimeseries(
        DEVICE + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        DEVICE + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZ4);
    session_src.executeNonQueryStatement("create user " + USERNAME + " '" + PASSWORD + "'");
    session_src.executeNonQueryStatement("grant read,write on root.** to user " + USERNAME);
    assertTrue(subs.getTopic(TOPIC_NAME).isPresent());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      if (consumer != null) {
        consumer.close();
      }
    } catch (final Exception ignored) {
    }
    try {
      subs.dropTopic(TOPIC_NAME);
    } catch (final Exception ignored) {
    }
    try {
      session_src.executeNonQueryStatement("drop user " + USERNAME);
    } catch (final Exception ignored) {
    }
    dropDB(DATABASE);
    super.tearDown();
  }

  @Test
  public void testSubscribeWithEncryptedPassword()
      throws TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException,
          InterruptedException {
    consumer = createConsumer("encrypted-password-group", ENCRYPTED_PASSWORD);

    consumer.open();
    consumer.subscribe(TOPIC_NAME);
    assertEquals(1, subs.getSubscriptions().size(), "subscribe with encrypted password");

    insertData(1706659200000L);
    consume_data(consumer, session_dest);
    check_count(
        4,
        "select count(s_0) from " + DEVICE + " where time >= 1706659200000",
        "encrypted password consumption");
  }

  @Test
  public void testSubscribeFailsWithWrongEncryptedPassword()
      throws IoTDBConnectionException, StatementExecutionException {
    consumer = createConsumer("wrong-encrypted-password-group", WRONG_ENCRYPTED_PASSWORD);

    try {
      consumer.open();
      consumer.subscribe(TOPIC_NAME);
      fail("subscribe should fail when encrypted password mismatches");
    } catch (final Exception ignored) {
      assertTrue(subs.getSubscriptions().isEmpty());
    }
  }

  private SubscriptionTreePullConsumer createConsumer(
      final String consumerGroupId, final String encryptedPassword) {
    return new SubscriptionTreePullConsumer.Builder()
        .host(SRC_HOST)
        .port(SRC_PORT)
        .username(USERNAME)
        .encryptedPassword(encryptedPassword)
        .consumerId("consumer_" + consumerGroupId)
        .consumerGroupId(consumerGroupId)
        .buildPullConsumer();
  }

  private void insertData(long timestamp)
      throws IoTDBConnectionException, StatementExecutionException {
    final Tablet tablet = new Tablet(DEVICE, SCHEMA_LIST, 10);
    for (int row = 0; row < 5; row++) {
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, row * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
      timestamp += row * 2000;
    }
    session_src.insertTablet(tablet);
  }
}
