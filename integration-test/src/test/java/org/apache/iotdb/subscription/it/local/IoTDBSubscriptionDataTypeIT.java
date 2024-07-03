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

package org.apache.iotdb.subscription.it.local;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessageType;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionDataTypeIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionDataTypeIT.class);

  // ----------------------------- //
  // SessionDataSetsHandler format //
  // ----------------------------- //

  @Test
  public void testSubscribeTabletBooleanData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE, TSDataType.BOOLEAN, "true", true);
  }

  @Test
  public void testSubscribeTabletIntData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE, TSDataType.INT32, "1", 1);
  }

  @Test
  public void testSubscribeTabletLongData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE, TSDataType.INT64, "1", 1L);
  }

  @Test
  public void testSubscribeTabletFloatData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE, TSDataType.FLOAT, "1.0", 1.0F);
  }

  @Test
  public void testSubscribeTabletDoubleData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE, TSDataType.DOUBLE, "1.0", 1.0);
  }

  @Test
  public void testSubscribeTabletTextData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE,
        TSDataType.TEXT,
        "'a'",
        new Binary("a", TSFileConfig.STRING_CHARSET));
  }

  @Test
  public void testSubscribeTabletTimestampData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE, TSDataType.TIMESTAMP, "123", 123L);
  }

  @Test
  public void testSubscribeTabletDateData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE,
        TSDataType.DATE,
        "'2011-03-01'",
        LocalDate.of(2011, 3, 1));
  }

  @Test
  public void testSubscribeBlobData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE,
        TSDataType.BLOB,
        "X'f013'",
        new Binary(new byte[] {(byte) 0xf0, 0x13}));
  }

  @Test
  public void testSubscribeStringData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE,
        TSDataType.STRING,
        "'a'",
        new Binary("a", TSFileConfig.STRING_CHARSET));
  }

  // -------------------- //
  // TsFileHandler format //
  // -------------------- //

  @Test
  public void testSubscribeTsFileBooleanData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE, TSDataType.BOOLEAN, "true", true);
  }

  @Test
  public void testSubscribeTsFileIntData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE, TSDataType.INT32, "1", 1);
  }

  @Test
  public void testSubscribeTsFileLongData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE, TSDataType.INT64, "1", 1L);
  }

  @Test
  public void testSubscribeTsFileFloatData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE, TSDataType.FLOAT, "1.0", 1.0F);
  }

  @Test
  public void testSubscribeTsFileDoubleData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE, TSDataType.DOUBLE, "1.0", 1.0);
  }

  @Test
  public void testSubscribeTsFileTextData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE,
        TSDataType.TEXT,
        "'a'",
        new Binary("a", TSFileConfig.STRING_CHARSET));
  }

  @Test
  public void testSubscribeTsFileTimestampData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE, TSDataType.TIMESTAMP, "123", 123L);
  }

  @Test
  public void testSubscribeTsFileDateData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE,
        TSDataType.DATE,
        "'2011-03-01'",
        LocalDate.of(2011, 3, 1));
  }

  @Test
  public void testSubscribeTsFileBlobData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE,
        TSDataType.BLOB,
        "X'f013'",
        new Binary(new byte[] {(byte) 0xf0, 0x13}));
  }

  @Test
  public void testSubscribeTsFileStringData() throws Exception {
    testPullConsumerSubscribeDataTemplate(
        TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE,
        TSDataType.STRING,
        "'a'",
        new Binary("a", TSFileConfig.STRING_CHARSET));
  }

  /////////////////////////////// utility ///////////////////////////////

  private void testPullConsumerSubscribeDataTemplate(
      final String topicFormat,
      final TSDataType type,
      final String valueStr,
      final Object expectedData)
      throws Exception {
    // Insert some historical data
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("create timeseries root.db.d1.s1 %s", type.toString()));
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, %s)", i, valueStr));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Create topic
    final String topicName = "topic1";
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      final Properties config = new Properties();
      config.put(TopicConstant.FORMAT_KEY, topicFormat);
      session.createTopic(topicName, config);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // Subscription
    final AtomicInteger rowCount = new AtomicInteger();
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Thread thread =
        new Thread(
            () -> {
              try (final SubscriptionPullConsumer consumer =
                  new SubscriptionPullConsumer.Builder()
                      .host(host)
                      .port(port)
                      .consumerId("c1")
                      .consumerGroupId("cg1")
                      .fileSaveDir(System.getProperty("java.io.tmpdir")) // hack for license check
                      .autoCommit(false)
                      .buildPullConsumer()) {
                consumer.open();
                consumer.subscribe(topicName);
                while (!isClosed.get()) {
                  LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS); // wait some time
                  final List<SubscriptionMessage> messages =
                      consumer.poll(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS);
                  for (final SubscriptionMessage message : messages) {
                    final short messageType = message.getMessageType();
                    if (!SubscriptionMessageType.isValidatedMessageType(messageType)) {
                      LOGGER.warn("unexpected message type: {}", messageType);
                      continue;
                    }
                    switch (SubscriptionMessageType.valueOf(messageType)) {
                      case SESSION_DATA_SETS_HANDLER:
                        for (final SubscriptionSessionDataSet dataSet :
                            message.getSessionDataSetsHandler()) {
                          while (dataSet.hasNext()) {
                            final RowRecord record = dataSet.next();
                            Assert.assertEquals(type.toString(), dataSet.getColumnTypes().get(1));
                            Assert.assertEquals(type, record.getFields().get(0).getDataType());
                            Assert.assertEquals(expectedData, getValue(type, dataSet.getTablet()));
                            Assert.assertEquals(
                                expectedData, record.getFields().get(0).getObjectValue(type));
                            rowCount.addAndGet(1);
                          }
                        }
                        break;
                      case TS_FILE_HANDLER:
                        try (final TsFileReader tsFileReader =
                            message.getTsFileHandler().openReader()) {
                          final QueryDataSet dataSet =
                              tsFileReader.query(
                                  QueryExpression.create(
                                      Collections.singletonList(new Path("root.db.d1", "s1", true)),
                                      null));
                          while (dataSet.hasNext()) {
                            final RowRecord record = dataSet.next();
                            Assert.assertEquals(type, record.getFields().get(0).getDataType());
                            Assert.assertEquals(
                                expectedData, record.getFields().get(0).getObjectValue(type));
                            rowCount.addAndGet(1);
                          }
                        }
                        break;
                      default:
                        LOGGER.warn("unexpected message type: {}", messageType);
                        break;
                    }
                  }
                  consumer.commitSync(messages);
                }
                consumer.unsubscribe(topicName);
              } catch (final Exception e) {
                e.printStackTrace();
                // Avoid failure
              } finally {
                LOGGER.info("consumer exiting...");
              }
            },
            String.format("%s - consumer", testName.getMethodName()));
    thread.start();

    // Check row count
    try {
      // Keep retrying if there are execution failures
      AWAIT.untilAsserted(() -> Assert.assertEquals(100, rowCount.get()));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      thread.join();
    }
  }

  private Object getValue(final TSDataType type, final Tablet tablet) {
    switch (type) {
      case BOOLEAN:
        return ((boolean[]) tablet.values[0])[0];
      case INT32:
        return ((int[]) tablet.values[0])[0];
      case INT64:
      case TIMESTAMP:
        return ((long[]) tablet.values[0])[0];
      case FLOAT:
        return ((float[]) tablet.values[0])[0];
      case DOUBLE:
        return ((double[]) tablet.values[0])[0];
      case TEXT:
      case BLOB:
      case STRING:
        return ((Binary[]) tablet.values[0])[0];
      case DATE:
        return ((LocalDate[]) tablet.values[0])[0];
      default:
        return null;
    }
  }
}
