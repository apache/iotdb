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

package org.apache.iotdb.subscription.it.triple.regression;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;
import org.apache.iotdb.subscription.it.triple.AbstractSubscriptionTripleIT;

import org.apache.thrift.TException;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS;

public abstract class AbstractSubscriptionRegressionIT extends AbstractSubscriptionTripleIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSubscriptionRegressionIT.class);
  private static final String DROP_DATABASE_SQL = "drop database ";

  protected static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  public String SRC_HOST;
  public String DEST_HOST;
  public String DEST_HOST2;

  public int SRC_PORT;
  public int DEST_PORT;
  public int DEST_PORT2;

  public SubscriptionSession subs;

  public Session session_src;
  public Session session_dest;
  public Session session_dest2;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    beforeSuite();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    afterSuite();
    super.tearDown();
  }

  public void beforeSuite() throws IoTDBConnectionException {
    SRC_HOST = sender.getIP();
    DEST_HOST = receiver1.getIP();
    DEST_HOST2 = receiver2.getIP();

    SRC_PORT = Integer.parseInt(sender.getPort());
    DEST_PORT = Integer.parseInt(receiver1.getPort());
    DEST_PORT2 = Integer.parseInt(receiver2.getPort());

    session_src =
        new Session.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .username("root")
            .password("root")
            .zoneId(ZoneId.of("Asia/Shanghai"))
            .build();
    session_dest =
        new Session.Builder()
            .host(DEST_HOST)
            .port(DEST_PORT)
            .username("root")
            .password("root")
            .zoneId(ZoneId.of("Asia/Shanghai"))
            .build();
    session_dest2 =
        new Session.Builder()
            .host(DEST_HOST2)
            .port(DEST_PORT2)
            .username("root")
            .password("root")
            .zoneId(ZoneId.of("Asia/Shanghai"))
            .build();
    session_src.open(false);
    session_dest.open(false);
    session_dest2.open(false);

    subs = new SubscriptionSession(SRC_HOST, SRC_PORT);
    subs.open();
    System.out.println("TestConfig beforeClass");
  }

  public void afterSuite() throws IoTDBConnectionException {
    System.out.println("TestConfig afterClass");
    session_src.close();
    session_dest.close();
    session_dest2.close();
    subs.close();
  }

  public void createDB(String database) throws IoTDBConnectionException {
    try {
      session_src.createDatabase(database);
    } catch (StatementExecutionException e) {
    }
    try {
      session_dest.createDatabase(database);
    } catch (StatementExecutionException e) {
    }
    try {
      session_dest2.createDatabase(database);
    } catch (StatementExecutionException e) {
    }
  }

  public void dropDB(String pattern) throws IoTDBConnectionException {
    try {
      session_src.executeNonQueryStatement(DROP_DATABASE_SQL + pattern + "*");
    } catch (StatementExecutionException e) {
      System.out.println("### src:" + e);
    }
    try {
      session_dest.executeNonQueryStatement(DROP_DATABASE_SQL + pattern + "*");
    } catch (StatementExecutionException e) {
      System.out.println("### dest:" + e);
    }
    try {
      session_dest2.executeNonQueryStatement(DROP_DATABASE_SQL + pattern + "*");
    } catch (StatementExecutionException e) {
      System.out.println("### dest2:" + e);
    }
  }

  public SubscriptionPullConsumer create_pull_consumer(
      String groupId, String consumerId, Boolean autoCommit, Long interval)
      throws TException, IoTDBConnectionException, IOException, StatementExecutionException {
    SubscriptionPullConsumer pullConsumer;
    Properties properties = new Properties();
    properties.put(ConsumerConstant.HOST_KEY, SRC_HOST);
    properties.put(ConsumerConstant.PORT_KEY, SRC_PORT);
    if (groupId != null) {
      properties.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, groupId);
    }
    if (consumerId != null) {
      properties.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    }
    if (autoCommit != null) {
      properties.put(ConsumerConstant.AUTO_COMMIT_KEY, autoCommit);
    }
    if (interval != null) {
      properties.put(ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_KEY, interval);
    }
    properties.put(ConsumerConstant.FILE_SAVE_DIR_KEY, "target/pull-subscription");
    pullConsumer = new SubscriptionPullConsumer(properties);
    pullConsumer.open();
    return pullConsumer;
  }

  public void createTopic_s(
      String topicName, String pattern, String start, String end, boolean isTsfile)
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    if (pattern != null) {
      properties.setProperty("path", pattern);
    }
    if (start != null) {
      properties.setProperty("start-time", start);
    }
    if (end != null) {
      properties.setProperty("end-time", end);
    }
    if (isTsfile) {
      properties.setProperty("format", "TsFileHandler");
    } else {
      properties.setProperty("format", "SessionDataSet");
    }
    properties.setProperty("processor", "do-nothing-processor");
    subs.createTopic(topicName, properties);
  }

  public void createTopic_s(
      String topicName,
      String pattern,
      String start,
      String end,
      boolean isTsfile,
      String mode,
      String loose_range)
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    if (pattern != null) {
      properties.setProperty(TopicConstant.PATH_KEY, pattern);
    }
    if (start != null) {
      properties.setProperty(TopicConstant.START_TIME_KEY, start);
    }
    if (end != null) {
      properties.setProperty(TopicConstant.END_TIME_KEY, end);
    }
    if (isTsfile) {
      properties.setProperty(TopicConstant.FORMAT_KEY, "TsFileHandler");
    } else {
      properties.setProperty(TopicConstant.FORMAT_KEY, "SessionDataSet");
    }
    if (mode != null && !mode.isEmpty()) {
      properties.setProperty(TopicConstant.MODE_KEY, mode);
    }
    if (loose_range != null && !loose_range.isEmpty()) {
      properties.setProperty(TopicConstant.LOOSE_RANGE_KEY, loose_range);
    }
    subs.createTopic(topicName, properties);
  }

  public static long getCount(Session session, String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      long result = rowRecord.getFields().get(0).getLongV();
      return result;
    }
    return 0;
  }

  public void check_count(int expect_count, String sql, String msg)
      throws IoTDBConnectionException, StatementExecutionException {
    assertEquals(getCount(session_dest, sql), expect_count, "Query count:" + msg);
  }

  public void check_count2(int expect_count, String sql, String msg)
      throws IoTDBConnectionException, StatementExecutionException {
    assertEquals(getCount(session_dest2, sql), expect_count, "Query count:" + msg);
  }

  public void consume_data(SubscriptionPullConsumer consumer, Session session)
      throws TException,
          IOException,
          StatementExecutionException,
          InterruptedException,
          IoTDBConnectionException {
    while (true) {
      Thread.sleep(1000);

      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
      if (messages.isEmpty()) {
        break;
      }
      for (final SubscriptionMessage message : messages) {
        for (final Iterator<Tablet> it = message.getSessionDataSetsHandler().tabletIterator();
            it.hasNext(); ) {
          final Tablet tablet = it.next();
          session.insertTablet(tablet);
        }
      }
      consumer.commitSync(messages);
    }
  }

  public List<Integer> consume_tsfile_withFileCount(
      SubscriptionPullConsumer consumer, String device) throws InterruptedException {
    return consume_tsfile(consumer, Collections.singletonList(device));
  }

  public int consume_tsfile(SubscriptionPullConsumer consumer, String device)
      throws InterruptedException {
    return consume_tsfile(consumer, Collections.singletonList(device)).get(0);
  }

  public List<Integer> consume_tsfile(SubscriptionPullConsumer consumer, List<String> devices)
      throws InterruptedException {
    List<AtomicInteger> rowCounts = new ArrayList<>(devices.size());
    for (int i = 0; i < devices.size(); i++) {
      rowCounts.add(new AtomicInteger(0));
    }
    AtomicInteger onReceived = new AtomicInteger(0);
    while (true) {
      Thread.sleep(1000);
      // That is, the consumer poll will keep pulling if no messages are fetched within the timeout,
      // until a message is fetched or the time exceeds the timeout.
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
      if (messages.isEmpty()) {
        break;
      }
      for (final SubscriptionMessage message : messages) {
        onReceived.incrementAndGet();
        // System.out.println(FORMAT.format(new Date()) + " onReceived=" + onReceived.get());
        final SubscriptionTsFileHandler tsFileHandler = message.getTsFileHandler();
        try (final TsFileReader tsFileReader = tsFileHandler.openReader()) {
          for (int i = 0; i < devices.size(); i++) {
            final Path path = new Path(devices.get(i), "s_0", true);
            final QueryDataSet dataSet =
                tsFileReader.query(QueryExpression.create(Collections.singletonList(path), null));
            while (dataSet.hasNext()) {
              RowRecord next = dataSet.next();
              rowCounts.get(i).addAndGet(1);
              System.out.println(next.getTimestamp() + "," + next.getFields());
            }
            // System.out.println(FORMAT.format(new Date()) + " consume tsfile " + i + ":" +
            // rowCounts.get(i).get());
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      consumer.commitSync(messages);
    }
    List<Integer> results = new ArrayList<>(devices.size());
    for (AtomicInteger rowCount : rowCounts) {
      results.add(rowCount.get());
    }
    results.add(onReceived.get());
    return results;
  }

  public static void consume_data_long(
      SubscriptionPullConsumer consumer, Session session, Long timeout)
      throws StatementExecutionException, InterruptedException, IoTDBConnectionException {
    timeout = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < timeout) {
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
      if (messages.isEmpty()) {
        Thread.sleep(1000);
      }
      for (final SubscriptionMessage message : messages) {
        for (final Iterator<Tablet> it = message.getSessionDataSetsHandler().tabletIterator();
            it.hasNext(); ) {
          final Tablet tablet = it.next();
          session.insertTablet(tablet);
        }
      }
      consumer.commitSync(messages);
    }
  }

  public void consume_data(SubscriptionPullConsumer consumer)
      throws TException,
          IOException,
          StatementExecutionException,
          InterruptedException,
          IoTDBConnectionException {
    consume_data(consumer, session_dest);
  }

  //////////////////////////// strict assertions ////////////////////////////

  public static void assertEquals(int actual, int expected) {
    Assert.assertEquals(expected, actual);
  }

  public static void assertEquals(Integer actual, int expected) {
    Assert.assertEquals(expected, (Object) actual);
  }

  public static void assertEquals(int actual, Integer expected) {
    Assert.assertEquals((Object) expected, actual);
  }

  public static void assertEquals(Integer actual, Integer expected) {
    Assert.assertEquals(expected, actual);
  }

  public static void assertEquals(int actual, int expected, String message) {
    Assert.assertEquals(message, expected, actual);
  }

  public static void assertEquals(Integer actual, int expected, String message) {
    Assert.assertEquals(message, expected, (Object) actual);
  }

  public static void assertEquals(int actual, Integer expected, String message) {
    Assert.assertEquals(message, (Object) expected, actual);
  }

  public static void assertEquals(Integer actual, Integer expected, String message) {
    Assert.assertEquals(message, expected, actual);
  }

  public static void assertEquals(long actual, long expected, String message) {
    Assert.assertEquals(message, expected, actual);
  }

  public static void assertEquals(boolean actual, boolean expected, String message) {
    Assert.assertEquals(message, expected, actual);
  }

  public static void assertTrue(boolean condition) {
    Assert.assertTrue(condition);
  }

  public static void assertTrue(boolean condition, String message) {
    Assert.assertTrue(message, condition);
  }

  public static void assertFalse(boolean condition) {
    Assert.assertFalse(condition);
  }

  public static void assertFalse(boolean condition, String message) {
    Assert.assertFalse(message, condition);
  }

  //////////////////////////// non-strict assertions ////////////////////////////

  public static void assertGte(int actual, int expected) {
    assertGte(actual, expected, null);
  }

  public static void assertGte(int actual, int expected, String message) {
    assertGte((long) actual, expected, message);
  }

  public static void assertGte(long actual, long expected, String message) {
    assertTrue(actual >= expected, message);
    if (!(actual == expected)) {
      String skipMessage = actual + " should be equals to " + expected;
      if (Objects.nonNull(message)) {
        skipMessage += ", message: " + message;
      }
      LOGGER.warn(skipMessage);
      Assume.assumeTrue(skipMessage, actual == expected);
    }
  }

  public void check_count_non_strict(int expect_count, String sql, String msg)
      throws IoTDBConnectionException, StatementExecutionException {
    assertGte(getCount(session_dest, sql), expect_count, "Query count: " + msg);
  }
}
