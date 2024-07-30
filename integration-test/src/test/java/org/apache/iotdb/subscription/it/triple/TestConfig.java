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

package org.apache.iotdb.subscription.it.triple;

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

import org.apache.thrift.TException;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.core.ConditionFactory;
import org.junit.Assert;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

public class TestConfig extends AbstractSubscriptionTripleIT {
  public String SRC_HOST;
  public String DEST_HOST;
  public String DEST_HOST2;

  public int SRC_PORT;
  public int DEST_PORT;
  public int DEST_PORT2;

  public SubscriptionSession subs;
  public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  public Session session_src;
  public Session session_dest;
  public Session session_dest2;
  private static final String dropDatabaseSql = "drop database ";

  public static final ConditionFactory AWAIT =
      await()
          .pollInSameThread()
          .pollDelay(10, TimeUnit.SECONDS)
          .pollInterval(10, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS);

  public void beforeSuite() throws IoTDBConnectionException, StatementExecutionException {
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
            .build();
    session_dest =
        new Session.Builder()
            .host(DEST_HOST)
            .port(DEST_PORT)
            .username("root")
            .password("root")
            .build();
    session_dest2 =
        new Session.Builder()
            .host(DEST_HOST2)
            .port(DEST_PORT2)
            .username("root")
            .password("root")
            .build();
    session_src.open(false);
    session_dest.open(false);
    session_dest2.open(false);

    subs = new SubscriptionSession(SRC_HOST, SRC_PORT);
    subs.open();
    System.out.println("TestConfig beforeClass");
  }

  public void afterSuite() throws IoTDBConnectionException, StatementExecutionException {
    System.out.println("TestConfig afterClass");
    //        if(session_src.checkTimeseriesExists("root.**")) {
    //            session_dest.executeNonQueryStatement("drop database root.**");
    //            session_dest2.executeNonQueryStatement("drop database root.**");
    //            session_src.executeNonQueryStatement("drop database root.**");
    //        }
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
      session_src.executeNonQueryStatement(dropDatabaseSql + pattern + "*");
    } catch (StatementExecutionException e) {
      System.out.println("### src:" + e);
    }
    try {
      session_dest.executeNonQueryStatement(dropDatabaseSql + pattern + "*");
    } catch (StatementExecutionException e) {
      System.out.println("### dest:" + e);
    }
    try {
      session_dest2.executeNonQueryStatement(dropDatabaseSql + pattern + "*");
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
      //            System.out.println(sql+" result="+result);
      return result;
    }
    return 0;
  }

  public void check_count(int expect_count, String sql, String msg)
      throws IoTDBConnectionException, StatementExecutionException {
    assertEquals(getCount(session_dest, sql), expect_count, "查询count:" + msg);
  }

  public void check_count2(int expect_count, String sql, String msg)
      throws IoTDBConnectionException, StatementExecutionException {
    assertEquals(getCount(session_dest2, sql), expect_count, "查询count:" + msg);
  }

  public void consume_data(SubscriptionPullConsumer consumer, Session session)
      throws TException,
          IOException,
          StatementExecutionException,
          InterruptedException,
          IoTDBConnectionException {
    while (true) {
      Thread.sleep(1000);

      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
      if (messages.isEmpty()) {
        break;
      }
      for (final SubscriptionMessage message : messages) {
        for (final Iterator<Tablet> it = message.getSessionDataSetsHandler().tabletIterator();
            it.hasNext(); ) {
          final Tablet tablet = it.next();
          session.insertTablet(tablet);
          //                    System.out.println("consume data:"+tablet.rowSize);
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
      // 就是 consumer poll 如果在 timeout 内没有拉取到消息就会一直拉取，直到拉取到消息或者是时间超过了 timeout
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
      if (messages.isEmpty()) {
        break;
      }
      for (final SubscriptionMessage message : messages) {
        onReceived.incrementAndGet();
        System.out.println(format.format(new Date()) + " onReceived=" + onReceived.get());
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
            System.out.println(
                format.format(new Date()) + " consume tsfile " + i + ":" + rowCounts.get(i).get());
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
      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
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

  public static void assertEquals(int actual, int expected, String message) {
    Assert.assertEquals(expected, actual);
  }

  public static void assertEquals(long actual, long expected, String message) {
    Assert.assertEquals(expected, actual);
  }

  public static void assertTrue(boolean condition, String message) {
    Assert.assertTrue(condition);
  }
}
