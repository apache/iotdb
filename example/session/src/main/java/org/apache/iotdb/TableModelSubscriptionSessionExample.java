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

package org.apache.iotdb;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class TableModelSubscriptionSessionExample {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;

  private static final String TOPIC_1 = "topic1";
  private static final String TOPIC_2 = "`'topic2'`";
  private static final String TOPIC_3 = "`\"topic3\"`";
  private static final String TOPIC_4 = "`\"top \\.i.c4\"`";

  private static final long SLEEP_NS = 1_000_000_000L;
  private static final long POLL_TIMEOUT_MS = 10_000L;
  private static final int MAX_RETRY_TIMES = 3;
  private static final int PARALLELISM = 8;
  private static final long CURRENT_TIME = System.currentTimeMillis();

  private static void createDataBaseAndTable(
      final ITableSession session, final String database, final String table)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("create database if not exists " + database);
    session.executeNonQueryStatement("use " + database);
    session.executeNonQueryStatement(
        String.format(
            "CREATE TABLE %s (s0 string tag, s1 string tag, s2 string tag, s3 string tag, s4 int64 field, s5 float field, s6 string field, s7 timestamp field, s8 int32 field, s9 double field, s10 date field, s11 text field)",
            table));
  }

  private static void insertData(
      final ITableSession session,
      final String dataBaseName,
      final String tableName,
      final int start,
      final int end)
      throws IoTDBConnectionException, StatementExecutionException {
    final List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (s0, s3, s2, s1, s4, s5, s6, s7, s8, s9, s10, s11, time) values ('t%s','t%s','t%s','t%s','%s', %s.0, %s, %s, %d, %d.0, '%s', '%s', %s)",
              tableName, i, i, i, i, i, i, i, i, i, i, getDateStr(i), i, i));
    }
    list.add("flush");
    session.executeNonQueryStatement("use " + dataBaseName);
    for (final String s : list) {
      session.executeNonQueryStatement(s);
    }
  }

  private static String getDateStr(final int value) {
    final Date date = new Date(value);
    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      return dateFormat.format(date);
    } catch (final Exception e) {
      return "1970-01-01";
    }
  }

  private static void dataSubscription() throws Exception {
    // Create topics
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host("127.0.0.1")
            .port(6667)
            .username("root")
            .password("root")
            .build()) {
      final Properties config = new Properties();
      config.put(TopicConstant.DATABASE_KEY, "db.*");
      config.put(TopicConstant.TABLE_KEY, "test.*");
      config.put(TopicConstant.START_TIME_KEY, 25);
      config.put(TopicConstant.END_TIME_KEY, 75);
      config.put(TopicConstant.STRICT_KEY, "true");
      session.createTopic(TOPIC_1, config);
    }

    int retryCount = 0;
    try (final ISubscriptionTablePullConsumer consumer1 =
        new SubscriptionTablePullConsumerBuilder()
            .consumerId("c1")
            .consumerGroupId("cg1")
            .build()) {
      consumer1.open();
      consumer1.subscribe(TOPIC_1);
      while (true) {
        final List<SubscriptionMessage> messages = consumer1.poll(POLL_TIMEOUT_MS);
        if (messages.isEmpty()) {
          retryCount++;
          if (retryCount >= MAX_RETRY_TIMES) {
            break;
          }
        }
        for (final SubscriptionMessage message : messages) {
          for (final SubscriptionSessionDataSet dataSet : message.getSessionDataSetsHandler()) {
            System.out.println(dataSet.getDatabaseName());
            System.out.println(dataSet.getTableName());
            System.out.println(dataSet.getColumnNames());
            System.out.println(dataSet.getColumnTypes());
            System.out.println(dataSet.getColumnCategories());
            while (dataSet.hasNext()) {
              System.out.println(dataSet.next());
            }
          }
        }
        // Auto commit
      }

      // Show topics and subscriptions
      try (final ISubscriptionTableSession session =
          new SubscriptionTableSessionBuilder()
              .host("127.0.0.1")
              .port(6667)
              .username("root")
              .password("root")
              .build()) {
        session.getTopics().forEach((System.out::println));
        session.getSubscriptions().forEach((System.out::println));
      }

      consumer1.unsubscribe(TOPIC_1);
    }
  }

  public static void main(final String[] args) throws Exception {
    try (final ITableSession session =
        new TableSessionBuilder()
            .nodeUrls(Collections.singletonList(HOST + ":" + PORT))
            .username("root")
            .password("root")
            .build()) {
      createDataBaseAndTable(session, "db1", "test1");
      createDataBaseAndTable(session, "db1", "test2");
      createDataBaseAndTable(session, "db2", "test1");
      createDataBaseAndTable(session, "db2", "test2");
      insertData(session, "db1", "test1", 0, 100);
      insertData(session, "db1", "test2", 0, 100);
      insertData(session, "db2", "test1", 0, 100);
      insertData(session, "db2", "test2", 0, 100);
      dataSubscription();
    }
  }
}
