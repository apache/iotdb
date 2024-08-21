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

package org.apache.iotdb.subscription.it.triple.regression.param;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBTestParamTopicIT extends AbstractSubscriptionRegressionIT {
  private static SubscriptionPullConsumer consumer;
  private static final String topicName = "TopicParam";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerGroupId("g_TestParamTopic")
            .consumerId("c1")
            .buildPullConsumer();
    consumer.open();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    consumer.close();
    subs.getTopics()
        .forEach(
            topic -> {
              try {
                subs.dropTopic(topic.getTopicName());
              } catch (Exception ignored) {
              }
            });
    super.tearDown();
  }

  private void printTopics(String msg)
      throws IoTDBConnectionException, StatementExecutionException {
    System.out.println(msg);
    subs.getTopics().forEach(System.out::println);
  }

  @Test // Will create a topic named null
  public void testCreateTopic_null() throws IoTDBConnectionException, StatementExecutionException {
    subs.createTopic(null);
    printTopics("testCreateTopic_null");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_emptyString()
      throws IoTDBConnectionException, StatementExecutionException {
    subs.createTopic("");
    printTopics("testCreateTopic_emptyString");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_dup() throws IoTDBConnectionException, StatementExecutionException {
    subs.createTopic(topicName);
    subs.createTopic(topicName);
    printTopics("testCreateTopic_dup");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_invalid()
      throws IoTDBConnectionException, StatementExecutionException {
    subs.createTopic("Topic-1");
    printTopics("testCreateTopic_invalid");
  }

  @Test(expected = StatementExecutionException.class) // path filter conditions are not checked
  public void testCreateTopic_invalidPath_no_root()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.PATH_KEY, "abc");
    subs.createTopic("topic_error_path1", properties);
    printTopics("testCreateTopic_invalidPath_no_root");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_invalidPath_endWithPoint()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.PATH_KEY, "root.");
    subs.createTopic("topic_error_path2", properties);
    printTopics("testCreateTopic_invalidPath_endWithPoint");
  }

  @Test // Need to pay attention to the default value
  public void testCreateTopic_invalidFormat()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.FORMAT_KEY, "abd");
    subs.createTopic("topic_error_path3", properties);
    printTopics("testCreateTopic_invalidFormat");
    subs.dropTopic("topic_error_path3");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_invalidTime()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.START_TIME_KEY, "abd");
    subs.createTopic("topic_error_path4", properties);
    printTopics("testCreateTopic_invalidTime");
  }

  @Test
  public void testCreateTopic_invalidTime2()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.START_TIME_KEY, "now");
    properties.put(TopicConstant.END_TIME_KEY, "now");
    subs.createTopic("topic_error_path5", properties);
    printTopics("testCreateTopic_invalidTime2");
    subs.dropTopic("topic_error_path5");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_invalidTime3()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.START_TIME_KEY, "2024-01-01");
    properties.put(TopicConstant.END_TIME_KEY, "2023-01-01");
    subs.createTopic("topic_error_path6", properties);
    printTopics("testCreateTopic_invalidTime3");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_invalidTime4()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.START_TIME_KEY, "now");
    properties.put(TopicConstant.END_TIME_KEY, "2024-01-01");
    subs.createTopic("topic_error_path7", properties);
    printTopics("testCreateTopic_invalidTime4");
  }

  @Test(expected = StatementExecutionException.class)
  public void testCreateTopic_invalidTime5()
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.START_TIME_KEY, "2023-01-32");
    properties.put(TopicConstant.END_TIME_KEY, "now");
    subs.createTopic("topic_error_path7", properties);
    printTopics("testCreateTopic_invalidTime5");
  }

  @Test
  public void testCreateTopic_invalidTime6()
      throws IoTDBConnectionException,
          StatementExecutionException,
          TException,
          IOException,
          InterruptedException {
    Properties properties = new Properties();
    properties.put(TopicConstant.START_TIME_KEY, "2023-02-29");
    properties.put(TopicConstant.END_TIME_KEY, "now");
    subs.createTopic("topic_error_path8", properties);
    printTopics("testCreateTopic_invalidTime6");
    consumer.subscribe("topic_error_path8");
    String database = "root.testClient";
    createDB(database);
    session_src.executeNonQueryStatement("create timeseries " + database + ".d_1.s_0 int32;");
    session_dest.executeNonQueryStatement("create timeseries " + database + ".d_1.s_0 int32;");
    session_src.executeNonQueryStatement(
        "insert into " + database + ".d_1(time, s_0) values(1677628800000,33);");
    consume_data(consumer, session_dest);
    check_count(1, "select count(s_0) from " + database + ".d_1;", "invalid date");
    consumer.unsubscribe("topic_error_path8");
    subs.dropTopic("topic_error_path8");
    dropDB(database);
  }

  @Test(expected = StatementExecutionException.class) // drop non-existent topic
  public void testDropTopic_null() throws IoTDBConnectionException, StatementExecutionException {
    subs.dropTopic(null);
  }

  @Test(expected = StatementExecutionException.class)
  public void testDropTopic_empty() throws IoTDBConnectionException, StatementExecutionException {
    subs.dropTopic("");
  }

  @Test(expected = StatementExecutionException.class) // drop non-existent topic
  public void testDropTopic_notCreate()
      throws IoTDBConnectionException, StatementExecutionException {
    subs.dropTopic("abab");
  }

  @Test(expected = StatementExecutionException.class)
  public void testDropTopic_dup() throws IoTDBConnectionException, StatementExecutionException {
    String dropName = "`topic-1*.`";
    subs.createTopic(dropName);
    subs.dropTopic(dropName);
    subs.dropTopic(dropName);
  }

  @Test
  public void testGetTopic_nonExist() throws IoTDBConnectionException, StatementExecutionException {
    System.out.println(subs.getTopic("xxx"));
    assertFalse(subs.getTopic("xxx").isPresent());
  }

  @Test
  public void testGetTopic_exist() throws IoTDBConnectionException, StatementExecutionException {
    subs.createTopic("exist_topic_name");
    assertTrue(subs.getTopic("exist_topic_name").isPresent());
    subs.dropTopic("exist_topic_name");
  }
}
