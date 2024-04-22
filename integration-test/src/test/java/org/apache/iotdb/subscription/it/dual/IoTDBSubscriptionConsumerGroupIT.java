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

package org.apache.iotdb.subscription.it.dual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2Subscription;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;

import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2Subscription.class})
public class IoTDBSubscriptionConsumerGroupIT extends AbstractSubscriptionDualIT {

  // Test dimensions:
  // 1. multi scenario of consumer, consumer group and subscribed topic
  // 2. historical or realtime data
  // 3. multi pipe sync protocol for reference

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionConsumerGroupIT.class);

  private static Map<String, String> ASYNC_CONNECTOR_ATTRIBUTES;
  private static Map<String, String> SYNC_CONNECTOR_ATTRIBUTES;
  private static Map<String, String> LEGACY_CONNECTOR_ATTRIBUTES;
  private static Map<String, String> AIR_GAP_CONNECTOR_ATTRIBUTES;

  private static Pair<List<SubscriptionInfo>, Map<String, String>> __3C_1CG_SUBSCRIBE_ONE_TOPIC;
  private static Pair<List<SubscriptionInfo>, Map<String, String>> __3C_3CG_SUBSCRIBE_ONE_TOPIC;
  private static Pair<List<SubscriptionInfo>, Map<String, String>> __3C_1CG_SUBSCRIBE_TWO_TOPIC;
  private static Pair<List<SubscriptionInfo>, Map<String, String>> __3C_3CG_SUBSCRIBE_TWO_TOPIC;
  private static Pair<List<SubscriptionInfo>, Map<String, String>> __4C_2CG_SUBSCRIBE_TWO_TOPIC;

  static final class SubscriptionInfo {
    final String consumerId;
    final String consumerGroupId;
    final Set<String> topicNames;

    SubscriptionInfo(
        final String consumerId, final String consumerGroupId, final Set<String> topicNames) {
      this.consumerId = consumerId;
      this.consumerGroupId = consumerGroupId;
      this.topicNames = topicNames;
    }
  }

  @Before
  public void setUp() {
    super.setUp();

    // Setup connector attributes
    ASYNC_CONNECTOR_ATTRIBUTES = new HashMap<>();
    ASYNC_CONNECTOR_ATTRIBUTES.put("connector", "iotdb-thrift-async-connector");
    ASYNC_CONNECTOR_ATTRIBUTES.put("connector.ip", receiverEnv.getIP());
    ASYNC_CONNECTOR_ATTRIBUTES.put("connector.port", receiverEnv.getPort());

    SYNC_CONNECTOR_ATTRIBUTES = new HashMap<>();
    SYNC_CONNECTOR_ATTRIBUTES.put("connector", "iotdb-thrift-sync-connector");
    SYNC_CONNECTOR_ATTRIBUTES.put("connector.ip", receiverEnv.getIP());
    SYNC_CONNECTOR_ATTRIBUTES.put("connector.port", receiverEnv.getPort());

    LEGACY_CONNECTOR_ATTRIBUTES = new HashMap<>();
    LEGACY_CONNECTOR_ATTRIBUTES.put("connector", "iotdb-legacy-pipe-connector");
    LEGACY_CONNECTOR_ATTRIBUTES.put("connector.ip", receiverEnv.getIP());
    LEGACY_CONNECTOR_ATTRIBUTES.put("connector.port", receiverEnv.getPort());

    final StringBuilder nodeUrlsBuilder = new StringBuilder();
    for (final DataNodeWrapper wrapper : receiverEnv.getDataNodeWrapperList()) {
      // Use default port for convenience
      nodeUrlsBuilder
          .append(wrapper.getIp())
          .append(":")
          .append(wrapper.getPipeAirGapReceiverPort())
          .append(",");
    }
    AIR_GAP_CONNECTOR_ATTRIBUTES = new HashMap<>();
    AIR_GAP_CONNECTOR_ATTRIBUTES.put("connector", "iotdb-air-gap-connector");
    AIR_GAP_CONNECTOR_ATTRIBUTES.put("connector.node-urls", nodeUrlsBuilder.toString());

    // Setup subscription info list with expected results
    {
      final List<SubscriptionInfo> subscriptionInfoList = new ArrayList<>();
      subscriptionInfoList.add(new SubscriptionInfo("c1", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(new SubscriptionInfo("c2", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(new SubscriptionInfo("c3", "cg1", Collections.singleton("topic1")));

      final Map<String, String> expectedHeaderWithResult = new HashMap<>();
      expectedHeaderWithResult.put("count(root.cg1.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic2.s)", "100");

      __3C_1CG_SUBSCRIBE_ONE_TOPIC = new Pair<>(subscriptionInfoList, expectedHeaderWithResult);
    }
    {
      final List<SubscriptionInfo> subscriptionInfoList = new ArrayList<>();
      subscriptionInfoList.add(new SubscriptionInfo("c1", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(new SubscriptionInfo("c2", "cg2", Collections.singleton("topic1")));
      subscriptionInfoList.add(new SubscriptionInfo("c3", "cg3", Collections.singleton("topic1")));

      final Map<String, String> expectedHeaderWithResult = new HashMap<>();
      expectedHeaderWithResult.put("count(root.cg1.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg2.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg3.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic2.s)", "100");

      __3C_3CG_SUBSCRIBE_ONE_TOPIC = new Pair<>(subscriptionInfoList, expectedHeaderWithResult);
    }
    {
      final List<SubscriptionInfo> subscriptionInfoList = new ArrayList<>();
      subscriptionInfoList.add(new SubscriptionInfo("c1", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(
          new SubscriptionInfo("c2", "cg1", new HashSet<>(Arrays.asList("topic1", "topic2"))));
      subscriptionInfoList.add(new SubscriptionInfo("c3", "cg1", Collections.singleton("topic2")));

      final Map<String, String> expectedHeaderWithResult = new HashMap<>();
      expectedHeaderWithResult.put("count(root.cg1.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg1.topic2.s)", "100");
      expectedHeaderWithResult.put("count(root.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic2.s)", "100");

      __3C_1CG_SUBSCRIBE_TWO_TOPIC = new Pair<>(subscriptionInfoList, expectedHeaderWithResult);
    }
    {
      final List<SubscriptionInfo> subscriptionInfoList = new ArrayList<>();
      subscriptionInfoList.add(new SubscriptionInfo("c1", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(
          new SubscriptionInfo("c2", "cg2", new HashSet<>(Arrays.asList("topic1", "topic2"))));
      subscriptionInfoList.add(new SubscriptionInfo("c3", "cg3", Collections.singleton("topic2")));

      final Map<String, String> expectedHeaderWithResult = new HashMap<>();
      expectedHeaderWithResult.put("count(root.cg1.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg2.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg2.topic2.s)", "100");
      expectedHeaderWithResult.put("count(root.cg3.topic2.s)", "100");
      expectedHeaderWithResult.put("count(root.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic2.s)", "100");

      __3C_3CG_SUBSCRIBE_TWO_TOPIC = new Pair<>(subscriptionInfoList, expectedHeaderWithResult);
    }
    {
      final List<SubscriptionInfo> subscriptionInfoList = new ArrayList<>();
      subscriptionInfoList.add(new SubscriptionInfo("c1", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(
          new SubscriptionInfo("c2", "cg2", new HashSet<>(Arrays.asList("topic1", "topic2"))));
      subscriptionInfoList.add(new SubscriptionInfo("c3", "cg1", Collections.singleton("topic1")));
      subscriptionInfoList.add(new SubscriptionInfo("c4", "cg2", Collections.singleton("topic2")));

      final Map<String, String> expectedHeaderWithResult = new HashMap<>();
      expectedHeaderWithResult.put("count(root.cg1.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg2.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.cg2.topic2.s)", "100");
      expectedHeaderWithResult.put("count(root.topic1.s)", "100");
      expectedHeaderWithResult.put("count(root.topic2.s)", "100");

      __4C_2CG_SUBSCRIBE_TWO_TOPIC = new Pair<>(subscriptionInfoList, expectedHeaderWithResult);
    }
  }

  private void testSubscriptionHistoricalDataTemplate(
      final Map<String, String> connectorAttributes,
      final List<SubscriptionInfo> subscriptionInfoList,
      final Map<String, String> expectedHeaderWithResult)
      throws Exception {
    final long currentTime = System.currentTimeMillis();

    // Insert some historical data
    insertData(currentTime);

    // Create topic 'topic1' and 'topic2'
    createTopics(currentTime);

    // Create pipe 'sync_topic1' and 'sync_topic2' with given connector attributes
    createPipes(currentTime, connectorAttributes);

    // Create subscription and check result
    pollMessagesAndCheck(
        subscriptionInfoList.stream()
            .map(
                (info) -> {
                  try {
                    return createConsumerAndSubscribeTopics(info);
                  } catch (final Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                    return null;
                  }
                })
            .collect(Collectors.toList()),
        expectedHeaderWithResult);
  }

  private void testSubscriptionRealtimeDataTemplate(
      final Map<String, String> connectorAttributes,
      final List<SubscriptionInfo> subscriptionInfoList,
      final Map<String, String> expectedHeaderWithResult)
      throws Exception {
    final long currentTime = System.currentTimeMillis();

    // Create topic 'topic1' and 'topic2'
    createTopics(currentTime);

    // Create pipe 'sync_topic1' and 'sync_topic2' with given connector attributes
    createPipes(currentTime, connectorAttributes);

    // Insert some realtime data
    insertData(currentTime);

    // Create subscription and check result
    pollMessagesAndCheck(
        subscriptionInfoList.stream()
            .map(
                (info) -> {
                  try {
                    return createConsumerAndSubscribeTopics(info);
                  } catch (final Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                    return null;
                  }
                })
            .collect(Collectors.toList()),
        expectedHeaderWithResult);
  }

  // -------------------------------------- //
  // 3 consumers, 1 consumer group, 1 topic //
  // -------------------------------------- //

  @Test
  public void test3C1CGSubscribeOneTopicHistoricalDataWithAsyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicHistoricalDataWithSyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicHistoricalDataWithLegacyConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicHistoricalDataWithAirGapConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicRealtimeDataWithAsyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicRealtimeDataWithSyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicRealtimeDataWithLegacyConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeOneTopicRealtimeDataWithAirGapConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_1CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  // --------------------------------------- //
  // 3 consumers, 3 consumer groups, 1 topic //
  // --------------------------------------- //

  @Test
  public void test3C3CGSubscribeOneTopicHistoricalDataWithAsyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicHistoricalDataWithSyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicHistoricalDataWithLegacyConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicHistoricalDataWithAirGapConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicRealtimeDataWithAsyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicRealtimeDataWithSyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicRealtimeDataWithLegacyConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeOneTopicRealtimeDataWithAirGapConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.left,
        __3C_3CG_SUBSCRIBE_ONE_TOPIC.right);
  }

  // --------------------------------------- //
  // 3 consumers, 1 consumer group, 2 topics //
  // --------------------------------------- //

  @Test
  public void test3C1CGSubscribeTwoTopicHistoricalDataWithAsyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicHistoricalDataWithSyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicHistoricalDataWithLegacyConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicHistoricalDataWithAirGapConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicRealtimeDataWithAsyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicRealtimeDataWithSyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicRealtimeDataWithLegacyConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C1CGSubscribeTwoTopicRealtimeDataWithAirGapConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_1CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  // ---------------------------------------- //
  // 3 consumers, 3 consumer groups, 2 topics //
  // ---------------------------------------- //

  @Test
  public void test3C3CGSubscribeTwoTopicHistoricalDataWithAsyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicHistoricalDataWithSyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicHistoricalDataWithLegacyConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicHistoricalDataWithAirGapConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicRealtimeDataWithAsyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicRealtimeDataWithSyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicRealtimeDataWithLegacyConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test3C3CGSubscribeTwoTopicRealtimeDataWithAirGapConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.left,
        __3C_3CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  // ---------------------------------------- //
  // 4 consumers, 2 consumer groups, 2 topics //
  // ---------------------------------------- //

  @Test
  public void test4C2CGSubscribeTwoTopicHistoricalDataWithAsyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicHistoricalDataWithSyncConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicHistoricalDataWithLegacyConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicHistoricalDataWithAirGapConnector() throws Exception {
    testSubscriptionHistoricalDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicRealtimeDataWithAsyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        ASYNC_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicRealtimeDataWithSyncConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        SYNC_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicRealtimeDataWithLegacyConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        LEGACY_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  @Test
  public void test4C2CGSubscribeTwoTopicRealtimeDataWithAirGapConnector() throws Exception {
    testSubscriptionRealtimeDataTemplate(
        AIR_GAP_CONNECTOR_ATTRIBUTES,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.left,
        __4C_2CG_SUBSCRIBE_TWO_TOPIC.right);
  }

  /////////////////////////////// utility ///////////////////////////////

  private void createTopics(final long currentTime) {
    // Create topics on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      session.executeNonQueryStatement(
          String.format("create topic topic1 with ('end-time'='%s')", currentTime - 1));
      session.executeNonQueryStatement(
          String.format("create topic topic2 with ('start-time'='%s')", currentTime));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void insertData(final long currentTime) {
    // Insert some data on sender
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.topic1(time, s) values (%s, 1)", i)); // topic1
        session.executeNonQueryStatement(
            String.format(
                "insert into root.topic2(time, s) values (%s, 1)", currentTime + i)); // topic2
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createPipes(final long currentTime, final Map<String, String> connectorAttributes) {
    // For sync reference
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();

      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("inclusion.exclusion", "data.delete");
      extractorAttributes.put("end-time", String.valueOf(currentTime - 1));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("sync_topic1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();

      extractorAttributes.put("inclusion", "data.insert");
      extractorAttributes.put("inclusion.exclusion", "data.delete");
      extractorAttributes.put("start-time", String.valueOf(currentTime));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("sync_topic2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private SubscriptionPullConsumer createConsumerAndSubscribeTopics(
      final SubscriptionInfo subscriptionInfo) throws Exception {
    final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(senderEnv.getIP())
            .port(Integer.parseInt(senderEnv.getPort()))
            .consumerId(subscriptionInfo.consumerId)
            .consumerGroupId(subscriptionInfo.consumerGroupId)
            .autoCommit(false)
            .buildPullConsumer();
    consumer.open();
    consumer.subscribe(subscriptionInfo.topicNames);
    return consumer;
  }

  private void pollMessagesAndCheck(
      final List<SubscriptionPullConsumer> consumers,
      final Map<String, String> expectedHeaderWithResult)
      throws Exception {
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final AtomicBoolean receiverCrashed = new AtomicBoolean(false);

    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < consumers.size(); ++i) {
      final int index = i;
      final String consumerId = consumers.get(index).getConsumerId();
      final String consumerGroupId = consumers.get(index).getConsumerGroupId();
      final Thread t =
          new Thread(
              () -> {
                try (final SubscriptionPullConsumer consumer = consumers.get(index)) {
                  while (!isClosed.get()) {
                    try {
                      Thread.sleep(1000); // wait some time
                    } catch (final InterruptedException e) {
                      break;
                    }
                    final List<SubscriptionMessage> messages =
                        consumer.poll(Duration.ofMillis(10000));
                    if (messages.isEmpty()) {
                      continue;
                    }
                    for (final SubscriptionMessage message : messages) {
                      final SubscriptionSessionDataSets payload =
                          (SubscriptionSessionDataSets) message.getPayload();
                      for (final SubscriptionSessionDataSet dataSet : payload) {
                        final List<String> columnNameList = dataSet.getColumnNames();
                        while (dataSet.hasNext()) {
                          final RowRecord record = dataSet.next();
                          if (!insertRowRecordEnrichedByConsumerGroupId(
                              columnNameList, record, consumerGroupId)) {
                            receiverCrashed.set(true);
                            throw new RuntimeException("detect receiver crashed");
                          }
                        }
                      }
                    }
                    consumer.commitSync(messages);
                  }
                  // No need to unsubscribe
                } catch (final Exception e) {
                  e.printStackTrace();
                  // Avoid failure
                } finally {
                  LOGGER.info(
                      "consumer {} (consumer group {}) exiting...", consumerId, consumerGroupId);
                }
              },
              String.format("%s_%s", consumerId, consumerGroupId));
      t.start();
      threads.add(t);
    }

    // Check data on receiver
    try {
      try (final Connection connection = receiverEnv.getConnection();
          final Statement statement = connection.createStatement()) {
        // Keep retrying if there are execution failures
        Awaitility.await()
            .pollDelay(1, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  if (receiverCrashed.get()) {
                    LOGGER.info("detect receiver crashed, skipping this test...");
                    return;
                  }
                  TestUtils.assertSingleResultSetEqual(
                      TestUtils.executeQueryWithRetry(statement, "select count(*) from root.**"),
                      expectedHeaderWithResult);
                });
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      isClosed.set(true);
      for (final Thread thread : threads) {
        thread.join();
      }
    }
  }

  /** @return false -> receiver crashed */
  private boolean insertRowRecordEnrichedByConsumerGroupId(
      final List<String> columnNameList, final RowRecord record, final String consumerGroupId)
      throws Exception {
    if (columnNameList.size() != 2) {
      LOGGER.warn("unexpected column name list: {}", columnNameList);
      throw new Exception("unexpected column name list");
    }
    final String columnName = columnNameList.get(1);
    if ("root.topic1.s".equals(columnName)) {
      final String sql =
          String.format(
              "insert into root.%s.topic1(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      LOGGER.info(sql);
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else if ("root.topic2.s".equals(columnName)) {
      final String sql =
          String.format(
              "insert into root.%s.topic2(time, s) values (%s, 1)",
              consumerGroupId, record.getTimestamp());
      LOGGER.info(sql);
      return TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, sql);
    } else {
      LOGGER.warn("unexpected column name: {}", columnName);
      throw new Exception("unexpected column name");
    }
  }
}
