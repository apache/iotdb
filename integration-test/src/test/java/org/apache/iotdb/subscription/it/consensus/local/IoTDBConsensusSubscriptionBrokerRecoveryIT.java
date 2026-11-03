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

package org.apache.iotdb.subscription.it.consensus.local;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleConsumerGroupMetaReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.PollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequest;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequestType;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.subscription.SubscriptionSessionWrapper;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBConsensusSubscriptionBrokerRecoveryIT extends AbstractSubscriptionIT {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(3)
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    // Recovery is driven explicitly below; a periodic metadata push must not mask a broken
    // handshake.
    final Properties config = new Properties();
    config.setProperty("subscription_enabled", "true");
    ((MppBaseConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(config);
    config.setProperty("subscription_meta_syncer_initial_sync_delay_minutes", "60");
    config.setProperty("subscription_meta_syncer_sync_interval_minutes", "60");
    ((MppBaseConfig) EnvFactory.getEnv().getConfig().getConfigNodeCommonConfig())
        .updateProperties(config);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      super.tearDown();
    }
  }

  @Test
  public void testHandshakeRepairsMissingBrokersWithoutResubscribing() throws Exception {
    testMissingBrokerRecovery(true);
  }

  @Test
  public void testMetadataPushRepairsMissingBrokersWithoutRestoringCanceledTopics()
      throws Exception {
    testMissingBrokerRecovery(false);
  }

  private void testMissingBrokerRecovery(final boolean repairByHandshake) throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers(
            repairByHandshake ? "broker_handshake" : "broker_metadata");
    final String canceledTopic = ids.getTopic() + "_canceled";
    final List<SubscriptionSessionWrapper> probes = new ArrayList<>();
    SubscriptionTreePullConsumer consumer = null;
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");
      ConsensusSubscriptionITSupport.createConsensusTopic(canceledTopic, ids.getDatabase() + ".**");
      consumer = createConsumer(ids, TimeUnit.HOURS.toMillis(1));
      consumer.subscribe(ids.getTopic());
      final Set<String> committedRows =
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 100L, 8, true);
      assertRowsConsumed(consumer, committedRows);
      // Creating a new subscription runs ConfigNode's commit-progress synchronization without
      // background metadata pushes, then this topic is left unsubscribed.
      consumer.subscribe(canceledTopic);
      consumer.unsubscribe(canceledTopic);
      awaitConfigNodeCheckpoint(ids, consumer.committedPositions(ids.getTopic()));
      final ConsumerGroupMeta authoritativeMeta = getConsumerGroupMeta(ids.getConsumerGroupId());
      Assert.assertEquals(
          Collections.singleton(ids.getTopic()), authoritativeMeta.getSubscribedTopicNames());
      final ConsumerConfig consumerConfig = createConsumerConfig(authoritativeMeta, consumer);

      for (final DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
        final SubscriptionSessionWrapper probe = getProviderSession(consumer, node);
        probes.add(probe);
        // A replacement-generation push can remove the old broker and then fail while resolving
        // topic metadata. Exercise that real RPC failure without changing ConfigNode's metadata.
        final ConsumerGroupMeta replacement =
            new ConsumerGroupMeta(
                ids.getConsumerGroupId(),
                authoritativeMeta.getCreationTime() + 1,
                authoritativeMeta.getConsumerMeta(ids.getConsumerId()));
        replacement.addSubscription(
            ids.getConsumerId(), Collections.singleton(ids.getTopic() + "_missing"));
        final TPushConsumerGroupMetaResp failure = pushMeta(clients, node, replacement);
        Assert.assertEquals(
            failure.toString(),
            TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode(),
            failure.getStatus().getCode());
        Assert.assertTrue(failure.isSetExceptionMessages());
        Assert.assertFalse(failure.getExceptionMessages().isEmpty());
        final TPipeSubscribeResp missingBroker =
            probe
                .getSessionConnection()
                .pipeSubscribe(
                    PipeSubscribePollReq.toTPipeSubscribeReq(
                        new SubscriptionPollRequest(
                            SubscriptionPollRequestType.POLL.getType(),
                            new PollPayload(Collections.singleton(ids.getTopic())),
                            1_000L,
                            1_048_576L)));
        Assert.assertEquals(
            missingBroker.getStatus().toString(),
            TSStatusCode.SUBSCRIPTION_POLL_ERROR.getStatusCode(),
            missingBroker.getStatus().getCode());
      }
      Assert.assertEquals(authoritativeMeta, getConsumerGroupMeta(ids.getConsumerGroupId()));
      final Set<String> pendingRows =
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 200L, 8, true);

      for (int index = 0; index < probes.size(); index++) {
        final SubscriptionSessionWrapper probe = probes.get(index);
        if (repairByHandshake) {
          assertSuccess(
              probe
                  .getSessionConnection()
                  .pipeSubscribe(PipeSubscribeHandshakeReq.toTPipeSubscribeReq(consumerConfig)));
        } else {
          final TPushConsumerGroupMetaResp repaired =
              pushMeta(clients, EnvFactory.getEnv().getDataNodeWrapper(index), authoritativeMeta);
          Assert.assertEquals(
              repaired.toString(),
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              repaired.getStatus().getCode());
        }
        final TPipeSubscribeResp heartbeat =
            probe.getSessionConnection().pipeSubscribe(SubscriptionHeartbeatReq.toThriftReq());
        assertSuccess(heartbeat);
        Assert.assertEquals(
            Collections.singleton(ids.getTopic()),
            PipeSubscribeHeartbeatResp.fromTPipeSubscribeResp(heartbeat).getTopics().keySet());
      }
      Assert.assertEquals(authoritativeMeta, getConsumerGroupMeta(ids.getConsumerGroupId()));
      assertRowsConsumed(consumer, pendingRows);
      assertRowsConsumed(
          consumer,
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 300L, 8, true));
    } finally {
      try {
        clients.close();
      } finally {
        ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
        try (final SubscriptionTreeSession session =
            new SubscriptionTreeSession(
                EnvFactory.getEnv().getIP(), Integer.parseInt(EnvFactory.getEnv().getPort()))) {
          session.open();
          session.dropTopicIfExists(canceledTopic);
        }
      }
    }
  }

  @Test
  public void testProviderReplacementPreservesUncommittedBatch() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("broker_provider_replacement");
    SubscriptionTreePullConsumer consumer = null;
    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");
      consumer = createConsumer(ids, 1_000L);
      consumer.subscribe(ids.getTopic());
      final Set<String> rows =
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 100L, 8, true);
      final ConsensusSubscriptionITSupport.PolledMessageBatch batch =
          ConsensusSubscriptionITSupport.pollFirstNonEmptyBatchWithoutCommit(
              consumer, 60, Duration.ofSeconds(1));
      Assert.assertFalse(batch.getMessages().isEmpty());
      final TopicProgress progressBeforeReplacement = consumer.committedPositions(ids.getTopic());
      final ConsumerGroupMeta metaBeforeReplacement =
          getConsumerGroupMeta(ids.getConsumerGroupId());
      final Map<?, ?> providers = getProviders(consumer);
      final Map<?, ?> oldProviders = new HashMap<>(providers);
      Assert.assertEquals(3, oldProviders.size());
      for (final Object provider : oldProviders.values()) {
        ((SubscriptionSessionWrapper) getField(provider, "session"))
            .getSessionConnection()
            .forceClose();
      }
      Awaitility.await()
          .pollInSameThread()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                Assert.assertEquals(oldProviders.keySet(), providers.keySet());
                for (final Object nodeId : oldProviders.keySet()) {
                  Assert.assertNotSame(oldProviders.get(nodeId), providers.get(nodeId));
                }
              });
      Assert.assertEquals(metaBeforeReplacement, getConsumerGroupMeta(ids.getConsumerGroupId()));
      Assert.assertEquals(progressBeforeReplacement, consumer.committedPositions(ids.getTopic()));
      consumer.commitSync(batch.getMessages());
      final ConsensusSubscriptionITSupport.ConsumedRecords consumed = batch.getConsumedRecords();
      consumed.merge(
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, rows.size() - consumed.getUniqueRowCount(), 20, Duration.ofSeconds(1)));
      ConsensusSubscriptionITSupport.assertExactRowKeys(rows, consumed);
      assertRowsConsumed(
          consumer,
          ConsensusSubscriptionITSupport.insertRows(ids.getDatabase(), "d0", 200L, 8, true));
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }

  private static SubscriptionTreePullConsumer createConsumer(
      final ConsensusSubscriptionITSupport.TestIdentifiers ids, final long controlIntervalMs)
      throws Exception {
    final SubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId(ids.getConsumerId())
            .consumerGroupId(ids.getConsumerGroupId())
            .heartbeatIntervalMs(controlIntervalMs)
            .endpointsSyncIntervalMs(controlIntervalMs)
            .autoCommit(false)
            .buildPullConsumer();
    consumer.open();
    return consumer;
  }

  private static ConsumerConfig createConsumerConfig(
      final ConsumerGroupMeta meta, final SubscriptionTreePullConsumer consumer) throws Exception {
    final Map<String, String> attributes =
        new HashMap<>(meta.getConsumerMeta(consumer.getConsumerId()).getConfig().getAttribute());
    attributes.put(
        ConsumerConstant.CONSUMER_INSTANCE_ID_KEY,
        (String) getField(consumer, "consumerInstanceId"));
    return new ConsumerConfig(attributes);
  }

  private static SubscriptionSessionWrapper getProviderSession(
      final SubscriptionTreePullConsumer consumer, final DataNodeWrapper node) throws Exception {
    final TEndPoint endpoint = new TEndPoint(node.getIp(), node.getPort());
    for (final Object provider : getProviders(consumer).values()) {
      if (endpoint.equals(getField(provider, "endPoint"))) {
        return (SubscriptionSessionWrapper) getField(provider, "session");
      }
    }
    throw new AssertionError("Missing subscription provider for " + endpoint);
  }

  private static ConsumerGroupMeta getConsumerGroupMeta(final String consumerGroupId)
      throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      final TGetAllSubscriptionInfoResp resp = client.getAllSubscriptionInfo();
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      for (final ByteBuffer buffer : resp.getAllSubscriptionInfo()) {
        final ConsumerGroupMeta meta = ConsumerGroupMeta.deserialize(buffer);
        if (consumerGroupId.equals(meta.getConsumerGroupId())) {
          return meta;
        }
      }
      throw new AssertionError("Missing consumer group " + consumerGroupId);
    }
  }

  private static TPushConsumerGroupMetaResp pushMeta(
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients,
      final DataNodeWrapper node,
      final ConsumerGroupMeta meta)
      throws Exception {
    try (final SyncDataNodeInternalServiceClient client =
        clients.borrowClient(new TEndPoint(node.getInternalAddress(), node.getInternalPort()))) {
      return client.pushSingleConsumerGroupMeta(
          new TPushSingleConsumerGroupMetaReq().setConsumerGroupMeta(meta.serialize()));
    }
  }

  private static void awaitConfigNodeCheckpoint(
      final ConsensusSubscriptionITSupport.TestIdentifiers ids, final TopicProgress checkpoint) {
    Assert.assertNotNull(checkpoint);
    Assert.assertFalse(checkpoint.getRegionProgress().isEmpty());
    Awaitility.await()
        .pollInSameThread()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              try (final SyncConfigNodeIServiceClient client =
                  (SyncConfigNodeIServiceClient)
                      EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
                for (final Map.Entry<String, RegionProgress> entry :
                    checkpoint.getRegionProgress().entrySet()) {
                  final TGetCommitProgressResp resp =
                      client.getCommitProgress(
                          new TGetCommitProgressReq(
                              ids.getConsumerGroupId(),
                              ids.getTopic(),
                              ConsensusGroupId.Factory.createFromString(entry.getKey()).getId(),
                              0));
                  Assert.assertEquals(
                      TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
                  Assert.assertTrue(resp.isSetCommittedRegionProgress());
                  Assert.assertEquals(
                      entry.getValue(),
                      RegionProgress.deserialize(
                          ByteBuffer.wrap(resp.getCommittedRegionProgress())));
                }
              }
            });
  }

  private static void assertRowsConsumed(
      final SubscriptionTreePullConsumer consumer, final Set<String> expectedRows)
      throws Exception {
    ConsensusSubscriptionITSupport.assertExactRowKeys(
        expectedRows,
        ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
            consumer, expectedRows.size(), 60, Duration.ofSeconds(1)));
  }

  private static void assertSuccess(final TPipeSubscribeResp resp) {
    Assert.assertEquals(
        resp.getStatus().toString(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        resp.getStatus().getCode());
  }

  private static Map<?, ?> getProviders(final SubscriptionTreePullConsumer consumer)
      throws Exception {
    return (Map<?, ?>) getField(getField(consumer, "providers"), "subscriptionProviders");
  }

  private static Object getField(final Object target, final String name) throws Exception {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (final NoSuchFieldException ignored) {
        // Client state is declared in the consumer/provider base classes.
      }
    }
    throw new NoSuchFieldException(name);
  }
}
