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

package org.apache.iotdb.confignode.procedure.impl.subscription.subscription;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.DataNodeTransportFailureUtils;
import org.apache.iotdb.confignode.client.async.handlers.rpc.PipePushMetaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.subscription.ConsumerGroupPushMetaRPCHandler;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.subscription.SubscriptionManager;
import org.apache.iotdb.confignode.manager.subscription.SubscriptionReadyDataNodeTracker;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DropSubscriptionProcedureTest {
  @Test
  public void transportFailureClassificationShouldUseExceptionTypes() {
    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportException(
            new TTransportException("connection refused")));
    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportException(new ConnectException("connection")));
    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportException(new TimeoutException("timeout")));
    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportException(new SocketTimeoutException("timeout")));
    Assert.assertTrue(
        DataNodeTransportFailureUtils.isTransportException(
            new RuntimeException(new TimeoutException("timeout"))));
    Assert.assertFalse(
        DataNodeTransportFailureUtils.isTransportException(
            new IllegalStateException("local failure")));
    Assert.assertFalse(
        DataNodeTransportFailureUtils.isTransportException(
            new TApplicationException("remote application failure")));
    Assert.assertFalse(
        DataNodeTransportFailureUtils.isTransportException(
            new TProtocolException("invalid reply")));
    Assert.assertFalse(
        DataNodeTransportFailureUtils.isTransportException(
            new TTransportException(TTransportException.CORRUPTED_DATA, "invalid frame")));
  }

  @Test
  public void metadataRpcHandlersShouldMarkOnlyTransportFailures() {
    final TDataNodeLocation target = new TDataNodeLocation().setDataNodeId(1);
    final Map<Integer, TDataNodeLocation> targetMap = Collections.singletonMap(1, target);
    for (final Exception exception :
        new Exception[] {
          new ConnectException("connection refused"),
          new TimeoutException("request timed out"),
          new TTransportException(TTransportException.END_OF_FILE, "connection closed"),
          new IllegalStateException("local dispatch failure"),
          new TApplicationException("remote application failure"),
          new TProtocolException("invalid reply"),
          new TTransportException(TTransportException.CORRUPTED_DATA, "invalid frame")
        }) {
      final Map<Integer, TPushPipeMetaResp> pipeResponses = new HashMap<>();
      new PipePushMetaRPCHandler(
              CnToDnAsyncRequestType.PIPE_PUSH_MULTI_META,
              1,
              target,
              targetMap,
              pipeResponses,
              new CountDownLatch(1))
          .onError(exception);
      final boolean expectedTransportFailure =
          exception instanceof ConnectException
              || exception instanceof TimeoutException
              || (exception instanceof TTransportException
                  && ((TTransportException) exception).getType()
                      == TTransportException.END_OF_FILE);
      assertEquals(
          expectedTransportFailure,
          DataNodeTransportFailureUtils.isTransportFailure(pipeResponses.get(1).getStatus()));
      assertEquals(
          TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(),
          pipeResponses.get(1).getStatus().getCode());

      final Map<Integer, TPushConsumerGroupMetaResp> consumerGroupResponses = new HashMap<>();
      new ConsumerGroupPushMetaRPCHandler(
              CnToDnAsyncRequestType.CONSUMER_GROUP_PUSH_SINGLE_META,
              1,
              target,
              targetMap,
              consumerGroupResponses,
              new CountDownLatch(1))
          .onError(exception);
      assertEquals(
          expectedTransportFailure,
          DataNodeTransportFailureUtils.isTransportFailure(
              consumerGroupResponses.get(1).getStatus()));
      assertEquals(
          TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode(),
          consumerGroupResponses.get(1).getStatus().getCode());
    }
  }

  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put("k1", "v1");
    topicAttributes.put("k2", "v2");

    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put("k3", "v3");
    consumerAttributes.put("k4", "v4");

    Set<String> unsubscribeTopics = new HashSet<>();
    unsubscribeTopics.add("test_topic1");
    unsubscribeTopics.add("test_topic2");

    DropSubscriptionProcedure proc =
        new DropSubscriptionProcedure(
            new TUnsubscribeReq("old_consumer", "test_consumer_group", unsubscribeTopics));

    ConsumerGroupMeta newConsumerGroupMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));
    newConsumerGroupMeta.addSubscription("old_consumer", unsubscribeTopics);
    AlterConsumerGroupProcedure alterConsumerGroupProcedure =
        new AlterConsumerGroupProcedure(newConsumerGroupMeta);

    List<DropPipeProcedureV2> pipeProcedures = new ArrayList<>();
    pipeProcedures.add(new DropPipeProcedureV2("pipe_topic1"));
    pipeProcedures.add(new DropPipeProcedureV2("pipe_topic2"));

    proc.setAlterConsumerGroupProcedure(alterConsumerGroupProcedure);
    proc.setDropPipeProcedures(pipeProcedures);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      DropSubscriptionProcedure proc2 =
          (DropSubscriptionProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(alterConsumerGroupProcedure, proc2.getAlterConsumerGroupProcedure());
      assertEquals(pipeProcedures, proc2.getDropPipeProcedures());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void executeFromOperateOnConfigNodesShouldFailOnTopLevelConsensusError() throws Exception {
    final DropSubscriptionProcedure proc =
        new DropSubscriptionProcedure(
            new TUnsubscribeReq(
                "old_consumer", "test_consumer_group", Collections.singleton("test_topic")));
    proc.setAlterConsumerGroupProcedure(Mockito.mock(AlterConsumerGroupProcedure.class));

    final DropPipeProcedureV2 dropPipeProcedure = Mockito.mock(DropPipeProcedureV2.class);
    Mockito.when(dropPipeProcedure.getPipeName()).thenReturn("pipe_topic");
    proc.setDropPipeProcedures(Collections.singletonList(dropPipeProcedure));

    try {
      proc.executeFromOperateOnConfigNodes(
          mockConsensusFailureEnv(
              new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                  .setMessage("consensus write failed")));
      fail();
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to drop subscription"));
    }
  }

  @Test
  public void executeFromOperateOnDataNodesShouldSkipEmptyPipeMetaSync() throws Exception {
    final DropSubscriptionProcedure procedure =
        new DropSubscriptionProcedure(
            new TUnsubscribeReq("old_consumer", "test_consumer_group", Collections.emptySet()));
    final AlterConsumerGroupProcedure alterConsumerGroupProcedure =
        Mockito.mock(AlterConsumerGroupProcedure.class);
    procedure.setAlterConsumerGroupProcedure(alterConsumerGroupProcedure);
    procedure.setDropPipeProcedures(Collections.emptyList());

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);

    procedure.executeFromOperateOnDataNodes(env);

    Mockito.verify(env, Mockito.never()).pushMultiPipeMetaToDataNodes(Mockito.anyList());
    Mockito.verify(env, Mockito.never()).dropMultiPipeOnDataNodes(Mockito.anyList());
    Mockito.verify(alterConsumerGroupProcedure).executeFromOperateOnDataNodesForUnsubscribe(env);
  }

  @Test
  public void executeFromOperateOnDataNodesShouldIgnoreOfflinePipeTransportFailure()
      throws Exception {
    final DropSubscriptionProcedure procedure = new DropSubscriptionProcedure();
    final AlterConsumerGroupProcedure alterConsumerGroupProcedure =
        Mockito.mock(AlterConsumerGroupProcedure.class);
    procedure.setAlterConsumerGroupProcedure(alterConsumerGroupProcedure);
    procedure.setDropPipeProcedures(Collections.singletonList(mockDropPipeProcedure("pipe")));

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Unknown);
    Mockito.when(env.dropMultiPipeOnDataNodes(Mockito.anyList()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                    transportFailureStatus(
                        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), "Connection refused"))));

    procedure.executeFromOperateOnDataNodes(env);

    Mockito.verify(alterConsumerGroupProcedure).executeFromOperateOnDataNodesForUnsubscribe(env);
  }

  @Test
  public void executeFromOperateOnDataNodesShouldNotIgnoreOfflineUnmarkedError() throws Exception {
    final DropSubscriptionProcedure procedure = new DropSubscriptionProcedure();
    procedure.setAlterConsumerGroupProcedure(Mockito.mock(AlterConsumerGroupProcedure.class));
    procedure.setDropPipeProcedures(Collections.singletonList(mockDropPipeProcedure("pipe")));

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Unknown);
    Mockito.when(env.dropMultiPipeOnDataNodes(Mockito.anyList()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                    new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode())
                        .setMessage("local dispatch failure"))));

    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected an unmarked request failure to fail the unsubscribe");
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("local dispatch failure"));
    }
  }

  @Test
  public void executeFromOperateOnDataNodesShouldNotIgnoreOfflinePipeMetaRejection()
      throws Exception {
    final DropSubscriptionProcedure procedure = new DropSubscriptionProcedure();
    final AlterConsumerGroupProcedure alterConsumerGroupProcedure =
        Mockito.mock(AlterConsumerGroupProcedure.class);
    procedure.setAlterConsumerGroupProcedure(alterConsumerGroupProcedure);
    procedure.setDropPipeProcedures(Collections.singletonList(mockDropPipeProcedure("pipe")));

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Unknown);
    Mockito.when(env.dropMultiPipeOnDataNodes(Mockito.anyList()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                        new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
                    .setExceptionMessages(
                        Collections.singletonList(
                            new TPushPipeMetaRespExceptionMessage(
                                "pipe", "metadata rejected", System.currentTimeMillis())))));

    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected the DataNode's metadata rejection to fail the unsubscribe");
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("metadata rejected"));
    }

    Mockito.verify(alterConsumerGroupProcedure, Mockito.never())
        .executeFromOperateOnDataNodesForUnsubscribe(Mockito.any());
  }

  @Test
  public void executeFromOperateOnDataNodesShouldNotIgnoreOnlinePipeTransportFailure()
      throws Exception {
    final DropSubscriptionProcedure procedure = new DropSubscriptionProcedure();
    final AlterConsumerGroupProcedure alterConsumerGroupProcedure =
        Mockito.mock(AlterConsumerGroupProcedure.class);
    procedure.setAlterConsumerGroupProcedure(alterConsumerGroupProcedure);
    procedure.setDropPipeProcedures(Collections.singletonList(mockDropPipeProcedure("pipe")));

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Running);
    Mockito.when(env.dropMultiPipeOnDataNodes(Mockito.anyList()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                    transportFailureStatus(
                        TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode(), "Connection refused"))));

    try {
      procedure.executeFromOperateOnDataNodes(env);
      fail("Expected a transport failure from an online DataNode to fail the unsubscribe");
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("Connection refused"));
    }

    Mockito.verify(alterConsumerGroupProcedure, Mockito.never())
        .executeFromOperateOnDataNodesForUnsubscribe(Mockito.any());
  }

  @Test
  public void alterConsumerGroupShouldIgnoreOfflineTransportFailureWhenUnsubscribing()
      throws Exception {
    final ConsumerGroupMeta existingMeta =
        new ConsumerGroupMeta("group", 1, new ConsumerMeta("consumer", 1, Collections.emptyMap()));
    existingMeta.addSubscription("consumer", Collections.singleton("topic"));
    final ConsumerGroupMeta updatedMeta = existingMeta.deepCopy();
    updatedMeta.removeSubscription("consumer", Collections.singleton("topic"));
    final AlterConsumerGroupProcedure procedure = new AlterConsumerGroupProcedure();
    procedure.setExistingConsumerGroupMeta(existingMeta);
    procedure.setUpdatedConsumerGroupMeta(updatedMeta);

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Unknown);
    final ConfigManager configManager = env.getConfigManager();
    final SubscriptionManager subscriptionManager = Mockito.mock(SubscriptionManager.class);
    final SubscriptionReadyDataNodeTracker readyDataNodeTracker =
        new SubscriptionReadyDataNodeTracker();
    Mockito.when(configManager.getSubscriptionManager()).thenReturn(subscriptionManager);
    Mockito.when(subscriptionManager.getSubscriptionReadyDataNodeTracker())
        .thenReturn(readyDataNodeTracker);
    readyDataNodeTracker.replaceFromFullPush(
        Collections.singletonList(existingMeta),
        Collections.singletonMap(
            1,
            new TPushConsumerGroupMetaResp(
                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))));
    Mockito.when(env.pushSingleConsumerGroupOnDataNode(Mockito.any(ByteBuffer.class)))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushConsumerGroupMetaResp(
                    transportFailureStatus(
                        TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode(),
                        "Connection refused"))));

    procedure.executeFromOperateOnDataNodesForUnsubscribe(env);
    procedure.rollbackFromOperateOnDataNodesForUnsubscribe(env);

    assertEquals(
        Collections.emptyList(),
        readyDataNodeTracker.getReadyDataNodeIds(
            "topic", "group", existingMeta.visibleUnder(true), Collections.singleton(1)));
  }

  @Test
  public void alterConsumerGroupShouldNotIgnoreOfflineUnmarkedErrorWhenUnsubscribing()
      throws Exception {
    final ConsumerGroupMeta existingMeta =
        new ConsumerGroupMeta("group", 1, new ConsumerMeta("consumer", 1, Collections.emptyMap()));
    final AlterConsumerGroupProcedure procedure = new AlterConsumerGroupProcedure();
    procedure.setExistingConsumerGroupMeta(existingMeta);
    procedure.setUpdatedConsumerGroupMeta(existingMeta.deepCopy());

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Unknown);
    final SubscriptionManager subscriptionManager = Mockito.mock(SubscriptionManager.class);
    Mockito.when(env.getConfigManager().getSubscriptionManager()).thenReturn(subscriptionManager);
    Mockito.when(subscriptionManager.getSubscriptionReadyDataNodeTracker())
        .thenReturn(Mockito.mock(SubscriptionReadyDataNodeTracker.class));
    Mockito.when(env.pushSingleConsumerGroupOnDataNode(Mockito.any(ByteBuffer.class)))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushConsumerGroupMetaResp(
                    new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode())
                        .setMessage("local dispatch failure"))));

    try {
      procedure.executeFromOperateOnDataNodesForUnsubscribe(env);
      fail("Expected an unmarked request failure to fail the consumer group update");
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("local dispatch failure"));
    }
    Assert.assertThrows(
        SubscriptionException.class,
        () -> procedure.rollbackFromOperateOnDataNodesForUnsubscribe(env));
  }

  @Test
  public void alterConsumerGroupShouldNotIgnoreOnlineTransportFailureWhenUnsubscribing()
      throws Exception {
    final ConsumerGroupMeta existingMeta =
        new ConsumerGroupMeta("group", 1, new ConsumerMeta("consumer", 1, Collections.emptyMap()));
    final AlterConsumerGroupProcedure procedure = new AlterConsumerGroupProcedure();
    procedure.setExistingConsumerGroupMeta(existingMeta);
    procedure.setUpdatedConsumerGroupMeta(existingMeta.deepCopy());

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Running);
    final SubscriptionManager subscriptionManager = Mockito.mock(SubscriptionManager.class);
    Mockito.when(env.getConfigManager().getSubscriptionManager()).thenReturn(subscriptionManager);
    Mockito.when(subscriptionManager.getSubscriptionReadyDataNodeTracker())
        .thenReturn(Mockito.mock(SubscriptionReadyDataNodeTracker.class));
    Mockito.when(env.pushSingleConsumerGroupOnDataNode(Mockito.any(ByteBuffer.class)))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushConsumerGroupMetaResp(
                    transportFailureStatus(
                        TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode(),
                        "Connection refused"))));

    try {
      procedure.executeFromOperateOnDataNodesForUnsubscribe(env);
      fail("Expected a transport failure from an online DataNode to fail the update");
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("Connection refused"));
    }
    Assert.assertThrows(
        SubscriptionException.class,
        () -> procedure.rollbackFromOperateOnDataNodesForUnsubscribe(env));
  }

  @Test
  public void alterConsumerGroupShouldNotIgnoreOfflineMetadataRejectionWhenUnsubscribing()
      throws Exception {
    final ConsumerGroupMeta existingMeta =
        new ConsumerGroupMeta("group", 1, new ConsumerMeta("consumer", 1, Collections.emptyMap()));
    final ConsumerGroupMeta updatedMeta = existingMeta.deepCopy();
    final AlterConsumerGroupProcedure procedure = new AlterConsumerGroupProcedure();
    procedure.setExistingConsumerGroupMeta(existingMeta);
    procedure.setUpdatedConsumerGroupMeta(updatedMeta);

    final ConfigNodeProcedureEnv env = mockEnvWithNodeStatus(NodeStatus.Unknown);
    final ConfigManager configManager = env.getConfigManager();
    final SubscriptionManager subscriptionManager = Mockito.mock(SubscriptionManager.class);
    Mockito.when(configManager.getSubscriptionManager()).thenReturn(subscriptionManager);
    Mockito.when(subscriptionManager.getSubscriptionReadyDataNodeTracker())
        .thenReturn(Mockito.mock(SubscriptionReadyDataNodeTracker.class));
    Mockito.when(env.pushSingleConsumerGroupOnDataNode(Mockito.any(ByteBuffer.class)))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushConsumerGroupMetaResp(
                        new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()))
                    .setExceptionMessages(
                        Collections.singletonList(
                            new TPushConsumerGroupMetaRespExceptionMessage(
                                "group", "metadata rejected", System.currentTimeMillis())))));

    try {
      procedure.executeFromOperateOnDataNodesForUnsubscribe(env);
      fail("Expected the DataNode's metadata rejection to fail the consumer group update");
    } catch (SubscriptionException e) {
      Assert.assertNotNull(e.getMessage());
    }
    Assert.assertThrows(
        SubscriptionException.class,
        () -> procedure.rollbackFromOperateOnDataNodesForUnsubscribe(env));
  }

  @Test
  public void executeFromValidateShouldResetDropPipeProceduresOnRetry() throws Exception {
    final Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put("username", "user");
    consumerAttributes.put("password", "password");

    final ConsumerGroupMeta consumerGroupMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));
    consumerGroupMeta.addSubscription("old_consumer", Collections.singleton("test_topic"));

    final SubscriptionInfo subscriptionInfo = Mockito.mock(SubscriptionInfo.class);
    Mockito.when(subscriptionInfo.getConsumerGroupMeta("test_consumer_group"))
        .thenReturn(consumerGroupMeta);
    Mockito.when(subscriptionInfo.deepCopyConsumerGroupMeta("test_consumer_group"))
        .thenAnswer(invocation -> consumerGroupMeta.deepCopy());
    Mockito.when(subscriptionInfo.deepCopyTopicMeta("test_topic", false))
        .thenReturn(new TopicMeta("test_topic", 1, Collections.emptyMap()));

    final PipeTaskInfo pipeTaskInfo = Mockito.mock(PipeTaskInfo.class);

    final DropSubscriptionProcedure proc =
        new DropSubscriptionProcedure(
            new TUnsubscribeReq(
                "old_consumer", "test_consumer_group", Collections.singleton("test_topic")));
    setField(proc, "subscriptionInfo", new AtomicReference<>(subscriptionInfo));
    setField(proc, "pipeTaskInfo", new AtomicReference<>(pipeTaskInfo));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    proc.executeFromValidate(env);
    Assert.assertEquals(1, proc.getDropPipeProcedures().size());

    proc.executeFromValidate(env);
    Assert.assertEquals(1, proc.getDropPipeProcedures().size());
  }

  @Test
  public void executeFromValidateShouldRetainInitialPipeWhenConfigured() throws Exception {
    for (final String mode :
        new String[] {TopicConstant.MODE_INITIAL_VALUE, TopicConstant.MODE_SNAPSHOT_VALUE}) {
      final ConsumerMeta consumerMeta = new ConsumerMeta("old_consumer", 1, Collections.emptyMap());
      final ConsumerGroupMeta consumerGroupMeta =
          new ConsumerGroupMeta("test_consumer_group", 1, consumerMeta);
      consumerGroupMeta.addSubscription("old_consumer", Collections.singleton("test_topic"));

      final Map<String, String> topicAttributes = new HashMap<>();
      topicAttributes.put(TopicConstant.MODE_KEY, mode);
      topicAttributes.put(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, "true");

      final SubscriptionInfo subscriptionInfo = Mockito.mock(SubscriptionInfo.class);
      Mockito.when(subscriptionInfo.deepCopyConsumerGroupMeta("test_consumer_group"))
          .thenAnswer(invocation -> consumerGroupMeta.deepCopy());
      Mockito.when(subscriptionInfo.getConsumerGroupMeta("test_consumer_group"))
          .thenReturn(consumerGroupMeta);
      Mockito.when(subscriptionInfo.deepCopyTopicMeta("test_topic", false))
          .thenReturn(new TopicMeta("test_topic", 1, topicAttributes));

      final PipeTaskInfo pipeTaskInfo = Mockito.mock(PipeTaskInfo.class);
      final DropSubscriptionProcedure procedure =
          new DropSubscriptionProcedure(
              new TUnsubscribeReq(
                  "old_consumer", "test_consumer_group", Collections.singleton("test_topic")));
      setField(procedure, "subscriptionInfo", new AtomicReference<>(subscriptionInfo));
      setField(procedure, "pipeTaskInfo", new AtomicReference<>(pipeTaskInfo));

      procedure.executeFromValidate(Mockito.mock(ConfigNodeProcedureEnv.class));

      Assert.assertTrue(procedure.getDropPipeProcedures().isEmpty());
      Mockito.verify(pipeTaskInfo, Mockito.never()).checkBeforeDropPipe(Mockito.anyString());
    }
  }

  private static ConfigNodeProcedureEnv mockConsensusFailureEnv(final TSStatus response)
      throws Exception {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);

    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any())).thenReturn(response);

    return env;
  }

  private static ConfigNodeProcedureEnv mockEnvWithNodeStatus(final NodeStatus nodeStatus) {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(loadManager.getNodeStatus(1)).thenReturn(nodeStatus);
    return env;
  }

  private static DropPipeProcedureV2 mockDropPipeProcedure(final String pipeName) {
    final DropPipeProcedureV2 procedure = Mockito.mock(DropPipeProcedureV2.class);
    Mockito.when(procedure.getPipeName()).thenReturn(pipeName);
    return procedure;
  }

  private static TSStatus transportFailureStatus(final int statusCode, final String message) {
    return DataNodeTransportFailureUtils.markTransportFailure(
        new TSStatus(statusCode).setMessage(message));
  }

  private static void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        final Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
