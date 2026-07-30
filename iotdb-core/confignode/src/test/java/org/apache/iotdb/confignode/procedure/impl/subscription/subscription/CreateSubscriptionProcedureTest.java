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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.plugin.PipePluginCoordinator;
import org.apache.iotdb.confignode.persistence.pipe.PipePluginInfo;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateSubscriptionProcedureTest {
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

    Set<String> subscribeTopics = new HashSet<>();
    subscribeTopics.add("test_topic1");
    subscribeTopics.add("test_topic2");

    CreateSubscriptionProcedure proc =
        new CreateSubscriptionProcedure(
            new TSubscribeReq("old_consumer", "test_consumer_group", subscribeTopics));

    ConsumerGroupMeta newConsumerGroupMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));
    newConsumerGroupMeta.addSubscription("old_consumer", subscribeTopics);
    AlterConsumerGroupProcedure alterConsumerGroupProcedure =
        new AlterConsumerGroupProcedure(newConsumerGroupMeta);

    List<CreatePipeProcedureV2> pipeProcedures = new ArrayList<>();
    pipeProcedures.add(
        new CreatePipeProcedureV2(
            new TCreatePipeReq("pipe_topic1", Collections.singletonMap("connector", "conn"))
                .setExtractorAttributes(Collections.singletonMap("extractor", "ex"))
                .setProcessorAttributes(Collections.singletonMap("processor", "pro"))));
    pipeProcedures.add(
        new CreatePipeProcedureV2(
            new TCreatePipeReq("pipe_topic2", Collections.singletonMap("connector", "conn"))
                .setExtractorAttributes(Collections.singletonMap("extractor", "ex"))
                .setProcessorAttributes(Collections.singletonMap("processor", "pro"))));

    proc.setAlterConsumerGroupProcedure(alterConsumerGroupProcedure);
    proc.setCreatePipeProcedures(pipeProcedures);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreateSubscriptionProcedure proc2 =
          (CreateSubscriptionProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(alterConsumerGroupProcedure, proc2.getAlterConsumerGroupProcedure());
      assertEquals(pipeProcedures, proc2.getCreatePipeProcedures());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void executeFromOperateOnConfigNodesShouldFailOnTopLevelConsensusError() throws Exception {
    final CreateSubscriptionProcedure proc =
        new CreateSubscriptionProcedure(
            new TSubscribeReq(
                "old_consumer", "test_consumer_group", Collections.singleton("test_topic")));
    proc.setAlterConsumerGroupProcedure(Mockito.mock(AlterConsumerGroupProcedure.class));

    final CreatePipeProcedureV2 createPipeProcedure = Mockito.mock(CreatePipeProcedureV2.class);
    Mockito.when(createPipeProcedure.constructPlan())
        .thenReturn(Mockito.mock(CreatePipePlanV2.class));
    proc.setCreatePipeProcedures(Collections.singletonList(createPipeProcedure));

    try {
      proc.executeFromOperateOnConfigNodes(
          mockConsensusFailureEnv(
              new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                  .setMessage("consensus write failed")));
      fail();
    } catch (SubscriptionException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to create subscription"));
    }
  }

  @Test
  public void executeFromValidateShouldResetCreatePipeProceduresOnRetry() throws Exception {
    final Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put("username", "user");
    consumerAttributes.put("password", "password");

    final ConsumerGroupMeta consumerGroupMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, Collections.emptyMap());

    final SubscriptionInfo subscriptionInfo = Mockito.mock(SubscriptionInfo.class);
    Mockito.when(subscriptionInfo.getConsumerGroupMeta("test_consumer_group"))
        .thenReturn(consumerGroupMeta);
    Mockito.when(subscriptionInfo.deepCopyConsumerGroupMeta("test_consumer_group"))
        .thenAnswer(invocation -> consumerGroupMeta.deepCopy());
    Mockito.when(
            subscriptionInfo.isTopicSubscribedByConsumerGroup("test_topic", "test_consumer_group"))
        .thenReturn(false);
    Mockito.when(subscriptionInfo.deepCopyTopicMeta("test_topic")).thenReturn(topicMeta);

    final PipeTaskInfo pipeTaskInfo = Mockito.mock(PipeTaskInfo.class);
    Mockito.when(pipeTaskInfo.checkBeforeCreatePipe(Mockito.any(TCreatePipeReq.class)))
        .thenReturn(true);

    final CreateSubscriptionProcedure proc =
        new CreateSubscriptionProcedure(
            new TSubscribeReq(
                "old_consumer", "test_consumer_group", Collections.singleton("test_topic")));
    setField(proc, "subscriptionInfo", new AtomicReference<>(subscriptionInfo));
    setField(proc, "pipeTaskInfo", new AtomicReference<>(pipeTaskInfo));

    final ConfigNodeProcedureEnv env = mockCreateSubscriptionValidationEnv();
    proc.executeFromValidate(env);
    Assert.assertEquals(1, proc.getCreatePipeProcedures().size());

    proc.executeFromValidate(env);
    Assert.assertEquals(1, proc.getCreatePipeProcedures().size());
  }

  @Test
  public void newlySubscribedConsensusTopicsShouldBeBasedOnGroupTopics() {
    final ConsumerMeta firstConsumer =
        new ConsumerMeta("first_consumer", 1, Collections.emptyMap());
    final ConsumerGroupMeta existingConsumerGroupMeta =
        new ConsumerGroupMeta("test_consumer_group", 1, firstConsumer);
    existingConsumerGroupMeta.addSubscription(
        firstConsumer.getConsumerId(), Collections.singleton("existing_consensus_topic"));

    final ConsumerMeta secondConsumer =
        new ConsumerMeta("second_consumer", 2, Collections.emptyMap());
    final ConsumerGroupMeta consumerAddedMeta = existingConsumerGroupMeta.deepCopy();
    consumerAddedMeta.addConsumer(secondConsumer);
    consumerAddedMeta.addSubscription(
        secondConsumer.getConsumerId(), Collections.singleton("existing_consensus_topic"));

    final Set<String> consensusTopicNames = new HashSet<>();
    consensusTopicNames.add("existing_consensus_topic");
    consensusTopicNames.add("new_consensus_topic");

    Assert.assertTrue(
        CreateSubscriptionProcedure.getNewlySubscribedConsensusTopicNames(
                existingConsumerGroupMeta, consumerAddedMeta, consensusTopicNames)
            .isEmpty());

    final ConsumerGroupMeta topicAddedMeta = consumerAddedMeta.deepCopy();
    final Set<String> newlySubscribedTopics = new HashSet<>();
    newlySubscribedTopics.add("new_consensus_topic");
    newlySubscribedTopics.add("new_pipe_topic");
    topicAddedMeta.addSubscription(secondConsumer.getConsumerId(), newlySubscribedTopics);

    assertEquals(
        Collections.singleton("new_consensus_topic"),
        CreateSubscriptionProcedure.getNewlySubscribedConsensusTopicNames(
            existingConsumerGroupMeta, topicAddedMeta, consensusTopicNames));
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

  private static ConfigNodeProcedureEnv mockCreateSubscriptionValidationEnv() {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final PermissionManager permissionManager = Mockito.mock(PermissionManager.class);
    final PipeManager pipeManager = Mockito.mock(PipeManager.class);
    final PipePluginCoordinator pipePluginCoordinator = Mockito.mock(PipePluginCoordinator.class);
    final PipePluginInfo pipePluginInfo = Mockito.mock(PipePluginInfo.class);
    final LoadManager loadManager = Mockito.mock(LoadManager.class);

    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getPermissionManager()).thenReturn(permissionManager);
    Mockito.when(configManager.getPipeManager()).thenReturn(pipeManager);
    Mockito.when(pipeManager.getPipePluginCoordinator()).thenReturn(pipePluginCoordinator);
    Mockito.when(pipePluginCoordinator.getPipePluginInfo()).thenReturn(pipePluginInfo);
    Mockito.when(configManager.getLoadManager()).thenReturn(loadManager);
    Mockito.when(loadManager.getRegionLeaderMap()).thenReturn(Collections.emptyMap());
    Mockito.when(permissionManager.login4Pipe(Mockito.anyString(), Mockito.any()))
        .thenReturn("hashedPassword");

    return env;
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
