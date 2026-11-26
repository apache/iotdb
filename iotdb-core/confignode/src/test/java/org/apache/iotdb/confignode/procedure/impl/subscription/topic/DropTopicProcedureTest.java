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

package org.apache.iotdb.confignode.procedure.impl.subscription.topic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.consumer.CommitProgressKeeper;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.CommitProgressHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DropTopicProcedureTest {

  @Test
  public void testTopicSnapshotSurvivesRecoveryWithoutRetainedGroups() throws Exception {
    for (final boolean retain : new boolean[] {false, true}) {
      final SubscriptionInfo subscriptions = new SubscriptionInfo();
      final TopicMeta topic =
          new TopicMeta(
              "topic",
              1L,
              Map.of(
                  TopicConstant.MODE_KEY,
                  TopicConstant.MODE_INITIAL_VALUE,
                  TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY,
                  String.valueOf(retain)));
      subscriptions.createTopic(new CreateTopicPlan(topic));
      final DropTopicProcedure procedure = new DropTopicProcedure("topic", false);
      setField(procedure, "subscriptionInfo", new AtomicReference<>(subscriptions));
      procedure.executeFromValidate(null);
      final PublicBAOS bytes = new PublicBAOS();
      procedure.serialize(new DataOutputStream(bytes));
      final ByteBuffer buffer = ByteBuffer.wrap(bytes.getBuf(), 0, bytes.size());
      final DropTopicProcedure recovered =
          (DropTopicProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(0, buffer.remaining());
      assertEquals(procedure, recovered);
      subscriptions.dropTopic(new DropTopicPlan("topic", false));
      assertNull(subscriptions.deepCopyTopicMeta("topic", false));
      final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
      final ConfigManager manager = Mockito.mock(ConfigManager.class);
      final ConsensusManager consensus = Mockito.mock(ConsensusManager.class);
      Mockito.when(env.getConfigManager()).thenReturn(manager);
      Mockito.when(manager.getConsensusManager()).thenReturn(consensus);
      Mockito.when(consensus.write(Mockito.any(CreateTopicPlan.class)))
          .thenAnswer(invocation -> subscriptions.createTopic(invocation.getArgument(0)));
      recovered.rollbackFromOperateOnConfigNodes(env);
      assertEquals(topic, subscriptions.deepCopyTopicMeta("topic", false));
    }
  }

  @Test
  public void testRetainedRuntimeCleanupAndRollbackAreModelScoped() throws Exception {
    for (final boolean dropTopic : new boolean[] {false, true}) {
      for (final boolean tableModel : new boolean[] {false, true}) {
        for (final String mode :
            new String[] {TopicConstant.MODE_INITIAL_VALUE, TopicConstant.MODE_SNAPSHOT_VALUE}) {
          assertRetainedRuntimeCleanupAndRollback(dropTopic, tableModel, mode);
        }
      }
    }
  }

  private void assertRetainedRuntimeCleanupAndRollback(
      final boolean dropTopic, final boolean tableModel, final String mode) throws Exception {
    final String topicName = "retained_topic";
    final SubscriptionInfo subscriptions = new SubscriptionInfo();
    final PipeTaskInfo pipes = new PipeTaskInfo();
    for (final boolean model : new boolean[] {false, true}) {
      final String dialect = model ? "table" : "tree";
      final Map<String, String> attributes = new HashMap<>();
      attributes.put(SystemConstant.SQL_DIALECT_KEY, dialect);
      attributes.put(TopicConstant.MODE_KEY, mode);
      attributes.put(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, "true");
      subscriptions.createTopic(new CreateTopicPlan(new TopicMeta(topicName, 1L, attributes)));
      final String groupId = "retained_group_" + model;
      final ConsumerGroupMeta group =
          new ConsumerGroupMeta(
              groupId,
              1L,
              new ConsumerMeta(
                  "consumer",
                  1L,
                  Collections.singletonMap(ConsumerConstant.SQL_DIALECT_KEY, dialect)));
      group.retainTopic(topicName);
      group.removeConsumer("consumer");
      subscriptions.alterConsumerGroup(new AlterConsumerGroupPlan(group));
      final PipeRuntimeMeta runtime = new PipeRuntimeMeta();
      runtime.getStatus().set(PipeStatus.RUNNING);
      runtime
          .getConsensusGroupId2TaskMetaMap()
          .put(1, new PipeTaskMeta(new SimpleProgressIndex(0, 42L), 1));
      pipes.createPipe(
          new CreatePipePlanV2(
              new PipeStaticMeta(
                  PipeStaticMeta.generateSubscriptionPipeName(topicName, groupId),
                  1L,
                  Collections.singletonMap(SystemConstant.SQL_DIALECT_KEY, dialect),
                  Collections.emptyMap(),
                  Collections.emptyMap()),
              runtime));
      final String regionId = "DataRegion[1]";
      subscriptions
          .getCommitProgressKeeper()
          .updateRegionProgress(
              CommitProgressKeeper.generateKey(groupId, topicName, regionId, 1),
              ByteBuffer.wrap(new byte[] {(byte) (model ? 2 : 1)}));
      subscriptions
          .getCommitProgressKeeper()
          .updateRegionProgress(
              CommitProgressKeeper.generateKey(groupId, "other_topic", regionId, 1),
              ByteBuffer.wrap(new byte[] {(byte) (model ? 4 : 3)}));
    }
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager manager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensus = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(manager);
    Mockito.when(manager.getConsensusManager()).thenReturn(consensus);
    Mockito.when(consensus.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenAnswer(
            invocation -> {
              final ConfigPhysicalPlan plan = invocation.getArgument(0);
              if (plan instanceof CreatePipePlanV2) {
                return pipes.createPipe((CreatePipePlanV2) plan);
              } else if (plan instanceof OperateMultiplePipesPlanV2) {
                return pipes.operateMultiplePipes((OperateMultiplePipesPlanV2) plan);
              } else if (plan instanceof AlterConsumerGroupPlan) {
                return subscriptions.alterConsumerGroup((AlterConsumerGroupPlan) plan);
              } else if (plan instanceof DropTopicPlan) {
                return subscriptions.dropTopic((DropTopicPlan) plan);
              } else if (plan instanceof CreateTopicPlan) {
                return subscriptions.createTopic((CreateTopicPlan) plan);
              } else if (plan instanceof AlterTopicPlan) {
                return subscriptions.alterTopic((AlterTopicPlan) plan);
              } else if (plan instanceof CommitProgressHandleMetaChangePlan) {
                return subscriptions.handleCommitProgressChanges(
                    (CommitProgressHandleMetaChangePlan) plan);
              }
              return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
            });

    final String groupId = "retained_group_" + tableModel;
    final String pipeName = PipeStaticMeta.generateSubscriptionPipeName(topicName, groupId);
    final String progressKey =
        CommitProgressKeeper.generateKey(groupId, topicName, "DataRegion[1]", 1);
    final String otherProgressKey =
        CommitProgressKeeper.generateKey(groupId, "other_topic", "DataRegion[1]", 1);
    final ByteBuffer originalProgress =
        subscriptions.getCommitProgressKeeper().getRegionProgress(progressKey);
    final ByteBuffer unrelatedProgress =
        subscriptions.getCommitProgressKeeper().getRegionProgress(otherProgressKey);
    final PipeMeta originalPipe = pipes.getPipeMetaByPipeName(pipeName, tableModel);
    final DropTopicProcedure drop = new DropTopicProcedure(topicName, tableModel);
    final AlterTopicProcedure alter =
        new AlterTopicProcedure(
            subscriptions.deepCopyTopicMetaWithUpdatedAttributes(
                topicName,
                Collections.singletonMap(
                    TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, "false"),
                tableModel));
    final Object procedure = dropTopic ? drop : alter;
    setField(procedure, "subscriptionInfo", new AtomicReference<>(subscriptions));
    setField(procedure, "pipeTaskInfo", new AtomicReference<>(pipes));
    if (dropTopic) {
      drop.executeFromValidate(env);
    } else {
      alter.executeFromValidate(env);
    }
    final PublicBAOS bytes = new PublicBAOS();
    if (dropTopic) {
      drop.serialize(new DataOutputStream(bytes));
    } else {
      alter.serialize(new DataOutputStream(bytes));
    }
    final ByteBuffer buffer = ByteBuffer.wrap(bytes.getBuf(), 0, bytes.size());
    final Object recovered = ProcedureFactory.getInstance().create(buffer);
    assertEquals(0, buffer.remaining());
    assertEquals(procedure, recovered);
    setField(recovered, "subscriptionInfo", new AtomicReference<>(subscriptions));
    setField(recovered, "pipeTaskInfo", new AtomicReference<>(pipes));
    if (dropTopic) {
      ((DropTopicProcedure) recovered).executeFromOperateOnConfigNodes(env);
    } else {
      ((AlterTopicProcedure) recovered).executeFromOperateOnConfigNodes(env);
    }
    assertFalse(pipes.isPipeExisted(pipeName, tableModel));
    assertNull(subscriptions.getCommitProgressKeeper().getRegionProgress(progressKey));
    assertTrue(subscriptions.getConsumerGroupsRetainingTopic(topicName, tableModel).isEmpty());
    assertEquals(1, subscriptions.getConsumerGroupsRetainingTopic(topicName, !tableModel).size());
    if (dropTopic) {
      ((DropTopicProcedure) recovered).rollbackFromOperateOnConfigNodes(env);
    } else {
      ((AlterTopicProcedure) recovered).rollbackFromOperateOnConfigNodes(env);
    }
    assertEquals(1, subscriptions.getConsumerGroupsRetainingTopic(topicName, tableModel).size());
    assertTrue(
        subscriptions
            .deepCopyTopicMeta(topicName, tableModel)
            .getConfig()
            .isProgressRetainedAfterUnsubscribe());
    assertEquals(
        originalPipe.getRuntimeMeta(),
        pipes.getPipeMetaByPipeName(pipeName, tableModel).getRuntimeMeta());
    assertEquals(
        originalProgress, subscriptions.getCommitProgressKeeper().getRegionProgress(progressKey));
    assertEquals(
        unrelatedProgress,
        subscriptions.getCommitProgressKeeper().getRegionProgress(otherProgressKey));
  }

  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    DropTopicProcedure proc = new DropTopicProcedure("test_topic");

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      DropTopicProcedure proc2 = (DropTopicProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void serializeDeserializeTableModelTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    DropTopicProcedure proc = new DropTopicProcedure("test_topic", true);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      DropTopicProcedure proc2 = (DropTopicProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void serializeDeserializeRetainedProgressStateTest() throws Exception {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.MODE_KEY, TopicConstant.MODE_INITIAL_VALUE);
    topicAttributes.put(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, "true");
    final TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);
    final ConsumerGroupMeta consumerGroupMeta =
        new ConsumerGroupMeta(
            "test_group", 1, new ConsumerMeta("consumer", 1, Collections.emptyMap()));
    consumerGroupMeta.retainTopic("test_topic");

    final DropTopicProcedure procedure = new DropTopicProcedure("test_topic", false);
    setField(procedure, "existedTopicMeta", topicMeta);
    final Field retainedConsumerGroupMetasField =
        DropTopicProcedure.class.getDeclaredField("retainedConsumerGroupMetas");
    retainedConsumerGroupMetasField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final List<ConsumerGroupMeta> retainedConsumerGroupMetas =
        (List<ConsumerGroupMeta>) retainedConsumerGroupMetasField.get(procedure);
    retainedConsumerGroupMetas.add(consumerGroupMeta);
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    procedure.serialize(new DataOutputStream(byteArrayOutputStream));
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final DropTopicProcedure deserializedProcedure =
        (DropTopicProcedure) ProcedureFactory.getInstance().create(buffer);

    assertEquals(procedure, deserializedProcedure);
    assertEquals(0, buffer.remaining());
  }

  private static void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        final Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (final NoSuchFieldException ignored) {
        // Look for inherited procedure state.
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
