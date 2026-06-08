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
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
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

public class DropSubscriptionProcedureTest {
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
