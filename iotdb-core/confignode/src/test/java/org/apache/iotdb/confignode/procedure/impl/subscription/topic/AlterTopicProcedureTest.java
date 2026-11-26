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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AlterTopicProcedureTest {

  @Test
  public void serializeDeserializeTest() throws Exception {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put("path", "root.db1.**");
    assertSerializeDeserialize(
        new AlterTopicProcedure(new TopicMeta("test_topic", 1, topicAttributes)));
  }

  @Test
  public void serializeDeserializeWithUpdatedAttributesTest() throws Exception {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put("path", "root.db1.**");
    final Map<String, String> updatedTopicAttributes = new HashMap<>();
    updatedTopicAttributes.put("processor", "processor1");
    assertSerializeDeserialize(
        new AlterTopicProcedure(
            new TopicMeta("test_topic", 1, topicAttributes), updatedTopicAttributes));
  }

  @Test
  public void serializeDeserializeWithRetainedProgressStateTest() throws Exception {
    final AlterTopicProcedure procedure = new AlterTopicProcedure(false, true);
    assertSerializeDeserialize(procedure);
  }

  private void assertSerializeDeserialize(final AlterTopicProcedure procedure) throws Exception {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    procedure.serialize(outputStream);
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final AlterTopicProcedure deserializedProcedure =
        (AlterTopicProcedure) ProcedureFactory.getInstance().create(buffer);
    assertEquals(procedure, deserializedProcedure);
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testRebaseUpdatedAttributesDuringValidate() throws Exception {
    final String topicName = "test_topic";
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> initialAttributes = new HashMap<>();
    initialAttributes.put("path", "root.db1.**");
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo
            .createTopic(new CreateTopicPlan(new TopicMeta(topicName, 1, initialAttributes)))
            .getCode());

    final Map<String, String> requestAttributes = new HashMap<>();
    requestAttributes.put("processor", "processor1");
    final TopicMeta staleUpdatedTopicMeta =
        subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(topicName, requestAttributes);

    final Map<String, String> concurrentAttributes = new HashMap<>();
    concurrentAttributes.put("source", "source1");
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo
            .alterTopic(
                new AlterTopicPlan(
                    subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(
                        topicName, concurrentAttributes)))
            .getCode());

    final AlterTopicProcedure procedure =
        new AlterTopicProcedure(
            staleUpdatedTopicMeta, requestAttributes, new AtomicReference<>(subscriptionInfo));
    procedure.executeFromValidate(null);

    assertEquals("processor1", procedure.getUpdatedTopicMeta().getConfig().getString("processor"));
    assertEquals("source1", procedure.getUpdatedTopicMeta().getConfig().getString("source"));
  }

  @Test
  public void testRejectCaseInsensitiveDuplicateAttributesBeforeMerge() throws Exception {
    final String topicName = "test_table_topic";
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> initialAttributes = new HashMap<>();
    initialAttributes.put("__system.sql-dialect", "table");
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        subscriptionInfo
            .createTopic(new CreateTopicPlan(new TopicMeta(topicName, 1, initialAttributes)))
            .getCode());

    final Map<String, String> requestAttributes = new HashMap<>();
    requestAttributes.put(TopicConstant.TAG_FILTER_KEY, "region = \"north\"");
    requestAttributes.put("TAG-FILTER", "region = \"south\"");
    final AlterTopicProcedure procedure =
        new AlterTopicProcedure(
            subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(
                topicName, requestAttributes, true),
            requestAttributes,
            new AtomicReference<>(subscriptionInfo));

    try {
      procedure.executeFromValidate(null);
      fail("Expected duplicate tag-filter attributes to be rejected");
    } catch (final SubscriptionException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("duplicate tag-filter"));
    }
  }

  @Test
  public void testRealtimeModeCanChangeBeforeSubscriptionPipeCreation() throws Exception {
    final String topicName = "test_topic";
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = new HashMap<>();
    originalAttributes.put(TopicConstant.PATH_KEY, "root.**");
    originalAttributes.put(
        TopicConstant.REALTIME_MODE_KEY, TopicConstant.REALTIME_MODE_STREAM_VALUE);
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta(topicName, 1L, originalAttributes)));

    final Map<String, String> updatedAttributes =
        Map.of(TopicConstant.REALTIME_MODE_KEY, TopicConstant.REALTIME_MODE_BATCH_VALUE);
    final AlterTopicProcedure procedure =
        new AlterTopicProcedure(
            subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(topicName, updatedAttributes),
            updatedAttributes,
            new AtomicReference<>(subscriptionInfo));
    procedure.executeFromValidate(null);

    assertEquals(
        TopicConstant.REALTIME_MODE_BATCH_VALUE,
        procedure.getUpdatedTopicMeta().getConfig().getRealtimeMode());
  }

  @Test
  public void testRealtimeModeCannotChangeWhileSubscriptionPipeExists() throws Exception {
    final String topicName = "test_topic";
    final String consumerGroupId = "test_group";
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = new HashMap<>();
    originalAttributes.put(TopicConstant.PATH_KEY, "root.**");
    originalAttributes.put(
        TopicConstant.REALTIME_MODE_KEY, TopicConstant.REALTIME_MODE_STREAM_VALUE);
    final TopicMeta topicMeta = new TopicMeta(topicName, 1L, originalAttributes);
    subscriptionInfo.createTopic(new CreateTopicPlan(topicMeta));

    final PipeStaticMeta pipeStaticMeta =
        new PipeStaticMeta(
            PipeStaticMeta.generateSubscriptionPipeName(topicName, consumerGroupId),
            1L,
            topicMeta.generateExtractorAttributes("user"),
            topicMeta.generateProcessorAttributes(),
            topicMeta.generateConnectorAttributes(consumerGroupId));
    final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();
    pipeTaskInfo.createPipe(new CreatePipePlanV2(pipeStaticMeta, new PipeRuntimeMeta()));

    final Map<String, String> updatedAttributes =
        Map.of(TopicConstant.REALTIME_MODE_KEY, TopicConstant.REALTIME_MODE_BATCH_VALUE);
    final AlterTopicProcedure procedure =
        new AlterTopicProcedure(
            subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(topicName, updatedAttributes),
            updatedAttributes,
            new AtomicReference<>(subscriptionInfo));
    setField(procedure, "pipeTaskInfo", new AtomicReference<>(pipeTaskInfo));

    try {
      procedure.executeFromValidate(null);
      fail("Expected mode change with an existing subscription Pipe to be rejected");
    } catch (final SubscriptionException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(TopicConstant.REALTIME_MODE_KEY));
    }
  }

  @Test
  public void testUnrelatedAlterMigratesLegacyRealtimeModeToStream() throws Exception {
    final String topicName = "test_topic";
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = new HashMap<>();
    originalAttributes.put(TopicConstant.PATH_KEY, "root.**");
    originalAttributes.put(TopicConstant.REALTIME_MODE_KEY, "hybrid");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta(topicName, 1L, originalAttributes)));

    final Map<String, String> updatedAttributes = Map.of("processor", "test_processor");
    final AlterTopicProcedure procedure =
        new AlterTopicProcedure(
            subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(topicName, updatedAttributes),
            updatedAttributes,
            new AtomicReference<>(subscriptionInfo));
    procedure.executeFromValidate(null);

    assertEquals(
        TopicConstant.REALTIME_MODE_STREAM_VALUE,
        procedure.getUpdatedTopicMeta().getConfig().getRealtimeMode());
  }

  @Test
  public void testRealtimeModeCanReplaceLegacyValue() throws Exception {
    final String topicName = "test_topic";
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    final Map<String, String> originalAttributes = new HashMap<>();
    originalAttributes.put(TopicConstant.PATH_KEY, "root.**");
    originalAttributes.put(TopicConstant.REALTIME_MODE_KEY, "hybrid");
    subscriptionInfo.createTopic(
        new CreateTopicPlan(new TopicMeta(topicName, 1L, originalAttributes)));

    final Map<String, String> updatedAttributes =
        Map.of(TopicConstant.REALTIME_MODE_KEY, TopicConstant.REALTIME_MODE_BATCH_VALUE);
    final AlterTopicProcedure procedure =
        new AlterTopicProcedure(
            subscriptionInfo.deepCopyTopicMetaWithUpdatedAttributes(topicName, updatedAttributes),
            updatedAttributes,
            new AtomicReference<>(subscriptionInfo));
    procedure.executeFromValidate(null);

    assertEquals(
        TopicConstant.REALTIME_MODE_BATCH_VALUE,
        procedure.getUpdatedTopicMeta().getConfig().getRealtimeMode());
  }

  private static void setField(final Object target, final String name, final Object value)
      throws Exception {
    Class<?> type = target.getClass();
    while (type != null) {
      try {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (final NoSuchFieldException ignored) {
        type = type.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }
}
