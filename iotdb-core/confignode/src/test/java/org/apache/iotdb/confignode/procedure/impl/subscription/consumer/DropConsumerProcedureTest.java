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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer;

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DropConsumerProcedureTest {
  @Test
  public void testClosingLastConsumerPersistsConfiguredInitialRuntimeMarker() throws Exception {
    for (final boolean retain : new boolean[] {false, true}) {
      for (final String dialect : new String[] {"tree", "table"}) {
        for (final String mode :
            new String[] {TopicConstant.MODE_INITIAL_VALUE, TopicConstant.MODE_SNAPSHOT_VALUE}) {
          final SubscriptionInfo subscriptions = new SubscriptionInfo();
          final Map<String, String> attributes = new HashMap<>();
          attributes.put(SystemConstant.SQL_DIALECT_KEY, dialect);
          attributes.put(TopicConstant.MODE_KEY, mode);
          attributes.put(
              TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, String.valueOf(retain));
          subscriptions.createTopic(new CreateTopicPlan(new TopicMeta("topic", 1L, attributes)));
          final ConsumerGroupMeta group =
              new ConsumerGroupMeta(
                  "group",
                  1L,
                  new ConsumerMeta(
                      "consumer",
                      1L,
                      Collections.singletonMap(ConsumerConstant.SQL_DIALECT_KEY, dialect)));
          group.addSubscription("consumer", Collections.singleton("topic"));
          subscriptions.alterConsumerGroup(new AlterConsumerGroupPlan(group));
          final DropConsumerProcedure procedure =
              new DropConsumerProcedure(new TCloseConsumerReq("consumer", "group"));
          final Field field =
              AbstractOperateSubscriptionProcedure.class.getDeclaredField("subscriptionInfo");
          field.setAccessible(true);
          field.set(procedure, new AtomicReference<>(subscriptions));
          procedure.executeFromValidate(null);
          assertEquals(
              retain,
              procedure.getUpdatedConsumerGroupMeta().getRetainedTopicNames().contains("topic"));
          assertTrue(procedure.getUpdatedConsumerGroupMeta().isEmpty());

          final PublicBAOS bytes = new PublicBAOS();
          procedure.serialize(new DataOutputStream(bytes));
          final ByteBuffer buffer = ByteBuffer.wrap(bytes.getBuf(), 0, bytes.size());
          final DropConsumerProcedure recovered =
              (DropConsumerProcedure) ProcedureFactory.getInstance().create(buffer);
          assertEquals(procedure, recovered);
          assertEquals(0, buffer.remaining());
          subscriptions.alterConsumerGroup(
              new AlterConsumerGroupPlan(recovered.getUpdatedConsumerGroupMeta()));
          assertEquals(retain, subscriptions.getConsumerGroupMeta("group") != null);
        }
      }
    }
  }

  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put("k1", "v1");
    consumerAttributes.put("k2", "v2");

    DropConsumerProcedure proc =
        new DropConsumerProcedure(new TCloseConsumerReq("old_consumer1", "test_consumer_group"));

    ConsumerGroupMeta oldMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));
    oldMeta.addConsumer(new ConsumerMeta("old_consumer1", 1, consumerAttributes));
    ConsumerGroupMeta newMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("old_consumer", 1, consumerAttributes));

    proc.setExistingConsumerGroupMeta(oldMeta);
    proc.setUpdatedConsumerGroupMeta(newMeta);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      DropConsumerProcedure proc2 =
          (DropConsumerProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(oldMeta, proc2.getExistingConsumerGroupMeta());
      assertEquals(newMeta, proc2.getUpdatedConsumerGroupMeta());
    } catch (Exception e) {
      fail();
    }
  }
}
