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

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

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

  private void assertSerializeDeserialize(final AlterTopicProcedure procedure) throws Exception {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    procedure.serialize(outputStream);
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final AlterTopicProcedure deserializedProcedure =
        (AlterTopicProcedure) ProcedureFactory.getInstance().create(buffer);
    assertEquals(procedure, deserializedProcedure);
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
}
