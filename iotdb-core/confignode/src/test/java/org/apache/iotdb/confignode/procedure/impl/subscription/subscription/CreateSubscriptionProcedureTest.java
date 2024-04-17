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

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    List<AlterTopicProcedure> topicProcedures = new ArrayList<>();
    TopicMeta newTopicMeta = new TopicMeta("t1", 1, topicAttributes);
    newTopicMeta.addSubscribedConsumerGroup("cg1");
    topicProcedures.add(new AlterTopicProcedure(newTopicMeta));

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
    proc.setAlterTopicProcedures(topicProcedures);
    proc.setCreatePipeProcedures(pipeProcedures);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreateSubscriptionProcedure proc2 =
          (CreateSubscriptionProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(alterConsumerGroupProcedure, proc2.getAlterConsumerGroupProcedure());
      assertEquals(topicProcedures, proc2.getAlterTopicProcedures());
      assertEquals(pipeProcedures, proc2.getCreatePipeProcedures());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
