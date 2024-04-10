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

package org.apache.iotdb.commons.subscription.consumer;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class ConsumerGroupDeSerTest {

  @Test
  public void test() throws IOException {
    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put("k1", "v1");
    consumerAttributes.put("k2", "v2");

    ConsumerGroupMeta consumerGroupMeta =
        new ConsumerGroupMeta(
            "test_consumer_group", 1, new ConsumerMeta("test_consumer1", 1, consumerAttributes));
    consumerGroupMeta.addConsumer(new ConsumerMeta("test_consumer2", 2, consumerAttributes));

    try {
      consumerGroupMeta.addSubscription("test_consumer3", Collections.singleton("test_topic"));
      fail();
    } catch (SubscriptionException ignored) {
    }
    consumerGroupMeta.addSubscription("test_consumer1", Collections.singleton("test_topic"));

    Assert.assertTrue(
        consumerGroupMeta.getConsumersSubscribingTopic("test_topic").contains("test_consumer1"));
    Assert.assertTrue(
        consumerGroupMeta.getTopicsSubscribedByConsumer("test_consumer1").contains("test_topic"));

    ByteBuffer byteBuffer = consumerGroupMeta.serialize();
    ConsumerGroupMeta consumerGroupMeta1 = ConsumerGroupMeta.deserialize(byteBuffer);
    ConsumerGroupMeta consumerGroupMeta2 = consumerGroupMeta1.deepCopy();

    Assert.assertEquals(consumerGroupMeta, consumerGroupMeta1);
    Assert.assertEquals(consumerGroupMeta, consumerGroupMeta2);
    Assert.assertEquals(
        consumerGroupMeta.getConsumerGroupId(), consumerGroupMeta2.getConsumerGroupId());
    Assert.assertEquals(consumerGroupMeta.getCreationTime(), consumerGroupMeta2.getCreationTime());
  }
}
