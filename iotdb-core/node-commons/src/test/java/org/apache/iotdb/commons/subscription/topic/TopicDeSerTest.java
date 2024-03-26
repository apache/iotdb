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

package org.apache.iotdb.commons.subscription.topic;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TopicDeSerTest {

  @Test
  public void test() throws IOException {
    Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put("k1", "v1");
    topicAttributes.put("k2", "v2");

    TopicMeta topicMeta = new TopicMeta("test_topic", 1, topicAttributes);

    topicMeta.addSubscribedConsumerGroup("test_consumer_group");
    Assert.assertTrue(topicMeta.isSubscribedByConsumerGroup("test_consumer_group"));

    ByteBuffer byteBuffer = topicMeta.serialize();
    TopicMeta topicMeta1 = TopicMeta.deserialize(byteBuffer);
    TopicMeta topicMeta2 = topicMeta1.deepCopy();

    Assert.assertEquals(topicMeta, topicMeta1);
    Assert.assertEquals(topicMeta, topicMeta2);
    Assert.assertEquals(topicMeta.getTopicName(), topicMeta2.getTopicName());
    Assert.assertEquals(topicMeta.getCreationTime(), topicMeta2.getCreationTime());
    Assert.assertEquals(topicMeta.getConfig(), topicMeta2.getConfig());
    Assert.assertEquals(
        topicMeta.getSubscribedConsumerGroupIds(), topicMeta2.getSubscribedConsumerGroupIds());
  }
}
