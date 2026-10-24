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
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public class TopicMetaTest {

  @Test
  public void testMixedCaseAlterReplacesBuiltInKeyAndPreservesExtensionKeys() throws Exception {
    final TopicMeta original =
        new TopicMeta(
            "topic",
            1L,
            Map.of("Tag-Filter", "region = \"north\"", "source.CustomOption", "CaseSensitive"));
    final TopicMeta updated =
        original.deepCopyWithUpdatedAttributes(Map.of(" TAG-FILTER ", "region = \"North\""));
    Assert.assertEquals(2, updated.getConfig().getAttribute().size());
    Assert.assertEquals("region = \"North\"", updated.getConfig().getTagFilter());
    Assert.assertEquals(
        "CaseSensitive", updated.getConfig().getAttribute().get("source.CustomOption"));
    Assert.assertEquals("region = \"north\"", original.getConfig().getTagFilter());

    Assert.assertEquals(updated, TopicMeta.deserialize(updated.serialize()));
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    updated.serialize(output);
    Assert.assertEquals(
        updated, TopicMeta.deserialize(new ByteArrayInputStream(output.toByteArray())));
  }

  @Test
  public void testDeserializationCanonicalizesLegacyMixedCaseAttributes() throws Exception {
    final TopicMeta topic = new TopicMeta("topic", 1L, Map.of());
    // Simulate metadata written before key canonicalization was introduced.
    topic.getConfig().getAttribute().put("Tag-Filter", "region = \"North\"");
    final TopicMeta fromBuffer = TopicMeta.deserialize(topic.serialize());
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    topic.serialize(output);
    final TopicMeta fromStream =
        TopicMeta.deserialize(new ByteArrayInputStream(output.toByteArray()));
    for (final TopicMeta restored : new TopicMeta[] {fromBuffer, fromStream}) {
      Assert.assertEquals(
          Map.of(TopicConstant.TAG_FILTER_KEY, "region = \"North\""),
          restored.getConfig().getAttribute());
    }
  }
}
