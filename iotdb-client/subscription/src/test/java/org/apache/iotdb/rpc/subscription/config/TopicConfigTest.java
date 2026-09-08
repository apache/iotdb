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

package org.apache.iotdb.rpc.subscription.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopicConfigTest {

  @Test
  public void testModeDefaultsToInitial() {
    final TopicConfig topicConfig = new TopicConfig();

    Assert.assertEquals(TopicConstant.MODE_INITIAL_VALUE, topicConfig.getMode());
    Assert.assertTrue(topicConfig.isInitialMode());
    Assert.assertFalse(topicConfig.isSnapshotMode());
    Assert.assertFalse(topicConfig.isIncrementalMode());
  }

  @Test
  public void testCanonicalModeValues() {
    Assert.assertTrue(TopicConfig.isValidMode(TopicConstant.MODE_INITIAL_VALUE));
    Assert.assertTrue(TopicConfig.isValidMode(TopicConstant.MODE_SNAPSHOT_VALUE));
    Assert.assertTrue(TopicConfig.isValidMode(TopicConstant.MODE_INCREMENTAL_VALUE));
    Assert.assertFalse(TopicConfig.isValidMode("wal"));

    Assert.assertTrue(topicConfigWithMode(" INITIAL ").isInitialMode());
    Assert.assertTrue(topicConfigWithMode(" INCREMENTAL ").isIncrementalMode());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testLegacyModeValues() {
    final TopicConfig liveTopicConfig = topicConfigWithMode(TopicConstant.MODE_LIVE_VALUE);
    Assert.assertTrue(TopicConfig.isValidMode(TopicConstant.MODE_LIVE_VALUE));
    Assert.assertEquals(TopicConstant.MODE_INITIAL_VALUE, liveTopicConfig.getMode());
    Assert.assertTrue(liveTopicConfig.isInitialMode());
    Assert.assertTrue(liveTopicConfig.isLiveMode());

    final TopicConfig consensusTopicConfig =
        topicConfigWithMode(TopicConstant.MODE_CONSENSUS_VALUE);
    Assert.assertTrue(TopicConfig.isValidMode(TopicConstant.MODE_CONSENSUS_VALUE));
    Assert.assertEquals(TopicConstant.MODE_INCREMENTAL_VALUE, consensusTopicConfig.getMode());
    Assert.assertTrue(consensusTopicConfig.isIncrementalMode());
    Assert.assertTrue(consensusTopicConfig.isConsensusMode());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testInitialModeMapsToPipeLiveMode() {
    Assert.assertEquals(
        TopicConstant.MODE_LIVE_VALUE,
        topicConfigWithMode(TopicConstant.MODE_INITIAL_VALUE)
            .getAttributesWithSourceMode()
            .get(TopicConstant.MODE_KEY));
    Assert.assertEquals(
        TopicConstant.MODE_LIVE_VALUE,
        topicConfigWithMode(TopicConstant.MODE_LIVE_VALUE)
            .getAttributesWithSourceMode()
            .get(TopicConstant.MODE_KEY));
  }

  @Test
  public void testColumnFilterKeyIsCaseInsensitive() {
    final TopicConfig topicConfig =
        new TopicConfig(Collections.singletonMap("Column-Filter", "column_name = \"s1\""));

    Assert.assertTrue(topicConfig.hasColumnFilter());
    Assert.assertEquals("column_name = \"s1\"", topicConfig.getColumnFilter());
    Assert.assertEquals(
        "column_name = \"s1\"",
        topicConfig.getAttributesWithSourceColumnFilter().get(TopicConstant.COLUMN_FILTER_KEY));
  }

  @Test
  public void testColumnFilterDefaultsToTrivialWhenAbsent() {
    final TopicConfig topicConfig = new TopicConfig(new HashMap<>());

    Assert.assertFalse(topicConfig.hasColumnFilter());
    Assert.assertTrue(topicConfig.isColumnFilterTrivial());
    Assert.assertEquals(TopicConstant.COLUMN_FILTER_DEFAULT_VALUE, topicConfig.getColumnFilter());
  }

  @Test
  public void testColumnFilterTrivialWithMixedCaseKeyAndValue() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("COLUMN-FILTER", " TRUE ");

    Assert.assertTrue(new TopicConfig(attributes).isColumnFilterTrivial());
  }

  private static TopicConfig topicConfigWithMode(final String mode) {
    return new TopicConfig(Collections.singletonMap(TopicConstant.MODE_KEY, mode));
  }
}
