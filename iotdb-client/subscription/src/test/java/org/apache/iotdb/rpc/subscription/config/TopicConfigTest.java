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
  public void testFilterVersionIgnoresOwnerRetentionAndUnknownProperties() {
    final TopicConfig original =
        new TopicConfig(Collections.singletonMap("Tag-Filter", "region = \"north\""));
    final Map<String, String> attributes = new HashMap<>(original.getAttribute());
    attributes.put(TopicConstant.OWNER_ID_KEY, "owner-1");
    attributes.put(TopicConstant.OWNER_EPOCH_KEY, "1");
    attributes.put(TopicConstant.RETENTION_MS_KEY, "60000");
    attributes.put(TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY, "1000");
    attributes.put("future.non-filtering-property", "value");
    final TopicConfig updated = new TopicConfig(attributes);
    updated.inheritFilterVersion(original);

    Assert.assertEquals(original.getFilterVersion(), updated.getFilterVersion());
  }

  @Test
  public void testFilterVersionChangesForPredicateBindingAndTreeCapture() {
    final TopicConfig original =
        new TopicConfig(
            Map.of(
                TopicConstant.TAG_FILTER_KEY, "region = \"north\"",
                TopicConstant.DATABASE_KEY, "db",
                TopicConstant.TABLE_KEY, "weather"));
    for (final Map.Entry<String, String> change :
        Map.of(
                TopicConstant.TAG_FILTER_KEY,
                "region = \"North\"",
                TopicConstant.DATABASE_KEY,
                "db2",
                TopicConstant.TABLE_KEY,
                "weather2",
                "source.capture.tree",
                "true")
            .entrySet()) {
      final TopicConfig updated = new TopicConfig(original);
      updated.getAttribute().put(change.getKey(), change.getValue());
      updated.inheritFilterVersion(original);
      Assert.assertNotEquals(original.getFilterVersion(), updated.getFilterVersion());
    }
  }

  @Test
  public void testSnapshotCopyPreservesFilterVersionAndOwnsAttributes() {
    final TopicConfig original =
        new TopicConfig(Collections.singletonMap(TopicConstant.TAG_FILTER_KEY, "false"));
    final TopicConfig snapshot = new TopicConfig(original);
    Assert.assertEquals(original.getFilterVersion(), snapshot.getFilterVersion());

    snapshot.getAttribute().put(TopicConstant.TAG_FILTER_KEY, "true");
    Assert.assertEquals("false", original.getTagFilter());
  }

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

  @Test
  public void testRecycleUncommittedEventInterval() {
    Assert.assertNull(new TopicConfig().getRecycleUncommittedEventIntervalMs());

    final Map<String, String> attributes = new HashMap<>();
    attributes.put(TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY, " 120000 ");
    Assert.assertEquals(
        Long.valueOf(120000L), new TopicConfig(attributes).getRecycleUncommittedEventIntervalMs());

    attributes.clear();
    attributes.put("RECYCLE-UNCOMMITTED-EVENT-INTERVAL-MS", "300000");
    Assert.assertEquals(
        Long.valueOf(300000L), new TopicConfig(attributes).getRecycleUncommittedEventIntervalMs());

    for (final long intervalMs : new long[] {0L, -1L, Long.MIN_VALUE}) {
      attributes.put("RECYCLE-UNCOMMITTED-EVENT-INTERVAL-MS", " " + intervalMs + " ");
      Assert.assertEquals(
          Long.valueOf(intervalMs),
          new TopicConfig(attributes).getRecycleUncommittedEventIntervalMs());
    }
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

  @Test
  public void testTagFilterKeyIsCaseInsensitive() {
    final TopicConfig topicConfig =
        new TopicConfig(Collections.singletonMap("Tag-Filter", "region = \"north\""));

    Assert.assertTrue(topicConfig.hasTagFilter());
    Assert.assertEquals("region = \"north\"", topicConfig.getTagFilter());
    Assert.assertEquals(
        "region = \"north\"",
        topicConfig.getAttributesWithSourceTagFilter().get(TopicConstant.TAG_FILTER_KEY));
  }

  @Test
  public void testTagFilterDefaultsToTrivialWhenAbsent() {
    final TopicConfig topicConfig = new TopicConfig(new HashMap<>());

    Assert.assertFalse(topicConfig.hasTagFilter());
    Assert.assertTrue(topicConfig.isTagFilterTrivial());
    Assert.assertEquals(TopicConstant.TAG_FILTER_DEFAULT_VALUE, topicConfig.getTagFilter());
  }

  @Test
  public void testTagFilterTrivialWithMixedCaseKeyAndValue() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("TAG-FILTER", " TRUE ");

    Assert.assertTrue(new TopicConfig(attributes).isTagFilterTrivial());
  }

  @Test
  public void testRetainProgressAfterUnsubscribeDefaultsToFalse() {
    Assert.assertFalse(new TopicConfig().isProgressRetainedAfterUnsubscribe());
  }

  @Test
  public void testRetainProgressAfterUnsubscribeIsCaseInsensitiveAndTrimmed() {
    Assert.assertTrue(
        new TopicConfig(Collections.singletonMap("Retain.Progress.After.Unsubscribe", " TRUE "))
            .isProgressRetainedAfterUnsubscribe());
  }

  private static TopicConfig topicConfigWithMode(final String mode) {
    return new TopicConfig(Collections.singletonMap(TopicConstant.MODE_KEY, mode));
  }
}
