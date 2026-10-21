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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.tagfilter.TabletTagFilter;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterEvaluationException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class SubscriptionFilterSnapshotTest {

  private static final String TOPIC = "filter_snapshot_test_topic";
  private static final String GROUP = "filter_snapshot_test_group";

  private TopicMeta topicMeta;
  private SubscriptionPrefetchingTabletQueue queue;

  @Before
  public void setUp() {
    topicMeta =
        new TopicMeta(
            TOPIC,
            1L,
            Collections.singletonMap(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE));
    Assert.assertNull(SubscriptionAgent.topic().handleSingleTopicMetaChanges(topicMeta));
    Assert.assertNull(
        SubscriptionAgent.consumer()
            .handleSingleConsumerGroupMetaChanges(
                new ConsumerGroupMeta(
                    GROUP,
                    1L,
                    new ConsumerMeta(
                        "consumer",
                        1L,
                        Collections.singletonMap(
                            ConsumerConstant.SQL_DIALECT_KEY,
                            SystemConstant.SQL_DIALECT_TABLE_VALUE)))));
    queue = Mockito.mock(SubscriptionPrefetchingTabletQueue.class);
    Mockito.when(queue.getTopicName()).thenReturn(TOPIC);
    Mockito.when(queue.getConsumerGroupId()).thenReturn(GROUP);
  }

  @After
  public void tearDown() {
    Assert.assertNull(SubscriptionAgent.consumer().handleDropConsumerGroup(GROUP));
    Assert.assertNull(SubscriptionAgent.topic().handleDropTopic(TOPIC, true));
  }

  @Test
  public void testOwnerAndRetentionUpdatesPreserveBatchButFilterUpdateRotatesIt() {
    final SubscriptionFilterSnapshot snapshot = SubscriptionFilterSnapshot.capture(queue);
    updateTopic(
        Map.of(
            TopicConstant.OWNER_ID_KEY, "owner-1",
            TopicConstant.OWNER_EPOCH_KEY, "1",
            TopicConstant.RETENTION_MS_KEY, "60000"));
    Assert.assertTrue(snapshot.isCurrent(queue));

    updateTopic(Collections.singletonMap(TopicConstant.TAG_FILTER_KEY, "false"));
    Assert.assertFalse(snapshot.isCurrent(queue));
    Assert.assertTrue(snapshot.getTagFilterMatcher().isMatchAll());
    final SubscriptionFilterSnapshot updated = SubscriptionFilterSnapshot.capture(queue);
    Assert.assertTrue(updated.isCurrent(queue));
    final Tablet tablet =
        new Tablet(
            "weather",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            1);
    tablet.addTimestamp(0, 1L);
    Assert.assertNull(TabletTagFilter.filter(tablet, updated.getTagFilterMatcher()));
  }

  @Test
  public void testMissingSchemaReportsTargetAndLocalCacheReason() {
    final String database = "missing_snapshot_database";
    final String table = "missing_table";
    updateTopic(
        Map.of(
            TopicConstant.DATABASE_KEY, database,
            TopicConstant.TABLE_KEY, table,
            TopicConstant.TAG_FILTER_KEY, "region = \"north\""));
    try {
      SubscriptionFilterSnapshot.capture(queue);
      Assert.fail("Expected unavailable table schema to fail closed");
    } catch (final TagFilterEvaluationException e) {
      Assert.assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains(
                  String.format(
                      DataNodeMiscMessages
                          .EXCEPTION_TABLE_SCHEMA_ARG_ARG_IS_ABSENT_FROM_THIS_DATANODE_S_TABLE_CACHE_THE_TABLE_MAY_NOT_EXIST_OR_ITS_SCHEMA_HAS_NOT_BEEN_SYNCHRONIZED_YET_23975BDF,
                      database,
                      table)));
    }
  }

  @Test
  public void testColumnBindingFailureRetainsMatchNoneMatcher() {
    updateTopic(
        Map.of(
            TopicConstant.DATABASE_KEY, "missing_snapshot_database",
            TopicConstant.TABLE_KEY, "missing_table",
            TopicConstant.COLUMN_FILTER_KEY, "column_name = \"temperature\""));
    final SubscriptionFilterSnapshot snapshot = SubscriptionFilterSnapshot.capture(queue);
    Assert.assertFalse(snapshot.getColumnFilterMatcher().isMatchAll());
    Assert.assertFalse(
        snapshot
            .getColumnFilterMatcher()
            .match("missing_snapshot_database", "missing_table", "temperature"));
  }

  @Test(expected = TagFilterEvaluationException.class)
  public void testUnavailableTreeViewProjectorFailsInsteadOfDroppingTablet() {
    final SubscriptionTreeViewProjector projector =
        new SubscriptionTreeViewProjector(new TopicConfig());
    Assert.assertTrue(projector.prepare());
    Assert.assertFalse(projector.isAvailable());
    projector.project(null);
  }

  private void updateTopic(final Map<String, String> attributes) {
    topicMeta = topicMeta.deepCopyWithUpdatedAttributes(attributes);
    Assert.assertNull(SubscriptionAgent.topic().handleSingleTopicMetaChanges(topicMeta));
  }
}
