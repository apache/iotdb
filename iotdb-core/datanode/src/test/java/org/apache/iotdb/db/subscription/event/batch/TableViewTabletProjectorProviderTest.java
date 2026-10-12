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

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TableViewTabletProjectorProviderTest {

  private final String topicName = "projector_topic_" + UUID.randomUUID();
  private final String groupId = "projector_group_" + UUID.randomUUID();
  private final String database = "projector_db_" + UUID.randomUUID();
  private final List<SubscriptionPipeTabletEventBatch> batches = new ArrayList<>();
  private SubscriptionPrefetchingTabletQueue queue;

  @Before
  public void setUp() {
    final ConsumerMeta consumer =
        new ConsumerMeta(
            "consumer",
            1L,
            Collections.singletonMap(
                ConsumerConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE));
    Assert.assertNull(
        SubscriptionAgent.consumer()
            .handleSingleConsumerGroupMetaChanges(new ConsumerGroupMeta(groupId, 1L, consumer)));
    Assert.assertTrue(SubscriptionAgent.consumer().isTableModel(groupId));
    queue = Mockito.mock(SubscriptionPrefetchingTabletQueue.class);
    Mockito.when(queue.getTopicName()).thenReturn(topicName);
    Mockito.when(queue.getConsumerGroupId()).thenReturn(groupId);
  }

  @After
  public void tearDown() {
    for (final SubscriptionPipeTabletEventBatch batch : batches) {
      batch.cleanUp(true);
    }
    SubscriptionAgent.topic().handleDropTopic(topicName);
    SubscriptionAgent.consumer().handleDropConsumerGroup(groupId);
    DataNodeTableCache.getInstance().invalid(database);
  }

  @Test
  public void testBatchWaitsForTopicAndSchemaThenRetainsItsProjector() {
    final SubscriptionPipeTabletEventBatch batch = createBatch();
    Assert.assertNull(batch.generateSubscriptionEvents());

    installTopic(true);
    Assert.assertNull(batch.generateSubscriptionEvents());

    final TsTable table = new TsTable("view");
    table.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.test.**");
    table.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    final FieldColumnSchema field = new FieldColumnSchema("temperature", TSDataType.DOUBLE);
    TreeViewSchema.setOriginalName(field, "temperature");
    table.addColumnSchema(field);
    DataNodeTableCache.getInstance().preUpdateTable(database, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(database, table.getTableName(), null);
    Assert.assertNotNull(batch.generateSubscriptionEvents());

    DataNodeTableCache.getInstance().invalid(database);
    final Tablet sourceTablet =
        new Tablet(
            "root.test.d1",
            Collections.singletonList(new MeasurementSchema("temperature", TSDataType.DOUBLE)),
            1);
    sourceTablet.addTimestamp(0, 1L);
    sourceTablet.addValue("temperature", 0, 2.5);
    batch.enrichedEvents.add(
        new PipeRawTabletInsertionEvent(
            false, "root.test", null, "root.test", sourceTablet, true, null, 0, null, null, false));
    Assert.assertNotNull(batch.generateSubscriptionEvents());
    final Pair<String, List<Tablet>> emittedTablets = batch.next();
    Assert.assertEquals(database, emittedTablets.left);
    Assert.assertEquals(1, emittedTablets.right.size());
    Assert.assertEquals("view", emittedTablets.right.get(0).getTableName());
    Assert.assertEquals(2.5, ((double[]) emittedTablets.right.get(0).getValues()[0])[0], 0.0);
    Assert.assertNull(createBatch().generateSubscriptionEvents());
  }

  @Test
  public void testOrdinaryTableTopicDoesNotWaitForSchema() {
    installTopic(false);
    Assert.assertNotNull(createBatch().generateSubscriptionEvents());
  }

  private void installTopic(final boolean captureTree) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, "table");
    attributes.put(TopicConstant.DATABASE_KEY, database);
    attributes.put(TopicConstant.TABLE_KEY, "view");
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY, Boolean.toString(captureTree));
    Assert.assertNull(
        SubscriptionAgent.topic()
            .handleSingleTopicMetaChanges(new TopicMeta(topicName, 1L, attributes)));
  }

  private SubscriptionPipeTabletEventBatch createBatch() {
    final SubscriptionPipeTabletEventBatch batch =
        new SubscriptionPipeTabletEventBatch(1, queue, 20, 1024);
    batches.add(batch);
    return batch;
  }
}
