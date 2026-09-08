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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SubscriptionTopicAgentTest {

  @Test
  public void testColumnFilterRefreshSkippedForOwnerOnlyUpdate() {
    final TopicMeta oldMeta = createTableTopicMeta("column_name = \"temperature\"", "db", "t1");
    final Map<String, String> updatedAttributes = new HashMap<>();
    updatedAttributes.put(TopicConstant.OWNER_ID_KEY, "owner-1");
    updatedAttributes.put(TopicConstant.OWNER_EPOCH_KEY, "1");

    Assert.assertFalse(
        SubscriptionTopicAgent.shouldRefreshColumnFilter(
            oldMeta, oldMeta.deepCopyWithUpdatedAttributes(updatedAttributes)));
  }

  @Test
  public void testColumnFilterRefreshTriggeredForBindingInputUpdate() {
    final TopicMeta oldMeta = createTableTopicMeta("column_name = \"temperature\"", "db", "t1");

    Assert.assertTrue(
        SubscriptionTopicAgent.shouldRefreshColumnFilter(
            oldMeta, createTableTopicMeta("column_name = \"status\"", "db", "t1")));
    Assert.assertTrue(
        SubscriptionTopicAgent.shouldRefreshColumnFilter(
            oldMeta, createTableTopicMeta("column_name = \"temperature\"", "db2", "t1")));
    Assert.assertTrue(
        SubscriptionTopicAgent.shouldRefreshColumnFilter(
            oldMeta, createTableTopicMeta("column_name = \"temperature\"", "db", "t2")));
  }

  @Test
  public void testColumnFilterRefreshTriggeredForNewTableTopicOnly() {
    Assert.assertTrue(
        SubscriptionTopicAgent.shouldRefreshColumnFilter(
            null, createTableTopicMeta("column_name = \"temperature\"", "db", "t1")));
    Assert.assertFalse(
        SubscriptionTopicAgent.shouldRefreshColumnFilter(null, createTreeTopicMeta()));
  }

  private static TopicMeta createTableTopicMeta(
      final String columnFilter, final String database, final String table) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("__system.sql-dialect", "table");
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
    attributes.put(TopicConstant.DATABASE_KEY, database);
    attributes.put(TopicConstant.TABLE_KEY, table);
    return new TopicMeta("topic", 1L, attributes);
  }

  private static TopicMeta createTreeTopicMeta() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("__system.sql-dialect", "tree");
    attributes.put(TopicConstant.PATH_KEY, "root.**");
    return new TopicMeta("topic", 1L, attributes);
  }
}
