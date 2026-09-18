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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription;

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterMatcher;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShowTopicsTaskTest {

  @Test
  public void testBuildTSBlockWritesTagFilterDiagnostics() throws Exception {
    final Map<String, TagFilterMatcher> matchers = new HashMap<>();
    matchers.put("all", TagFilterMatcher.matchAll());
    matchers.put("none", TagFilterMatcher.matchNone());
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    attributes.put(TopicConstant.TAG_FILTER_KEY, "region = \"east\"");
    matchers.put("active", TagFilterMatcher.fromTopicConfig(new TopicConfig(attributes)));
    matchers.put("error", TagFilterMatcher.failure(new IllegalStateException("binding failed")));
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    ShowTopicsTask.buildTSBlock(
        Arrays.asList(topic("all"), topic("none"), topic("active"), topic("error")),
        true,
        matchers::get,
        future);

    final ConfigTaskResult result = future.get();
    final TsBlock resultSet = result.getResultSet();
    assertEquals(TSStatusCode.SUCCESS_STATUS, result.getStatusCode());
    assertEquals(
        ColumnHeaderConstant.TAG_FILTER_STATUS,
        result.getResultSetHeader().getRespColumns().get(2));
    assertEquals(
        ColumnHeaderConstant.TAG_FILTER_MESSAGE,
        result.getResultSetHeader().getRespColumns().get(3));
    assertEquals("MATCH_ALL", resultSet.getColumn(2).getBinary(0).toString());
    assertEquals("MATCH_NONE", resultSet.getColumn(2).getBinary(1).toString());
    assertEquals("ACTIVE", resultSet.getColumn(2).getBinary(2).toString());
    assertEquals("ERROR", resultSet.getColumn(2).getBinary(3).toString());
    assertTrue(resultSet.getColumn(3).isNull(0));
    assertTrue(resultSet.getColumn(3).isNull(1));
    assertTrue(resultSet.getColumn(3).isNull(2));
    assertEquals("binding failed", resultSet.getColumn(3).getBinary(3).toString());
  }

  @Test
  public void testBuildTreeModelTSBlockLeavesTagFilterDiagnosticsNull() throws Exception {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    ShowTopicsTask.buildTSBlock(
        Arrays.asList(topic("tree")),
        false,
        ignored -> TagFilterMatcher.failure(new IllegalStateException()),
        future);

    final TsBlock resultSet = future.get().getResultSet();
    assertEquals(2, resultSet.getValueColumnCount());
  }

  private static TShowTopicInfo topic(final String name) {
    return new TShowTopicInfo(name, 1L).setTopicAttributes("{}");
  }
}
