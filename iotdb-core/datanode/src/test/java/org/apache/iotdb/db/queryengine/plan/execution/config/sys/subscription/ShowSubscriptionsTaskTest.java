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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TSubscriptionProgressInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShowSubscriptionsTaskTest {

  @Test
  public void testBuildDetailsTSBlock() throws Exception {
    final TSubscriptionProgressInfo progressInfo =
        new TSubscriptionProgressInfo(
            "topic",
            "group",
            "DataRegion[1]",
            3,
            true,
            true,
            "CATCHING_UP",
            5L,
            20L,
            6L,
            2L,
            1L,
            2L,
            100L,
            81L,
            123L,
            120L,
            "consumer",
            4L);
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    ShowSubscriptionsTask.buildDetailsTSBlock(Collections.singletonList(progressInfo), future);

    final ConfigTaskResult result = future.get();
    final TsBlock resultSet = result.getResultSet();
    assertEquals(TSStatusCode.SUCCESS_STATUS, result.getStatusCode());
    assertEquals(
        ColumnHeaderConstant.SUBSCRIPTION_ID, result.getResultSetHeader().getRespColumns().get(0));
    assertEquals(
        ColumnHeaderConstant.SEEK_GENERATION, result.getResultSetHeader().getRespColumns().get(19));
    assertEquals(1, resultSet.getPositionCount());
    assertEquals("topic_group", resultSet.getColumn(0).getBinary(0).toString());
    assertEquals("CATCHING_UP", resultSet.getColumn(5).getBinary(0).toString());
    assertTrue(resultSet.getColumn(6).getBoolean(0));
    assertEquals(5L, resultSet.getColumn(8).getLong(0));
    assertEquals(20L, resultSet.getColumn(9).getLong(0));
    assertEquals(4L, resultSet.getColumn(19).getLong(0));
  }
}
