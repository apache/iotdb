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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDB;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CountDBTaskTest {

  @Test
  public void testBuildTSBlockCountsVisibleDatabases() throws Exception {
    final Map<String, Object> databaseInfoMap = new HashMap<>();
    databaseInfoMap.put("db1", new Object());
    databaseInfoMap.put("db2", new Object());

    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    CountDBTask.buildTSBlock(databaseInfoMap, future, databaseName -> !"db2".equals(databaseName));

    final ConfigTaskResult result = future.get();
    final TsBlock resultSet = result.getResultSet();

    assertEquals(TSStatusCode.SUCCESS_STATUS, result.getStatusCode());
    assertEquals(
        Collections.singletonList(IoTDBConstant.COLUMN_COUNT),
        result.getResultSetHeader().getRespColumns());
    assertEquals(1, resultSet.getPositionCount());
    assertEquals(2, resultSet.getColumn(0).getInt(0));
  }

  @Test
  public void testBuildTSBlockCanHideInformationSchema() throws Exception {
    final Map<String, Object> databaseInfoMap = new HashMap<>();
    databaseInfoMap.put("db1", new Object());
    databaseInfoMap.put("db2", new Object());

    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    CountDBTask.buildTSBlock(databaseInfoMap, future, databaseName -> "db1".equals(databaseName));

    final ConfigTaskResult result = future.get();
    assertEquals(1, result.getResultSet().getColumn(0).getInt(0));
  }

  @Test
  public void testBuildTSBlockDoesNotDoubleCountInformationSchema() throws Exception {
    final Map<String, Object> databaseInfoMap = new HashMap<>();
    databaseInfoMap.put("db1", new Object());
    databaseInfoMap.put(INFORMATION_DATABASE, new Object());

    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    CountDBTask.buildTSBlock(databaseInfoMap, future, databaseName -> true);

    final ConfigTaskResult result = future.get();
    assertEquals(2, result.getResultSet().getColumn(0).getInt(0));
  }

  @Test
  public void testExecuteDelegatesToExecutor() throws Exception {
    final CountDB node = new CountDB(new NodeLocation(1, 1));
    final Predicate<String> canSeenDB = databaseName -> true;
    final CountDBTask task = new CountDBTask(node, canSeenDB);
    final IConfigTaskExecutor executor = Mockito.mock(IConfigTaskExecutor.class);
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    when(executor.countDatabases(same(node), same(canSeenDB))).thenReturn(future);

    assertSame(future, task.execute(executor));
    verify(executor).countDatabases(same(node), same(canSeenDB));
  }
}
