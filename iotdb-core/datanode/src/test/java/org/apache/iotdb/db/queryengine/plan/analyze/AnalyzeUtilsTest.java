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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnalyzeUtilsTest {

  @Test
  public void testParseDeletePredicateWithRenamedTimeColumn() {
    TsTable table = new TsTable("table1");
    table.addColumnSchema(new TimeColumnSchema("ts", TSDataType.TIMESTAMP));
    Expression expression =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
            new Identifier("ts"),
            new LongLiteral("100"));

    List<TableDeletionEntry> entries = AnalyzeUtils.parseExpressions2ModEntries(expression, table);

    assertEquals(1, entries.size());
    assertEquals(Long.MIN_VALUE, entries.get(0).getStartTime());
    assertEquals(100, entries.get(0).getEndTime());
  }

  @Test
  public void testFetchDeleteReplicaSetsOnlyQueriesTargetDatabaseRegions() throws Exception {
    final Delete delete = new Delete(new Table(QualifiedName.of("table1")));
    delete.setDatabaseName("root.db1");
    delete.setTableDeletionEntries(
        Arrays.asList(
            new TableDeletionEntry(new DeletionPredicate("table1"), new TimeRange(10, 20)),
            new TableDeletionEntry(new DeletionPredicate("table1"), new TimeRange(30, 40))));

    final TRegionReplicaSet regionReplicaSet1 = dataRegionReplicaSet(1);
    final TRegionReplicaSet regionReplicaSet2 = dataRegionReplicaSet(2);
    final TGetRegionGroupsByTimeResp resp1 =
        successRegionGroupsResp(Collections.singleton(regionReplicaSet1));
    final TGetRegionGroupsByTimeResp resp2 =
        successRegionGroupsResp(new HashSet<>(Arrays.asList(regionReplicaSet1, regionReplicaSet2)));
    final ConfigNodeClient configNodeClient = Mockito.mock(ConfigNodeClient.class);
    Mockito.when(
            configNodeClient.getRegionGroupsByTime(Mockito.any(TGetRegionGroupsByTimeReq.class)))
        .thenReturn(resp1, resp2);

    final Set<TRegionReplicaSet> result =
        AnalyzeUtils.fetchDeleteReplicaSets(configNodeClient, delete);

    assertEquals(2, result.size());
    assertTrue(result.contains(regionReplicaSet1));
    assertTrue(result.contains(regionReplicaSet2));

    final ArgumentCaptor<TGetRegionGroupsByTimeReq> reqCaptor =
        ArgumentCaptor.forClass(TGetRegionGroupsByTimeReq.class);
    Mockito.verify(configNodeClient, Mockito.times(2)).getRegionGroupsByTime(reqCaptor.capture());
    Mockito.verify(configNodeClient, Mockito.never()).getLatestRegionRouteMap();

    final List<TGetRegionGroupsByTimeReq> requests = reqCaptor.getAllValues();
    assertEquals("root.db1", requests.get(0).getDatabase());
    assertEquals(10, requests.get(0).getStartTime());
    assertEquals(20, requests.get(0).getEndTime());
    assertEquals("root.db1", requests.get(1).getDatabase());
    assertEquals(30, requests.get(1).getStartTime());
    assertEquals(40, requests.get(1).getEndTime());
  }

  private static TGetRegionGroupsByTimeResp successRegionGroupsResp(
      final Set<TRegionReplicaSet> replicaSets) {
    final TGetRegionGroupsByTimeResp resp =
        new TGetRegionGroupsByTimeResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    resp.setRegionReplicaSets(replicaSets);
    return resp;
  }

  private static TRegionReplicaSet dataRegionReplicaSet(final int regionId) {
    return new TRegionReplicaSet(
        new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId), Collections.emptyList());
  }
}
