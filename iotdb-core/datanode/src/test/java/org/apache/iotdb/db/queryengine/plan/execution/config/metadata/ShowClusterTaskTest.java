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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowClusterTaskTest {

  private static final int CONFIG_NODE_ID = 0;
  private static final int DATA_NODE_ID = 1;

  private static TShowClusterResp buildClusterResp(final Map<Integer, String> statusReasonMap) {
    final TConfigNodeLocation configNode =
        new TConfigNodeLocation(
            CONFIG_NODE_ID, new TEndPoint("127.0.0.1", 1000), new TEndPoint("127.0.0.1", 1001));
    final TDataNodeLocation dataNode =
        new TDataNodeLocation(
            DATA_NODE_ID,
            new TEndPoint("127.0.0.1", 2000),
            new TEndPoint("127.0.0.1", 2001),
            new TEndPoint("127.0.0.1", 2002),
            new TEndPoint("127.0.0.1", 2003),
            new TEndPoint("127.0.0.1", 2004));

    final Map<Integer, String> nodeStatus = new HashMap<>();
    nodeStatus.put(CONFIG_NODE_ID, NodeStatus.Running.getStatus());
    nodeStatus.put(DATA_NODE_ID, NodeStatus.ReadOnly.getStatus());

    final Map<Integer, TNodeVersionInfo> nodeVersionInfo = new HashMap<>();
    final TNodeVersionInfo versionInfo = new TNodeVersionInfo("2.0.11", "build");
    nodeVersionInfo.put(CONFIG_NODE_ID, versionInfo);
    nodeVersionInfo.put(DATA_NODE_ID, versionInfo);

    final TShowClusterResp resp =
        new TShowClusterResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            Collections.singletonList(configNode),
            Collections.singletonList(dataNode),
            Collections.emptyList(),
            nodeStatus,
            nodeVersionInfo);
    // An old ConfigNode response leaves the optional nodeStatusReason field unset.
    if (statusReasonMap != null) {
      resp.setNodeStatusReason(statusReasonMap);
    }
    return resp;
  }

  private static ConfigTaskResult execute(final TShowClusterResp resp) throws Exception {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowClusterTask.buildTsBlock(resp, future);
    return future.get();
  }

  private static List<String> columnNames(final List<ColumnHeader> headers) {
    return headers.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Test
  public void testOldLayoutWhenNoReasonFieldPresent() throws Exception {
    // A response from an old ConfigNode (or a cluster without any reason) keeps the legacy layout:
    // no StatusReason column at all.
    final ConfigTaskResult result = execute(buildClusterResp(null));
    final List<ColumnHeader> headers = result.getResultSetHeader().getColumnHeaders();

    Assert.assertEquals(
        columnNames(ColumnHeaderConstant.showClusterColumnHeaders), columnNames(headers));
    Assert.assertEquals(
        ColumnHeaderConstant.showClusterColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList()),
        result.getResultSetHeader().getRespDataTypes());
    final Column[] columns = result.getResultSet().getValueColumns();
    Assert.assertEquals(headers.size(), columns.length);

    // The Status column carries the plain status, never a merged "ReadOnly(Manual)" string.
    Assert.assertEquals(NodeStatus.Running.getStatus(), columns[2].getBinary(0).toString());
    Assert.assertEquals(NodeStatus.ReadOnly.getStatus(), columns[2].getBinary(1).toString());
  }

  @Test
  public void testReasonColumnInsertedWithNullForMissingReasons() throws Exception {
    final Map<Integer, String> statusReasonMap = new HashMap<>();
    statusReasonMap.put(DATA_NODE_ID, NodeStatus.MANUAL);
    final ConfigTaskResult result = execute(buildClusterResp(statusReasonMap));
    final List<ColumnHeader> headers = result.getResultSetHeader().getColumnHeaders();

    // The StatusReason column is dynamically inserted right after the Status column.
    Assert.assertEquals(ColumnHeaderConstant.showClusterColumnHeaders.size() + 1, headers.size());
    Assert.assertEquals(ColumnHeaderConstant.STATUS_REASON, headers.get(3).getColumnName());
    Assert.assertEquals(TSDataType.TEXT, headers.get(3).getColumnType());
    // The header, its data type list and the TsBlock column count stay consistent.
    Assert.assertEquals(
        headers.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList()),
        result.getResultSetHeader().getRespDataTypes());
    final Column[] columns = result.getResultSet().getValueColumns();
    Assert.assertEquals(headers.size(), columns.length);

    // The ConfigNode has no reason: its StatusReason cell is NULL at the right position.
    Assert.assertTrue(columns[3].isNull(0));
    // The DataNode has a reason: it shows the reason next to its plain status.
    Assert.assertFalse(columns[3].isNull(1));
    Assert.assertEquals(NodeStatus.MANUAL, columns[3].getBinary(1).toString());
    Assert.assertEquals(NodeStatus.ReadOnly.getStatus(), columns[2].getBinary(1).toString());
  }

  @Test
  public void testReasonColumnAbsentWhenAllReasonsAreEmpty() throws Exception {
    // An all-empty reason map behaves like a missing field: the legacy layout is kept.
    final Map<Integer, String> statusReasonMap = new HashMap<>();
    statusReasonMap.put(CONFIG_NODE_ID, "");
    statusReasonMap.put(DATA_NODE_ID, "");
    final ConfigTaskResult result = execute(buildClusterResp(statusReasonMap));

    Assert.assertEquals(
        columnNames(ColumnHeaderConstant.showClusterColumnHeaders),
        columnNames(result.getResultSetHeader().getColumnHeaders()));
    Assert.assertEquals(
        ColumnHeaderConstant.showClusterColumnHeaders.size(),
        result.getResultSet().getValueColumns().length);
  }
}
