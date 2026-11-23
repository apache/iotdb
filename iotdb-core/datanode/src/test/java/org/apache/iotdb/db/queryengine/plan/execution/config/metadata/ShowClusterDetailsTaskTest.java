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

import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
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

public class ShowClusterDetailsTaskTest {

  private static final int CONFIG_NODE_ID = 0;
  private static final int DATA_NODE_ID = 1;
  private static final int AI_NODE_ID = 2;

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
    final TAINodeLocation aiNode =
        new TAINodeLocation(AI_NODE_ID, new TEndPoint("127.0.0.1", 3000));

    final Map<Integer, String> nodeStatus = new HashMap<>();
    nodeStatus.put(CONFIG_NODE_ID, NodeStatus.Running.getStatus());
    nodeStatus.put(DATA_NODE_ID, NodeStatus.ReadOnly.getStatus());
    nodeStatus.put(AI_NODE_ID, NodeStatus.Running.getStatus());

    final Map<Integer, TNodeVersionInfo> nodeVersionInfo = new HashMap<>();
    final TNodeVersionInfo versionInfo = new TNodeVersionInfo("2.0.11", "build");
    nodeVersionInfo.put(CONFIG_NODE_ID, versionInfo);
    nodeVersionInfo.put(DATA_NODE_ID, versionInfo);
    nodeVersionInfo.put(AI_NODE_ID, versionInfo);

    final TShowClusterResp resp =
        new TShowClusterResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
            Collections.singletonList(configNode),
            Collections.singletonList(dataNode),
            Collections.singletonList(aiNode),
            nodeStatus,
            nodeVersionInfo);
    if (statusReasonMap != null) {
      resp.setNodeStatusReason(statusReasonMap);
    }
    return resp;
  }

  private static ConfigTaskResult execute(final TShowClusterResp resp) throws Exception {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowClusterDetailsTask.buildTSBlock(resp, future);
    return future.get();
  }

  private static List<String> columnNames(final List<ColumnHeader> headers) {
    return headers.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Test
  public void testOldLayoutWhenNoReasonFieldPresent() throws Exception {
    final ConfigTaskResult result = execute(buildClusterResp(null));
    final List<ColumnHeader> headers = result.getResultSetHeader().getColumnHeaders();

    Assert.assertEquals(
        columnNames(ColumnHeaderConstant.showClusterDetailsColumnHeaders), columnNames(headers));
    Assert.assertEquals(
        ColumnHeaderConstant.showClusterDetailsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList()),
        result.getResultSetHeader().getRespDataTypes());
    final Column[] columns = result.getResultSet().getValueColumns();
    Assert.assertEquals(headers.size(), columns.length);

    Assert.assertEquals(NodeStatus.Running.getStatus(), columns[2].getBinary(0).toString());
    Assert.assertEquals(NodeStatus.ReadOnly.getStatus(), columns[2].getBinary(1).toString());
    Assert.assertEquals("2002", columns[8].getBinary(1).toString());
    Assert.assertEquals("2004", columns[9].getBinary(1).toString());
    Assert.assertEquals("2003", columns[10].getBinary(1).toString());
    Assert.assertEquals(AI_NODE_ID, columns[0].getInt(2));
    Assert.assertEquals(3000, columns[4].getInt(2));
    Assert.assertEquals("build", columns[12].getBinary(2).toString());
  }

  @Test
  public void testReasonColumnAppendedWithNullForMissingReasons() throws Exception {
    final Map<Integer, String> statusReasonMap = new HashMap<>();
    statusReasonMap.put(DATA_NODE_ID, NodeStatus.DISK_FULL);
    final ConfigTaskResult result = execute(buildClusterResp(statusReasonMap));
    final List<ColumnHeader> headers = result.getResultSetHeader().getColumnHeaders();

    Assert.assertEquals(
        ColumnHeaderConstant.showClusterDetailsColumnHeaders.size() + 1, headers.size());
    Assert.assertEquals(
        columnNames(ColumnHeaderConstant.showClusterDetailsColumnHeaders),
        columnNames(headers.subList(0, 13)));
    Assert.assertEquals(ColumnHeaderConstant.STATUS_REASON, headers.get(13).getColumnName());
    Assert.assertEquals(TSDataType.TEXT, headers.get(13).getColumnType());
    Assert.assertEquals(
        headers.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList()),
        result.getResultSetHeader().getRespDataTypes());
    final Column[] columns = result.getResultSet().getValueColumns();
    Assert.assertEquals(headers.size(), columns.length);

    Assert.assertTrue(columns[13].isNull(0));
    Assert.assertFalse(columns[13].isNull(1));
    Assert.assertEquals(NodeStatus.DISK_FULL, columns[13].getBinary(1).toString());
    Assert.assertEquals(NodeStatus.ReadOnly.getStatus(), columns[2].getBinary(1).toString());
    Assert.assertEquals("127.0.0.1", columns[3].getBinary(1).toString());
    Assert.assertEquals(2001, columns[4].getInt(1));
    Assert.assertEquals("1001", columns[5].getBinary(0).toString());
    Assert.assertEquals("", columns[6].getBinary(0).toString());
    Assert.assertEquals("127.0.0.1", columns[6].getBinary(1).toString());
    Assert.assertEquals("2000", columns[7].getBinary(1).toString());
    Assert.assertEquals("2002", columns[8].getBinary(1).toString());
    Assert.assertEquals("2004", columns[9].getBinary(1).toString());
    Assert.assertEquals("2003", columns[10].getBinary(1).toString());
    Assert.assertEquals("2.0.11", columns[11].getBinary(1).toString());
    Assert.assertEquals("build", columns[12].getBinary(1).toString());
    Assert.assertEquals(AI_NODE_ID, columns[0].getInt(2));
    Assert.assertEquals("AINode", columns[1].getBinary(2).toString());
    Assert.assertEquals("127.0.0.1", columns[3].getBinary(2).toString());
    Assert.assertEquals(3000, columns[4].getInt(2));
    Assert.assertEquals("", columns[5].getBinary(2).toString());
    Assert.assertEquals("2.0.11", columns[11].getBinary(2).toString());
    Assert.assertEquals("build", columns[12].getBinary(2).toString());
    Assert.assertTrue(columns[13].isNull(2));
  }
}
