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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShowDataNodesTaskTest {

  private static final int DATA_NODE_WITH_REASON_ID = 1;
  private static final int DATA_NODE_WITHOUT_REASON_ID = 2;

  private static TShowDataNodesResp buildResp(final boolean setReasonOnFirstNode) {
    final TDataNodeInfo dataNodeWithReason =
        new TDataNodeInfo(
            DATA_NODE_WITH_REASON_ID, NodeStatus.ReadOnly.getStatus(), "127.0.0.1", 6667, 1, 1);
    if (setReasonOnFirstNode) {
      dataNodeWithReason.setStatusReason(NodeStatus.MANUAL);
    }
    final TDataNodeInfo dataNodeWithoutReason =
        new TDataNodeInfo(
            DATA_NODE_WITHOUT_REASON_ID, NodeStatus.Running.getStatus(), "127.0.0.1", 6668, 1, 1);
    return new TShowDataNodesResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
        .setDataNodesInfoList(Arrays.asList(dataNodeWithReason, dataNodeWithoutReason));
  }

  private static ConfigTaskResult execute(final TShowDataNodesResp resp) throws Exception {
    final SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowDataNodesTask.buildTSBlock(resp, future);
    return future.get();
  }

  private static List<String> columnNames(final List<ColumnHeader> headers) {
    return headers.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Test
  public void testOldLayoutWhenNoReasonIsSet() throws Exception {
    // Responses from an old ConfigNode never set the per-node statusReason field.
    final ConfigTaskResult result = execute(buildResp(false));
    final List<ColumnHeader> headers = result.getResultSetHeader().getColumnHeaders();

    Assert.assertEquals(
        columnNames(ColumnHeaderConstant.showDataNodesColumnHeaders), columnNames(headers));
    final Column[] columns = result.getResultSet().getValueColumns();
    Assert.assertEquals(headers.size(), columns.length);

    // The Status column carries the plain status, never a merged "ReadOnly(Manual)" string.
    Assert.assertEquals(NodeStatus.ReadOnly.getStatus(), columns[1].getBinary(0).toString());
    Assert.assertEquals(NodeStatus.Running.getStatus(), columns[1].getBinary(1).toString());
  }

  @Test
  public void testReasonColumnInsertedWithNullForMissingReasons() throws Exception {
    final ConfigTaskResult result = execute(buildResp(true));
    final List<ColumnHeader> headers = result.getResultSetHeader().getColumnHeaders();

    Assert.assertEquals(ColumnHeaderConstant.showDataNodesColumnHeaders.size() + 1, headers.size());
    Assert.assertEquals(ColumnHeaderConstant.STATUS_REASON, headers.get(2).getColumnName());
    Assert.assertEquals(TSDataType.TEXT, headers.get(2).getColumnType());
    Assert.assertEquals(
        headers.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList()),
        result.getResultSetHeader().getRespDataTypes());
    final Column[] columns = result.getResultSet().getValueColumns();
    Assert.assertEquals(headers.size(), columns.length);

    // The first DataNode has the reason; the second keeps a NULL at the same position.
    Assert.assertFalse(columns[2].isNull(0));
    Assert.assertEquals(NodeStatus.MANUAL, columns[2].getBinary(0).toString());
    Assert.assertTrue(columns[2].isNull(1));
    Assert.assertEquals(NodeStatus.ReadOnly.getStatus(), columns[1].getBinary(0).toString());
  }
}
