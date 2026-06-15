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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeRegistry;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.CONNECTION_COUNT;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LAST_HANDSHAKE_TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.LAST_TRANSFER_TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PIPE_COUNT;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PIPE_IDS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.PROTOCOL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_NODE_ID;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_NODE_TYPE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.RECEIVER_USER_NAME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_ADDRESS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_CLUSTER_ID;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.SENDER_PORTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShowReceiversOperatorTest {

  private final PipeReceiverRuntimeRegistry registry = PipeReceiverRuntimeRegistry.getInstance();

  @Before
  public void setUp() {
    registry.clear();
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @After
  public void tearDown() {
    registry.clear();
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @Test
  public void testShowReceiversHeaderColumns() {
    assertEquals(
        ImmutableList.of(
            RECEIVER_NODE_TYPE,
            RECEIVER_NODE_ID,
            PROTOCOL,
            SENDER_ADDRESS,
            SENDER_PORTS,
            CONNECTION_COUNT,
            PIPE_COUNT,
            PIPE_IDS,
            RECEIVER_USER_NAME,
            SENDER_CLUSTER_ID,
            LAST_HANDSHAKE_TIME,
            LAST_TRANSFER_TIME),
        DatasetHeaderFactory.getShowReceiversHeader().getRespColumns());
    assertTrue(DatasetHeaderFactory.getShowReceiversHeader().isIgnoreTimestamp());
  }

  @Test
  public void testSingleSenderSingleDataNodeSingleConnectionSinglePipeOutput() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.markTransfer("data-1", 200);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals(12, tsBlock.getValueColumnCount());
    assertEquals(PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE, getText(tsBlock, 0));
    assertEquals(1, tsBlock.getColumn(1).getInt(0));
    assertEquals(PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT, getText(tsBlock, 2));
    assertEquals("10.0.0.1", getText(tsBlock, 3));
    assertEquals("9001", getText(tsBlock, 4));
    assertEquals(1, tsBlock.getColumn(5).getInt(0));
    assertEquals(1, tsBlock.getColumn(6).getInt(0));
    assertTrue(getText(tsBlock, 7).contains("pipe-a@"));
    assertEquals("root", getText(tsBlock, 8));
    assertEquals("cluster-a", getText(tsBlock, 9));
    assertEquals(DateTimeUtils.convertLongToDate(100, "ms"), getText(tsBlock, 10));
    assertEquals(DateTimeUtils.convertLongToDate(200, "ms"), getText(tsBlock, 11));
  }

  @Test
  public void testMultiplePipesFromSameSenderAreAggregatedInOutput() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.registerOrUpdateSession(
        "data-2",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9002,
        "root",
        "cluster-a",
        "pipe-b",
        2,
        200);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals("9001,9002", getText(tsBlock, 4));
    assertEquals(2, tsBlock.getColumn(5).getInt(0));
    assertEquals(2, tsBlock.getColumn(6).getInt(0));
    assertTrue(getText(tsBlock, 7).contains("pipe-a@"));
    assertTrue(getText(tsBlock, 7).contains("pipe-b@"));
    assertFalse(operator.hasNext());
  }

  @Test
  public void testLastTransferTimeUsesHandshakeTimeBeforeTransfer() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals(DateTimeUtils.convertLongToDate(100, "ms"), getText(tsBlock, 10));
    assertEquals(DateTimeUtils.convertLongToDate(100, "ms"), getText(tsBlock, 11));
  }

  @Test
  public void testUnknownReceiverNodeIdIsNull() {
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals(12, tsBlock.getValueColumnCount());
    assertTrue(tsBlock.getColumn(1).isNull(0));
  }

  @Test
  public void testNormalUserOnlySeesOwnReceiverSnapshots() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(
            null, new PlanNodeId("show-receivers"), new UserEntity(1L, "user1", "127.0.0.1"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals("10.0.0.1", getText(tsBlock, 3));
    assertEquals("user1", getText(tsBlock, 8));
    assertFalse(operator.hasNext());
  }

  @Test
  public void testRootUserSeesAllReceiverSnapshots() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(
            null,
            new PlanNodeId("show-receivers"),
            new UserEntity(
                AuthorityChecker.SUPER_USER_ID, AuthorityChecker.SUPER_USER, "127.0.0.1"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(2, tsBlock.getPositionCount());
    assertEquals("user1", getText(tsBlock, 8, 0));
    assertEquals("user2", getText(tsBlock, 8, 1));
    assertFalse(operator.hasNext());
  }

  @Test
  public void testSystemUserSeesAllReceiverSnapshots() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final User systemUser = new User("system_user", "password");
    systemUser.grantSysPrivilege(PrivilegeType.SYSTEM, false);
    AuthorityChecker.getAuthorityFetcher()
        .getAuthorCache()
        .putUserCache(systemUser.getName(), systemUser);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(
            null, new PlanNodeId("show-receivers"), new UserEntity(2L, "system_user", "127.0.0.1"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(2, tsBlock.getPositionCount());
    assertEquals("user1", getText(tsBlock, 8, 0));
    assertEquals("user2", getText(tsBlock, 8, 1));
    assertFalse(operator.hasNext());
  }

  @Test
  public void testShowReceiversOutputKeepsDefaultSnapshotOrdering() {
    registry.registerOrUpdateSession(
        "data-user-b",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.2",
        9006,
        "bob",
        "cluster-a",
        "pipe-f",
        6,
        600);
    registry.registerOrUpdateSession(
        "data-address-a",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9004,
        "root",
        "cluster-a",
        "pipe-d",
        4,
        400);
    registry.registerOrUpdateSession(
        "config-node-2",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        2,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9002,
        "root",
        "cluster-a",
        "pipe-b",
        2,
        200);
    registry.registerOrUpdateSession(
        "data-user-a",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.2",
        9005,
        "alice",
        "cluster-a",
        "pipe-e",
        5,
        500);
    registry.registerOrUpdateSession(
        "config-node-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.registerOrUpdateSession(
        "data-air-gap",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "10.0.0.2",
        9003,
        "root",
        "cluster-a",
        "pipe-c",
        3,
        300);

    final ShowReceiversOperator operator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"));

    assertTrue(operator.hasNext());
    final TsBlock tsBlock = operator.next();

    assertEquals(6, tsBlock.getPositionCount());
    assertRowKey(
        tsBlock,
        0,
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        "root");
    assertRowKey(
        tsBlock,
        1,
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        2,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        "root");
    assertRowKey(
        tsBlock,
        2,
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "10.0.0.2",
        "root");
    assertRowKey(
        tsBlock,
        3,
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.1",
        "root");
    assertRowKey(
        tsBlock,
        4,
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.2",
        "alice");
    assertRowKey(
        tsBlock,
        5,
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "10.0.0.2",
        "bob");
    assertFalse(operator.hasNext());
  }

  private void registerUserSession(
      String connectionKey,
      String senderAddress,
      int senderPort,
      String userName,
      String senderClusterId,
      String pipeName,
      long pipeCreationTime,
      long handshakeTime) {
    registry.registerOrUpdateSession(
        connectionKey,
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        senderAddress,
        senderPort,
        userName,
        senderClusterId,
        pipeName,
        pipeCreationTime,
        handshakeTime);
  }

  private static String getText(final TsBlock tsBlock, final int columnIndex) {
    return getText(tsBlock, columnIndex, 0);
  }

  private static String getText(final TsBlock tsBlock, final int columnIndex, final int position) {
    return tsBlock.getColumn(columnIndex).getBinary(position).toString();
  }

  private static void assertRowKey(
      final TsBlock tsBlock,
      final int position,
      final String receiverNodeType,
      final int receiverNodeId,
      final String protocol,
      final String senderAddress,
      final String userName) {
    assertEquals(receiverNodeType, getText(tsBlock, 0, position));
    assertEquals(receiverNodeId, tsBlock.getColumn(1).getInt(position));
    assertEquals(protocol, getText(tsBlock, 2, position));
    assertEquals(senderAddress, getText(tsBlock, 3, position));
    assertEquals(userName, getText(tsBlock, 8, position));
  }
}
