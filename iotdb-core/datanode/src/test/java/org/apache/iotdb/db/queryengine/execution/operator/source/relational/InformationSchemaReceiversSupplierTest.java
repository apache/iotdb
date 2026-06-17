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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.pipe.receiver.runtime.PipeReceiverRuntimeRegistry;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.execution.operator.source.ShowReceiversOperator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InformationSchemaReceiversSupplierTest {

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
  public void testReceiversInformationSchemaTableDefinition() {
    final List<TsTableColumnSchema> columns = receiversTable().getColumnList();

    assertEquals(12, columns.size());
    assertColumn(
        columns.get(0), "receiver_node_type", TSDataType.STRING, TsTableColumnCategory.TAG);
    assertColumn(columns.get(1), "receiver_node_id", TSDataType.INT32, TsTableColumnCategory.TAG);
    assertColumn(columns.get(2), "protocol", TSDataType.STRING, TsTableColumnCategory.TAG);
    assertColumn(columns.get(3), "sender_address", TSDataType.STRING, TsTableColumnCategory.TAG);
    assertColumn(
        columns.get(4), "sender_ports", TSDataType.STRING, TsTableColumnCategory.ATTRIBUTE);
    assertColumn(
        columns.get(5), "connection_count", TSDataType.INT32, TsTableColumnCategory.ATTRIBUTE);
    assertColumn(columns.get(6), "pipe_count", TSDataType.INT32, TsTableColumnCategory.ATTRIBUTE);
    assertColumn(columns.get(7), "pipe_ids", TSDataType.STRING, TsTableColumnCategory.ATTRIBUTE);
    assertColumn(columns.get(8), "user_name", TSDataType.STRING, TsTableColumnCategory.TAG);
    assertColumn(
        columns.get(9), "sender_cluster_id", TSDataType.STRING, TsTableColumnCategory.ATTRIBUTE);
    assertColumn(
        columns.get(10),
        "last_handshake_time",
        TSDataType.TIMESTAMP,
        TsTableColumnCategory.ATTRIBUTE);
    assertColumn(
        columns.get(11),
        "last_transfer_time",
        TSDataType.TIMESTAMP,
        TsTableColumnCategory.ATTRIBUTE);
  }

  @Test
  public void testReceiversSupplierBuildsTableModelRows() {
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

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), rootUser(), receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(1, tsBlock.getPositionCount());
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
    assertEquals(100L, tsBlock.getColumn(10).getLong(0));
    assertEquals(200L, tsBlock.getColumn(11).getLong(0));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierUsesHandshakeTimeAsInitialTransferTime() {
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

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), rootUser(), receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals(100L, tsBlock.getColumn(10).getLong(0));
    assertEquals(100L, tsBlock.getColumn(11).getLong(0));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierWritesUnknownFieldsAsNulls() {
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        -1,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "10.0.0.2",
        9002,
        "root",
        "cluster-b",
        null,
        Long.MIN_VALUE,
        0);

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), rootUser(), receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals(PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE, getText(tsBlock, 0));
    assertTrue(tsBlock.getColumn(1).isNull(0));
    assertEquals(PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP, getText(tsBlock, 2));
    assertEquals(0, tsBlock.getColumn(6).getInt(0));
    assertEquals(PipeReceiverRuntimeRegistry.UNKNOWN, getText(tsBlock, 7));
    assertTrue(tsBlock.getColumn(10).isNull(0));
    assertTrue(tsBlock.getColumn(11).isNull(0));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierFiltersByNormalUser() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), new UserEntity(1L, "user1", "127.0.0.1"), receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(1, tsBlock.getPositionCount());
    assertEquals("10.0.0.1", getText(tsBlock, 3));
    assertEquals("user1", getText(tsBlock, 8));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierRootSeesAllReceiverSnapshots() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), rootUser(), receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(2, tsBlock.getPositionCount());
    assertEquals("user1", getText(tsBlock, 8, 0));
    assertEquals("user2", getText(tsBlock, 8, 1));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierSystemUserSeesAllReceiverSnapshots() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final User systemUser = new User("system_user", "password");
    systemUser.grantSysPrivilege(PrivilegeType.SYSTEM, false);
    AuthorityChecker.getAuthorityFetcher()
        .getAuthorCache()
        .putUserCache(systemUser.getName(), systemUser);

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), new UserEntity(2L, "system_user", "127.0.0.1"), receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(2, tsBlock.getPositionCount());
    assertEquals("user1", getText(tsBlock, 8, 0));
    assertEquals("user2", getText(tsBlock, 8, 1));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierNullUserEntitySeesAllReceiverSnapshots() {
    registerUserSession("data-user1", "10.0.0.1", 9001, "user1", "cluster-a", "pipe-a", 1, 100);
    registerUserSession("data-user2", "10.0.0.2", 9002, "user2", "cluster-b", "pipe-b", 2, 200);

    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), null, receiversScanNode());

    assertTrue(supplier.hasNext());
    final TsBlock tsBlock = supplier.next();

    assertEquals(2, tsBlock.getPositionCount());
    assertEquals("user1", getText(tsBlock, 8, 0));
    assertEquals("user2", getText(tsBlock, 8, 1));
    assertFalse(supplier.hasNext());
  }

  @Test
  public void testReceiversSupplierMatchesShowReceiversOutput() {
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

    final ShowReceiversOperator showReceiversOperator =
        new ShowReceiversOperator(null, new PlanNodeId("show-receivers"), rootUser());
    assertTrue(showReceiversOperator.hasNext());
    final TsBlock showReceiversTsBlock = showReceiversOperator.next();
    final InformationSchemaContentSupplierFactory.IInformationSchemaContentSupplier supplier =
        InformationSchemaContentSupplierFactory.getSupplier(
            null, dataTypes(), rootUser(), receiversScanNode());
    assertTrue(supplier.hasNext());
    final TsBlock informationSchemaTsBlock = supplier.next();

    assertEquals(
        showReceiversTsBlock.getPositionCount(), informationSchemaTsBlock.getPositionCount());
    assertEquals(1, informationSchemaTsBlock.getPositionCount());
    for (int columnIndex : new int[] {0, 2, 3, 4, 7, 8, 9}) {
      assertEquals(
          getText(showReceiversTsBlock, columnIndex),
          getText(informationSchemaTsBlock, columnIndex));
    }
    assertEquals(
        showReceiversTsBlock.getColumn(1).getInt(0),
        informationSchemaTsBlock.getColumn(1).getInt(0));
    assertEquals(
        showReceiversTsBlock.getColumn(5).getInt(0),
        informationSchemaTsBlock.getColumn(5).getInt(0));
    assertEquals(
        showReceiversTsBlock.getColumn(6).getInt(0),
        informationSchemaTsBlock.getColumn(6).getInt(0));
    assertFalse(supplier.hasNext());
  }

  private static InformationSchemaTableScanNode receiversScanNode() {
    final List<Symbol> outputSymbols = new ArrayList<>();
    final Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    for (TsTableColumnSchema columnSchema : receiversTable().getColumnList()) {
      final Symbol symbol = new Symbol(columnSchema.getColumnName());
      outputSymbols.add(symbol);
      assignments.put(
          symbol,
          new ColumnSchema(
              columnSchema.getColumnName(),
              TypeFactory.getType(columnSchema.getDataType()),
              false,
              columnSchema.getColumnCategory()));
    }
    return new InformationSchemaTableScanNode(
        new PlanNodeId("information-schema-receivers"),
        new QualifiedObjectName(
            InformationSchema.INFORMATION_DATABASE, InformationSchema.RECEIVERS),
        outputSymbols,
        assignments);
  }

  private static List<TSDataType> dataTypes() {
    final List<TSDataType> dataTypes = new ArrayList<>();
    for (TsTableColumnSchema columnSchema : receiversTable().getColumnList()) {
      dataTypes.add(columnSchema.getDataType());
    }
    return dataTypes;
  }

  private static TsTable receiversTable() {
    return InformationSchema.getSchemaTables().get(InformationSchema.RECEIVERS);
  }

  private static void assertColumn(
      final TsTableColumnSchema column,
      final String columnName,
      final TSDataType dataType,
      final TsTableColumnCategory columnCategory) {
    assertEquals(columnName, column.getColumnName());
    assertEquals(dataType, column.getDataType());
    assertEquals(columnCategory, column.getColumnCategory());
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

  private static UserEntity rootUser() {
    return new UserEntity(AuthorityChecker.SUPER_USER_ID, AuthorityChecker.SUPER_USER, "127.0.0.1");
  }

  private static String getText(final TsBlock tsBlock, final int columnIndex) {
    return getText(tsBlock, columnIndex, 0);
  }

  private static String getText(final TsBlock tsBlock, final int columnIndex, final int position) {
    return tsBlock.getColumn(columnIndex).getBinary(position).toString();
  }
}
