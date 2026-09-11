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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.table.ColumnInDeletionException;
import org.apache.iotdb.commons.exception.table.TableInDeletionException;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.table.DescTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.table.FetchTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.table.ShowTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreAlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaQuotaStatistics;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TablePreDeleteTest {
  private static final String DATABASE = "root.pre_delete_test";
  private static final String TABLE = "table1";
  private ClusterSchemaInfo schemaInfo;
  private ClusterSchemaManager schemaManager;

  @Before
  public void setUp() throws Exception {
    schemaInfo = new ClusterSchemaInfo();
    schemaManager =
        new ClusterSchemaManager(
            Mockito.mock(IManager.class),
            schemaInfo,
            Mockito.mock(ClusterSchemaQuotaStatistics.class));
    assertSuccess(
        schemaInfo.createDatabase(
            new DatabaseSchemaPlan(
                ConfigPhysicalPlanType.CreateDatabase,
                new TDatabaseSchema(DATABASE).setIsTableModel(true))));
    createTable(TABLE);
  }

  @After
  public void tearDown() {
    schemaInfo.clear();
  }

  @Test
  public void testShowTablesIncludesPreDeleteButNotPreCreate() {
    createTable("using_table");
    assertSuccess(
        schemaInfo.preCreateTable(new PreCreateTablePlan(DATABASE, new TsTable("creating"))));
    assertSuccess(schemaInfo.preDeleteTable(new PreDeleteTablePlan(DATABASE, TABLE)));

    final TShowTableResp basic =
        schemaInfo.showTables(new ShowTablePlan(DATABASE, false)).convertToTShowTableResp();
    assertSuccess(basic.getStatus());
    assertEquals(
        Arrays.asList(TABLE, "using_table"),
        basic.getTableInfoList().stream()
            .map(TTableInfo::getTableName)
            .sorted()
            .collect(Collectors.toList()));

    final TShowTableResp details =
        schemaInfo.showTables(new ShowTablePlan(DATABASE, true)).convertToTShowTableResp();
    final Map<String, Integer> states =
        details.getTableInfoList().stream()
            .collect(Collectors.toMap(TTableInfo::getTableName, TTableInfo::getState));
    assertEquals(Integer.valueOf(TableNodeStatus.PRE_DELETE.ordinal()), states.get(TABLE));
    assertEquals(Integer.valueOf(TableNodeStatus.PRE_CREATE.ordinal()), states.get("creating"));
    assertEquals(Integer.valueOf(TableNodeStatus.USING.ordinal()), states.get("using_table"));

    assertSuccess(schemaInfo.dropTable(new CommitDeleteTablePlan(DATABASE, TABLE)));
    assertEquals(
        Collections.singletonList("using_table"),
        schemaInfo
            .showTables(new ShowTablePlan(DATABASE, false))
            .convertToTShowTableResp()
            .getTableInfoList()
            .stream()
            .map(TTableInfo::getTableName)
            .collect(Collectors.toList()));
  }

  @Test
  public void testDescRetainsPreDeletedColumnsAndAlteredTypes() {
    assertSuccess(schemaInfo.preDeleteColumn(new PreDeleteColumnPlan(DATABASE, TABLE, "field")));
    assertSuccess(
        schemaInfo.preDeleteColumn(new PreDeleteColumnPlan(DATABASE, TABLE, "attribute")));
    assertSuccess(
        schemaInfo.preAlterColumnDataType(
            new PreAlterColumnDataTypePlan(DATABASE, TABLE, "live", TSDataType.INT64)));

    TDescTableResp basic = describe(false);
    TsTable table = TsTableInternalRPCUtil.deserializeSingleTsTable(basic.getTableInfo());
    assertNotNull(table.getColumnSchema("field"));
    assertNotNull(table.getColumnSchema("attribute"));
    assertEquals(TSDataType.INT64, table.getColumnSchema("live").getDataType());
    assertFalse(basic.isSetPreDeletedColumns());

    final TDescTableResp details = describe(true);
    assertTrue(details.getPreDeletedColumns().containsAll(Arrays.asList("field", "attribute")));
    assertEquals(
        Byte.valueOf(TSDataType.INT64.serialize()), details.getPreAlteredColumns().get("live"));

    assertSuccess(
        schemaInfo.commitDeleteColumn(new CommitDeleteColumnPlan(DATABASE, TABLE, "field")));
    table = TsTableInternalRPCUtil.deserializeSingleTsTable(describe(false).getTableInfo());
    assertNull(table.getColumnSchema("field"));

    assertSuccess(schemaInfo.preDeleteTable(new PreDeleteTablePlan(DATABASE, TABLE)));
    assertNotNull(
        TsTableInternalRPCUtil.deserializeSingleTsTable(describe(false).getTableInfo())
            .getColumnSchema("attribute"));
  }

  @Test
  public void testColumnExtensionRejectsPreDeletedNames() throws Exception {
    for (final String column : Arrays.asList("field", "attribute")) {
      assertSuccess(schemaInfo.preDeleteColumn(new PreDeleteColumnPlan(DATABASE, TABLE, column)));
      final List<TsTableColumnSchema> columns =
          new ArrayList<>(Arrays.asList(field("new_field"), field(column)));
      final ColumnInDeletionException exception =
          assertThrows(
              ColumnInDeletionException.class,
              () ->
                  schemaManager.tableColumnCheckForColumnExtension(
                      DATABASE, TABLE, columns, false));
      assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), exception.getErrorCode());
      assertEquals(
          new ColumnInDeletionException(DATABASE, TABLE, column).getMessage(),
          exception.getMessage());
      assertEquals(2, columns.size());
      assertNull(
          schemaInfo
              .getTsTableIfExists(DATABASE, TABLE)
              .get()
              .getLeft()
              .getColumnSchema("new_field"));
    }
    assertEquals(
        TSStatusCode.COLUMN_ALREADY_EXISTS.getStatusCode(),
        schemaManager
            .tableColumnCheckForColumnExtension(
                DATABASE, TABLE, new ArrayList<>(Collections.singletonList(field("live"))), false)
            .getLeft()
            .getCode());

    assertSuccess(
        schemaInfo.commitDeleteColumn(new CommitDeleteColumnPlan(DATABASE, TABLE, "field")));
    assertSuccess(
        schemaManager
            .tableColumnCheckForColumnExtension(
                DATABASE, TABLE, new ArrayList<>(Collections.singletonList(field("field"))), false)
            .getLeft());
  }

  @Test
  public void testPreDeletedTableRejectsCreationAndColumnExtension() {
    assertSuccess(schemaInfo.preDeleteTable(new PreDeleteTablePlan(DATABASE, TABLE)));
    final TSStatus create =
        schemaInfo.preCreateTable(new PreCreateTablePlan(DATABASE, new TsTable(TABLE)));
    assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), create.getCode());
    assertEquals(new TableInDeletionException(DATABASE, TABLE).getMessage(), create.getMessage());
    final TableInDeletionException exception =
        assertThrows(
            TableInDeletionException.class,
            () ->
                schemaManager.tableColumnCheckForColumnExtension(
                    DATABASE,
                    TABLE,
                    new ArrayList<>(Collections.singletonList(field("new_field"))),
                    false));
    assertEquals(create.getMessage(), exception.getMessage());
  }

  @Test
  public void testDataNodeSchemasExcludePreDeletedColumns() throws Exception {
    assertSuccess(schemaInfo.preDeleteColumn(new PreDeleteColumnPlan(DATABASE, TABLE, "field")));
    assertSuccess(
        schemaInfo.preDeleteColumn(new PreDeleteColumnPlan(DATABASE, TABLE, "attribute")));

    assertOnlyLiveColumns(schemaInfo.getAllUsingTables().get(DATABASE).get(0));
    final TsTable fetched =
        TsTableInternalRPCUtil.deserializeTsTableFetchResult(
                schemaInfo
                    .fetchTables(
                        new FetchTablePlan(
                            Collections.singletonMap(DATABASE, Collections.singleton(TABLE)),
                            Collections.singleton(TableNodeStatus.USING)))
                    .convertToTFetchTableResp()
                    .getTableInfoMap())
            .get(DATABASE)
            .get(TABLE);
    assertOnlyLiveColumns(fetched);

    final TsTable expanded =
        schemaManager
            .tableColumnCheckForColumnExtension(
                DATABASE,
                TABLE,
                new ArrayList<>(Collections.singletonList(field("new_field"))),
                false)
            .getRight();
    assertOnlyLiveColumns(expanded);
    assertNotNull(expanded.getColumnSchema("new_field"));

    // Preparing a cache snapshot must not change the complete schema used by DESC.
    final TsTable described =
        TsTableInternalRPCUtil.deserializeSingleTsTable(describe(false).getTableInfo());
    assertNotNull(described.getColumnSchema("field"));
    assertNotNull(described.getColumnSchema("attribute"));
  }

  private static void assertOnlyLiveColumns(final TsTable table) {
    assertNotNull(table.getColumnSchema("live"));
    assertNull(table.getColumnSchema("field"));
    assertNull(table.getColumnSchema("attribute"));
  }

  private TDescTableResp describe(final boolean details) {
    final TDescTableResp resp =
        schemaInfo.descTable(new DescTablePlan(DATABASE, TABLE, details)).convertToTDescTableResp();
    assertSuccess(resp.getStatus());
    return resp;
  }

  private void createTable(final String name) {
    final TsTable table = new TsTable(name);
    table.addColumnSchema(field("field"));
    table.addColumnSchema(field("live"));
    table.addColumnSchema(new AttributeColumnSchema("attribute", TSDataType.STRING));
    assertSuccess(schemaInfo.preCreateTable(new PreCreateTablePlan(DATABASE, table)));
    assertSuccess(schemaInfo.commitCreateTable(new CommitCreateTablePlan(DATABASE, name)));
  }

  private static FieldColumnSchema field(final String name) {
    return new FieldColumnSchema(name, TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4);
  }

  private static void assertSuccess(final TSStatus status) {
    assertEquals(
        status.getMessage(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }
}
