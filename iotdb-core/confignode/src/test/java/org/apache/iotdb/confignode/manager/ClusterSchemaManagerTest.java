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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.commons.exception.table.ColumnInAlterException;
import org.apache.iotdb.commons.exception.table.ColumnInDeletionException;
import org.apache.iotdb.commons.exception.table.TableInDeletionException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreAlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaQuotaStatistics;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSchemaManagerTest {

  private static final String DATABASE = "root.pending_column_test";
  private static final String TABLE = "table1";

  @Test
  public void testCalcMaxRegionGroupNum() {

    // The maxRegionGroupNum should be great or equal to the leastRegionGroupNum
    Assert.assertEquals(100, ClusterSchemaManager.calcMaxRegionGroupNum(100, 1.0, 3, 1, 3, 0));

    // The maxRegionGroupNum should be great or equal to the allocatedRegionGroupCount
    Assert.assertEquals(100, ClusterSchemaManager.calcMaxRegionGroupNum(3, 1.0, 6, 2, 3, 100));

    // (resourceWeight * resource) / (createdStorageGroupNum * replicationFactor)
    Assert.assertEquals(20, ClusterSchemaManager.calcMaxRegionGroupNum(3, 1.0, 120, 2, 3, 5));
  }

  @Test
  public void testNeedLastCacheDefaultsToTrueWhenUnset() {
    final TDatabaseSchema unsetSchema = new TDatabaseSchema();
    Assert.assertTrue(ClusterSchemaManager.isNeedLastCacheEnabled(unsetSchema));

    final TDatabaseSchema enabledSchema = new TDatabaseSchema();
    enabledSchema.setNeedLastCache(true);
    Assert.assertTrue(ClusterSchemaManager.isNeedLastCacheEnabled(enabledSchema));

    final TDatabaseSchema disabledSchema = new TDatabaseSchema();
    disabledSchema.setNeedLastCache(false);
    Assert.assertFalse(ClusterSchemaManager.isNeedLastCacheEnabled(disabledSchema));
  }

  @Test
  public void testGetAllTableInfoForDataNodeActivationWithDeletedDatabase() {
    final IManager configManager = Mockito.mock(IManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    final ClusterSchemaInfo clusterSchemaInfo = Mockito.mock(ClusterSchemaInfo.class);

    Mockito.when(configManager.getProcedureManager()).thenReturn(procedureManager);
    Mockito.when(procedureManager.getAllExecutingTables())
        .thenReturn(Collections.singletonMap("test", null));
    Mockito.when(clusterSchemaInfo.getAllUsingTables()).thenReturn(new HashMap<>());
    Mockito.when(clusterSchemaInfo.getAllPreDeleteTables()).thenReturn(new HashMap<>());
    Mockito.when(clusterSchemaInfo.getAllPreCreateTables()).thenReturn(new HashMap<>());

    final ClusterSchemaManager clusterSchemaManager =
        new ClusterSchemaManager(
            configManager, clusterSchemaInfo, Mockito.mock(ClusterSchemaQuotaStatistics.class));

    final Pair<Map<String, List<TsTable>>, Map<String, List<TsTable>>> tableInfo =
        TsTableInternalRPCUtil.deserializeTableInitializationInfo(
            clusterSchemaManager.getAllTableInfoForDataNodeActivation());

    Assert.assertTrue(tableInfo.left.isEmpty());
    Assert.assertEquals(Collections.singleton("test"), tableInfo.right.keySet());
    Assert.assertTrue(tableInfo.right.get("test").isEmpty());
  }

  @Test
  public void testGetTableWithUsingStatusIfExists() throws Exception {
    final String database = "root.pre_delete_manager_test";
    final String table = "table1";
    final ClusterSchemaInfo clusterSchemaInfo = new ClusterSchemaInfo();
    final ClusterSchemaManager clusterSchemaManager = managerOf(clusterSchemaInfo);
    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase,
            new TDatabaseSchema(database).setIsTableModel(true)));

    // A missing table yields an empty result instead of an exception.
    Assert.assertFalse(
        clusterSchemaManager.getTableWithUsingStatusIfExists(database, table).isPresent());

    clusterSchemaInfo.preCreateTable(new PreCreateTablePlan(database, new TsTable(table)));
    clusterSchemaInfo.commitCreateTable(new CommitCreateTablePlan(database, table));

    // A table in the using status is returned.
    Assert.assertTrue(
        clusterSchemaManager.getTableWithUsingStatusIfExists(database, table).isPresent());

    clusterSchemaInfo.preDeleteTable(new PreDeleteTablePlan(database, table));

    // A table in the pre-delete status is rejected with the dedicated status code.
    final TableInDeletionException exception =
        Assert.assertThrows(
            TableInDeletionException.class,
            () -> clusterSchemaManager.getTableWithUsingStatusIfExists(database, table));
    Assert.assertEquals(TSStatusCode.TABLE_IN_PRE_DELETE.getStatusCode(), exception.getErrorCode());
  }

  @Test
  public void testTableChecksRejectTableInPreDelete() throws Exception {
    final String database = "root.pre_delete_manager_test";
    final String table = "table1";
    final ClusterSchemaInfo clusterSchemaInfo = new ClusterSchemaInfo();
    final ClusterSchemaManager clusterSchemaManager = managerOf(clusterSchemaInfo);
    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase,
            new TDatabaseSchema(database).setIsTableModel(true)));
    clusterSchemaInfo.preCreateTable(new PreCreateTablePlan(database, new TsTable(table)));
    clusterSchemaInfo.commitCreateTable(new CommitCreateTablePlan(database, table));
    clusterSchemaInfo.preDeleteTable(new PreDeleteTablePlan(database, table));

    // Every check that guards a table procedure must reject the table, so that no procedure keeps
    // modifying a table that is being deleted.
    Assert.assertThrows(
        TableInDeletionException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnExtension(
                database, table, new ArrayList<>(), false));
    Assert.assertThrows(
        TableInDeletionException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnAltering(
                database, table, "field", TSDataType.INT32, false));
    Assert.assertThrows(
        TableInDeletionException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnRenaming(
                database, table, "field", "field2", false));
    Assert.assertThrows(
        TableInDeletionException.class,
        () -> clusterSchemaManager.tableCheckForRenaming(database, table, "table2", false));
    Assert.assertThrows(
        TableInDeletionException.class,
        () ->
            clusterSchemaManager.updateTableProperties(
                database, table, new HashMap<>(), new HashMap<>(), false));
  }

  @Test
  public void testAddColumnRejectsPendingColumnNames() throws Exception {
    final ClusterSchemaManager clusterSchemaManager = prepareTableWithPendingColumns();

    // A name whose deletion has not finished, and a name whose data type change has not finished,
    // are both rejected by the entry point the ADD COLUMN procedure uses.
    Assert.assertThrows(
        ColumnInDeletionException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnExtension(
                DATABASE, TABLE, columns("deleting"), false));
    Assert.assertThrows(
        ColumnInAlterException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnExtension(
                DATABASE, TABLE, columns("altering"), false));
    // A name that no pending column takes can still be added.
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        clusterSchemaManager
            .tableColumnCheckForColumnExtension(DATABASE, TABLE, columns("added"), false)
            .getLeft()
            .getCode());
  }

  @Test
  public void testAlterColumnTypeRejectsDeletingColumn() throws Exception {
    final ClusterSchemaManager clusterSchemaManager = prepareTableWithPendingColumns();

    // Changing the data type of a column whose deletion has not finished is rejected.
    Assert.assertThrows(
        ColumnInDeletionException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnAltering(
                DATABASE, TABLE, "deleting", TSDataType.INT64, false));
    // A column that already has a pending data type change may be changed again. Only the check is
    // used here, because the entry point above writes the new pending type through the consensus
    // layer, which a unit test does not provide.
    Assert.assertTrue(
        clusterSchemaManager
            .checkTableAndColumnForAltering(DATABASE, TABLE, "altering")
            .isPresent());
  }

  @Test
  public void testRenameColumnRejectsPendingNames() throws Exception {
    final ClusterSchemaManager clusterSchemaManager = prepareTableWithPendingColumns();

    // The entry point the RENAME COLUMN procedure uses rejects an old name that is being deleted,
    // and a new name that a pending column already takes.
    Assert.assertThrows(
        ColumnInDeletionException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnRenaming(
                DATABASE, TABLE, "deleting", "renamed", false));
    Assert.assertThrows(
        ColumnInAlterException.class,
        () ->
            clusterSchemaManager.tableColumnCheckForColumnRenaming(
                DATABASE, TABLE, "live", "altering", false));
    // Renaming between two names that no pending column takes is allowed.
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        clusterSchemaManager
            .tableColumnCheckForColumnRenaming(DATABASE, TABLE, "live", "added", false)
            .getLeft()
            .getCode());
  }

  /** A table whose {@code deleting} column is being deleted and {@code altering} column altered. */
  private static ClusterSchemaManager prepareTableWithPendingColumns() throws Exception {
    final ClusterSchemaInfo clusterSchemaInfo = new ClusterSchemaInfo();
    clusterSchemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase,
            new TDatabaseSchema(DATABASE).setIsTableModel(true)));
    final TsTable table = new TsTable(TABLE);
    table.addColumnSchema(field("live"));
    table.addColumnSchema(field("deleting"));
    table.addColumnSchema(field("altering"));
    clusterSchemaInfo.preCreateTable(new PreCreateTablePlan(DATABASE, table));
    clusterSchemaInfo.commitCreateTable(new CommitCreateTablePlan(DATABASE, TABLE));
    clusterSchemaInfo.preDeleteColumn(new PreDeleteColumnPlan(DATABASE, TABLE, "deleting"));
    clusterSchemaInfo.preAlterColumnDataType(
        new PreAlterColumnDataTypePlan(DATABASE, TABLE, "altering", TSDataType.INT64));
    return managerOf(clusterSchemaInfo);
  }

  private static List<TsTableColumnSchema> columns(final String... columnNames) {
    final List<TsTableColumnSchema> columnSchemaList = new ArrayList<>();
    for (final String columnName : columnNames) {
      columnSchemaList.add(field(columnName));
    }
    return columnSchemaList;
  }

  private static FieldColumnSchema field(final String columnName) {
    return new FieldColumnSchema(columnName, TSDataType.INT32, TSEncoding.RLE, CompressionType.LZ4);
  }

  private static ClusterSchemaManager managerOf(final ClusterSchemaInfo clusterSchemaInfo) {
    return new ClusterSchemaManager(
        Mockito.mock(IManager.class),
        clusterSchemaInfo,
        Mockito.mock(ClusterSchemaQuotaStatistics.class));
  }
}
