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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.WritableViewInsertRewriteSupport;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.WritableViewSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.getChildrenNode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WritableViewWriteRewriteTest {

  private static final String DATABASE = "writable_view_db";
  private static final String SOURCE_TABLE = "source_table";
  private static final String VIEW_TABLE = "writable_view";
  private static final String SOURCE_TAG = "id1";
  private static final String SOURCE_ATTR = "attr1";
  private static final String SOURCE_FIELD = "m1";
  private static final String VIEW_TAG = "id_alias";
  private static final String VIEW_ATTR = "attr_alias";
  private static final String VIEW_FIELD = "m_alias";

  private MetadataForWritableView metadata;
  private SessionInfo sessionInfo;
  private SqlParser sqlParser;

  @BeforeClass
  public static void initDataNodeId() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Before
  public void setUp() {
    resetWritableView(
        createSourceTable(true, true),
        createWritableView(null),
        createSourceTableSchema(true, true),
        createWritableViewSchema(null));
    sessionInfo = new SessionInfo(0, "test", ZoneId.systemDefault(), DATABASE, SqlDialect.TABLE);
    sqlParser = new SqlParser();
  }

  @After
  public void tearDown() {
    DataNodeTableCache.getInstance().invalid(DATABASE);
  }

  @Test
  public void testInsertRowIntoWritableViewRewritesToSourceTable() {
    final InsertRowStatement insertRowStatement = StatementTestUtils.genInsertRowStatement(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {VIEW_TABLE}));
    insertRowStatement.setMeasurements(new String[] {VIEW_TAG, VIEW_ATTR, VIEW_FIELD});

    final MPPQueryContext context =
        new MPPQueryContext("", new QueryId("query_insert_row"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(
            new InsertRow(insertRowStatement, context), metadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context, metadata, sessionInfo, new SymbolAllocator(), WarningCollector.NOOP)
            .plan(analysis);
    final RelationalInsertRowNode insertNode =
        (RelationalInsertRowNode) logicalQueryPlan.getRootNode();

    assertEquals(SOURCE_TABLE, insertNode.getTableName());
    assertArrayEquals(new String[] {SOURCE_TAG, SOURCE_FIELD}, insertNode.getMeasurements());
    assertEquals(SOURCE_TABLE, insertNode.getDeviceID().getTableName());
  }

  @Test
  public void testInsertSelectIntoWritableViewRewritesIntoTarget() {
    final String sql =
        "insert into writable_view_db.writable_view(time, id_alias, attr_alias, m_alias) "
            + "select time, id1, attr1, m1 from writable_view_db.source_table";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("query_insert_select"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context, metadata, sessionInfo, new SymbolAllocator(), WarningCollector.NOOP)
            .plan(analysis);
    final PlanNode rootNode = logicalQueryPlan.getRootNode();
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof IntoNode);
    final IntoNode intoNode = (IntoNode) getChildrenNode(rootNode, 1);

    assertEquals(DATABASE, intoNode.getDatabase());
    assertEquals(SOURCE_TABLE, intoNode.getTable());
    assertEquals(
        Arrays.asList("time", SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD),
        intoNode.getColumns().stream()
            .map(ColumnSchema::getName)
            .collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testInsertSelectWithoutTargetColumnsUsesSourceTableOrder() {
    final String sql =
        "insert into writable_view_db.writable_view "
            + "select time, id1, attr1, m1 from writable_view_db.source_table";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(
            sql, new QueryId("query_insert_select_without_columns"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context, metadata, sessionInfo, new SymbolAllocator(), WarningCollector.NOOP)
            .plan(analysis);
    final IntoNode intoNode = (IntoNode) getChildrenNode(logicalQueryPlan.getRootNode(), 1);

    assertEquals(DATABASE, intoNode.getDatabase());
    assertEquals(SOURCE_TABLE, intoNode.getTable());
    assertEquals(
        Arrays.asList("time", SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD),
        intoNode.getColumns().stream()
            .map(ColumnSchema::getName)
            .collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testInsertSelectWithoutTargetColumnsDoesNotDropUnmappedViewColumns() {
    resetWritableView(
        createSourceTable(true, true),
        createWritableView(
            new FieldColumnSchema(
                "local_field", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED)),
        createSourceTableSchema(true, true),
        createWritableViewSchema(
            new ColumnSchema(
                "local_field",
                TypeFactory.getType(TSDataType.DOUBLE),
                false,
                TsTableColumnCategory.FIELD)));

    final String sql =
        "insert into writable_view_db.writable_view "
            + "select time, id1, attr1, m1 from writable_view_db.source_table";
    assertSemanticException(
        () -> {
          final Statement statement =
              sqlParser.createStatement(
                  sql, ZoneId.systemDefault(), new InternalClientSession("test"));
          final MPPQueryContext context =
              new MPPQueryContext(
                  sql,
                  new QueryId("query_insert_select_without_columns_local_field"),
                  sessionInfo,
                  null,
                  null);
          AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);
        },
        "Column 'local_field' in writable view 'writable_view_db.writable_view' cannot be resolved because it does not exist in source table 'writable_view_db.source_table'.");
  }

  @Test
  public void testWritableViewSchemaFallsBackToColumnSourceNameWhenMapMissing() {
    final TableSchema sourceTableSchema = createSourceTableSchema(true, true);
    final MetadataForWritableView metadataWithColumnSourceNames =
        new MetadataForWritableView(
            sourceTableSchema, createWritableViewSchemaFromColumnSourceNames(sourceTableSchema));

    final String sql =
        "insert into writable_view_db.writable_view(time, id_alias, attr_alias, m_alias) "
            + "select time, id1, attr1, m1 from writable_view_db.source_table";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(
            sql, new QueryId("query_insert_select_column_source_names"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(
            statement, metadataWithColumnSourceNames, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context,
                metadataWithColumnSourceNames,
                sessionInfo,
                new SymbolAllocator(),
                WarningCollector.NOOP)
            .plan(analysis);
    final IntoNode intoNode = (IntoNode) getChildrenNode(logicalQueryPlan.getRootNode(), 1);

    assertEquals(DATABASE, intoNode.getDatabase());
    assertEquals(SOURCE_TABLE, intoNode.getTable());
    assertEquals(
        Arrays.asList("time", SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD),
        intoNode.getColumns().stream()
            .map(ColumnSchema::getName)
            .collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testWritableViewScanUsesSourceTagOrder() {
    final String sql = "select id_alias, attr_alias, m_alias from writable_view_db.writable_view";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("query_scan_writable_view"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context, metadata, sessionInfo, new SymbolAllocator(), WarningCollector.NOOP)
            .plan(analysis);
    final DeviceTableScanNode scanNode =
        (DeviceTableScanNode) ((OutputNode) logicalQueryPlan.getRootNode()).getChild();

    assertEquals(0, (int) scanNode.getTagAndAttributeIndexMap().get(Symbol.of(VIEW_TAG)));
    assertEquals(SOURCE_TAG, scanNode.getAssignments().get(Symbol.of(VIEW_TAG)).getName());
  }

  @Test
  public void testShowAndCountDevicesOnWritableViewRewritePredicateBeforeTraverse() {
    final String showSql =
        "show devices from writable_view_db.writable_view "
            + "where id_alias = 'id:0' and attr_alias = 'attr:0'";
    final ShowDevice showDevice =
        (ShowDevice)
            sqlParser.createStatement(
                showSql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext showContext =
        new MPPQueryContext(showSql, new QueryId("query_show_device"), sessionInfo, null, null);
    AnalyzerTest.analyzeStatement(showDevice, metadata, showContext, sqlParser, sessionInfo);

    assertEquals(DATABASE, showDevice.getDatabase());
    assertEquals(SOURCE_TABLE, showDevice.getTableName());
    assertSourcePredicate(showDevice.getWhere().get());
    assertEquals(SOURCE_TAG, showDevice.getColumnHeaderList().get(0).getColumnName());
    assertEquals(VIEW_TAG, showDevice.getOutputColumnHeaderList().get(0).getColumnName());

    final String countSql =
        "count devices from writable_view_db.writable_view "
            + "where id_alias = 'id:0' and attr_alias = 'attr:0'";
    final CountDevice countDevice =
        (CountDevice)
            sqlParser.createStatement(
                countSql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext countContext =
        new MPPQueryContext(countSql, new QueryId("query_count_device"), sessionInfo, null, null);
    AnalyzerTest.analyzeStatement(countDevice, metadata, countContext, sqlParser, sessionInfo);

    assertEquals(DATABASE, countDevice.getDatabase());
    assertEquals(SOURCE_TABLE, countDevice.getTableName());
    assertSourcePredicate(countDevice.getWhere().get());
    assertEquals(SOURCE_TAG, countDevice.getColumnHeaderList().get(0).getColumnName());
    assertEquals(VIEW_TAG, countDevice.getOutputColumnHeaderList().get(0).getColumnName());
  }

  @Test
  public void testWritableViewAggregationScanUsesSourceTableAndColumns() {
    final String sql =
        "select count(m_alias) from writable_view_db.writable_view where id_alias = 'id:0'";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(
            sql, new QueryId("query_aggregation_writable_view"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context, metadata, sessionInfo, new SymbolAllocator(), WarningCollector.NOOP)
            .plan(analysis);
    final AggregationTableScanNode scanNode =
        findFirstNode(logicalQueryPlan.getRootNode(), AggregationTableScanNode.class);

    assertTrue(scanNode != null);
    assertEquals(SOURCE_TABLE, scanNode.getQualifiedObjectName().getObjectName());
    assertTrue(scanNode.getOriginalWritableViewName().isPresent());
    assertEquals(DATABASE, scanNode.getOriginalWritableViewName().get().getDatabaseName());
    assertEquals(VIEW_TABLE, scanNode.getOriginalWritableViewName().get().getObjectName());
    // The tag predicate is consumed by metadata index scan, so the tag symbol is pruned here.
    assertFalse(scanNode.getAssignments().containsKey(Symbol.of(VIEW_TAG)));
    assertEquals(SOURCE_FIELD, scanNode.getAssignments().get(Symbol.of(VIEW_FIELD)).getName());
    assertFalse(
        scanNode.getAssignments().values().stream()
            .anyMatch(
                column ->
                    VIEW_FIELD.equals(column.getName()) || VIEW_TAG.equals(column.getName())));
  }

  @Test
  public void testWritableViewScanUsesTableCacheInsteadOfMetadataSourceSchemaLookup() {
    final TableSchema sourceTableSchema = createSourceTableSchema(true, true);
    final MetadataForWritableView cacheBackedMetadata =
        new MetadataForWritableView(
            sourceTableSchema,
            withResolvedSourceTableSchema(createWritableViewSchema(null), sourceTableSchema)) {
          @Override
          public Optional<TableSchema> getTableSchema(
              final SessionInfo session, final QualifiedObjectName name) {
            if (name.getDatabaseName().equalsIgnoreCase(DATABASE)
                && name.getObjectName().equalsIgnoreCase(SOURCE_TABLE)) {
              fail("Writable view scan should not fetch source table schema from metadata");
            }
            return super.getTableSchema(session, name);
          }
        };

    final String sql = "select id_alias, attr_alias, m_alias from writable_view_db.writable_view";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(
            sql, new QueryId("query_scan_cache_fast_path"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(
            statement, cacheBackedMetadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context,
                cacheBackedMetadata,
                sessionInfo,
                new SymbolAllocator(),
                WarningCollector.NOOP)
            .plan(analysis);
    final DeviceTableScanNode scanNode =
        (DeviceTableScanNode) ((OutputNode) logicalQueryPlan.getRootNode()).getChild();

    assertEquals(SOURCE_TABLE, scanNode.getQualifiedObjectName().getObjectName());
    assertTrue(scanNode.getOriginalWritableViewName().isPresent());
  }

  @Test
  public void testIdentityWritableViewScanSkipsSourceSchemaLookup() {
    final MetadataForWritableView identityFastPathMetadata =
        new MetadataForWritableView(null, createIdentityWritableViewFastPathSchema()) {
          @Override
          public Optional<TableSchema> getTableSchema(
              final SessionInfo session, final QualifiedObjectName name) {
            if (name.getDatabaseName().equalsIgnoreCase(DATABASE)
                && name.getObjectName().equalsIgnoreCase(SOURCE_TABLE)) {
              fail("Identity writable view scan should not fetch source table schema");
            }
            return super.getTableSchema(session, name);
          }
        };

    final String sql = "select id1, attr1, m1 from writable_view_db.writable_view";
    final Statement statement =
        sqlParser.createStatement(sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(
            sql, new QueryId("query_identity_scan_fast_path"), sessionInfo, null, null);
    final Analysis analysis =
        AnalyzerTest.analyzeStatement(
            statement, identityFastPathMetadata, context, sqlParser, sessionInfo);

    final LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                context,
                identityFastPathMetadata,
                sessionInfo,
                new SymbolAllocator(),
                WarningCollector.NOOP)
            .plan(analysis);
    final DeviceTableScanNode scanNode =
        (DeviceTableScanNode) ((OutputNode) logicalQueryPlan.getRootNode()).getChild();

    assertEquals(SOURCE_TABLE, scanNode.getQualifiedObjectName().getObjectName());
    assertTrue(scanNode.getOriginalWritableViewName().isPresent());
    assertEquals(0, (int) scanNode.getTagAndAttributeIndexMap().get(Symbol.of(SOURCE_TAG)));
    assertEquals(SOURCE_FIELD, scanNode.getAssignments().get(Symbol.of(SOURCE_FIELD)).getName());
  }

  @Test
  public void testUpdateOnWritableViewRewritesAssignmentsAndPredicate() {
    final String sql =
        "update writable_view_db.writable_view set attr_alias = id_alias "
            + "where id_alias = 'id:0' and attr_alias = 'attr:0'";
    final Update statement =
        (Update)
            sqlParser.createStatement(
                sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("query_update"), sessionInfo, null, null);
    AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    assertEquals(DATABASE, statement.getDatabase());
    assertEquals(SOURCE_TABLE, statement.getTableName());

    final UpdateAssignment assignment = statement.getAssignments().get(0);
    assertEquals(SOURCE_ATTR, ((SymbolReference) assignment.getName()).getName());
    assertEquals(SOURCE_TAG, ((SymbolReference) assignment.getValue()).getName());

    assertSourcePredicate(statement.getWhere().get());
  }

  @Test
  public void testDeleteDataOnWritableViewRewritesModEntries() throws Exception {
    final String sql =
        "delete from writable_view_db.writable_view where time <= 100 and id_alias = 'id:0'";
    final Delete statement =
        (Delete)
            sqlParser.createStatement(
                sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("query_delete_data"), sessionInfo, null, null);
    final Method validateSchema =
        AnalyzeUtils.class.getDeclaredMethod("validateSchema", Delete.class, MPPQueryContext.class);
    validateSchema.setAccessible(true);
    validateSchema.invoke(null, statement, context);

    assertEquals(DATABASE, statement.getDatabaseName());
    assertEquals(1, statement.getTableDeletionEntries().size());

    final TableDeletionEntry deletionEntry = statement.getTableDeletionEntries().get(0);
    assertEquals(SOURCE_TABLE, deletionEntry.getTableName());
    assertEquals(Long.MIN_VALUE, deletionEntry.getStartTime());
    assertEquals(100, deletionEntry.getEndTime());
    assertTrue(
        deletionEntry.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {SOURCE_TABLE, "id:0"}), 1, 1));
    assertFalse(
        deletionEntry.affects(
            Factory.DEFAULT_FACTORY.create(new String[] {SOURCE_TABLE, "id:1"}), 1, 1));
  }

  @Test
  public void testDeleteDevicesOnWritableViewRewritesPredicate() {
    final String sql =
        "delete devices from writable_view_db.writable_view "
            + "where id_alias = 'id:0' and attr_alias = 'attr:0'";
    final DeleteDevice statement =
        (DeleteDevice)
            sqlParser.createStatement(
                sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("query_delete_device"), sessionInfo, null, null);
    AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    assertEquals(DATABASE, statement.getDatabase());
    assertEquals(SOURCE_TABLE, statement.getTableName());

    final LogicalExpression where = (LogicalExpression) statement.getWhere().get();
    final List<String> columnNames = new ArrayList<>();
    for (final Expression term : where.getTerms()) {
      final ComparisonExpression comparisonExpression = (ComparisonExpression) term;
      columnNames.add(((SymbolReference) comparisonExpression.getLeft()).getName());
    }
    assertTrue(columnNames.contains(SOURCE_TAG));
    assertTrue(columnNames.contains(SOURCE_ATTR));
    assertFalse(columnNames.contains(VIEW_TAG));
    assertFalse(columnNames.contains(VIEW_ATTR));
  }

  @Test
  public void testDeleteDevicesOnBaseTableKeepsIdentifierPredicateForModEntries() {
    final String sql =
        "delete devices from writable_view_db.source_table "
            + "where id1 = 'id:0' and attr1 = 'attr:0'";
    final DeleteDevice statement =
        (DeleteDevice)
            sqlParser.createStatement(
                sql, ZoneId.systemDefault(), new InternalClientSession("test"));
    final MPPQueryContext context =
        new MPPQueryContext(
            sql, new QueryId("query_delete_device_source"), sessionInfo, null, null);
    AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);

    assertEquals(DATABASE, statement.getDatabase());
    assertEquals(SOURCE_TABLE, statement.getTableName());
    assertTrue(statement.isMayDeleteDevice());

    final LogicalExpression where = (LogicalExpression) statement.getWhere().get();
    final List<String> columnNames = new ArrayList<>();
    for (final Expression term : where.getTerms()) {
      final ComparisonExpression comparisonExpression = (ComparisonExpression) term;
      columnNames.add(((SymbolReference) comparisonExpression.getLeft()).getName());
    }
    assertTrue(columnNames.contains(SOURCE_TAG));
    assertTrue(columnNames.contains(SOURCE_ATTR));
  }

  @Test
  public void testWritableViewScanWithMissingSourceFieldThrowsSemanticException() {
    resetWritableView(
        createSourceTable(true, false),
        createWritableView(null),
        createSourceTableSchema(true, false),
        createWritableViewSchema(null));

    final String sql = "select m_alias from writable_view_db.writable_view";
    assertSemanticException(
        () -> {
          final Statement statement =
              sqlParser.createStatement(
                  sql, ZoneId.systemDefault(), new InternalClientSession("test"));
          final MPPQueryContext context =
              new MPPQueryContext(
                  sql, new QueryId("query_missing_source_field"), sessionInfo, null, null);
          final Analysis analysis =
              AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);
          new TableLogicalPlanner(
                  context, metadata, sessionInfo, new SymbolAllocator(), WarningCollector.NOOP)
              .plan(analysis);
        },
        "Column 'm_alias' in writable view 'writable_view_db.writable_view' cannot be resolved because source column 'm1' does not exist in source table 'writable_view_db.source_table'.");
  }

  @Test
  public void testInsertSelectIntoWritableViewWithLocalOnlyFieldThrowsSemanticException() {
    resetWritableView(
        createSourceTable(true, true),
        createWritableView(
            new FieldColumnSchema(
                "local_field", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED)),
        createSourceTableSchema(true, true),
        createWritableViewSchema(
            new ColumnSchema(
                "local_field",
                TypeFactory.getType(TSDataType.DOUBLE),
                false,
                TsTableColumnCategory.FIELD)));

    final String sql =
        "insert into writable_view_db.writable_view(time, id_alias, attr_alias, local_field) "
            + "select time, id1, attr1, m1 from writable_view_db.source_table";
    assertSemanticException(
        () -> {
          final Statement statement =
              sqlParser.createStatement(
                  sql, ZoneId.systemDefault(), new InternalClientSession("test"));
          final MPPQueryContext context =
              new MPPQueryContext(
                  sql, new QueryId("query_insert_select_local_field"), sessionInfo, null, null);
          AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);
        },
        "Column 'local_field' in writable view 'writable_view_db.writable_view' cannot be resolved because it does not exist in source table 'writable_view_db.source_table'.");
  }

  @Test
  public void testUpdateOnWritableViewWithMissingSourceAttributeThrowsSemanticException() {
    resetWritableView(
        createSourceTable(false, true),
        createWritableView(null),
        createSourceTableSchema(false, true),
        createWritableViewSchema(null));

    final String sql =
        "update writable_view_db.writable_view set attr_alias = id_alias where id_alias = 'id:0'";
    assertSemanticException(
        () -> {
          final Update statement =
              (Update)
                  sqlParser.createStatement(
                      sql, ZoneId.systemDefault(), new InternalClientSession("test"));
          final MPPQueryContext context =
              new MPPQueryContext(
                  sql, new QueryId("query_update_missing_source_attr"), sessionInfo, null, null);
          AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);
        },
        "Column 'attr_alias' in writable view 'writable_view_db.writable_view' cannot be resolved because source column 'attr1' does not exist in source table 'writable_view_db.source_table'.");
  }

  @Test
  public void testInsertIntoWritableViewWithMissingSourceTableThrowsClearSemanticException() {
    resetWritableView(
        createSourceTable(true, true),
        createWritableView(null),
        null,
        createWritableViewSchema(null));

    final InsertRowStatement insertRowStatement = StatementTestUtils.genInsertRowStatement(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {VIEW_TABLE}));
    insertRowStatement.setMeasurements(new String[] {VIEW_TAG, VIEW_ATTR, VIEW_FIELD});

    assertSemanticException(
        () -> {
          final MPPQueryContext context =
              new MPPQueryContext(
                  "", new QueryId("query_missing_source_table"), sessionInfo, null, null);
          final InsertRow wrappedInsertStatement = new InsertRow(insertRowStatement, context);
          final Method rewriteMethod =
              org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement.class
                  .getDeclaredMethod(
                      "rewriteWritableViewTargetIfNecessary",
                      org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata.class,
                      String.class,
                      org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement
                          .class);
          rewriteMethod.setAccessible(true);
          try {
            rewriteMethod.invoke(
                wrappedInsertStatement,
                metadata,
                DATABASE,
                wrappedInsertStatement.getInnerTreeStatement());
          } catch (final InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
              throw (Exception) e.getCause();
            }
            throw new RuntimeException(e.getCause());
          }
        },
        "The source table 'writable_view_db.source_table' of writable view 'writable_view_db.writable_view' does not exist.");
  }

  @Test
  public void testInsertIntoWritableViewUsesMetadataRewriteFastPath() throws Exception {
    final InsertRowStatement insertRowStatement = StatementTestUtils.genInsertRowStatement(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {VIEW_TABLE}));
    insertRowStatement.setMeasurements(new String[] {VIEW_TAG, VIEW_ATTR, VIEW_FIELD});

    final MetadataForWritableView fastPathMetadata =
        new MetadataForWritableView(null, null) {
          @Override
          public Optional<TableSchema> getTableSchema(
              final SessionInfo session, final QualifiedObjectName name) {
            fail("Writable view rewrite should not fall back to getTableSchema");
            return Optional.empty();
          }

          @Override
          public Optional<WritableViewInsertRewriteSupport> getWritableViewInsertRewriteSupport(
              final SessionInfo session, final QualifiedObjectName name) {
            assertEquals(DATABASE, name.getDatabaseName());
            assertEquals(VIEW_TABLE, name.getObjectName());
            return Optional.of(
                new WritableViewInsertRewriteSupport(
                    name,
                    new QualifiedObjectName(DATABASE, SOURCE_TABLE),
                    createWritableViewColumnMap(),
                    true,
                    columnName ->
                        "time".equalsIgnoreCase(columnName)
                            || SOURCE_TAG.equals(columnName)
                            || SOURCE_ATTR.equals(columnName)
                            || SOURCE_FIELD.equals(columnName)));
          }
        };

    final MPPQueryContext context =
        new MPPQueryContext("", new QueryId("query_fast_path_rewrite"), sessionInfo, null, null);
    final InsertRow wrappedInsertStatement = new InsertRow(insertRowStatement, context);
    final Method rewriteMethod =
        org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement.class
            .getDeclaredMethod(
                "rewriteWritableViewTargetIfNecessary",
                org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata.class,
                String.class,
                org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement.class);
    rewriteMethod.setAccessible(true);
    rewriteMethod.invoke(
        wrappedInsertStatement,
        fastPathMetadata,
        DATABASE,
        wrappedInsertStatement.getInnerTreeStatement());

    assertEquals(SOURCE_TABLE, insertRowStatement.getDevicePath().getFullPath());
    assertEquals(Optional.of(DATABASE), insertRowStatement.getDatabaseName());
    assertArrayEquals(
        new String[] {SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD}, insertRowStatement.getMeasurements());
  }

  @Test
  public void testInsertIntoWritableViewUsesEmbeddedSourceSchemaFastPath() throws Exception {
    final InsertRowStatement insertRowStatement = StatementTestUtils.genInsertRowStatement(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {VIEW_TABLE}));
    insertRowStatement.setMeasurements(new String[] {VIEW_TAG, VIEW_ATTR, VIEW_FIELD});

    final TableSchema sourceTableSchema = createSourceTableSchema(true, true);
    final MetadataForWritableView metadataWithEmbeddedSource =
        new MetadataForWritableView(
            sourceTableSchema,
            withResolvedSourceTableSchema(createWritableViewSchema(null), sourceTableSchema)) {
          @Override
          public Optional<TableSchema> getTableSchema(
              final SessionInfo session, final QualifiedObjectName name) {
            if (name.getDatabaseName().equalsIgnoreCase(DATABASE)
                && name.getObjectName().equalsIgnoreCase(SOURCE_TABLE)) {
              fail("Writable view rewrite should not fetch source table schema");
            }
            return super.getTableSchema(session, name);
          }
        };

    final MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("query_embedded_source_fast_path_rewrite"), sessionInfo, null, null);
    final InsertRow wrappedInsertStatement = new InsertRow(insertRowStatement, context);
    final Method rewriteMethod =
        org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement.class
            .getDeclaredMethod(
                "rewriteWritableViewTargetIfNecessary",
                org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata.class,
                String.class,
                org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement.class);
    rewriteMethod.setAccessible(true);
    rewriteMethod.invoke(
        wrappedInsertStatement,
        metadataWithEmbeddedSource,
        DATABASE,
        wrappedInsertStatement.getInnerTreeStatement());

    assertEquals(SOURCE_TABLE, insertRowStatement.getDevicePath().getFullPath());
    assertEquals(Optional.of(DATABASE), insertRowStatement.getDatabaseName());
    assertArrayEquals(
        new String[] {SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD}, insertRowStatement.getMeasurements());
  }

  @Test
  public void testInsertIntoIdentityMappedWritableViewSkipsPerMeasurementRewrite()
      throws Exception {
    final InsertRowStatement insertRowStatement = StatementTestUtils.genInsertRowStatement(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {VIEW_TABLE}));
    insertRowStatement.setMeasurements(new String[] {SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD});

    final AtomicInteger sourceColumnLookupCount = new AtomicInteger();
    final MetadataForWritableView fastPathMetadata =
        new MetadataForWritableView(null, null) {
          @Override
          public Optional<TableSchema> getTableSchema(
              final SessionInfo session, final QualifiedObjectName name) {
            fail("Identity writable view rewrite should not fall back to getTableSchema");
            return Optional.empty();
          }

          @Override
          public Optional<WritableViewInsertRewriteSupport> getWritableViewInsertRewriteSupport(
              final SessionInfo session, final QualifiedObjectName name) {
            return Optional.of(
                new WritableViewInsertRewriteSupport(
                    name,
                    new QualifiedObjectName(DATABASE, SOURCE_TABLE),
                    createIdentityWritableViewColumnMap(),
                    true,
                    columnName -> {
                      sourceColumnLookupCount.incrementAndGet();
                      return true;
                    }));
          }
        };

    final MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("query_identity_fast_path_rewrite"), sessionInfo, null, null);
    final InsertRow wrappedInsertStatement = new InsertRow(insertRowStatement, context);
    final Method rewriteMethod =
        org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement.class
            .getDeclaredMethod(
                "rewriteWritableViewTargetIfNecessary",
                org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata.class,
                String.class,
                org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement.class);
    rewriteMethod.setAccessible(true);
    rewriteMethod.invoke(
        wrappedInsertStatement,
        fastPathMetadata,
        DATABASE,
        wrappedInsertStatement.getInnerTreeStatement());

    assertEquals(0, sourceColumnLookupCount.get());
    assertEquals(SOURCE_TABLE, insertRowStatement.getDevicePath().getFullPath());
    assertEquals(Optional.of(DATABASE), insertRowStatement.getDatabaseName());
    assertArrayEquals(
        new String[] {SOURCE_TAG, SOURCE_ATTR, SOURCE_FIELD}, insertRowStatement.getMeasurements());
  }

  @Test
  public void testShowDevicesOnWritableViewWithLocalOnlyAttributeThrowsSemanticException() {
    resetWritableView(
        createSourceTable(true, true),
        createWritableView(new AttributeColumnSchema("local_attr", TSDataType.STRING)),
        createSourceTableSchema(true, true),
        createWritableViewSchema(
            new ColumnSchema(
                "local_attr",
                TypeFactory.getType(TSDataType.STRING),
                false,
                TsTableColumnCategory.ATTRIBUTE)));

    final String sql = "show devices from writable_view_db.writable_view";
    assertSemanticException(
        () -> {
          final Statement statement =
              sqlParser.createStatement(
                  sql, ZoneId.systemDefault(), new InternalClientSession("test"));
          final MPPQueryContext context =
              new MPPQueryContext(
                  sql, new QueryId("query_show_devices_local_attr"), sessionInfo, null, null);
          AnalyzerTest.analyzeStatement(statement, metadata, context, sqlParser, sessionInfo);
        },
        "Column 'local_attr' in writable view 'writable_view_db.writable_view' cannot be resolved because it does not exist in source table 'writable_view_db.source_table'.");
  }

  private void resetWritableView(
      final TsTable sourceTable,
      final WritableView writableView,
      final TableSchema sourceTableSchema,
      final WritableViewSchema writableViewSchema) {
    DataNodeTableCache.getInstance().invalid(DATABASE);
    DataNodeTableCache.getInstance().preUpdateTable(DATABASE, sourceTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable(DATABASE, SOURCE_TABLE, null);
    DataNodeTableCache.getInstance().preUpdateTable(DATABASE, writableView, null);
    DataNodeTableCache.getInstance().commitUpdateTable(DATABASE, VIEW_TABLE, null);
    metadata =
        new MetadataForWritableView(
            sourceTableSchema,
            withResolvedSourceTableSchema(writableViewSchema, sourceTableSchema));
  }

  private void assertSemanticException(
      final ThrowingRunnable runnable, final String expectedMessage) {
    try {
      runnable.run();
      fail("Expected SemanticException with message: " + expectedMessage);
    } catch (final Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
    }
  }

  private void assertSourcePredicate(final Expression expression) {
    final LogicalExpression where = (LogicalExpression) expression;
    final List<String> columnNames = new ArrayList<>();
    for (final Expression term : where.getTerms()) {
      final ComparisonExpression comparisonExpression = (ComparisonExpression) term;
      columnNames.add(((SymbolReference) comparisonExpression.getLeft()).getName());
    }
    assertTrue(columnNames.contains(SOURCE_TAG));
    assertTrue(columnNames.contains(SOURCE_ATTR));
    assertFalse(columnNames.contains(VIEW_TAG));
    assertFalse(columnNames.contains(VIEW_ATTR));
  }

  private <T extends PlanNode> T findFirstNode(final PlanNode node, final Class<T> targetClass) {
    if (targetClass.isInstance(node)) {
      return targetClass.cast(node);
    }
    for (final PlanNode child : node.getChildren()) {
      final T found = findFirstNode(child, targetClass);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static TsTable createSourceTable(
      final boolean includeAttribute, final boolean includeField) {
    final TsTable tsTable = new TsTable(SOURCE_TABLE);
    tsTable.addColumnSchema(new TagColumnSchema(SOURCE_TAG, TSDataType.STRING));
    if (includeAttribute) {
      tsTable.addColumnSchema(new AttributeColumnSchema(SOURCE_ATTR, TSDataType.STRING));
    }
    if (includeField) {
      tsTable.addColumnSchema(
          new FieldColumnSchema(
              SOURCE_FIELD, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    }
    return tsTable;
  }

  private static WritableView createWritableView(
      final org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema extraColumn) {
    final WritableView writableView = new WritableView(VIEW_TABLE, DATABASE, SOURCE_TABLE, false);
    writableView.addColumnSchema(new TagColumnSchema(VIEW_TAG, TSDataType.STRING));
    writableView.addColumnSchema(new AttributeColumnSchema(VIEW_ATTR, TSDataType.STRING));
    writableView.addColumnSchema(
        new FieldColumnSchema(
            VIEW_FIELD, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    if (extraColumn != null) {
      writableView.addColumnSchema(extraColumn);
    }
    writableView.setViewColumnToSourceColumnMap(createWritableViewColumnMap());
    return writableView;
  }

  private static TableSchema createSourceTableSchema(
      final boolean includeAttribute, final boolean includeField) {
    final List<ColumnSchema> columns = new ArrayList<>();
    columns.add(
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    columns.add(
        new ColumnSchema(
            SOURCE_TAG, TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    if (includeAttribute) {
      columns.add(
          new ColumnSchema(
              SOURCE_ATTR,
              TypeFactory.getType(TSDataType.STRING),
              false,
              TsTableColumnCategory.ATTRIBUTE));
    }
    if (includeField) {
      columns.add(
          new ColumnSchema(
              SOURCE_FIELD,
              TypeFactory.getType(TSDataType.DOUBLE),
              false,
              TsTableColumnCategory.FIELD));
    }
    return new TableSchema(SOURCE_TABLE, columns);
  }

  private static WritableViewSchema createWritableViewSchema(final ColumnSchema extraColumnSchema) {
    final List<ColumnSchema> columns = new ArrayList<>();
    columns.add(
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    columns.add(
        new ColumnSchema(
            VIEW_TAG, TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    columns.add(
        new ColumnSchema(
            VIEW_ATTR,
            TypeFactory.getType(TSDataType.STRING),
            false,
            TsTableColumnCategory.ATTRIBUTE));
    columns.add(
        new ColumnSchema(
            VIEW_FIELD,
            TypeFactory.getType(TSDataType.DOUBLE),
            false,
            TsTableColumnCategory.FIELD));
    if (extraColumnSchema != null) {
      columns.add(extraColumnSchema);
    }
    return new WritableViewSchema(
        VIEW_TABLE,
        columns,
        new QualifiedObjectName(DATABASE, SOURCE_TABLE),
        createWritableViewColumnMap());
  }

  private static WritableViewSchema createWritableViewSchemaFromColumnSourceNames(
      final TableSchema sourceTableSchema) {
    final List<ColumnSchema> columns = new ArrayList<>();
    columns.add(
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    columns.add(
        createMappedColumnSchema(
            VIEW_TAG, TSDataType.STRING, TsTableColumnCategory.TAG, SOURCE_TAG));
    columns.add(
        createMappedColumnSchema(
            VIEW_ATTR, TSDataType.STRING, TsTableColumnCategory.ATTRIBUTE, SOURCE_ATTR));
    columns.add(
        createMappedColumnSchema(
            VIEW_FIELD, TSDataType.DOUBLE, TsTableColumnCategory.FIELD, SOURCE_FIELD));
    return new WritableViewSchema(
        VIEW_TABLE,
        columns,
        new QualifiedObjectName(DATABASE, SOURCE_TABLE),
        null,
        sourceTableSchema);
  }

  private static ColumnSchema createMappedColumnSchema(
      final String viewColumnName,
      final TSDataType dataType,
      final TsTableColumnCategory category,
      final String sourceColumnName) {
    final ColumnSchema columnSchema =
        new ColumnSchema(viewColumnName, TypeFactory.getType(dataType), false, category);
    final Map<String, String> props = new HashMap<>();
    props.put(ViewColumnSchemaUtils.SOURCE_NAME, sourceColumnName);
    columnSchema.setProps(props);
    return columnSchema;
  }

  private static WritableViewSchema withResolvedSourceTableSchema(
      final WritableViewSchema writableViewSchema, final TableSchema sourceTableSchema) {
    if (writableViewSchema == null
        || sourceTableSchema == null
        || writableViewSchema.getSourceTableSchema().isPresent()) {
      return writableViewSchema;
    }
    return new WritableViewSchema(
        writableViewSchema.getTableName(),
        writableViewSchema.getColumns(),
        writableViewSchema.getSourceTableName(),
        writableViewSchema.getViewColumnToSourceColumnMap(),
        sourceTableSchema);
  }

  private static WritableViewSchema createIdentityWritableViewFastPathSchema() {
    final List<ColumnSchema> columns = new ArrayList<>();
    columns.add(
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    columns.add(
        new ColumnSchema(
            SOURCE_TAG, TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    columns.add(
        new ColumnSchema(
            SOURCE_ATTR,
            TypeFactory.getType(TSDataType.STRING),
            false,
            TsTableColumnCategory.ATTRIBUTE));
    columns.add(
        new ColumnSchema(
            SOURCE_FIELD,
            TypeFactory.getType(TSDataType.DOUBLE),
            false,
            TsTableColumnCategory.FIELD));
    return new WritableViewSchema(
        VIEW_TABLE,
        columns,
        new QualifiedObjectName(DATABASE, SOURCE_TABLE),
        createIdentityWritableViewColumnMap(),
        null,
        true,
        true);
  }

  private static Map<String, String> createWritableViewColumnMap() {
    final Map<String, String> columnMap = new HashMap<>();
    columnMap.put("time", "time");
    columnMap.put(VIEW_TAG, SOURCE_TAG);
    columnMap.put(VIEW_ATTR, SOURCE_ATTR);
    columnMap.put(VIEW_FIELD, SOURCE_FIELD);
    return columnMap;
  }

  private static Map<String, String> createIdentityWritableViewColumnMap() {
    final Map<String, String> columnMap = new HashMap<>();
    columnMap.put("time", "time");
    columnMap.put(SOURCE_TAG, SOURCE_TAG);
    columnMap.put(SOURCE_ATTR, SOURCE_ATTR);
    columnMap.put(SOURCE_FIELD, SOURCE_FIELD);
    return columnMap;
  }

  private static class MetadataForWritableView extends TestMetadata {

    private final TableSchema sourceTableSchema;
    private final WritableViewSchema writableViewSchema;

    private MetadataForWritableView(
        final TableSchema sourceTableSchema, final WritableViewSchema writableViewSchema) {
      this.sourceTableSchema = sourceTableSchema;
      this.writableViewSchema =
          withResolvedSourceTableSchema(writableViewSchema, sourceTableSchema);
    }

    @Override
    public boolean tableExists(final QualifiedObjectName name) {
      return name.getDatabaseName().equalsIgnoreCase(DATABASE)
              && ((name.getObjectName().equalsIgnoreCase(SOURCE_TABLE) && sourceTableSchema != null)
                  || (name.getObjectName().equalsIgnoreCase(VIEW_TABLE)
                      && writableViewSchema != null))
          || super.tableExists(name);
    }

    @Override
    public Optional<TableSchema> getTableSchema(
        final SessionInfo session, final QualifiedObjectName name) {
      if (name.getDatabaseName().equalsIgnoreCase(DATABASE)) {
        if (name.getObjectName().equalsIgnoreCase(SOURCE_TABLE)) {
          return Optional.ofNullable(sourceTableSchema);
        }
        if (name.getObjectName().equalsIgnoreCase(VIEW_TABLE)) {
          return Optional.ofNullable(writableViewSchema);
        }
      }
      return super.getTableSchema(session, name);
    }

    @Override
    public void validateInsertNodeMeasurements(
        final String database,
        final InsertNodeMeasurementInfo measurementInfo,
        final MPPQueryContext context,
        final boolean allowCreateTable,
        final TableHeaderSchemaValidator.MeasurementValidator measurementValidator,
        final TableHeaderSchemaValidator.TagColumnHandler tagColumnHandler) {
      TableHeaderSchemaValidator.getInstance()
          .validateInsertNodeMeasurements(
              database,
              measurementInfo,
              context,
              allowCreateTable,
              measurementValidator,
              tagColumnHandler);
    }

    @Override
    public void validateDeviceSchema(
        final ITableDeviceSchemaValidation schemaValidation, final MPPQueryContext context) {
      assertEquals(DATABASE, schemaValidation.getDatabase());
      assertEquals(SOURCE_TABLE, schemaValidation.getTableName());
      assertEquals(1, schemaValidation.getDeviceIdList().size());
      assertEquals(1, schemaValidation.getAttributeColumnNameList().size());
      assertEquals(SOURCE_ATTR, schemaValidation.getAttributeColumnNameList().get(0));
      final Object[] attributeValues = schemaValidation.getAttributeValueList().get(0);
      assertEquals(1, attributeValues.length);
      assertEquals(
          ((Binary) StatementTestUtils.genValues(0)[1]).toString(), attributeValues[0].toString());
    }

    @Override
    public DataPartition getOrCreateDataPartition(
        final List<DataPartitionQueryParam> dataPartitionQueryParams, final String userName) {
      final Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();

      for (final DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
        assertEquals(DATABASE, dataPartitionQueryParam.getDatabaseName());
        assertEquals(SOURCE_TABLE, dataPartitionQueryParam.getDeviceID().getTableName());
        for (final TTimePartitionSlot timePartitionSlot :
            dataPartitionQueryParam.getTimePartitionSlotList()) {
          dataPartitionMap
              .computeIfAbsent(DATABASE, key -> new HashMap<>())
              .computeIfAbsent(new TSeriesPartitionSlot(0), key -> new HashMap<>())
              .computeIfAbsent(timePartitionSlot, key -> new ArrayList<>())
              .add(
                  new TRegionReplicaSet(
                      new TConsensusGroupId(TConsensusGroupType.DataRegion, 0),
                      Arrays.asList(new TDataNodeLocation(0, null, null, null, null, null))));
        }
      }

      return new DataPartition(dataPartitionMap, "hash", 1);
    }

    @Override
    public Optional<TableSchema> validateTableHeaderSchema4TsFile(
        final String database,
        final TableSchema schema,
        final MPPQueryContext context,
        final boolean allowCreateTable,
        final boolean isStrictTagColumn,
        final AtomicBoolean needDecode4DifferentTimeColumn) {
      return Optional.of(schema);
    }
  }
}
