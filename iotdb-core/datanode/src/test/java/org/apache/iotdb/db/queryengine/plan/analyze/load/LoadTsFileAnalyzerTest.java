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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.exception.table.TableNotExistsException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeInvalidPathException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeMissingSchemaException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileTableSchemaRegistrationUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadTsFileAnalyzerTest {

  private int dataNodeId;

  @Before
  public void setUp() {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
  }

  @Test
  public void testTableLoadDoesNotAutoCreateSchemaWhenRequestDisablesIt() throws Exception {
    for (final boolean tableModelStatement : new boolean[] {true, false}) {
      assertTableLoadDoesNotAutoCreateSchema(tableModelStatement, true, false, true);
    }
  }

  @Test
  public void testTableLoadDoesNotAutoCreateSchemaWhenGlobalConfigDisablesIt() throws Exception {
    for (final boolean tableModelStatement : new boolean[] {true, false}) {
      assertTableLoadDoesNotAutoCreateSchema(tableModelStatement, false, true, true);
    }
  }

  @Test
  public void testTableLoadDoesNotAutoCreateSchemaWhenSessionDisablesIt() throws Exception {
    for (final boolean tableModelStatement : new boolean[] {true, false}) {
      assertTableLoadDoesNotAutoCreateSchema(tableModelStatement, true, true, false);
    }
  }

  @Test
  public void testTableTsFileAnalysisPreservesSessionAutoCreateSchemaSetting() throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    final long originalTabletConversionThresholdBytes =
        IoTDBDescriptor.getInstance().getConfig().getLoadTabletConversionThresholdBytes();
    final File tsFile = File.createTempFile("table-load-session-schema", ".tsfile");
    writeTableTsFileWithMixedDevices(tsFile);

    try {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
      IoTDBDescriptor.getInstance().getConfig().setLoadTabletConversionThresholdBytes(-1);
      for (final boolean tableModelStatement : new boolean[] {true, false}) {
        final String database = "load_session_schema_" + tableModelStatement;
        final MPPQueryContext context =
            createLoadQueryContext(
                "load_session_schema_" + tableModelStatement,
                database,
                tableModelStatement ? SqlDialect.TABLE : SqlDialect.TREE,
                false);
        final LoadTsFile tableStatement =
            LoadTsFile.createUnchecked(null, tsFile.getAbsolutePath(), Collections.emptyMap())
                .setDatabase(database);
        tableStatement.setAutoCreateDatabase(false);
        final LoadTsFileStatement treeStatement =
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
        treeStatement.setDatabase(database);
        treeStatement.setAutoCreateSchema(true);
        treeStatement.setAutoCreateDatabase(false);
        final boolean[] schemaCreationEnabledDuringValidation = {true};
        final SqlDialect[] validationDialect = {SqlDialect.TREE};
        final Metadata metadata = mock(Metadata.class);
        when(metadata.validateTableHeaderSchema4TsFile(
                anyString(),
                any(
                    org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema
                        .class),
                any(MPPQueryContext.class),
                anyBoolean(),
                anyBoolean(),
                any(AtomicBoolean.class)))
            .thenAnswer(
                invocation -> {
                  final MPPQueryContext validationContext = invocation.getArgument(2);
                  schemaCreationEnabledDuringValidation[0] =
                      validationContext.getSession().isEnableAutoCreateSchema();
                  validationDialect[0] = validationContext.getSession().getSqlDialect();
                  return java.util.Optional.empty();
                });

        try (final LoadTsFileAnalyzer analyzer =
            tableModelStatement
                ? new LoadTsFileAnalyzer(tableStatement, false, context)
                : new LoadTsFileAnalyzer(treeStatement, false, context)) {
          final Field metadataField = LoadTsFileAnalyzer.class.getDeclaredField("metadata");
          metadataField.setAccessible(true);
          metadataField.set(analyzer, metadata);
          final Method analyzeSingleTsFileMethod =
              LoadTsFileAnalyzer.class.getDeclaredMethod(
                  "analyzeSingleTsFile", File.class, int.class);
          analyzeSingleTsFileMethod.setAccessible(true);
          final InvocationTargetException exception =
              Assert.assertThrows(
                  InvocationTargetException.class,
                  () -> analyzeSingleTsFileMethod.invoke(analyzer, tsFile, 0));
          Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeException);
        } finally {
          DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
        }

        Assert.assertFalse(schemaCreationEnabledDuringValidation[0]);
        Assert.assertEquals(SqlDialect.TABLE, validationDialect[0]);
        Assert.assertFalse(context.getSession().isEnableAutoCreateSchema());
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setLoadTabletConversionThresholdBytes(originalTabletConversionThresholdBytes);
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testTreeTsFileAnalysisPreservesSessionAutoCreateSchemaSetting() throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    final long originalTabletConversionThresholdBytes =
        IoTDBDescriptor.getInstance().getConfig().getLoadTabletConversionThresholdBytes();
    final File tsFile = File.createTempFile("tree-load-session-schema", ".tsfile");
    final List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64));
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerTimeseries(new Path("root.load_session_schema.d1"), schemas.get(0));
      final Tablet tablet = new Tablet("root.load_session_schema.d1", schemas, 1);
      tablet.addTimestamp(0, 1);
      tablet.addValue("s1", 0, 1L);
      writer.writeTree(tablet);
    }

    try {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
      IoTDBDescriptor.getInstance().getConfig().setLoadTabletConversionThresholdBytes(-1);
      final MPPQueryContext context =
          spy(
              createLoadQueryContext(
                  "tree_load_session_schema", "root.load_session_schema", SqlDialect.TREE, false));
      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      statement.setAutoCreateSchema(true);
      statement.setAutoCreateDatabase(false);
      statement.setVerifySchema(true);

      final ISchemaFetcher schemaFetcher = mock(ISchemaFetcher.class);
      final ClusterSchemaTree existingSchema = new ClusterSchemaTree();
      existingSchema.appendSingleMeasurement(
          new PartialPath("root.load_session_schema.d1.s1"),
          schemas.get(0),
          null,
          null,
          null,
          false);
      when(schemaFetcher.fetchSchemaList(anyList(), anyList(), any(MPPQueryContext.class)))
          .thenReturn(existingSchema);

      try (final LoadTsFileAnalyzer analyzer = new LoadTsFileAnalyzer(statement, false, context)) {
        final Field schemaFetcherField = LoadTsFileAnalyzer.class.getDeclaredField("schemaFetcher");
        schemaFetcherField.setAccessible(true);
        schemaFetcherField.set(analyzer, schemaFetcher);
        final Analysis analysis = new Analysis();
        analyzer.analyzeFileByFile(analysis);
        Assert.assertFalse(analysis.isFailed());
      } finally {
        DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
      }

      verify(schemaFetcher).fetchSchemaList(anyList(), anyList(), any(MPPQueryContext.class));
      verify(schemaFetcher, never())
          .fetchSchemaListWithAutoCreate(
              anyList(),
              anyList(),
              anyList(),
              anyList(),
              anyList(),
              anyList(),
              any(MPPQueryContext.class));
      final ArgumentCaptor<SessionInfo> analysisSessions =
          ArgumentCaptor.forClass(SessionInfo.class);
      verify(context, atLeastOnce()).setSession(analysisSessions.capture());
      for (final SessionInfo session : analysisSessions.getAllValues()) {
        Assert.assertFalse(session.isEnableAutoCreateSchema());
        Assert.assertEquals(SqlDialect.TREE, session.getSqlDialect());
      }
      Assert.assertFalse(context.getSession().isEnableAutoCreateSchema());
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setLoadTabletConversionThresholdBytes(originalTabletConversionThresholdBytes);
      Assert.assertTrue(tsFile.delete());
    }
  }

  private void assertTableLoadDoesNotAutoCreateSchema(
      final boolean tableModelStatement,
      final boolean globalAutoCreateSchemaEnabled,
      final boolean requestAutoCreateSchema,
      final boolean sessionAutoCreateSchemaEnabled)
      throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    final boolean originalEnablePartialInsert =
        IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();
    final File tsFile = File.createTempFile("table-load-auto-create-schema", ".tsfile");
    final String database = "load_auto_create_schema_test";
    final MPPQueryContext context =
        createTableQueryContext(
            "load_auto_create_schema", database, sessionAutoCreateSchemaEnabled);
    final ITableCache tableCache = DataNodeTableCache.getInstance();

    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(globalAutoCreateSchemaEnabled);
      IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(false);
      tableCache.invalid(database);
      final TsTable existingTable = new TsTable("table1");
      existingTable.addColumnSchema(new TagColumnSchema("tag1", TSDataType.STRING));
      existingTable.addColumnSchema(new FieldColumnSchema("s1", TSDataType.INT64));
      tableCache.preUpdateTable(database, existingTable, null);
      tableCache.commitUpdateTable(database, "table1", null, null);

      final LoadTsFile tableStatement =
          LoadTsFile.createUnchecked(
                  null,
                  tsFile.getAbsolutePath(),
                  requestAutoCreateSchema
                      ? Collections.emptyMap()
                      : Collections.singletonMap("auto-create-schema", "false"))
              .setDatabase(database);
      tableStatement.setAutoCreateDatabase(true);
      final LoadTsFileStatement treeStatement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      treeStatement.setAutoCreateSchema(requestAutoCreateSchema);
      treeStatement.setAutoCreateDatabase(true);

      try (final LoadTsFileAnalyzer analyzer =
          tableModelStatement
              ? new LoadTsFileAnalyzer(tableStatement, false, context)
              : new LoadTsFileAnalyzer(treeStatement, false, context)) {
        final Method getOrCreateTableSchemaCacheMethod =
            LoadTsFileAnalyzer.class.getDeclaredMethod("getOrCreateTableSchemaCache");
        getOrCreateTableSchemaCacheMethod.setAccessible(true);
        final LoadTsFileTableSchemaCache schemaCache =
            (LoadTsFileTableSchemaCache) getOrCreateTableSchemaCacheMethod.invoke(analyzer);
        schemaCache.setDatabase(database);

        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    "missing_table", createTsFileTableSchema("missing_table", false))));
        final SemanticException missingTableException =
            Assert.assertThrows(
                SemanticException.class,
                () -> schemaCache.createTableAndDatabaseIfNecessary("missing_table"));
        Assert.assertTrue(missingTableException.getCause() instanceof TableNotExistsException);
        Assert.assertNull(tableCache.getTableInCache(database, "missing_table"));

        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap("table1", createTsFileTableSchema("table1", true))));
        final SemanticException missingColumnException =
            Assert.assertThrows(
                SemanticException.class,
                () -> schemaCache.createTableAndDatabaseIfNecessary("table1"));
        Assert.assertEquals(
            TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode(), missingColumnException.getErrorCode());
        Assert.assertNull(tableCache.getTableInCache(database, "table1").getColumnSchema("s2"));

        // Existing partial-insert behavior still applies without creating the missing field.
        IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap("table1", createTsFileTableSchema("table1", true))));
        schemaCache.createTableAndDatabaseIfNecessary("table1");
        Assert.assertNull(tableCache.getTableInCache(database, "table1").getColumnSchema("s2"));

        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap("table1", createTsFileTableSchema("table1", false))));
        schemaCache.createTableAndDatabaseIfNecessary("table1");
      }
    } finally {
      DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
      tableCache.invalid(database);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
      IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(originalEnablePartialInsert);
      Assert.assertTrue(tsFile.delete());
    }
  }

  private TableSchema createTsFileTableSchema(final String tableName, final boolean extraField) {
    return new TableSchema(
        tableName,
        extraField
            ? Arrays.asList(
                new MeasurementSchema("tag1", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.INT64),
                new MeasurementSchema("s2", TSDataType.DOUBLE))
            : Arrays.asList(
                new MeasurementSchema("tag1", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.INT64)),
        extraField
            ? Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD)
            : Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
  }

  @Test
  public void testAnalyzeSingleTableFileShouldNotCountTimestampInPointCount() throws Exception {
    final File tsFile = new File("load-table-mixed-null-device.tsfile");
    writeTableTsFileWithMixedDevices(tsFile);

    final LoadTsFile statement =
        new LoadTsFile(null, tsFile.getAbsolutePath(), Collections.emptyMap()).setDatabase("db");
    final TrackingLoadTsFileTableSchemaCache schemaCache = new TrackingLoadTsFileTableSchemaCache();
    try (final LoadTsFileAnalyzer analyzer =
            new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")));
        final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      injectTableSchemaCache(analyzer, schemaCache);

      final Method method =
          LoadTsFileAnalyzer.class.getDeclaredMethod(
              "doAnalyzeSingleTableFile",
              File.class,
              TsFileSequenceReader.class,
              TsFileSequenceReaderTimeseriesMetadataIterator.class,
              java.util.Map.class);
      method.setAccessible(true);

      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      method.invoke(
          analyzer, tsFile, reader, timeseriesMetadataIterator, reader.getTableSchemaMap());
    } finally {
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }

    Assert.assertEquals(1, statement.getResources().size());
    final TsFileResource resource = statement.getResources().get(0);
    Assert.assertTrue(containsDevice(resource.getDevices(), "table1", "tagA"));
    Assert.assertTrue(containsDevice(resource.getDevices(), "table1", "tagB"));
    Assert.assertEquals(6L, statement.getWritePointCount(0));
    Assert.assertTrue(schemaCache.containsDevice("table1", "tagA"));
    Assert.assertTrue(schemaCache.containsDevice("table1", "tagB"));
    Assert.assertEquals(2, schemaCache.getVerifiedDeviceCount());
  }

  @Test
  public void testTableLoadEmptyPathIsRejected() {
    try {
      new LoadTsFile(null, "", Collections.emptyMap());
      Assert.fail("Expected empty LOAD TSFILE path to be rejected.");
    } catch (final RuntimeException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(DataNodeQueryMessages.EXCEPTION_LOAD_TSFILE_PATH_CANNOT_BE_EMPTY_2B106181));
    }
  }

  @Test
  public void testTableSchemaCacheShouldThrowMismatchWhenVerifyingDataType() throws Exception {
    final LoadTsFileTableSchemaCache schemaCache = createTableSchemaCache(true);
    try {
      final InvocationTargetException exception =
          Assert.assertThrows(
              InvocationTargetException.class,
              () ->
                  getVerifyTableDataTypeMethod()
                      .invoke(
                          schemaCache,
                          createTableSchema(TSDataType.INT64),
                          createTableSchema(TSDataType.DOUBLE)));

      Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeTypeMismatchException);
    } finally {
      schemaCache.close();
    }
  }

  @Test
  public void testTableSchemaCacheShouldNotThrowMismatchWhenSkippingDataTypeVerification()
      throws Exception {
    final LoadTsFileTableSchemaCache schemaCache = createTableSchemaCache(false);
    try {
      getVerifyTableDataTypeMethod()
          .invoke(
              schemaCache,
              createTableSchema(TSDataType.INT64),
              createTableSchema(TSDataType.DOUBLE));
    } finally {
      schemaCache.close();
    }
  }

  @Test
  public void testTreeSchemaVerifierShouldThrowMismatchWhenVerifyingDataType() throws Exception {
    final File tsFile = new File("load-tree-type-mismatch.tsfile");
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    Assert.assertTrue(tsFile.createNewFile());

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            false,
            new MPPQueryContext(new QueryId("load_tree_test")))) {
      final TreeSchemaAutoCreatorAndVerifier verifier =
          new TreeSchemaAutoCreatorAndVerifier(analyzer);
      try {
        final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
        final LoadTsFileTreeSchemaCache schemaCache = getTreeSchemaCache(verifier);
        schemaCache.addTimeSeries(device, new MeasurementSchema("s1", TSDataType.BOOLEAN));
        schemaCache.addIsAlignedCache(device, true, true);

        final ClusterSchemaTree schemaTree = new ClusterSchemaTree();
        schemaTree.appendSingleMeasurement(
            new PartialPath("root.sg.d1.s1"),
            new MeasurementSchema("s1", TSDataType.INT32),
            null,
            null,
            null,
            true);

        final InvocationTargetException exception =
            Assert.assertThrows(
                InvocationTargetException.class,
                () -> getVerifyTreeSchemaMethod().invoke(verifier, schemaTree));
        Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeTypeMismatchException);
      } finally {
        verifier.close();
      }
    } finally {
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }
  }

  @Test
  public void testTreeSchemaVerifierShouldRejectDeviceWithEmptyPathNode() throws Exception {
    final File tsFile = File.createTempFile("load-tree-illegal-device", ".tsfile");

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            false,
            new MPPQueryContext(new QueryId("load_tree_illegal_device_test")))) {
      final TreeSchemaAutoCreatorAndVerifier verifier =
          new TreeSchemaAutoCreatorAndVerifier(analyzer);
      try {
        final IDeviceID device = new StringArrayDeviceID(new String[] {"root", ""});
        getTreeSchemaCache(verifier)
            .addTimeSeries(device, new MeasurementSchema("s1", TSDataType.INT32));

        final InvocationTargetException exception =
            Assert.assertThrows(
                InvocationTargetException.class,
                () -> getAutoCreateDatabaseMethod().invoke(verifier));
        Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeInvalidPathException);
        Assert.assertThrows(LoadAnalyzeInvalidPathException.class, verifier::flush);
      } finally {
        verifier.close();
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testTreeSchemaVerifierShouldIgnoreLegacyDatabaseWithEmptyPathNode() throws Exception {
    final File tsFile = File.createTempFile("load-tree-legacy-database", ".tsfile");

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            false,
            new MPPQueryContext(new QueryId("load_tree_legacy_database_test")))) {
      final TreeSchemaAutoCreatorAndVerifier verifier =
          new TreeSchemaAutoCreatorAndVerifier(analyzer);
      try {
        final PartialPath database = new PartialPath("root.sg");
        final PartialPath databaseWithSameStringPrefix = new PartialPath("root.sg1");
        final Set<PartialPath> databasesNeededToBeSet =
            new HashSet<>(Arrays.asList(database, databaseWithSameStringPrefix));

        verifier.filterAlreadySetDatabases(databasesNeededToBeSet, Collections.singleton("root."));

        Assert.assertEquals(
            new HashSet<>(Arrays.asList(database, databaseWithSameStringPrefix)),
            databasesNeededToBeSet);
        Assert.assertTrue(getTreeSchemaCache(verifier).getAlreadySetDatabases().isEmpty());

        verifier.filterAlreadySetDatabases(
            databasesNeededToBeSet, Collections.singleton(database.getFullPath()));

        Assert.assertEquals(
            Collections.singleton(databaseWithSameStringPrefix), databasesNeededToBeSet);
        Assert.assertEquals(
            Collections.singleton(database), getTreeSchemaCache(verifier).getAlreadySetDatabases());

        getTreeSchemaCache(verifier)
            .addTimeSeries(
                new StringArrayDeviceID(new String[] {"root.sg", "d1"}),
                new MeasurementSchema("s1", TSDataType.INT32));
        // A valid device still uses its existing database despite the legacy root. entry.
        getAutoCreateDatabaseMethod().invoke(verifier);
      } finally {
        verifier.close();
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testPipeGeneratedLoadMissingSchemaShouldBeTemporaryWhenAutoCreateDisabled()
      throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    final File tsFile = File.createTempFile("missing-schema", ".tsfile");
    tsFile.deleteOnExit();

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            true,
            new MPPQueryContext(new QueryId("load_pipe_test")))) {
      Assert.assertTrue(
          analyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
              new LoadAnalyzeMissingSchemaException("missing device schema")));
      Assert.assertTrue(
          analyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
              new RuntimeException(
                  "wrapped", new LoadAnalyzeMissingSchemaException("missing measurement schema"))));
      Assert.assertFalse(
          analyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
              new LoadAnalyzeException("Data type mismatch for measurement root.sg.d1.s1")));
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
    }
  }

  @Test
  public void testPipeGeneratedLoadMissingSchemaShouldBeTemporaryWhenPerLoadAutoCreateDisabled()
      throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    final File tsFile = File.createTempFile("missing-schema-per-load", ".tsfile");

    try {
      final LoadTsFileStatement waitingStatement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      waitingStatement.setAutoCreateSchema(false);
      try (final LoadTsFileAnalyzer waitingAnalyzer =
          new LoadTsFileAnalyzer(
              waitingStatement, true, new MPPQueryContext(new QueryId("load_pipe_waiting_test")))) {
        Assert.assertFalse(waitingAnalyzer.isAutoCreateSchemaRequested());
        Assert.assertTrue(
            waitingAnalyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
                new LoadAnalyzeMissingSchemaException("missing schema")));
      }

      try (final LoadTsFileAnalyzer defaultAnalyzer =
          new LoadTsFileAnalyzer(
              LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
              true,
              new MPPQueryContext(new QueryId("load_pipe_default_test")))) {
        Assert.assertTrue(defaultAnalyzer.isAutoCreateSchemaRequested());
        Assert.assertFalse(
            defaultAnalyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
                new LoadAnalyzeMissingSchemaException("missing schema")));
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testGlobalAutoCreateDisabledKeepsPerLoadAutoCreatePermission() throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    final File tsFile = File.createTempFile("global-auto-create-disabled", ".tsfile");

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            true,
            new MPPQueryContext(new QueryId("load_global_auto_create_disabled_test")))) {
      Assert.assertFalse(analyzer.isAutoCreateSchemaEnabled());
      Assert.assertTrue(analyzer.isAutoCreateSchemaRequested());
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
      Assert.assertTrue(tsFile.delete());
    }
  }

  private void writeTableTsFileWithMixedDevices(final File tsFile) throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.DOUBLE));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(schema);

      writeDevice(writer, tableSchemaList, new String[] {"table1", "tagA"}, false);
      writeDevice(writer, tableSchemaList, new String[] {"table1", "tagB"}, true);

      writer.endFile();
    }
  }

  private void writeDevice(
      final TsFileIOWriter writer,
      final List<IMeasurementSchema> tableSchemaList,
      final String[] deviceSegments,
      final boolean areAllFieldsNull)
      throws Exception {
    writer.startChunkGroup(new StringArrayDeviceID(deviceSegments));

    final AlignedChunkWriterImpl chunkWriter =
        new AlignedChunkWriterImpl(tableSchemaList.subList(1, tableSchemaList.size()));
    for (int i = 0; i < 3; i++) {
      final long time = 100 + i;
      chunkWriter.getTimeChunkWriter().write(time);
      chunkWriter.getValueChunkWriterByIndex(0).write(time, (long) i, areAllFieldsNull);
      chunkWriter.getValueChunkWriterByIndex(1).write(time, 0.5 + i, areAllFieldsNull);
    }
    chunkWriter.writeToFileWriter(writer);
    writer.endChunkGroup();
  }

  private void injectTableSchemaCache(
      final LoadTsFileAnalyzer analyzer, final TrackingLoadTsFileTableSchemaCache schemaCache)
      throws Exception {
    final Field tableSchemaCacheField =
        LoadTsFileAnalyzer.class.getDeclaredField("tableSchemaCache");
    tableSchemaCacheField.setAccessible(true);
    tableSchemaCacheField.set(analyzer, schemaCache);
  }

  private LoadTsFileTreeSchemaCache getTreeSchemaCache(
      final TreeSchemaAutoCreatorAndVerifier verifier) throws Exception {
    final Field schemaCacheField =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredField("schemaCache");
    schemaCacheField.setAccessible(true);
    return (LoadTsFileTreeSchemaCache) schemaCacheField.get(verifier);
  }

  private LoadTsFileTableSchemaCache createTableSchemaCache(final boolean shouldVerifyDataType)
      throws LoadRuntimeOutOfMemoryException {
    return new LoadTsFileTableSchemaCache(
        null, new MPPQueryContext(new QueryId("load_test")), false, shouldVerifyDataType);
  }

  private Method getVerifyTableDataTypeMethod() throws NoSuchMethodException {
    final Method method =
        LoadTsFileTableSchemaCache.class.getDeclaredMethod(
            "verifyTableDataTypeAndGenerateTagColumnMapper",
            org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema.class,
            org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema.class);
    method.setAccessible(true);
    return method;
  }

  private Method getVerifyTreeSchemaMethod() throws NoSuchMethodException {
    final Method method =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredMethod("verifySchema", ISchemaTree.class);
    method.setAccessible(true);
    return method;
  }

  private Method getAutoCreateDatabaseMethod() throws NoSuchMethodException {
    final Method method =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredMethod("autoCreateDatabase");
    method.setAccessible(true);
    return method;
  }

  private org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema
      createTableSchema(final TSDataType fieldType) {
    return new org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema(
        "table1",
        Arrays.asList(
            new ColumnSchema(
                "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG),
            new ColumnSchema(
                "s1", TypeFactory.getType(fieldType), false, TsTableColumnCategory.FIELD)));
  }

  @Test
  public void testDetectTableModelTsFile() throws Exception {
    final File dir = Files.createTempDirectory("detect-table-model").toFile();
    final File markedFile = new File(dir, "marked.tsfile");
    final File treeFile = new File(dir, "tree.tsfile");
    final File legacyTableFile = new File(dir, "legacy-table.tsfile");
    try {
      // A table-model file without any TableSchema entry is still recognized by its file property
      try (final TsFileIOWriter writer = new TsFileIOWriter(markedFile)) {
        TsFileTableSchemaRegistrationUtils.markTableModel(writer);
        writer.endFile();
      }
      try (final TsFileSequenceReader reader = new TsFileSequenceReader(markedFile.getPath())) {
        final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
        Assert.assertTrue(tableSchemaMap.isEmpty());
        Assert.assertTrue(LoadTsFileAnalyzer.detectTableModelTsFile(reader, tableSchemaMap));
      }

      // A tree-model file has neither the property nor a TableSchema map
      try (final TsFileIOWriter writer = new TsFileIOWriter(treeFile)) {
        writer.endFile();
      }
      try (final TsFileSequenceReader reader = new TsFileSequenceReader(treeFile.getPath())) {
        final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
        Assert.assertFalse(LoadTsFileAnalyzer.detectTableModelTsFile(reader, tableSchemaMap));
      }

      // A table-model file written before the property existed falls back to the TableSchema map
      try (final TsFileIOWriter writer = new TsFileIOWriter(legacyTableFile)) {
        writer.getSchema().registerTableSchema(new TableSchema("t1"));
        writer.endFile();
      }
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(legacyTableFile.getPath())) {
        final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
        Assert.assertFalse(tableSchemaMap.isEmpty());
        Assert.assertTrue(LoadTsFileAnalyzer.detectTableModelTsFile(reader, tableSchemaMap));
      }
    } finally {
      Files.deleteIfExists(markedFile.toPath());
      Files.deleteIfExists(treeFile.toPath());
      Files.deleteIfExists(legacyTableFile.toPath());
      Files.deleteIfExists(dir.toPath());
    }
  }

  private boolean containsDevice(final Set<IDeviceID> devices, final String... expectedSegments) {
    return devices.stream()
        .anyMatch(device -> Arrays.equals(device.getSegments(), expectedSegments));
  }

  private MPPQueryContext createTableQueryContext(
      final String queryId, final String database, final boolean enableAutoCreateSchema) {
    return createLoadQueryContext(queryId, database, SqlDialect.TABLE, enableAutoCreateSchema);
  }

  private MPPQueryContext createLoadQueryContext(
      final String queryId,
      final String database,
      final SqlDialect dialect,
      final boolean enableAutoCreateSchema) {
    final MPPQueryContext context = new MPPQueryContext(new QueryId(queryId));
    context.setSession(
        new SessionInfo(
            0,
            new UserEntity(AuthorityChecker.SUPER_USER_ID, AuthorityChecker.SUPER_USER, ""),
            ZoneId.systemDefault(),
            IoTDBConstant.ClientVersion.V_1_0,
            database,
            dialect,
            enableAutoCreateSchema));
    return context;
  }

  private static class TrackingLoadTsFileTableSchemaCache extends LoadTsFileTableSchemaCache {

    private final Set<List<Object>> verifiedDevices = new HashSet<>();

    private TrackingLoadTsFileTableSchemaCache() throws LoadRuntimeOutOfMemoryException {
      super(null, new MPPQueryContext(new QueryId("load_test")), false, true);
    }

    @Override
    public void autoCreateAndVerify(final IDeviceID device) {
      verifiedDevices.add(Arrays.asList(device.getSegments()));
    }

    @Override
    public boolean isDeviceDeletedByMods(final IDeviceID device) {
      return false;
    }

    private boolean containsDevice(final String... expectedSegments) {
      return verifiedDevices.contains(Arrays.asList((Object[]) expectedSegments));
    }

    private int getVerifiedDeviceCount() {
      return verifiedDevices.size();
    }
  }
}
