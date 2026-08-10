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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeMissingSchemaException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.FullExactMatch;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LoadTsFileAnalyzerTest {

  private int dataNodeId;
  private boolean skipFailedTableSchemaCheck;

  @Before
  public void setUp() {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    skipFailedTableSchemaCheck =
        IoTDBDescriptor.getInstance().getConfig().isSkipFailedTableSchemaCheck();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSkipFailedTableSchemaCheck(skipFailedTableSchemaCheck);
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
      Assert.assertTrue(e.getMessage().contains("The LOAD TSFILE path cannot be empty."));
    }
  }

  @Test
  public void testIdentityWritableViewTargetUsesNativeLoadFastPath() throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String database = "load_writable_view_ut";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable sourceTable = new TsTable(sourceName);
      sourceTable.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      sourceTable.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, sourceTable, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView writableView = new WritableView(viewName, database, sourceName, true);
      writableView.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      writableView.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, writableView, null);
      cache.commitUpdateTable(database, viewName, null);

      final LoadTsFileTableSchemaCache schemaCache =
          new LoadTsFileTableSchemaCache(
              new TableMetadataImpl(), createTableQueryContext("load_view", database), false, true);
      try {
        schemaCache.setDatabase(database);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    viewName,
                    new TableSchema(
                        viewName,
                        Arrays.asList(
                            new MeasurementSchema("device_id", TSDataType.STRING),
                            new MeasurementSchema("temperature", TSDataType.INT32)),
                        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)))));
        IoTDBDescriptor.getInstance().getConfig().setSkipFailedTableSchemaCheck(true);

        schemaCache.autoCreateAndVerify(new StringArrayDeviceID(viewName, "d0"));

        Assert.assertEquals(
            sourceName, schemaCache.getWritableViewTableNameRewriteMap().get(viewName));
      } finally {
        schemaCache.close();
      }
    } finally {
      cache.invalid(database);
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
  public void testProjectionWritableViewTargetUsesNativeLoadFastPath() throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String database = "load_projection_writable_view_ut";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable sourceTable = new TsTable(sourceName);
      sourceTable.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      sourceTable.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      sourceTable.addColumnSchema(new FieldColumnSchema("humidity", TSDataType.INT32));
      cache.preUpdateTable(database, sourceTable, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView writableView = new WritableView(viewName, database, sourceName, true);
      writableView.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      writableView.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, writableView, null);
      cache.commitUpdateTable(database, viewName, null);

      final LoadTsFileTableSchemaCache schemaCache =
          new LoadTsFileTableSchemaCache(
              new TableMetadataImpl(),
              createTableQueryContext("load_projection_view", database),
              false,
              true);
      try {
        schemaCache.setDatabase(database);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    viewName,
                    new TableSchema(
                        viewName,
                        Arrays.asList(
                            new MeasurementSchema("device_id", TSDataType.STRING),
                            new MeasurementSchema("temperature", TSDataType.INT32)),
                        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)))));
        IoTDBDescriptor.getInstance().getConfig().setSkipFailedTableSchemaCheck(true);

        schemaCache.autoCreateAndVerify(new StringArrayDeviceID(viewName, "d0"));

        Assert.assertEquals(
            sourceName, schemaCache.getWritableViewTableNameRewriteMap().get(viewName));
        Assert.assertTrue(schemaCache.getWritableViewColumnNameRewriteMap().isEmpty());
      } finally {
        schemaCache.close();
      }
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testIdentityWritableViewTargetWithObjectUsesNativeLoadFastPath() throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String database = "load_writable_view_object_ut";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable sourceTable = new TsTable(sourceName);
      sourceTable.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      sourceTable.addColumnSchema(new FieldColumnSchema("payload", TSDataType.OBJECT));
      cache.preUpdateTable(database, sourceTable, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView writableView = new WritableView(viewName, database, sourceName, true);
      writableView.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      writableView.addColumnSchema(new FieldColumnSchema("payload", TSDataType.OBJECT));
      cache.preUpdateTable(database, writableView, null);
      cache.commitUpdateTable(database, viewName, null);

      final LoadTsFileTableSchemaCache schemaCache =
          new LoadTsFileTableSchemaCache(
              new TableMetadataImpl(),
              createTableQueryContext("load_view_object", database),
              false,
              true);
      try {
        schemaCache.setDatabase(database);
        schemaCache.setCurrentFileContainsObjectColumn(true);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    viewName,
                    new TableSchema(
                        viewName,
                        Arrays.asList(
                            new MeasurementSchema("device_id", TSDataType.STRING),
                            new MeasurementSchema("payload", TSDataType.OBJECT)),
                        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)))));
        IoTDBDescriptor.getInstance().getConfig().setSkipFailedTableSchemaCheck(true);

        schemaCache.autoCreateAndVerify(new StringArrayDeviceID(viewName, "d0"));

        Assert.assertEquals(
            sourceName, schemaCache.getWritableViewTableNameRewriteMap().get(viewName));
      } finally {
        schemaCache.close();
      }
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testIdentityWritableViewTargetWithModsUsesNativeLoadFastPath() throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String database = "load_writable_view_mod_ut";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable sourceTable = new TsTable(sourceName);
      sourceTable.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      sourceTable.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, sourceTable, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView writableView = new WritableView(viewName, database, sourceName, true);
      writableView.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      writableView.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, writableView, null);
      cache.commitUpdateTable(database, viewName, null);

      final LoadTsFileTableSchemaCache schemaCache =
          new LoadTsFileTableSchemaCache(
              new TableMetadataImpl(),
              createTableQueryContext("load_view_mod", database),
              false,
              true);
      try {
        schemaCache.setDatabase(database);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    viewName,
                    new TableSchema(
                        viewName,
                        Arrays.asList(
                            new MeasurementSchema("device_id", TSDataType.STRING),
                            new MeasurementSchema("temperature", TSDataType.INT32)),
                        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)))));
        appendCurrentModification(
            schemaCache,
            new TableDeletionEntry(
                new DeletionPredicate(
                    viewName, new FullExactMatch(new StringArrayDeviceID(viewName, "d0"))),
                new TimeRange(100, 101)));
        IoTDBDescriptor.getInstance().getConfig().setSkipFailedTableSchemaCheck(true);

        schemaCache.autoCreateAndVerify(new StringArrayDeviceID(viewName, "d0"));

        Assert.assertEquals(
            sourceName, schemaCache.getWritableViewTableNameRewriteMap().get(viewName));
      } finally {
        schemaCache.close();
      }
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testIdentityWritableViewTargetWithFullyDeletedDeviceRecordsRewrite()
      throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String database = "load_writable_view_full_deleted_mod_ut";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable sourceTable = new TsTable(sourceName);
      sourceTable.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      sourceTable.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, sourceTable, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView writableView = new WritableView(viewName, database, sourceName, true);
      writableView.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      writableView.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, writableView, null);
      cache.commitUpdateTable(database, viewName, null);

      final LoadTsFileTableSchemaCache schemaCache =
          new LoadTsFileTableSchemaCache(
              new TableMetadataImpl(),
              createTableQueryContext("load_view_fully_deleted_mod", database),
              false,
              true);
      try {
        schemaCache.setDatabase(database);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    viewName,
                    new TableSchema(
                        viewName,
                        Arrays.asList(
                            new MeasurementSchema("device_id", TSDataType.STRING),
                            new MeasurementSchema("temperature", TSDataType.INT32)),
                        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)))));
        appendCurrentModification(
            schemaCache,
            new TableDeletionEntry(
                new DeletionPredicate(
                    viewName, new FullExactMatch(new StringArrayDeviceID(viewName, "d0"))),
                new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)));
        IoTDBDescriptor.getInstance().getConfig().setSkipFailedTableSchemaCheck(true);

        schemaCache.autoCreateAndVerify(new StringArrayDeviceID(viewName, "d0"));

        Assert.assertEquals(
            sourceName, schemaCache.getWritableViewTableNameRewriteMap().get(viewName));
      } finally {
        schemaCache.close();
      }
    } finally {
      cache.invalid(database);
    }
  }

  @Test
  public void testAliasWritableViewTargetUsesNativeLoadFastPath() throws Exception {
    final ITableCache cache = DataNodeTableCache.getInstance();
    final String database = "load_alias_writable_view_ut";
    final String sourceName = "source_table";
    final String viewName = "writable_view";

    cache.invalid(database);
    try {
      final TsTable sourceTable = new TsTable(sourceName);
      sourceTable.addColumnSchema(new TagColumnSchema("source_device", TSDataType.STRING));
      sourceTable.addColumnSchema(new FieldColumnSchema("temperature", TSDataType.INT32));
      cache.preUpdateTable(database, sourceTable, null);
      cache.commitUpdateTable(database, sourceName, null);

      final WritableView writableView = new WritableView(viewName, database, sourceName, true);
      writableView.addColumnSchema(new TagColumnSchema("device_id", TSDataType.STRING));
      writableView.addColumnSchema(new FieldColumnSchema("temp", TSDataType.INT32));
      writableView.putViewColumnSourceColumnMapping("device_id", "source_device");
      writableView.putViewColumnSourceColumnMapping("temp", "temperature");
      cache.preUpdateTable(database, writableView, null);
      cache.commitUpdateTable(database, viewName, null);

      final LoadTsFileTableSchemaCache schemaCache =
          new LoadTsFileTableSchemaCache(
              new TableMetadataImpl(),
              createTableQueryContext("load_non_identity_view", database),
              false,
              true);
      try {
        schemaCache.setDatabase(database);
        schemaCache.setTableSchemaMap(
            new HashMap<>(
                Collections.singletonMap(
                    viewName,
                    new TableSchema(
                        viewName,
                        Arrays.asList(
                            new MeasurementSchema("device_id", TSDataType.STRING),
                            new MeasurementSchema("temp", TSDataType.INT32)),
                        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD)))));
        appendCurrentModification(
            schemaCache,
            new TableDeletionEntry(
                new DeletionPredicate(
                    viewName,
                    new FullExactMatch(new StringArrayDeviceID(viewName, "d0")),
                    Collections.singletonList("temp")),
                new TimeRange(100, 101)));
        IoTDBDescriptor.getInstance().getConfig().setSkipFailedTableSchemaCheck(true);

        schemaCache.autoCreateAndVerify(new StringArrayDeviceID(viewName, "d0"));

        Assert.assertEquals(
            sourceName, schemaCache.getWritableViewTableNameRewriteMap().get(viewName));
        Assert.assertEquals(
            "source_device",
            schemaCache.getWritableViewColumnNameRewriteMap().get(viewName).get("device_id"));
        Assert.assertEquals(
            "temperature",
            schemaCache.getWritableViewColumnNameRewriteMap().get(viewName).get("temp"));
      } finally {
        schemaCache.close();
      }
    } finally {
      cache.invalid(database);
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
        Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeException);
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

  private void appendCurrentModification(
      final LoadTsFileTableSchemaCache schemaCache, final TableDeletionEntry modification)
      throws Exception {
    final Field currentModificationListField =
        LoadTsFileTableSchemaCache.class.getDeclaredField("currentModificationList");
    currentModificationListField.setAccessible(true);
    currentModificationListField.set(schemaCache, Collections.singletonList(modification));

    final Field currentModificationsField =
        LoadTsFileTableSchemaCache.class.getDeclaredField("currentModifications");
    currentModificationsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final org.apache.iotdb.commons.path.PatternTreeMap<
            org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry,
            org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer>
        currentModifications =
            (org.apache.iotdb.commons.path.PatternTreeMap<
                    org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry,
                    org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer>)
                currentModificationsField.get(schemaCache);
    currentModifications.append(modification.keyOfPatternTree(), modification);
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

  private boolean containsDevice(final Set<IDeviceID> devices, final String... expectedSegments) {
    return devices.stream()
        .anyMatch(device -> Arrays.equals(device.getSegments(), expectedSegments));
  }

  private MPPQueryContext createTableQueryContext(final String queryId, final String database) {
    final MPPQueryContext context = new MPPQueryContext(new QueryId(queryId));
    context.setSession(
        new SessionInfo(
            0,
            new UserEntity(AuthorityChecker.SUPER_USER_ID, AuthorityChecker.SUPER_USER, ""),
            ZoneId.systemDefault(),
            database,
            SqlDialect.TABLE));
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
