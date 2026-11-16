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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeMissingDatabaseException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeMissingSchemaException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ASYNC_LOAD_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.AUTO_CREATE_SCHEMA_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.DATABASE_NAME_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({
  LoadTsFileAnalyzer.class,
  LoadUtil.class,
  SchemaValidator.class,
  DataNodeTableCache.class,
  Coordinator.class,
  ClusterConfigTaskExecutor.class
})
public class LoadTsFileAnalyzerDiagnosticsTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LoadTsFileDataTypeConverter converter;
  private int originalDataNodeId;

  @Before
  public void setUp() throws Exception {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    converter = mock(LoadTsFileDataTypeConverter.class);
    when(converter.isSuccessful(any())).thenCallRealMethod();
    PowerMockito.whenNew(LoadTsFileDataTypeConverter.class)
        .withAnyArguments()
        .thenReturn(converter);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testUnavailableSchemaRegionsShouldRemainRetryable() throws Exception {
    final TSStatus status = analyzeAfterSchemaFailure(TSStatusCode.NO_AVAILABLE_REGION_GROUP);

    Assert.assertEquals(
        TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains("schema request failed"));
  }

  @Test
  public void testPermanentSchemaErrorShouldNotBecomeRetryable() throws Exception {
    final TSStatus status = analyzeAfterSchemaFailure(TSStatusCode.METADATA_ERROR);

    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains("schema request failed"));
  }

  @Test
  public void testWrappedUnavailableSchemaRegionsShouldRemainRetryable() throws Exception {
    // Template activation preserves the status in a checked exception wrapped by SemanticException.
    final TSStatus status =
        analyzeAfterSchemaFailure(
            new SemanticException(
                new IoTDBException(
                    "schema request failed",
                    TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())));

    Assert.assertEquals(
        TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains("schema request failed"));
  }

  @Test
  public void testWrappedPermanentSchemaErrorShouldNotBecomeRetryable() throws Exception {
    final TSStatus status =
        analyzeAfterSchemaFailure(
            new SemanticException(
                new IoTDBException(
                    "schema request failed", TSStatusCode.METADATA_ERROR.getStatusCode())));

    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains("schema request failed"));
  }

  @Test
  public void testUnavailableRegionsDuringSingleFileAnalysisShouldRetry() throws Exception {
    final File file = temporaryFolder.newFile("regions-recovering.tsfile");
    try (final FileOutputStream output = new FileOutputStream(file)) {
      output.write(1);
    }
    PowerMockito.whenNew(TsFileSequenceReader.class)
        .withArguments(file.getAbsolutePath())
        .thenThrow(
            new IoTDBRuntimeException(
                "region request failed", TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode()));

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(file.getPath()),
            false,
            new MPPQueryContext(new QueryId("load_region_recovery")))) {
      final IAnalysis analysis = analyzer.analyzeFileByFile(new Analysis());

      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      Assert.assertEquals(
          analysis.getFailStatus().getMessage(),
          TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
          analysis.getFailStatus().getCode());
      Assert.assertEquals("region request failed", analysis.getFailStatus().getMessage());
      Assert.assertTrue(file.exists());
    }
  }

  private TSStatus analyzeAfterSchemaFailure(final TSStatusCode code) throws Exception {
    return analyzeAfterSchemaFailure(
        new IoTDBRuntimeException("schema request failed", code.getStatusCode()));
  }

  private TSStatus analyzeAfterSchemaFailure(final RuntimeException exception) throws Exception {
    PowerMockito.mockStatic(SchemaValidator.class);
    when(SchemaValidator.validate(
            any(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyBoolean(),
            any()))
        .thenThrow(exception);
    final File file = temporaryFolder.newFile("schema-failure.tsfile");
    final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(file.getPath());
    statement.setAutoCreateDatabase(false);

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            statement, false, new MPPQueryContext(new QueryId("load_schema_recovery")))) {
      // Model a batch whose metadata was accumulated before the final schema flush.
      final TreeSchemaAutoCreatorAndVerifier verifier =
          new TreeSchemaAutoCreatorAndVerifier(analyzer);
      final LoadTsFileTreeSchemaCache cache = Whitebox.getInternalState(verifier, "schemaCache");
      final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d");
      cache.addTimeSeries(device, new MeasurementSchema("s", TSDataType.INT64));
      cache.addIsAlignedCache(device, false, true);
      Whitebox.setInternalState(analyzer, "treeSchemaAutoCreatorAndVerifier", verifier);

      final IAnalysis analysis = analyzer.analyzeFileByFile(new Analysis());
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      Assert.assertTrue(file.exists());
      return analysis.getFailStatus();
    }
  }

  @Test
  public void testModelReadFailureReportsCurrentError() throws Exception {
    final File file = temporaryFolder.newFile("unreadable.tsfile");
    PowerMockito.whenNew(TsFileSequenceReader.class)
        .withArguments(file.getAbsolutePath(), true)
        .thenThrow(new IOException("cannot read TsFile footer"));

    final TSStatus status = convertAfterAnalysisFailure(file);

    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains(file.getPath()));
    Assert.assertTrue(status.getMessage().contains("cannot read TsFile footer"));
    Assert.assertFalse(status.getMessage().contains("original analysis failure"));
  }

  @Test
  public void testConversionExceptionReportsCurrentErrorForBothModels() throws Exception {
    when(converter.convertForTreeModel(any()))
        .thenThrow(new IllegalStateException("tree conversion failed"));
    when(converter.convertForTableModel(any()))
        .thenThrow(new IllegalStateException("table conversion failed"));

    for (final boolean tableModel : new boolean[] {false, true}) {
      mockModelReader(tableModel);
      final File file = temporaryFolder.newFile(tableModel ? "table.tsfile" : "tree.tsfile");

      final TSStatus status = convertAfterAnalysisFailure(file);

      Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
      Assert.assertTrue(status.getMessage().contains(file.getPath()));
      Assert.assertTrue(
          status
              .getMessage()
              .contains(tableModel ? "table conversion failed" : "tree conversion failed"));
      Assert.assertFalse(status.getMessage().contains("original analysis failure"));
    }
  }

  @Test
  public void testConversionExceptionWithoutMessageStillHasDetails() throws Exception {
    mockModelReader(false);
    when(converter.convertForTreeModel(any())).thenThrow(new IllegalStateException());

    final File file = temporaryFolder.newFile("no-message.tsfile");
    final TSStatus status = convertAfterAnalysisFailure(file);

    Assert.assertTrue(status.getMessage().contains(file.getPath()));
    Assert.assertTrue(status.getMessage().contains(IllegalStateException.class.getName()));
  }

  @Test
  public void testMissingConversionStatusHasExplicitReason() throws Exception {
    mockModelReader(false);
    when(converter.convertForTreeModel(any())).thenReturn(Optional.empty());

    final File file = temporaryFolder.newFile("no-status.tsfile");
    final TSStatus status = convertAfterAnalysisFailure(file);

    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains(file.getPath()));
    Assert.assertTrue(
        status
            .getMessage()
            .contains(DataNodeQueryMessages.MESSAGE_TABLET_CONVERSION_RETURNED_NO_STATUS_88BAB5D3));
    Assert.assertFalse(status.getMessage().contains("original analysis failure"));
  }

  @Test
  public void testConversionStatusKeepsCodeAndMetadataWithoutMutatingOriginal() throws Exception {
    mockModelReader(false);
    final TSStatus original =
        new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
            .setMessage("tablet insertion failed")
            .setSubStatus(
                Collections.singletonList(
                    new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
                        .setMessage("missing WRITE_DATA privilege")))
            .setRedirectNode(new TEndPoint("127.0.0.2", 10730))
            .setNeedRetry(true);
    final TSStatus snapshot = original.deepCopy();
    when(converter.convertForTreeModel(any())).thenReturn(Optional.of(original));

    final File file = temporaryFolder.newFile("denied.tsfile");
    final TSStatus status = convertAfterAnalysisFailure(file);

    Assert.assertNotSame(original, status);
    Assert.assertEquals(snapshot, original);
    Assert.assertEquals(original.getCode(), status.getCode());
    Assert.assertEquals(original.getSubStatus(), status.getSubStatus());
    Assert.assertEquals(original.getRedirectNode(), status.getRedirectNode());
    Assert.assertEquals(original.isNeedRetry(), status.isNeedRetry());
    Assert.assertTrue(status.getMessage().contains(file.getPath()));
    Assert.assertTrue(status.getMessage().contains("missing WRITE_DATA privilege"));
  }

  @Test
  public void testMiniFileTableConversionPreservesAutoCreateSchema() throws Exception {
    assertTableConversionPreservesAutoCreateSchema(true);
  }

  @Test
  public void testFallbackTableConversionPreservesAutoCreateSchema() throws Exception {
    assertTableConversionPreservesAutoCreateSchema(false);
  }

  @Test
  public void testMiniFileTreeConversionPreservesAutoCreateSchema() throws Exception {
    assertTreeConversionPreservesAutoCreateSchema(true);
  }

  @Test
  public void testFallbackTreeConversionPreservesAutoCreateSchema() throws Exception {
    assertTreeConversionPreservesAutoCreateSchema(false);
  }

  private void assertTreeConversionPreservesAutoCreateSchema(final boolean miniFile)
      throws Exception {
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    final File file = temporaryFolder.newFile("tree-conversion.tsfile");
    for (final boolean tableModelRequest : new boolean[] {false, true}) {
      for (final boolean requested : new boolean[] {false, true, false}) {
        for (final boolean generatedByPipe : new boolean[] {false, true}) {
          clearInvocations(converter);
          final Map<String, String> attributes =
              Collections.singletonMap(AUTO_CREATE_SCHEMA_KEY, String.valueOf(requested));
          final LoadTsFile tableStatement =
              LoadTsFile.createUnchecked(null, file.getAbsolutePath(), attributes);
          final LoadTsFileStatement treeStatement =
              LoadTsFileStatement.createUnchecked(file.getAbsolutePath());
          treeStatement.setAutoCreateSchema(requested);
          final MPPQueryContext context = new MPPQueryContext(new QueryId("tree_conversion"));
          try (final LoadTsFileAnalyzer analyzer =
              tableModelRequest
                  ? new LoadTsFileAnalyzer(tableStatement, generatedByPipe, context)
                  : new LoadTsFileAnalyzer(treeStatement, generatedByPipe, context)) {
            final List<Boolean> tableModelFiles =
                Whitebox.getInternalState(analyzer, "isTableModelTsFile");
            tableModelFiles.set(0, false);
            Whitebox.setInternalState(analyzer, "isTableModelTsFileReliableIndex", 0);
            if (miniFile) {
              Assert.assertTrue(
                  (Boolean) Whitebox.invokeMethod(analyzer, "handleSingleMiniFile", 0));
            } else {
              final Analysis analysis = new Analysis();
              Whitebox.invokeMethod(
                  analyzer,
                  "executeTabletConversionOnException",
                  (IAnalysis) analysis,
                  new LoadAnalyzeException("original analysis failure"));
              Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
              Assert.assertFalse(analysis.isFailed());
            }
            final ArgumentCaptor<LoadTsFileStatement> statement =
                ArgumentCaptor.forClass(LoadTsFileStatement.class);
            verify(converter).convertForTreeModel(statement.capture());
            Assert.assertEquals(requested, statement.getValue().isAutoCreateSchema());
            Assert.assertEquals(Collections.singletonList(file), statement.getValue().getTsFiles());
          }
        }
      }
    }
  }

  @Test
  public void testMissingDatabaseDoesNotTryTabletConversion() throws Exception {
    final File file = temporaryFolder.newFile("missing-database-conversion.tsfile");
    final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(file.getPath());
    statement.setAutoCreateSchema(false);
    final String message =
        String.format(
            DataNodeQueryMessages
                .EXCEPTION_THE_DATABASE_ARG_DOES_NOT_EXIST_AND_AUTO_CREATE_SCHEMA_IS_DISABLED_28F8D49A,
            "missing_db");
    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            statement, false, new MPPQueryContext(new QueryId("missing_database_conversion")))) {
      final Analysis analysis = new Analysis();
      Whitebox.invokeMethod(
          analyzer,
          "executeTabletConversionOnException",
          (IAnalysis) analysis,
          new LoadAnalyzeMissingDatabaseException(message));
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      Assert.assertEquals(
          TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), analysis.getFailStatus().getCode());
      Assert.assertEquals(message, analysis.getFailStatus().getMessage());
      verifyZeroInteractions(converter);
      Assert.assertTrue(file.exists());
    }
  }

  @Test
  public void testDatabaseAutoCreationHonorsRequestPolicy() throws Exception {
    assertDatabaseAutoCreationDisabled(true, false, true);
  }

  @Test
  public void testDatabaseAutoCreationHonorsSessionPolicy() throws Exception {
    assertDatabaseAutoCreationDisabled(true, true, false);
  }

  @Test
  public void testDatabaseAutoCreationHonorsGlobalPolicy() throws Exception {
    assertDatabaseAutoCreationDisabled(false, true, true);
  }

  private void assertDatabaseAutoCreationDisabled(
      final boolean globalEnabled, final boolean requested, final boolean sessionEnabled)
      throws Exception {
    final boolean originalGlobalEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    final IClientManager<ConfigRegionId, ConfigNodeClient> originalClientManager =
        Whitebox.getInternalState(
            TreeSchemaAutoCreatorAndVerifier.class, "CONFIG_NODE_CLIENT_MANAGER");
    final IClientManager<ConfigRegionId, ConfigNodeClient> clientManager =
        mock(IClientManager.class);
    final ConfigNodeClient client = mock(ConfigNodeClient.class);
    when(clientManager.borrowClient(any())).thenReturn(client);
    when(client.showDatabase(any()))
        .thenReturn(new TShowDatabaseResp().setDatabaseInfoMap(Collections.emptyMap()));
    final DataNodeTableCache tableCache = mock(DataNodeTableCache.class);
    PowerMockito.mockStatic(DataNodeTableCache.class);
    when(DataNodeTableCache.getInstance()).thenReturn(tableCache);
    final ClusterConfigTaskExecutor executor = mock(ClusterConfigTaskExecutor.class);
    PowerMockito.mockStatic(ClusterConfigTaskExecutor.class);
    when(ClusterConfigTaskExecutor.getInstance()).thenReturn(executor);
    final Coordinator coordinator = mock(Coordinator.class);
    PowerMockito.mockStatic(Coordinator.class);
    when(Coordinator.getInstance()).thenReturn(coordinator);
    final Metadata metadata = mock(Metadata.class);
    try {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(globalEnabled);
      Whitebox.setInternalState(
          TreeSchemaAutoCreatorAndVerifier.class, "CONFIG_NODE_CLIENT_MANAGER", clientManager);
      for (final boolean tableModelRequest : new boolean[] {false, true}) {
        final File file =
            temporaryFolder.newFile("missing-database-" + tableModelRequest + ".tsfile");
        final Map<String, String> attributes =
            Collections.singletonMap(AUTO_CREATE_SCHEMA_KEY, String.valueOf(requested));
        final LoadTsFile tableStatement =
            LoadTsFile.createUnchecked(null, file.getAbsolutePath(), attributes);
        final LoadTsFileStatement treeStatement =
            LoadTsFileStatement.createUnchecked(file.getAbsolutePath());
        treeStatement.setAutoCreateSchema(requested);
        // Leave the database-creation path enabled, as it is by default with global creation on.
        tableStatement.setAutoCreateDatabase(true);
        treeStatement.setAutoCreateDatabase(true);
        final MPPQueryContext context = new MPPQueryContext(new QueryId("missing_database"));
        context.setSession(
            new SessionInfo(
                0,
                new UserEntity(0, "root", ""),
                ZoneOffset.UTC,
                IoTDBConstant.ClientVersion.V_1_0,
                "missing_db",
                tableModelRequest ? SqlDialect.TABLE : SqlDialect.TREE,
                sessionEnabled));
        try (final LoadTsFileAnalyzer analyzer =
            tableModelRequest
                ? new LoadTsFileAnalyzer(tableStatement, false, context)
                : new LoadTsFileAnalyzer(treeStatement, false, context)) {
          Assert.assertFalse(analyzer.isAutoCreateSchemaEnabled());
          Whitebox.setInternalState(analyzer, "metadata", metadata);
          final LoadTsFileTableSchemaCache tableSchemaCache =
              Whitebox.invokeMethod(analyzer, "getOrCreateTableSchemaCache");
          tableSchemaCache.setDatabase("missing_db");
          tableSchemaCache.setTableSchemaMap(
              new HashMap<>(
                  Collections.singletonMap(
                      "table1",
                      new TableSchema(
                          "table1",
                          Collections.singletonList(new MeasurementSchema("s", TSDataType.INT64)),
                          Collections.singletonList(ColumnCategory.FIELD)))));
          final LoadAnalyzeMissingSchemaException tableFailure =
              Assert.assertThrows(
                  LoadAnalyzeMissingSchemaException.class,
                  () -> tableSchemaCache.createTableAndDatabaseIfNecessary("table1"));
          Assert.assertEquals(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_THE_DATABASE_ARG_DOES_NOT_EXIST_AND_AUTO_CREATE_SCHEMA_IS_DISABLED_28F8D49A,
                  "missing_db"),
              tableFailure.getMessage());
          final TreeSchemaAutoCreatorAndVerifier verifier =
              Whitebox.invokeMethod(analyzer, "getOrCreateTreeSchemaVerifier");
          final LoadTsFileTreeSchemaCache treeCache =
              Whitebox.getInternalState(verifier, "schemaCache");
          treeCache.addTimeSeries(
              IDeviceID.Factory.DEFAULT_FACTORY.create("root.missing_db.d"),
              new MeasurementSchema("s", TSDataType.INT64));
          final LoadAnalyzeMissingSchemaException treeFailure =
              Assert.assertThrows(
                  LoadAnalyzeMissingSchemaException.class,
                  () -> Whitebox.invokeMethod(verifier, "autoCreateDatabase"));
          Assert.assertEquals(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_THE_DATABASE_ARG_DOES_NOT_EXIST_AND_AUTO_CREATE_SCHEMA_IS_DISABLED_28F8D49A,
                  "root.missing_db"),
              treeFailure.getMessage());
          // An existing database remains usable under the same disabled policy.
          treeCache.addAlreadySetDatabase(new PartialPath("root.missing_db"));
          Whitebox.invokeMethod(verifier, "autoCreateDatabase");
          Assert.assertEquals(sessionEnabled, context.getSession().isEnableAutoCreateSchema());
        }
      }
      verifyZeroInteractions(executor, metadata, coordinator);
      verify(client, never()).setDatabase(any());
    } finally {
      Whitebox.setInternalState(
          TreeSchemaAutoCreatorAndVerifier.class,
          "CONFIG_NODE_CLIENT_MANAGER",
          originalClientManager);
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(originalGlobalEnabled);
    }
  }

  @Test
  public void testAsyncTableSqlLoadPreservesSessionSchemaCreationSetting() throws Exception {
    assertAsyncLoadPreservesSessionSchemaCreationSetting(true);
  }

  @Test
  public void testAsyncTreeSqlLoadPreservesSessionSchemaCreationSetting() throws Exception {
    assertAsyncLoadPreservesSessionSchemaCreationSetting(false);
  }

  private void assertAsyncLoadPreservesSessionSchemaCreationSetting(final boolean tableModelRequest)
      throws Exception {
    final AtomicReference<Map<String, String>> handedOffAttributes = new AtomicReference<>();
    PowerMockito.mockStatic(LoadUtil.class);
    when(LoadUtil.loadTsFileAsyncToActiveDir(anyList(), anyMap(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              handedOffAttributes.set(new HashMap<>(invocation.getArgument(1)));
              return true;
            });
    final File file = temporaryFolder.newFile("async-schema.tsfile");
    final File pendingDir = temporaryFolder.newFolder("pending");
    final boolean originalGlobalEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    try {
      for (final boolean globalEnabled : new boolean[] {true, false}) {
        IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(globalEnabled);
        for (final boolean sessionEnabled : new boolean[] {false, true, false}) {
          for (final boolean requested : new boolean[] {false, true, false}) {
            handedOffAttributes.set(null);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ASYNC_LOAD_KEY, "true");
            attributes.put(DATABASE_NAME_KEY, "db");
            if (!requested) {
              attributes.put(AUTO_CREATE_SCHEMA_KEY, "false");
            }
            final LoadTsFile tableStatement =
                LoadTsFile.createUnchecked(null, file.getAbsolutePath(), attributes);
            final LoadTsFileStatement treeStatement =
                LoadTsFileStatement.createUnchecked(file.getAbsolutePath());
            treeStatement.setLoadAttributes(attributes);
            final SessionInfo sessionInfo =
                new SessionInfo(
                    0,
                    new UserEntity(0, "root", ""),
                    ZoneOffset.UTC,
                    IoTDBConstant.ClientVersion.V_1_0,
                    "db",
                    tableModelRequest ? SqlDialect.TABLE : SqlDialect.TREE,
                    sessionEnabled);
            final MPPQueryContext context = new MPPQueryContext(new QueryId("async_load_schema"));
            context.setSession(sessionInfo);
            try (final LoadTsFileAnalyzer analyzer =
                tableModelRequest
                    ? new LoadTsFileAnalyzer(tableStatement, false, context)
                    : new LoadTsFileAnalyzer(treeStatement, false, context)) {
              final Analysis analysis = new Analysis();
              analyzer.analyzeFileByFile(analysis);
              Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
              Assert.assertFalse(analysis.isFailed());
            }

            Assert.assertNotNull(handedOffAttributes.get());
            // Reconstruct the request as the active loader does with its own internal session.
            final File queuedFile =
                new File(
                    ActiveLoadPathHelper.resolveTargetDir(pendingDir, handedOffAttributes.get()),
                    file.getName());
            final Map<String, String> parsedAttributes =
                ActiveLoadPathHelper.parseAttributes(queuedFile, pendingDir);
            final LoadTsFileStatement activeStatement =
                LoadTsFileStatement.createUnchecked(file.getAbsolutePath());
            ActiveLoadPathHelper.applyAttributesToStatement(
                parsedAttributes, activeStatement, false);
            Assert.assertEquals(
                globalEnabled && requested && sessionEnabled, activeStatement.isAutoCreateSchema());
            Assert.assertEquals("db", activeStatement.getDatabase());
            Assert.assertSame(sessionInfo, context.getSession());
            Assert.assertEquals(sessionEnabled, sessionInfo.isEnableAutoCreateSchema());
            Assert.assertEquals(requested, tableStatement.isAutoCreateSchema());
            Assert.assertEquals(requested, treeStatement.isAutoCreateSchema());
            Assert.assertTrue(file.exists());
          }
        }
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(originalGlobalEnabled);
    }
  }

  private void assertTableConversionPreservesAutoCreateSchema(final boolean miniFile)
      throws Exception {
    when(converter.convertForTableModel(any()))
        .thenReturn(Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    for (final boolean tableModelRequest : new boolean[] {false, true}) {
      for (final boolean requested : new boolean[] {false, true}) {
        for (final boolean generatedByPipe : new boolean[] {false, true}) {
          clearInvocations(converter);
          final File file =
              temporaryFolder.newFile(
                  "conversion-"
                      + tableModelRequest
                      + "-"
                      + requested
                      + "-"
                      + generatedByPipe
                      + ".tsfile");
          final LoadTsFile tableStatement =
              LoadTsFile.createUnchecked(
                      null,
                      file.getAbsolutePath(),
                      requested
                          ? Collections.emptyMap()
                          : Collections.singletonMap("auto-create-schema", "false"))
                  .setDatabase("db");
          final LoadTsFileStatement treeStatement =
              LoadTsFileStatement.createUnchecked(file.getAbsolutePath());
          treeStatement.setAutoCreateSchema(requested);
          treeStatement.setDatabase("db");
          final MPPQueryContext context =
              new MPPQueryContext(new QueryId("load_conversion_attributes"));
          try (final LoadTsFileAnalyzer analyzer =
              tableModelRequest
                  ? new LoadTsFileAnalyzer(tableStatement, generatedByPipe, context)
                  : new LoadTsFileAnalyzer(treeStatement, generatedByPipe, context)) {
            final List<Boolean> tableModelFiles =
                Whitebox.getInternalState(analyzer, "isTableModelTsFile");
            tableModelFiles.set(0, true);
            Whitebox.setInternalState(analyzer, "isTableModelTsFileReliableIndex", 0);
            if (miniFile) {
              Assert.assertTrue(
                  (Boolean) Whitebox.invokeMethod(analyzer, "handleSingleMiniFile", 0));
            } else {
              final Analysis analysis = new Analysis();
              Whitebox.invokeMethod(
                  analyzer,
                  "executeTabletConversionOnException",
                  (IAnalysis) analysis,
                  new LoadAnalyzeException("original analysis failure"));
              Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
            }
            final ArgumentCaptor<LoadTsFile> statement = ArgumentCaptor.forClass(LoadTsFile.class);
            verify(converter).convertForTableModel(statement.capture());
            Assert.assertEquals(requested, statement.getValue().isAutoCreateSchema());
            Assert.assertEquals("db", statement.getValue().getDatabase());
          }
        }
      }
    }
  }

  private void mockModelReader(final boolean tableModel) throws Exception {
    final TsFileSequenceReader reader = mock(TsFileSequenceReader.class);
    when(reader.getTableSchemaMap())
        .thenReturn(
            tableModel
                ? Collections.singletonMap("table1", mock(TableSchema.class))
                : Collections.emptyMap());
    PowerMockito.whenNew(TsFileSequenceReader.class).withAnyArguments().thenReturn(reader);
  }

  private TSStatus convertAfterAnalysisFailure(final File file) throws Exception {
    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(file.getPath()).setConvertOnTypeMismatch(true),
            false,
            new MPPQueryContext(new QueryId("load_diagnostics")))) {
      final Analysis analysis = new Analysis();
      Whitebox.invokeMethod(
          analyzer,
          "executeTabletConversionOnException",
          (IAnalysis) analysis,
          new LoadAnalyzeException("original analysis failure"));
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      return analysis.getFailStatus();
    }
  }
}
