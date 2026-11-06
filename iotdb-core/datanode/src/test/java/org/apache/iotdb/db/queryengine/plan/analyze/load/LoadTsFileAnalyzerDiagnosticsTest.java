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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadAnalyzeException;
import org.apache.iotdb.db.exception.LoadAnalyzeMissingDatabaseException;
import org.apache.iotdb.db.exception.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadUtil;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ASYNC_LOAD_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.AUTO_CREATE_SCHEMA_KEY;
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
@PrepareForTest({LoadTsFileAnalyzer.class, SchemaValidator.class, ActiveLoadUtil.class})
public class LoadTsFileAnalyzerDiagnosticsTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LoadTsFileDataTypeConverter converter;

  @Before
  public void setUp() throws Exception {
    converter = Mockito.mock(LoadTsFileDataTypeConverter.class);
    when(converter.isSuccessful(any())).thenCallRealMethod();
    PowerMockito.whenNew(LoadTsFileDataTypeConverter.class)
        .withAnyArguments()
        .thenReturn(converter);
  }

  @Test
  public void testConversionExceptionReportsCurrentError() throws Exception {
    when(converter.convertForTreeModel(any()))
        .thenThrow(new IllegalStateException("tree conversion failed"));

    final File file = temporaryFolder.newFile("conversion-error.tsfile");
    final TSStatus status = convertAfterAnalysisFailure(file);

    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains(file.getPath()));
    Assert.assertTrue(status.getMessage().contains("tree conversion failed"));
    Assert.assertFalse(status.getMessage().contains("original analysis failure"));
  }

  @Test
  public void testMissingConversionStatusHasExplicitReason() throws Exception {
    when(converter.convertForTreeModel(any())).thenReturn(Optional.empty());

    final File file = temporaryFolder.newFile("no-status.tsfile");
    final TSStatus status = convertAfterAnalysisFailure(file);

    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(status.getMessage().contains(file.getPath()));
    Assert.assertTrue(
        status
            .getMessage()
            .contains(DataNodeQueryMessages.MESSAGE_TABLET_CONVERSION_RETURNED_NO_STATUS_88BAB5D3));
  }

  @Test
  public void testConversionStatusKeepsMetadataWithoutMutatingOriginal() throws Exception {
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
  public void testWrappedTypeMismatchShouldKeepConversionPriority() throws Exception {
    final LoadAnalyzeTypeMismatchException mismatch =
        new LoadAnalyzeTypeMismatchException("schema type mismatch");
    mismatch.initCause(
        new IoTDBRuntimeException(
            "schema request failed", TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode()));
    when(converter.convertForTreeModel(any()))
        .thenReturn(Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));

    final TSStatus status = analyzeAfterSchemaFailure(new SemanticException(mismatch));

    // Successful conversion leaves the analysis failure status unset in 1.3.
    Assert.assertNull(status);
    Mockito.verify(converter).convertForTreeModel(any());
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
            new MPPQueryContext(new QueryId("load_region_recovery")),
            Mockito.mock(IPartitionFetcher.class),
            Mockito.mock(ISchemaFetcher.class))) {
      final Analysis analysis = analyzer.analyzeFileByFile(new Analysis());

      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      Assert.assertEquals(
          TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode(),
          analysis.getFailStatus().getCode());
      Assert.assertEquals("region request failed", analysis.getFailStatus().getMessage());
      Assert.assertTrue(file.exists());
    }
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
    for (final boolean requested : new boolean[] {false, true, false}) {
      for (final boolean generatedByPipe : new boolean[] {false, true}) {
        clearInvocations(converter);
        final LoadTsFileStatement treeStatement =
            LoadTsFileStatement.createUnchecked(file.getPath());
        treeStatement.setAutoCreateSchema(requested);
        if (generatedByPipe) {
          treeStatement.markIsGeneratedByPipe();
        }
        try (final LoadTsFileAnalyzer analyzer = newAnalyzer(treeStatement, true)) {
          if (miniFile) {
            Assert.assertTrue((Boolean) Whitebox.invokeMethod(analyzer, "handleSingleMiniFile", 0));
          } else {
            final Analysis analysis = new Analysis();
            Whitebox.invokeMethod(
                analyzer,
                "executeTabletConversionOnException",
                analysis,
                new LoadAnalyzeException("original analysis failure"));
            Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
            Assert.assertFalse(analysis.isFailed());
          }
          final ArgumentCaptor<LoadTsFileStatement> statement =
              ArgumentCaptor.forClass(LoadTsFileStatement.class);
          verify(converter).convertForTreeModel(statement.capture());
          Assert.assertEquals(requested, statement.getValue().isAutoCreateSchema());
          Assert.assertEquals(generatedByPipe, statement.getValue().isGeneratedByPipe());
          Assert.assertEquals(Collections.singletonList(file), statement.getValue().getTsFiles());
          Assert.assertTrue(file.exists());
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
            "root.missing_db");
    try (final LoadTsFileAnalyzer analyzer = newAnalyzer(statement, true)) {
      final Analysis analysis = new Analysis();
      Whitebox.invokeMethod(
          analyzer,
          "executeTabletConversionOnException",
          analysis,
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
        Whitebox.getInternalState(LoadTsFileAnalyzer.class, "CONFIG_NODE_CLIENT_MANAGER");
    final IClientManager<ConfigRegionId, ConfigNodeClient> clientManager =
        mock(IClientManager.class);
    final ConfigNodeClient client = mock(ConfigNodeClient.class);
    when(clientManager.borrowClient(any())).thenReturn(client);
    when(client.showDatabase(any()))
        .thenReturn(new TShowDatabaseResp().setDatabaseInfoMap(Collections.emptyMap()));
    try {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(globalEnabled);
      Whitebox.setInternalState(
          LoadTsFileAnalyzer.class, "CONFIG_NODE_CLIENT_MANAGER", clientManager);
      final File file = temporaryFolder.newFile("missing-database.tsfile");
      final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(file.getPath());
      statement.setAutoCreateSchema(requested);
      statement.setAutoCreateDatabase(true);
      try (final LoadTsFileAnalyzer analyzer = newAnalyzer(statement, sessionEnabled)) {
        Assert.assertFalse(analyzer.isAutoCreateSchemaEnabled());
        final Object verifier = Whitebox.getInternalState(analyzer, "schemaAutoCreatorAndVerifier");
        final Object cache = Whitebox.getInternalState(verifier, "schemaCache");
        Whitebox.invokeMethod(
            cache,
            "addTimeSeries",
            new PlainDeviceID("root.missing_db.d"),
            new MeasurementSchema("s", TSDataType.INT64));
        final LoadAnalyzeMissingDatabaseException failure =
            Assert.assertThrows(
                LoadAnalyzeMissingDatabaseException.class,
                () -> Whitebox.invokeMethod(verifier, "autoCreateDatabase"));
        Assert.assertEquals(
            String.format(
                DataNodeQueryMessages
                    .EXCEPTION_THE_DATABASE_ARG_DOES_NOT_EXIST_AND_AUTO_CREATE_SCHEMA_IS_DISABLED_28F8D49A,
                "root.missing_db"),
            failure.getMessage());
        Whitebox.invokeMethod(cache, "addAlreadySetDatabase", new PartialPath("root.missing_db"));
        Whitebox.invokeMethod(verifier, "autoCreateDatabase");
        Assert.assertTrue(file.exists());
      }
      verify(client, never()).setDatabase(any());
      verifyZeroInteractions(converter);
    } finally {
      Whitebox.setInternalState(
          LoadTsFileAnalyzer.class, "CONFIG_NODE_CLIENT_MANAGER", originalClientManager);
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(originalGlobalEnabled);
    }
  }

  @Test
  public void testAsyncTreeSqlLoadPreservesSchemaCreationSetting() throws Exception {
    final AtomicReference<Map<String, String>> handedOffAttributes = new AtomicReference<>();
    PowerMockito.mockStatic(ActiveLoadUtil.class);
    when(ActiveLoadUtil.loadTsFileAsyncToActiveDir(anyList(), anyMap(), anyBoolean()))
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
            attributes.put(AUTO_CREATE_SCHEMA_KEY, String.valueOf(requested));
            final LoadTsFileStatement statement =
                LoadTsFileStatement.createUnchecked(file.getPath());
            statement.setLoadAttributes(attributes);
            try (final LoadTsFileAnalyzer analyzer = newAnalyzer(statement, sessionEnabled)) {
              final Analysis analysis = analyzer.analyzeFileByFile(new Analysis());
              Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
              Assert.assertEquals(
                  TSStatusCode.SUCCESS_STATUS.getStatusCode(), analysis.getFailStatus().getCode());
            }
            Assert.assertNotNull(handedOffAttributes.get());
            final File queuedFile =
                new File(
                    ActiveLoadPathHelper.resolveTargetDir(pendingDir, handedOffAttributes.get()),
                    file.getName());
            final Map<String, String> parsed =
                ActiveLoadPathHelper.parseAttributes(queuedFile, pendingDir);
            final LoadTsFileStatement active = LoadTsFileStatement.createUnchecked(file.getPath());
            ActiveLoadPathHelper.applyAttributesToStatement(parsed, active, false);
            Assert.assertEquals(
                globalEnabled && requested && sessionEnabled, active.isAutoCreateSchema());
            Assert.assertEquals(requested, statement.isAutoCreateSchema());
            Assert.assertTrue(file.exists());
          }
        }
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(originalGlobalEnabled);
    }
  }

  private LoadTsFileAnalyzer newAnalyzer(
      final LoadTsFileStatement statement, final boolean allowed) {
    return new LoadTsFileAnalyzer(
        statement,
        new MPPQueryContext(
            "",
            new QueryId("tree_schema_policy"),
            new SessionInfo(
                0, "root", ZoneOffset.UTC, IoTDBConstant.ClientVersion.V_1_0, "", allowed),
            null,
            null),
        mock(IPartitionFetcher.class),
        mock(ISchemaFetcher.class));
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
            statement,
            new MPPQueryContext(new QueryId("load_schema_recovery")),
            Mockito.mock(IPartitionFetcher.class),
            Mockito.mock(ISchemaFetcher.class))) {
      final Field verifierField =
          analyzer.getClass().getDeclaredField("schemaAutoCreatorAndVerifier");
      verifierField.setAccessible(true);
      final Object verifier = verifierField.get(analyzer);
      final Field schemaCacheField = verifier.getClass().getDeclaredField("schemaCache");
      schemaCacheField.setAccessible(true);
      final Object schemaCache = schemaCacheField.get(verifier);
      final IDeviceID device = new PlainDeviceID("root.sg.d");
      final Method addTimeSeries =
          schemaCache
              .getClass()
              .getDeclaredMethod("addTimeSeries", IDeviceID.class, MeasurementSchema.class);
      addTimeSeries.setAccessible(true);
      addTimeSeries.invoke(schemaCache, device, new MeasurementSchema("s", TSDataType.INT64));
      final Method addIsAlignedCache =
          schemaCache
              .getClass()
              .getDeclaredMethod(
                  "addIsAlignedCache", IDeviceID.class, boolean.class, boolean.class);
      addIsAlignedCache.setAccessible(true);
      addIsAlignedCache.invoke(schemaCache, device, false, true);

      final Analysis analysis = analyzer.analyzeFileByFile(new Analysis());
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      Assert.assertTrue(file.exists());
      return analysis.getFailStatus();
    }
  }

  private TSStatus convertAfterAnalysisFailure(final File file) throws Exception {
    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(file.getPath()).setConvertOnTypeMismatch(true),
            new MPPQueryContext(new QueryId("load_diagnostics")),
            Mockito.mock(IPartitionFetcher.class),
            Mockito.mock(ISchemaFetcher.class))) {
      final Analysis analysis = new Analysis();
      final Method conversionMethod =
          LoadTsFileAnalyzer.class.getDeclaredMethod(
              "executeTabletConversionOnException", Analysis.class, LoadAnalyzeException.class);
      conversionMethod.setAccessible(true);
      conversionMethod.invoke(
          analyzer, analysis, new LoadAnalyzeException("original analysis failure"));
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      return analysis.getFailStatus();
    }
  }
}
