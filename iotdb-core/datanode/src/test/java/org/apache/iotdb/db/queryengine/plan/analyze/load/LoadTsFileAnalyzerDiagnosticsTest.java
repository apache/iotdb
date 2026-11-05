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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.util.LoadUtil;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({LoadTsFileAnalyzer.class, LoadUtil.class})
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
        ActiveLoadPathHelper.applyAttributesToStatement(parsedAttributes, activeStatement, false);
        Assert.assertEquals(requested && sessionEnabled, activeStatement.isAutoCreateSchema());
        Assert.assertEquals("db", activeStatement.getDatabase());
        Assert.assertSame(sessionInfo, context.getSession());
        Assert.assertEquals(sessionEnabled, sessionInfo.isEnableAutoCreateSchema());
        Assert.assertEquals(requested, tableStatement.isAutoCreateSchema());
        Assert.assertEquals(requested, treeStatement.isAutoCreateSchema());
        Assert.assertTrue(file.exists());
      }
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
