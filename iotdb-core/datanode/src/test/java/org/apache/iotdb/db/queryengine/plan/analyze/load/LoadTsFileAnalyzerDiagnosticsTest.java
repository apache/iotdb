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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest(LoadTsFileAnalyzer.class)
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
