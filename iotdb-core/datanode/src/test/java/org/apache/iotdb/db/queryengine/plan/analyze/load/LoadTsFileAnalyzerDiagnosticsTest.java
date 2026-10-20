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
import org.apache.iotdb.db.exception.LoadAnalyzeException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest(LoadTsFileAnalyzer.class)
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

  private TSStatus convertAfterAnalysisFailure(final File file) throws Exception {
    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(file.getPath()).setConvertOnTypeMismatch(true),
            new MPPQueryContext(new QueryId("load_diagnostics")),
            Mockito.mock(IPartitionFetcher.class),
            Mockito.mock(ISchemaFetcher.class))) {
      final Analysis analysis = new Analysis();
      Whitebox.invokeMethod(
          analyzer,
          "executeTabletConversionOnException",
          analysis,
          new LoadAnalyzeException("original analysis failure"));
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      return analysis.getFailStatus();
    }
  }
}
