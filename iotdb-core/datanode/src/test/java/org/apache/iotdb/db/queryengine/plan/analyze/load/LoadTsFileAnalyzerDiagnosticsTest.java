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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.exception.LoadAnalyzeException;
import org.apache.iotdb.db.exception.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
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
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({LoadTsFileAnalyzer.class, SchemaValidator.class})
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
      final Object verifier = Whitebox.getInternalState(analyzer, "schemaAutoCreatorAndVerifier");
      final Object schemaCache = Whitebox.getInternalState(verifier, "schemaCache");
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
