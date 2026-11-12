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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.nio.file.Files;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadTsFileTableSchemaRecoveryTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private int originalDataNodeId;

  @Before
  public void setUp() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testUnavailableTableSchemaRegionsShouldRetryAndRecover() throws Exception {
    assertTableSchemaFailure(
        new IoTDBRuntimeException(
            "schema request failed", TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode()),
        TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION);
  }

  @Test
  public void testWrappedUnavailableTableSchemaRegionsShouldRetry() throws Exception {
    assertTableSchemaFailure(
        new SemanticException(
            new IoTDBRuntimeException(
                "schema request failed", TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())),
        TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION);
  }

  @Test
  public void testCheckedUnavailableTableSchemaRegionsShouldRetry() throws Exception {
    assertTableSchemaFailure(
        new SemanticException(
            new IoTDBException(
                "schema request failed", TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode())),
        TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION);
  }

  @Test
  public void testPermanentTableSchemaErrorShouldNotBecomeRetryable() throws Exception {
    assertTableSchemaFailure(
        new IoTDBRuntimeException(
            "schema request failed", TSStatusCode.METADATA_ERROR.getStatusCode()),
        TSStatusCode.LOAD_FILE_ERROR);
  }

  @Test
  public void testTableSchemaPermissionErrorShouldNotBecomeRetryable() throws Exception {
    assertTableSchemaFailure(
        new SemanticException(
            new IoTDBException(
                "schema request failed", TSStatusCode.NO_PERMISSION.getStatusCode())),
        TSStatusCode.LOAD_FILE_ERROR);
  }

  private void assertTableSchemaFailure(
      final RuntimeException failure, final TSStatusCode expectedCode) throws Exception {
    final File file = temporaryFolder.newFile("table-schema-failure.tsfile");
    final TableSchema schema =
        new TableSchema(
            "table1",
            Collections.singletonList(new MeasurementSchema("s", TSDataType.INT64)),
            Collections.singletonList(ColumnCategory.FIELD));
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema);
      final Tablet tablet =
          new Tablet(
              "table1",
              Collections.singletonList("s"),
              Collections.singletonList(TSDataType.INT64),
              Collections.singletonList(ColumnCategory.FIELD),
              1);
      tablet.addTimestamp(0, 10);
      tablet.addValue("s", 0, 1L);
      writer.writeTable(tablet);
    }
    final byte[] originalInput = Files.readAllBytes(file.toPath());
    final Metadata metadata = mock(Metadata.class);
    when(metadata.validateTableHeaderSchema4TsFile(
            any(), any(), any(), anyBoolean(), anyBoolean(), any()))
        .thenReturn(
            Optional.of(
                org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema
                    .fromTsFileTableSchema("table1", schema)));
    doThrow(failure).doNothing().when(metadata).validateDeviceSchema(any(), any());

    // A new analyzer on each attempt models Active LOAD rescanning the same pending file.
    final int attempts = expectedCode == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION ? 2 : 1;
    for (int attempt = 0; attempt < attempts; attempt++) {
      final LoadTsFileStatement statement = LoadTsFileStatement.createUnchecked(file.getPath());
      statement.setDatabase("load_schema_recovery");
      statement.setAutoCreateDatabase(false);
      statement.setTabletConversionThresholdBytes(-1);
      final MPPQueryContext context = new MPPQueryContext(new QueryId("table_schema_recovery"));
      context.setSession(new SessionInfo(0, new UserEntity(0, "root", ""), ZoneOffset.UTC));
      try (final LoadTsFileAnalyzer analyzer = new LoadTsFileAnalyzer(statement, false, context)) {
        Whitebox.setInternalState(analyzer, "metadata", metadata);
        final IAnalysis analysis = analyzer.analyzeFileByFile(new Analysis());
        if (attempt == 0) {
          Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
          Assert.assertEquals(
              analysis.getFailStatus().getMessage(),
              expectedCode.getStatusCode(),
              analysis.getFailStatus().getCode());
          Assert.assertTrue(
              analysis.getFailStatus().getMessage().contains("schema request failed"));
        } else {
          Assert.assertFalse(analysis.isFinishQueryAfterAnalyze());
          Assert.assertNull(analysis.getFailStatus());
          Assert.assertEquals(1, statement.getResources().size());
        }
        verify(metadata, times(attempt + 1)).validateDeviceSchema(any(), any());
        Assert.assertArrayEquals(originalInput, Files.readAllBytes(file.toPath()));
      }
    }
  }
}
