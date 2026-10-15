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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.Mockito.mock;

public class LoadTsFileInvalidPathTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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
  public void testAnalysisRejectsEmptyDeviceNodeWithAndWithoutResource() throws Exception {
    final IDeviceID device = new PlainDeviceID("root.");
    final File file = writeTsFile(device);
    assertAnalysisRejectsInvalidPath(file, device, true, true, -1);

    final TsFileResource resource = new TsFileResource(file);
    resource.updateStartTime(device, 1);
    resource.updateEndTime(device, 1);
    resource.serialize();
    Assert.assertTrue(resource.resourceFileExists());
    assertAnalysisRejectsInvalidPath(file, device, true, true, -1);
  }

  @Test
  public void testAnalysisRejectsEmptyInnerDeviceNode() throws Exception {
    final IDeviceID device = new PlainDeviceID("root.sg..d1");
    assertAnalysisRejectsInvalidPath(writeTsFile(device), device, true, true, -1);
  }

  @Test
  public void testAnalysisRejectsInvalidDeviceWithoutAutoCreateDatabase() throws Exception {
    final IDeviceID device = new PlainDeviceID("root.");
    assertAnalysisRejectsInvalidPath(writeTsFile(device), device, false, true, -1);
  }

  @Test
  public void testAnalysisRejectsInvalidDeviceWithoutSchemaChecks() throws Exception {
    final IDeviceID device = new PlainDeviceID("root.");
    assertAnalysisRejectsInvalidPath(writeTsFile(device), device, false, false, -1);
  }

  @Test
  public void testMiniFileAnalysisRejectsInvalidDevice() throws Exception {
    final IDeviceID device = new PlainDeviceID("root.");
    assertAnalysisRejectsInvalidPath(writeTsFile(device), device, true, true, Long.MAX_VALUE);
  }

  @Test
  public void testConversionReportsInvalidPathAndReleasesMemory() throws Exception {
    assertConversionRejectsInvalidPath(new PlainDeviceID("root."));
  }

  @Test
  public void testConversionRejectsEmptyInnerDeviceNode() throws Exception {
    assertConversionRejectsInvalidPath(new PlainDeviceID("root.sg..d1"));
  }

  private void assertConversionRejectsInvalidPath(final IDeviceID device) throws Exception {
    final File file = writeTsFile(device);
    final long memoryBefore = LoadTsFileMemoryManager.getInstance().getUsedMemorySizeInBytes();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              Assert.fail("An invalid device must not be inserted.");
              return null;
            });

    final Optional<TSStatus> status =
        visitor.visitLoadFile(LoadTsFileStatement.createUnchecked(file.getAbsolutePath()), null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());
    Assert.assertNotNull(status.get().getMessage());
    Assert.assertEquals(
        new IllegalPathException(((PlainDeviceID) device).toStringID()).getMessage(),
        status.get().getMessage());
    Assert.assertEquals(
        memoryBefore, LoadTsFileMemoryManager.getInstance().getUsedMemorySizeInBytes());
  }

  private void assertAnalysisRejectsInvalidPath(
      final File file,
      final IDeviceID device,
      final boolean autoCreateDatabase,
      final boolean checkSchema,
      final long conversionThreshold)
      throws Exception {
    final LoadTsFileStatement statement =
        LoadTsFileStatement.createUnchecked(file.getAbsolutePath());
    statement.setAutoCreateDatabase(autoCreateDatabase);
    statement.setConvertOnTypeMismatch(true);
    statement.setAutoCreateSchema(checkSchema);
    statement.setVerifySchema(checkSchema);
    statement.setTabletConversionThresholdBytes(conversionThreshold);
    final MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("load_invalid_path_test"),
            new SessionInfo(0, "root", ZoneId.systemDefault(), ""),
            null,
            null);

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            statement, context, mock(IPartitionFetcher.class), mock(ISchemaFetcher.class))) {
      final Analysis analysis = new Analysis();
      analyzer.analyzeFileByFile(analysis);
      Assert.assertTrue(analysis.isFinishQueryAfterAnalyze());
      Assert.assertNotNull(analysis.getFailStatus());
      Assert.assertEquals(
          TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), analysis.getFailStatus().getCode());
      Assert.assertEquals(
          String.format(
              "Loading file %s failed. Detail: %s",
              file.getAbsolutePath(),
              new IllegalPathException(((PlainDeviceID) device).toStringID()).getMessage()),
          analysis.getFailStatus().getMessage());
      Assert.assertTrue(file.exists());
    }
  }

  private File writeTsFile(final IDeviceID device) throws Exception {
    final File file = new File(temporaryFolder.getRoot(), "1-1-0-0.tsfile");
    // Use the low-level writer to preserve legacy empty device nodes in the file.
    try (final TsFileIOWriter writer = new TsFileIOWriter(file)) {
      writer.startChunkGroup(device);
      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(
              Arrays.asList(
                  new MeasurementSchema("quality", TSDataType.INT32),
                  new MeasurementSchema("value", TSDataType.DOUBLE)));
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getValueChunkWriterByIndex(0).write(1, 1, false);
      chunkWriter.getValueChunkWriterByIndex(1).write(1, 1.0, false);
      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
    return file;
  }
}
