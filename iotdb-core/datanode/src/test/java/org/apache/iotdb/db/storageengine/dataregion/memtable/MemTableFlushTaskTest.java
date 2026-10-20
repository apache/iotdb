/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MemTableFlushTaskTest {

  private RestorableTsFileIOWriter writer;
  private String storageGroup = "storage_group1";
  private String dataRegionId = "1";
  private String filePath =
      TestConstant.OUTPUT_DATA_DIR.concat("testUnsealedTsFileProcessor.tsfile");
  private IMemTable memTable;
  private long startTime = 1;
  private long endTime = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    writer = new RestorableTsFileIOWriter(FSFactoryProducer.getFSFactory().getFile(filePath));
    memTable = new PrimitiveMemTable(storageGroup, dataRegionId);
  }

  @After
  public void tearDown() throws Exception {
    writer.close();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
  }

  @Test
  public void testFlushMemTable()
      throws ExecutionException, InterruptedException, IllegalPathException {
    MemTableTestUtils.produceData(
        memTable,
        startTime,
        endTime,
        MemTableTestUtils.deviceId0,
        MemTableTestUtils.measurementId0,
        MemTableTestUtils.dataType0);
    MemTableFlushTask memTableFlushTask =
        new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId);
    assertTrue(
        writer
            .getVisibleMetadataList(
                MemTableTestUtils.deviceId0,
                MemTableTestUtils.measurementId0,
                MemTableTestUtils.dataType0)
            .isEmpty());
    memTableFlushTask.syncFlushMemTable();
    writer.makeMetadataVisible();
    assertEquals(
        1,
        writer
            .getVisibleMetadataList(
                MemTableTestUtils.deviceId0,
                MemTableTestUtils.measurementId0,
                MemTableTestUtils.dataType0)
            .size());
    ChunkMetadata chunkMetaData =
        writer
            .getVisibleMetadataList(
                MemTableTestUtils.deviceId0,
                MemTableTestUtils.measurementId0,
                MemTableTestUtils.dataType0)
            .get(0);
    assertEquals(MemTableTestUtils.measurementId0, chunkMetaData.getMeasurementUid());
    assertEquals(startTime, chunkMetaData.getStartTime());
    assertEquals(endTime, chunkMetaData.getEndTime());
    assertEquals(MemTableTestUtils.dataType0, chunkMetaData.getDataType());
    assertEquals(endTime - startTime + 1, chunkMetaData.getNumOfPoints());
  }

  @Test
  public void testFlushVectorMemTable()
      throws ExecutionException, InterruptedException, IllegalPathException, WriteProcessException {
    MemTableTestUtils.produceVectorData(memTable);
    MemTableFlushTask memTableFlushTask =
        new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId);
    assertTrue(
        writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, "sensor0", TSDataType.BOOLEAN)
            .isEmpty());
    memTableFlushTask.syncFlushMemTable();
    writer.makeMetadataVisible();
    assertEquals(
        1,
        writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, "sensor0", TSDataType.BOOLEAN)
            .size());
    ChunkMetadata chunkMetaData =
        writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, "sensor0", TSDataType.BOOLEAN)
            .get(0);
    assertEquals("sensor0", chunkMetaData.getMeasurementUid());
    assertEquals(startTime, chunkMetaData.getStartTime());
    assertEquals(endTime, chunkMetaData.getEndTime());
    assertEquals(TSDataType.BOOLEAN, chunkMetaData.getDataType());
    assertEquals(endTime - startTime + 1, chunkMetaData.getNumOfPoints());
  }

  @Test
  public void testFlushNullableVectorMemTable()
      throws ExecutionException, InterruptedException, IllegalPathException, WriteProcessException {
    MemTableTestUtils.produceNullableVectorData(memTable);
    MemTableFlushTask memTableFlushTask =
        new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId);
    assertTrue(
        writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, "sensor0", TSDataType.BOOLEAN)
            .isEmpty());
    memTableFlushTask.syncFlushMemTable();
    writer.makeMetadataVisible();
    assertEquals(
        1,
        writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, "sensor0", TSDataType.BOOLEAN)
            .size());
    ChunkMetadata chunkMetaData =
        writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, "sensor0", TSDataType.BOOLEAN)
            .get(0);
    assertEquals("sensor0", chunkMetaData.getMeasurementUid());
    assertEquals(startTime, chunkMetaData.getStartTime());
    assertEquals(endTime, chunkMetaData.getEndTime());
    assertEquals(TSDataType.BOOLEAN, chunkMetaData.getDataType());
    assertEquals(endTime - startTime + 1, chunkMetaData.getNumOfPoints());
  }

  @Test
  public void testAlignedFlushWithoutDeletedMeasurementsSkipsColumnMapping() {
    TrackingAlignedWritableMemChunk memChunk = createTrackingAlignedMemChunk();
    memChunk.putAlignedRow(1, new Object[] {1, 1L});
    memChunk.sortTvListForFlush();

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    memChunk.encodeWorkingAlignedTVList(ioTaskQueue, 100, 100);

    assertFalse(memChunk.isColumnMappingBuilt());
    assertFalse(ioTaskQueue.isEmpty());
  }

  @Test
  public void testAlignedFlushWithDeletedMeasurementsKeepsColumnMapping() {
    TrackingAlignedWritableMemChunk memChunk = createTrackingAlignedMemChunk();
    memChunk.putAlignedRow(1, new Object[] {1, 1L});
    memChunk.removeColumn("s1");
    memChunk.sortTvListForFlush();

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    memChunk.encodeWorkingAlignedTVList(ioTaskQueue, 100, 100);

    assertTrue(memChunk.isColumnMappingBuilt());
    assertFalse(ioTaskQueue.isEmpty());
  }

  @Test
  public void testAlignedFlushMergesDuplicateTimestampsUsingLastNonNullValue() throws Exception {
    checkAlignedFlushMergesDuplicateTimestamps(false);
  }

  @Test
  public void testAlignedFlushMergesDuplicateTimestampsAfterMeasurementDeletion() throws Exception {
    checkAlignedFlushMergesDuplicateTimestamps(true);
  }

  private void checkAlignedFlushMergesDuplicateTimestamps(boolean removeSecondMeasurement)
      throws Exception {
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    AlignedWritableMemChunk memChunk = new AlignedWritableMemChunk(schemas);
    String alignedFilePath =
        TestConstant.OUTPUT_DATA_DIR.concat(
            "duplicateAlignedRows-" + removeSecondMeasurement + ".tsfile");

    try {
      memChunk.putAlignedRow(1, new Object[] {10L, 100L});
      memChunk.putAlignedRow(2, new Object[] {20L, null});
      memChunk.putAlignedRow(2, new Object[] {null, 200L});
      memChunk.putAlignedRow(2, new Object[] {null, null});
      memChunk.putAlignedRow(3, new Object[] {30L, null});
      if (removeSecondMeasurement) {
        memChunk.removeColumn("s1");
      }
      memChunk.sortTvListForFlush();

      BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
      memChunk.encodeWorkingAlignedTVList(ioTaskQueue, 100, 100);
      try (TsFileIOWriter alignedWriter = new TsFileIOWriter(new File(alignedFilePath))) {
        alignedWriter.startChunkGroup(new PlainDeviceID("root.d"));
        Object task;
        while ((task = ioTaskQueue.poll()) != null) {
          if (task instanceof IChunkWriter) {
            ((IChunkWriter) task).writeToFileWriter(alignedWriter);
          }
        }
        alignedWriter.endChunkGroup();
        alignedWriter.endFile();
      }

      try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(alignedFilePath);
          TsFileReader fileReader = new TsFileReader(sequenceReader)) {
        List<Path> paths = new ArrayList<>();
        paths.add(new Path("root.d", "s0", false));
        if (!removeSecondMeasurement) {
          paths.add(new Path("root.d", "s1", false));
        }
        QueryDataSet dataSet = fileReader.query(QueryExpression.create(paths, null));

        RowRecord row = dataSet.next();
        assertEquals(1, row.getTimestamp());
        assertEquals(10L, row.getFields().get(0).getLongV());
        if (!removeSecondMeasurement) {
          assertEquals(100L, row.getFields().get(1).getLongV());
        }

        row = dataSet.next();
        assertEquals(2, row.getTimestamp());
        assertEquals(20L, row.getFields().get(0).getLongV());
        if (!removeSecondMeasurement) {
          assertEquals(200L, row.getFields().get(1).getLongV());
        }

        row = dataSet.next();
        assertEquals(3, row.getTimestamp());
        assertEquals(30L, row.getFields().get(0).getLongV());
        if (!removeSecondMeasurement) {
          assertTrue(
              row.getFields().get(1) == null || row.getFields().get(1).getDataType() == null);
        }
        assertFalse(dataSet.hasNext());
      }
    } finally {
      memChunk.release();
    }
  }

  private TrackingAlignedWritableMemChunk createTrackingAlignedMemChunk() {
    List<IMeasurementSchema> schemas =
        new ArrayList<>(
            Arrays.asList(
                new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN),
                new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN)));
    return new TrackingAlignedWritableMemChunk(schemas);
  }

  private static class TrackingAlignedWritableMemChunk extends AlignedWritableMemChunk {

    private boolean columnMappingBuilt;

    private TrackingAlignedWritableMemChunk(List<IMeasurementSchema> schemaList) {
      super(schemaList);
    }

    @Override
    public List<Integer> buildColumnIndexList(List<IMeasurementSchema> schemaList) {
      columnMappingBuilt = true;
      return super.buildColumnIndexList(schemaList);
    }

    private boolean isColumnMappingBuilt() {
      return columnMappingBuilt;
    }
  }
}
