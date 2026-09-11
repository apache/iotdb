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
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
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
  public void testAlignedFastPathKeepsPagesAndValuesAlignedAfterPartialSegmentSort()
      throws IOException, InterruptedException {
    // Covers one moved segment and several untouched segments across multiple logical pages.
    int rowCount = 10_000;
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    AlignedWritableMemChunk memChunk = new AlignedWritableMemChunk(schemas, false);
    String alignedFilePath = TestConstant.OUTPUT_DATA_DIR.concat("testAlignedFastPath.tsfile");

    try {
      for (int index = 0; index < rowCount; index++) {
        long time = index;
        if (index == 100) {
          time = 101;
        } else if (index == 101) {
          time = 100;
        }
        memChunk.putAlignedRow(time, new Object[] {time, time * 10});
      }
      memChunk.sortTvListForFlush();

      BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
      memChunk.encodeWorkingAlignedTVList(ioTaskQueue, rowCount, 1024);
      try (TsFileIOWriter alignedWriter = new TsFileIOWriter(new File(alignedFilePath))) {
        alignedWriter.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"));
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
        QueryDataSet dataSet =
            fileReader.query(
                QueryExpression.create(
                    Arrays.asList(new Path("root.d", "s0", false), new Path("root.d", "s1", false)),
                    null));
        int index = 0;
        while (dataSet.hasNext()) {
          RowRecord row = dataSet.next();
          assertEquals(index, row.getTimestamp());
          assertEquals((long) index, row.getFields().get(0).getLongV());
          assertEquals((long) index * 10, row.getFields().get(1).getLongV());
          index++;
        }
        assertEquals(rowCount, index);
      }
    } finally {
      memChunk.release();
    }
  }

  @Test
  public void testAlignedFastPathEncodesUnmaterializedSegments() throws Exception {
    // Exercise all six value representations with null/dense/null segments, partial nulls, an
    // entirely empty column, and page/chunk boundaries inside backing arrays.
    checkUnmaterializedSegments(false);
  }

  @Test
  public void testAlignedFastPathDoesNotSkipValuesMovedIntoUnmaterializedSegments()
      throws Exception {
    // Sorting swaps rows between a null array and a materialized array. A null array at the
    // sorted segment's offset must not hide a non-null value mapped from the other segment.
    checkUnmaterializedSegments(true);
  }

  private void checkUnmaterializedSegments(boolean moved) throws Exception {
    int rowCount = ARRAY_SIZE * 12 + 7;
    int movedRow = ARRAY_SIZE + 1;
    if (movedRow % 5 == 0) {
      movedRow++;
    }
    List<TSDataType> types =
        Arrays.asList(
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT);
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("anchor", TSDataType.INT64, TSEncoding.PLAIN));
    for (int column = 0; column < types.size(); column++) {
      schemas.add(new MeasurementSchema("s" + column, types.get(column), TSEncoding.PLAIN));
    }
    schemas.add(new MeasurementSchema("empty", TSDataType.INT64, TSEncoding.PLAIN));
    AlignedWritableMemChunk chunk = new AlignedWritableMemChunk(schemas, true);
    Object[][] expected = new Object[rowCount][types.size()];
    String path = TestConstant.OUTPUT_DATA_DIR.concat("unmaterialized-" + moved + ".tsfile");
    try {
      for (int row = 0; row < rowCount; row++) {
        int time = row;
        if (moved && row == 1) {
          time = movedRow;
        } else if (moved && row == movedRow) {
          time = 1;
        }
        Object[] values = new Object[schemas.size()];
        values[0] = (long) time;
        // Every third segment stays unmaterialized, and other segments also contain nulls.
        if (row / ARRAY_SIZE % 3 != 0 && row % 5 != 0) {
          Object[] typedValues = {
            time % 2 == 0,
            time,
            (long) time,
            time + 0.5f,
            time + 0.25d,
            new Binary("value-" + time, StandardCharsets.UTF_8)
          };
          for (int column = 0; column < types.size(); column++) {
            values[column + 1] = typedValues[column];
            expected[time][column] = typedValues[column];
          }
        }
        chunk.putAlignedRow(time, values);
      }
      // Verify that the test actually exercises lazy null arrays, not merely marked bitmaps.
      assertTrue(chunk.getWorkingTVList().getValues().get(1).get(0) == null);
      assertTrue(chunk.getWorkingTVList().getValues().get(1).get(1) != null);
      if (moved) {
        assertTrue(expected[1][0] != null);
      }
      chunk.sortTvListForFlush();
      BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
      chunk.encodeWorkingAlignedTVList(queue, ARRAY_SIZE * 5 + 3, ARRAY_SIZE + 3);
      try (TsFileIOWriter fileWriter = new TsFileIOWriter(new File(path))) {
        fileWriter.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"));
        Object task;
        while ((task = queue.poll()) != null) {
          if (task instanceof IChunkWriter chunkWriter) {
            chunkWriter.writeToFileWriter(fileWriter);
          }
        }
        fileWriter.endChunkGroup();
        fileWriter.endFile();
      }
      try (TsFileSequenceReader sequence = new TsFileSequenceReader(path);
          TsFileReader reader = new TsFileReader(sequence)) {
        List<Path> paths = new ArrayList<>();
        for (IMeasurementSchema schema : schemas) {
          paths.add(new Path("root.d", schema.getMeasurementName(), false));
        }
        QueryDataSet data = reader.query(QueryExpression.create(paths, null));
        int row = 0;
        while (data.hasNext()) {
          RowRecord record = data.next();
          assertEquals(row, record.getTimestamp());
          assertEquals((long) row, record.getFields().get(0).getLongV());
          for (int column = 0; column < types.size(); column++) {
            assertEquals(
                expected[row][column],
                record.getFields().get(column + 1) == null
                    ? null
                    : record.getFields().get(column + 1).getObjectValue(types.get(column)));
          }
          assertTrue(
              record.getFields().get(schemas.size() - 1) == null
                  || record.getFields().get(schemas.size() - 1).getDataType() == null);
          row++;
        }
        assertEquals(rowCount, row);
      }
    } finally {
      chunk.release();
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
      super(schemaList, false);
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
