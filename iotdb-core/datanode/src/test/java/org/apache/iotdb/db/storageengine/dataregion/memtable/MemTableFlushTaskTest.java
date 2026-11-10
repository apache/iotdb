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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
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
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  @Test(timeout = 20000)
  public void testIoFailureReleasesTemporaryReservation() throws Exception {
    MemTableTestUtils.produceVectorData(memTable);
    SystemInfo system = SystemInfo.getInstance();
    boolean old = system.isEncodingFasterThanIo();
    long before = system.getFlushMemoryManager().getMemoryUsageInBytes();
    RestorableTsFileIOWriter failing = mock(RestorableTsFileIOWriter.class);
    when(failing.getFile()).thenReturn(new File(filePath));
    IOException error = new IOException("injected IO failure");
    doThrow(error).when(failing).startChunkGroup(any(IDeviceID.class));
    try {
      system.setEncodingFasterThanIo(true);
      ExecutionException actual =
          assertThrows(
              ExecutionException.class,
              () ->
                  new MemTableFlushTask(memTable, failing, storageGroup, dataRegionId, true)
                      .syncFlushMemTable());
      assertSame(error, actual.getCause());
      assertEquals(before, system.getFlushMemoryManager().getMemoryUsageInBytes());
    } finally {
      system.setEncodingFasterThanIo(old);
    }
  }

  @Test(timeout = 20000)
  public void testInterruptedFlushWaitsForIoExitBeforeRelease() throws Exception {
    MemTableTestUtils.produceVectorData(memTable);
    SystemInfo system = SystemInfo.getInstance();
    boolean old = system.isEncodingFasterThanIo();
    long before = system.getFlushMemoryManager().getMemoryUsageInBytes();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    CountDownLatch mayExit = new CountDownLatch(1);
    RestorableTsFileIOWriter slow = mock(RestorableTsFileIOWriter.class);
    when(slow.getFile()).thenReturn(new File(filePath));
    doAnswer(
            invocation -> {
              entered.countDown();
              while (true) {
                try {
                  mayExit.await();
                  break;
                } catch (InterruptedException e) {
                  interrupted.countDown();
                }
              }
              return null;
            })
        .when(slow)
        .startChunkGroup(any(IDeviceID.class));
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread caller =
        new Thread(
            () -> {
              try {
                new MemTableFlushTask(memTable, slow, storageGroup, dataRegionId, true)
                    .syncFlushMemTable();
                failure.set(new AssertionError("flush should be interrupted"));
              } catch (InterruptedException expected) {
                if (!Thread.currentThread().isInterrupted())
                  failure.set(new AssertionError("interrupt flag lost"));
              } catch (Throwable e) {
                failure.set(e);
              }
            });
    try {
      system.setEncodingFasterThanIo(true);
      caller.start();
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      caller.interrupt();
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      // Another interruption during cancellation must not let cleanup release live worker memory.
      caller.interrupt();
      assertTrue(caller.isAlive());
      assertTrue(system.getFlushMemoryManager().getMemoryUsageInBytes() > before);
      mayExit.countDown();
      caller.join(5000);
      assertFalse(caller.isAlive());
      if (failure.get() != null) throw new AssertionError(failure.get());
      assertEquals(before, system.getFlushMemoryManager().getMemoryUsageInBytes());
    } finally {
      mayExit.countDown();
      caller.interrupt();
      caller.join(5000);
      system.setEncodingFasterThanIo(old);
    }
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
        new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId, true);
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
        new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId, true);
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
        new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId, true);
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
    memChunk.encodeWorkingAlignedTVList(ioTaskQueue, 100, 100, true);

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
    memChunk.encodeWorkingAlignedTVList(ioTaskQueue, 100, 100, true);

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
      memChunk.encodeWorkingAlignedTVList(ioTaskQueue, 100, 100, false);
      try (TsFileIOWriter alignedWriter = new TsFileIOWriter(new File(alignedFilePath))) {
        alignedWriter.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"));
        Object task;
        while ((task = ioTaskQueue.poll()) != null) {
          if (task instanceof IChunkWriter chunkWriter) {
            chunkWriter.writeToFileWriter(alignedWriter);
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

  @Test
  public void testAlignedFastPathKeepsPagesAndValuesAlignedAfterPartialSegmentSort()
      throws IOException, InterruptedException {
    // Covers one moved segment and several untouched segments across multiple logical pages.
    int rowCount = 10_000;
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    AlignedWritableMemChunk memChunk = new AlignedWritableMemChunk(schemas);
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
      memChunk.encodeWorkingAlignedTVList(ioTaskQueue, rowCount, 1024, true);
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
  public void testAlignedFlushKeepsValuesAlignedAfterTimeDeletion() throws IOException {
    // Deleted rows must be omitted from both time and value pages, including after sorting.
    checkAlignedFlushAfterTimeDeletion(false);
  }

  @Test
  public void testAlignedFlushKeepsValuesAlignedAfterTimeAndColumnDeletion() throws IOException {
    // Removing the first measurement also exercises the remapped value-column encoding path.
    checkAlignedFlushAfterTimeDeletion(true);
  }

  private void checkAlignedFlushAfterTimeDeletion(boolean removeColumn) throws IOException {
    for (int pageSize : new int[] {2, 100}) {
      List<IMeasurementSchema> schemas =
          Arrays.asList(
              new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN),
              new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
      AlignedWritableMemChunk memChunk = new AlignedWritableMemChunk(schemas);
      String alignedFilePath =
          TestConstant.OUTPUT_DATA_DIR.concat("testAlignedTimeDeletion" + pageSize + ".tsfile");
      try {
        for (int time : new int[] {4, 1, 6, 2, 5, 3}) {
          memChunk.putAlignedRow(time, new Object[] {time, time * 10L});
        }
        memChunk.deleteTime(2, 2);
        memChunk.deleteTime(6, 6);
        if (removeColumn) {
          memChunk.removeColumn("s0");
        }
        memChunk.sortTvListForFlush();

        BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
        // Cover a single page and boundaries between pages and chunks.
        memChunk.encodeWorkingAlignedTVList(ioTaskQueue, pageSize + 1, pageSize, true);
        try (TsFileIOWriter alignedWriter = new TsFileIOWriter(new File(alignedFilePath))) {
          alignedWriter.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"));
          Object task;
          while ((task = ioTaskQueue.poll()) != null) {
            if (task instanceof IChunkWriter chunkWriter) {
              chunkWriter.writeToFileWriter(alignedWriter);
            }
          }
          alignedWriter.endChunkGroup();
          alignedWriter.endFile();
        }

        try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(alignedFilePath);
            TsFileReader fileReader = new TsFileReader(sequenceReader)) {
          List<Path> paths = new ArrayList<>();
          paths.add(new Path("root.d", "s1", false));
          if (!removeColumn) {
            paths.add(new Path("root.d", "s0", false));
          }
          QueryDataSet dataSet = fileReader.query(QueryExpression.create(paths, null));
          for (int time : new int[] {1, 3, 4, 5}) {
            assertTrue(dataSet.hasNext());
            RowRecord row = dataSet.next();
            assertEquals(time, row.getTimestamp());
            assertEquals(TSDataType.INT64, row.getFields().get(0).getDataType());
            assertEquals(time * 10L, row.getFields().get(0).getLongV());
            if (!removeColumn) {
              assertEquals(TSDataType.INT32, row.getFields().get(1).getDataType());
              assertEquals(time, row.getFields().get(1).getIntV());
            }
          }
          assertFalse(dataSet.hasNext());
        }
      } finally {
        memChunk.release();
      }
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
    AlignedWritableMemChunk chunk = new AlignedWritableMemChunk(schemas);
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
      chunk.encodeWorkingAlignedTVList(queue, ARRAY_SIZE * 5 + 3, ARRAY_SIZE + 3, false);
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

  /**
   * With the IO consumer stalled the encoder fills the IO queue; the temporary reservation must
   * cover the capacity of the queued chunk buffers, which grow by doubling and can exceed the raw
   * data counted by memSize (PLAIN time and values: 9% to 37% here, up to 2x in general).
   */
  @Test(timeout = 60000)
  public void testReservationCoversQueuedChunkBuffersWithStalledIo() throws Exception {
    String oldTimeEncoder = TSFileDescriptor.getInstance().getConfig().getTimeEncoder();
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    try {
      assertReservationCoversQueuedBuffers(9, 60_000); // fewer series than queue slots
      assertReservationCoversQueuedBuffers(1, 60_000);
      assertReservationCoversQueuedBuffers(300, 6_000); // more series than queue slots
    } finally {
      TSFileDescriptor.getInstance().getConfig().setTimeEncoder(oldTimeEncoder);
    }
  }

  private void assertReservationCoversQueuedBuffers(int seriesNumber, int rowCount)
      throws Exception {
    IMemTable table = new PrimitiveMemTable(storageGroup, dataRegionId);
    String[] measurements = new String[seriesNumber];
    TSDataType[] dataTypes = new TSDataType[seriesNumber];
    MeasurementSchema[] schemas = new MeasurementSchema[seriesNumber];
    Object[] columns = new Object[seriesNumber];
    long[] times = new long[rowCount];
    for (int row = 0; row < rowCount; row++) {
      times[row] = row;
    }
    for (int i = 0; i < seriesNumber; i++) {
      measurements[i] = String.format("s%03d", i);
      dataTypes[i] = TSDataType.DOUBLE;
      schemas[i] =
          new MeasurementSchema(
              measurements[i], TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      double[] column = new double[rowCount];
      for (int row = 0; row < rowCount; row++) {
        column[row] = row * 1.5 + i;
      }
      columns[i] = column;
    }
    table.insertTablet(
        new InsertTabletNode(
            new PlanNodeId("stalled-io"),
            new PartialPath("root.sg.d1"),
            false,
            measurements,
            dataTypes,
            schemas,
            times,
            null,
            columns,
            rowCount),
        0,
        rowCount);

    SystemInfo system = SystemInfo.getInstance();
    boolean old = system.isEncodingFasterThanIo();
    long before = system.getFlushMemoryManager().getMemoryUsageInBytes();
    CountDownLatch stalled = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    RestorableTsFileIOWriter stalledWriter = mock(RestorableTsFileIOWriter.class);
    when(stalledWriter.getFile()).thenReturn(new File(filePath));
    doAnswer(
            invocation -> {
              stalled.countDown();
              release.await();
              throw new IOException("stalled IO released");
            })
        .when(stalledWriter)
        .startChunkGroup(any(IDeviceID.class));
    system.setEncodingFasterThanIo(true);
    MemTableFlushTask task =
        new MemTableFlushTask(table, stalledWriter, storageGroup, dataRegionId, false);
    Field queueField = MemTableFlushTask.class.getDeclaredField("ioTaskQueue");
    queueField.setAccessible(true);
    BlockingQueue<?> ioQueue = (BlockingQueue<?>) queueField.get(task);
    Thread flusher =
        new Thread(
            () -> {
              try {
                task.syncFlushMemTable();
              } catch (ExecutionException expected) {
                // the stalled writer fails once released
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    try {
      flusher.start();
      assertTrue(stalled.await(10, TimeUnit.SECONDS));
      // the encoder either blocks on the full queue or has queued every series, the end of the
      // chunk group and the end of the task
      int expectedQueued = Math.min(config().getIoTaskQueueSizeForFlushing(), seriesNumber + 2);
      long deadline = System.currentTimeMillis() + 20_000;
      while (ioQueue.size() < expectedQueued && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(expectedQueued, ioQueue.size());
      long reserved = system.getFlushMemoryManager().getMemoryUsageInBytes() - before;
      long buffers = 0;
      for (Object queued : new ArrayList<>(ioQueue)) {
        if (queued instanceof IChunkWriter) {
          buffers += retainedArrayBytes(queued);
        }
      }
      assertTrue(buffers > table.memSize() / seriesNumber); // the probe found the buffers
      assertTrue(
          String.format(
              "%d series: queued chunk buffers %d B exceed the reservation %d B (memSize %d B)",
              seriesNumber, buffers, reserved, table.memSize()),
          buffers <= reserved);
    } finally {
      release.countDown();
      flusher.join(10_000);
      system.setEncodingFasterThanIo(old);
    }
    assertEquals(before, system.getFlushMemoryManager().getMemoryUsageInBytes());
  }

  private static org.apache.iotdb.db.conf.IoTDBConfig config() {
    return org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig();
  }

  /** Capacity of the byte buffers and primitive arrays a queued chunk writer keeps alive. */
  private static long retainedArrayBytes(Object root) throws IllegalAccessException {
    IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
    ArrayDeque<Object> todo = new ArrayDeque<>();
    todo.add(root);
    long bytes = 0;
    while (!todo.isEmpty()) {
      Object o = todo.poll();
      if (seen.put(o, Boolean.TRUE) != null) {
        continue;
      }
      Class<?> c = o.getClass();
      if (o instanceof PublicBAOS) {
        bytes += ((PublicBAOS) o).getBuf().length;
      } else if (c.isArray()) {
        if (c.getComponentType().isPrimitive()) {
          bytes += (long) Array.getLength(o) * primitiveBytes(c.getComponentType());
        } else {
          for (int i = 0; i < Array.getLength(o); i++) {
            Object element = Array.get(o, i);
            if (element != null) {
              todo.add(element);
            }
          }
        }
      } else if (!c.getName().startsWith("java.")) {
        for (Class<?> k = c; k != null && !k.getName().startsWith("java."); k = k.getSuperclass()) {
          for (Field f : k.getDeclaredFields()) {
            if (!Modifier.isStatic(f.getModifiers()) && !f.getType().isPrimitive()) {
              f.setAccessible(true);
              Object value = f.get(o);
              if (value != null) {
                todo.add(value);
              }
            }
          }
        }
      }
    }
    return bytes;
  }

  private static int primitiveBytes(Class<?> type) {
    if (type == long.class || type == double.class) {
      return 8;
    }
    if (type == int.class || type == float.class) {
      return 4;
    }
    return type == short.class || type == char.class ? 2 : 1;
  }
}
