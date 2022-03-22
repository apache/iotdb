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
package org.apache.iotdb.db.wal.recover;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.buffer.IWALBuffer;
import org.apache.iotdb.db.wal.buffer.WALBuffer;
import org.apache.iotdb.db.wal.buffer.WALEdit;
import org.apache.iotdb.db.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.wal.recover.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.db.wal.utils.listener.IResultListener;
import org.apache.iotdb.db.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WALRecoverManagerTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String SG_NAME = "root.recover_sg";
  private static final String DEVICE1_NAME = SG_NAME.concat(".d1");
  private static final String DEVICE2_NAME = SG_NAME.concat(".d2");
  private static final String FILE_WITH_WAL_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 0, 1);
  private static final String FILE_WITHOUT_WAL_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 1, 1);
  private static final String WAL_NODE_IDENTIFIER = "10";
  private static final String WAL_NODE_FOLDER =
      config.getWalDirs()[0].concat(File.separator + WAL_NODE_IDENTIFIER);
  private static final WALRecoverManager recoverManager = WALRecoverManager.getInstance();

  private IWALBuffer walBuffer;
  private CheckpointManager checkpointManager;
  private TsFileResource tsFileWithWALResource;
  private TsFileResource tsFileWithoutWALResource;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(new File(FILE_WITH_WAL_NAME).getParent());
    EnvironmentUtils.envSetUp();
    walBuffer = new WALBuffer(WAL_NODE_IDENTIFIER, WAL_NODE_FOLDER);
    checkpointManager = new CheckpointManager(WAL_NODE_IDENTIFIER, WAL_NODE_FOLDER);
    IoTDB.metaManager.setStorageGroup(new PartialPath(SG_NAME));
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE1_NAME.concat(".s1")),
        TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE1_NAME.concat(".s2")),
        TSDataType.INT64,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE2_NAME.concat(".s1")),
        TSDataType.FLOAT,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    IoTDB.metaManager.createTimeseries(
        new PartialPath(DEVICE2_NAME.concat(".s2")),
        TSDataType.DOUBLE,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    if (tsFileWithWALResource != null) {
      tsFileWithWALResource.close();
    }
    if (tsFileWithoutWALResource != null) {
      tsFileWithoutWALResource.close();
    }
    EnvironmentUtils.cleanDir(new File(FILE_WITH_WAL_NAME).getParent());
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testNormalProcedure() throws Exception {
    prepareCheckpointAndWALFile();
    List<WALRecoverListener> recoverListeners = prepareCrashedTsFile();
    // recover
    recoverManager.setAllVsgScannedLatch(new CountDownLatch(0));
    recoverManager.recover();
    // check recover listeners
    for (WALRecoverListener recoverListener : recoverListeners) {
      assertEquals(IResultListener.Status.SUCCESS, recoverListener.getResult());
    }

    // region check file with wal
    // check file content
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_WITH_WAL_NAME);
    List<ChunkMetadata> chunkMetadataList =
        reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s2"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s2"));
    assertNotNull(chunkMetadataList);
    assertEquals(2, chunkMetadataList.size());
    Chunk chunk = reader.readMemChunk(chunkMetadataList.get(0));
    assertEquals(3, chunk.getChunkStatistic().getEndTime());
    chunk = reader.readMemChunk(chunkMetadataList.get(1));
    assertEquals(4, chunk.getChunkStatistic().getEndTime());
    reader.close();
    // check .resource file in memory
    assertEquals(1, tsFileWithWALResource.getStartTime(DEVICE1_NAME));
    assertEquals(2, tsFileWithWALResource.getEndTime(DEVICE1_NAME));
    assertEquals(3, tsFileWithWALResource.getStartTime(DEVICE2_NAME));
    assertEquals(4, tsFileWithWALResource.getEndTime(DEVICE2_NAME));
    // check file existence
    assertTrue(new File(FILE_WITH_WAL_NAME).exists());
    assertTrue(new File(FILE_WITH_WAL_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    // endregion

    // region check file without wal
    // check file content
    reader = new TsFileSequenceReader(FILE_WITHOUT_WAL_NAME);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s2"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s2"));
    assertNotNull(chunkMetadataList);
    assertEquals(1, chunkMetadataList.size());
    chunk = reader.readMemChunk(chunkMetadataList.get(0));
    assertEquals(3, chunk.getChunkStatistic().getEndTime());
    reader.close();
    // check .resource file in memory
    assertEquals(1, tsFileWithoutWALResource.getStartTime(DEVICE1_NAME));
    assertEquals(2, tsFileWithoutWALResource.getEndTime(DEVICE1_NAME));
    assertEquals(3, tsFileWithoutWALResource.getStartTime(DEVICE2_NAME));
    assertEquals(3, tsFileWithoutWALResource.getEndTime(DEVICE2_NAME));
    // check file existence
    assertTrue(new File(FILE_WITHOUT_WAL_NAME).exists());
    assertTrue(new File(FILE_WITHOUT_WAL_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    // endregion
  }

  private void prepareCheckpointAndWALFile()
      throws IllegalPathException, ExecutionException, InterruptedException {
    // write useless .wal files, start write threads to write concurrently
    int threadsNum = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    int firstWALVersionId = walBuffer.getCurrentLogVersion();
    for (int i = 0; i < threadsNum; ++i) {
      IMemTable fakeMemTable = new NotifyFlushMemTable();
      int memTableId = fakeMemTable.getMemTableId();
      Callable<Void> writeTask =
          () -> {
            checkpointManager.makeCreateMemTableCP(
                new MemTableInfo(fakeMemTable.getMemTableId(), "fake.tsfile", 0));
            try {
              while (walBuffer.getCurrentLogVersion() - firstWALVersionId < 2) {
                WALEdit walEdit =
                    new WALEdit(
                        memTableId, getInsertTabletPlan(SG_NAME.concat("test_d" + memTableId)));
                walBuffer.write(walEdit);
              }
            } catch (IllegalPathException e) {
              fail();
            }
            checkpointManager.makeFlushMemTableCP(fakeMemTable.getMemTableId());
            return null;
          };
      Future<Void> future = executorService.submit(writeTask);
      futures.add(future);
    }
    // wait until all write tasks are done
    for (Future<Void> future : futures) {
      future.get();
    }
    // wait a moment
    while (!walBuffer.isAllWALEditConsumed()) {
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    // write normal .wal files
    int firstValidVersionId = walBuffer.getCurrentLogVersion();
    IMemTable targetMemTable = new NotifyFlushMemTable();
    WALEdit walEdit =
        new WALEdit(targetMemTable.getMemTableId(), getInsertRowPlan(DEVICE2_NAME, 4L));
    walBuffer.write(walEdit);
    // write .checkpoint file
    checkpointManager.makeCreateMemTableCP(
        new MemTableInfo(targetMemTable.getMemTableId(), FILE_WITH_WAL_NAME, firstValidVersionId));
    checkpointManager.fsyncCheckpointFile();
  }

  private InsertRowPlan getInsertRowPlan(String devicePath, long time) throws IllegalPathException {
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE};
    String[] columns = new String[] {1 + "", 1.0 + ""};
    return new InsertRowPlan(
        new PartialPath(devicePath), time, new String[] {"s1", "s2"}, dataTypes, columns);
  }

  private InsertTabletPlan getInsertTabletPlan(String devicePath) throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(
            new PartialPath(devicePath),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setBitMaps(bitMaps);
    return insertTabletPlan;
  }

  private List<WALRecoverListener> prepareCrashedTsFile()
      throws IOException, WriteProcessException {
    List<WALRecoverListener> recoverListeners = new ArrayList<>();

    // prepare file with wal
    File fileWithWAL = new File(FILE_WITH_WAL_NAME);
    generateCrashedFile(fileWithWAL);
    tsFileWithWALResource = new TsFileResource(fileWithWAL);
    UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileWithWALResource, true, null, performer -> assertFalse(performer.canWrite()));
    recoverManager.addRecoverPerformer(recoverPerformer);
    recoverListeners.add(recoverPerformer.getRecoverListener());

    // prepare file without wal
    File fileWithoutWAL = new File(FILE_WITHOUT_WAL_NAME);
    generateCrashedFile(fileWithoutWAL);
    tsFileWithoutWALResource = new TsFileResource(fileWithoutWAL);
    recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileWithoutWALResource, true, null, performer -> assertFalse(performer.canWrite()));
    recoverManager.addRecoverPerformer(recoverPerformer);
    recoverListeners.add(recoverPerformer.getRecoverListener());

    return recoverListeners;
  }

  private void generateCrashedFile(File tsFile) throws IOException, WriteProcessException {
    long truncateSize;
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerTimeseries(
          new Path(DEVICE1_NAME), new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE1_NAME), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE2_NAME), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE2_NAME), new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
      writer.write(
          new TSRecord(1, DEVICE1_NAME)
              .addTuple(new IntDataPoint("s1", 1))
              .addTuple(new LongDataPoint("s2", 1)));
      writer.write(
          new TSRecord(2, DEVICE1_NAME)
              .addTuple(new IntDataPoint("s1", 2))
              .addTuple(new LongDataPoint("s2", 2)));
      writer.write(
          new TSRecord(3, DEVICE2_NAME)
              .addTuple(new FloatDataPoint("s1", 3))
              .addTuple(new DoubleDataPoint("s2", 3)));
      writer.flushAllChunkGroups();
      try (FileChannel channel = new FileInputStream(tsFile).getChannel()) {
        truncateSize = channel.size();
      }
      writer.write(
          new TSRecord(4, DEVICE2_NAME)
              .addTuple(new FloatDataPoint("s1", 4))
              .addTuple(new DoubleDataPoint("s2", 4)));
      writer.flushAllChunkGroups();
      try (FileChannel channel = new FileInputStream(tsFile).getChannel()) {
        truncateSize = (truncateSize + channel.size()) / 2;
      }
    }
    try (FileChannel channel = new FileOutputStream(tsFile, true).getChannel()) {
      channel.truncate(truncateSize);
    }
  }
}
