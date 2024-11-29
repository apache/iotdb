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
package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
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
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static final String SG_NAME = "root.recover_sg";
  private static final String DATA_REGION_ID = "1";
  private static final IDeviceID DEVICE1_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d1"));
  private static final IDeviceID DEVICE2_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d2"));
  private static final String FILE_WITH_WAL_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 0, 1);
  private static final String FILE_WITHOUT_WAL_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 1, 1);
  private static final String WAL_NODE_IDENTIFIER = String.valueOf(Integer.MAX_VALUE);
  private static final String WAL_NODE_FOLDER =
      commonConfig.getWalDirs()[0].concat(File.separator + WAL_NODE_IDENTIFIER);
  private static final WALRecoverManager recoverManager = WALRecoverManager.getInstance();

  private WALMode prevMode;
  private WALBuffer walBuffer;
  private CheckpointManager checkpointManager;
  private TsFileResource tsFileWithWALResource;
  private TsFileResource tsFileWithoutWALResource;
  private long originWALThreshold =
      IoTDBDescriptor.getInstance().getConfig().getWalFileSizeThresholdInByte();
  private boolean bufferClosed = false;

  @Before
  public void setUp() throws Exception {
    WALRecoverManager.getInstance().clear();
    IoTDBDescriptor.getInstance().getConfig().setWalFileSizeThresholdInByte(1 * 1024 * 1024);
    EnvironmentUtils.cleanDir(new File(FILE_WITH_WAL_NAME).getParent());
    EnvironmentUtils.envSetUp();
    prevMode = config.getWalMode();
    config.setWalMode(WALMode.SYNC);
    walBuffer = new WALBuffer(WAL_NODE_IDENTIFIER, WAL_NODE_FOLDER);
    checkpointManager = walBuffer.getCheckpointManager();
    bufferClosed = false;
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setWalFileSizeThresholdInByte(originWALThreshold);
    if (tsFileWithWALResource != null) {
      tsFileWithWALResource.close();
    }
    if (tsFileWithoutWALResource != null) {
      tsFileWithoutWALResource.close();
    }
    checkpointManager.close();
    if (!bufferClosed) {
      walBuffer.close();
    }
    config.setWalMode(prevMode);
    EnvironmentUtils.cleanDir(new File(FILE_WITH_WAL_NAME).getParent());
    EnvironmentUtils.cleanDir(new File(FILE_WITHOUT_WAL_NAME).getParent());
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testNormalProcedure() throws Exception {
    prepareCheckpointAndWALFileForNormal();
    WALRecoverManager.getInstance().clear();
    walBuffer.close();
    bufferClosed = true;
    recoverAndCheck();
  }

  private void prepareCheckpointAndWALFileForNormal()
      throws MetadataException, ExecutionException, InterruptedException, QueryProcessException {
    // write useless .wal files, start write threads to write concurrently
    int threadsNum = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    long firstWALVersionId = walBuffer.getCurrentWALFileVersion();
    for (int i = 0; i < threadsNum; ++i) {
      IMemTable fakeMemTable = new PrimitiveMemTable(SG_NAME, DATA_REGION_ID);
      long memTableId = fakeMemTable.getMemTableId();
      Callable<Void> writeTask =
          () -> {
            MemTableInfo memTableInfo = new MemTableInfo(fakeMemTable, "fake.tsfile", 0);
            checkpointManager.makeCreateMemTableCPInMemory(memTableInfo);
            checkpointManager.makeCreateMemTableCPOnDisk(memTableInfo.getMemTableId());
            try {
              while (walBuffer.getCurrentWALFileVersion() - firstWALVersionId < 2) {
                WALEntry walEntry =
                    new WALInfoEntry(
                        memTableId, getInsertTabletNode(SG_NAME.concat("test_d" + memTableId)));
                walBuffer.write(walEntry);
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
    executorService.shutdown();
    // wait a moment
    Awaitility.await().until(() -> walBuffer.isAllWALEntriesConsumed());
    // write normal .wal files
    long firstValidVersionId = walBuffer.getCurrentWALFileVersion();
    IMemTable targetMemTable = new PrimitiveMemTable(SG_NAME, DATA_REGION_ID);
    WALEntry walEntry =
        new WALInfoEntry(
            targetMemTable.getMemTableId(), getInsertRowNode(DEVICE2_NAME.toString(), 4L), true);
    walBuffer.write(walEntry);
    walEntry.getWalFlushListener().waitForResult();
    walEntry =
        new WALInfoEntry(
            targetMemTable.getMemTableId(), getInsertRowsNode(DEVICE2_NAME.toString(), 5L), true);
    walBuffer.write(walEntry);
    walEntry.getWalFlushListener().waitForResult();
    // write .checkpoint file
    MemTableInfo memTableInfo =
        new MemTableInfo(targetMemTable, FILE_WITH_WAL_NAME, firstValidVersionId);
    checkpointManager.makeCreateMemTableCPInMemory(memTableInfo);
    checkpointManager.makeCreateMemTableCPOnDisk(memTableInfo.getMemTableId());
    checkpointManager.fsyncCheckpointFile();
  }

  @Test
  public void testMemTableSnapshot() throws Exception {
    prepareCheckpointAndWALFileForSnapshot();
    WALRecoverManager.getInstance().clear();
    walBuffer.close();
    bufferClosed = true;
    recoverAndCheck();
  }

  private void prepareCheckpointAndWALFileForSnapshot()
      throws MetadataException, ExecutionException, InterruptedException, QueryProcessException {
    // write useless .wal files, start write threads to write concurrently
    int threadsNum = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    long firstWALVersionId = walBuffer.getCurrentWALFileVersion();
    for (int i = 0; i < threadsNum; ++i) {
      IMemTable fakeMemTable = new PrimitiveMemTable(SG_NAME, DATA_REGION_ID);
      long memTableId = fakeMemTable.getMemTableId();
      Callable<Void> writeTask =
          () -> {
            MemTableInfo memTableInfo = new MemTableInfo(fakeMemTable, "fake.tsfile", 0);
            checkpointManager.makeCreateMemTableCPInMemory(memTableInfo);
            checkpointManager.makeCreateMemTableCPOnDisk(memTableInfo.getMemTableId());
            try {
              while (walBuffer.getCurrentWALFileVersion() - firstWALVersionId < 2) {
                WALEntry walEntry =
                    new WALInfoEntry(
                        memTableId, getInsertTabletNode(SG_NAME.concat("test_d" + memTableId)));
                walBuffer.write(walEntry);
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
    executorService.shutdown();
    // wait a moment
    Awaitility.await().until(() -> walBuffer.isAllWALEntriesConsumed());
    // write normal .wal files
    long firstValidVersionId = walBuffer.getCurrentWALFileVersion();
    IMemTable targetMemTable = new PrimitiveMemTable(SG_NAME, DATA_REGION_ID);
    InsertRowNode insertRowNode = getInsertRowNode(DEVICE2_NAME.toString(), 4L);
    targetMemTable.insert(insertRowNode);

    WALEntry walEntry = new WALInfoEntry(targetMemTable.getMemTableId(), insertRowNode, true);
    walBuffer.write(walEntry);
    walEntry.getWalFlushListener().waitForResult();

    InsertRowsNode insertRowsNode = getInsertRowsNode(DEVICE2_NAME.toString(), 5L);
    for (InsertRowNode node : insertRowsNode.getInsertRowNodeList()) {
      targetMemTable.insert(node);
    }

    walEntry = new WALInfoEntry(targetMemTable.getMemTableId(), insertRowsNode, true);
    walBuffer.write(walEntry);
    walEntry.getWalFlushListener().waitForResult();

    walEntry = new WALInfoEntry(targetMemTable.getMemTableId(), targetMemTable, true);
    walBuffer.write(walEntry);
    walEntry.getWalFlushListener().waitForResult();
    // write .checkpoint file
    MemTableInfo memTableInfo =
        new MemTableInfo(targetMemTable, FILE_WITH_WAL_NAME, firstValidVersionId);
    checkpointManager.makeCreateMemTableCPInMemory(memTableInfo);
    checkpointManager.makeCreateMemTableCPOnDisk(memTableInfo.getMemTableId());
    checkpointManager.fsyncCheckpointFile();
  }

  private void recoverAndCheck() throws Exception {
    // prepare tsFiles
    List<WALRecoverListener> recoverListeners = prepareCrashedTsFile();
    // recover
    recoverManager.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));
    recoverManager.recover();
    // check recover listeners
    try {
      for (WALRecoverListener recoverListener : recoverListeners) {
        assertEquals(WALRecoverListener.Status.SUCCESS, recoverListener.waitForResult());
      }
    } catch (NullPointerException e) {
      // ignore
    }

    // region check file with wal
    // check file content
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_WITH_WAL_NAME);
    List<ChunkMetadata> chunkMetadataList =
        reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s1", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s2", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s1", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s2", true));
    assertNotNull(chunkMetadataList);
    assertEquals(2, chunkMetadataList.size());
    Chunk chunk = reader.readMemChunk(chunkMetadataList.get(0));
    assertEquals(3, chunk.getChunkStatistic().getEndTime());
    chunk = reader.readMemChunk(chunkMetadataList.get(1));
    assertEquals(15, chunk.getChunkStatistic().getEndTime());
    reader.close();
    // check .resource file in memory
    assertEquals(1, tsFileWithWALResource.getStartTime(DEVICE1_NAME));
    assertEquals(2, tsFileWithWALResource.getEndTime(DEVICE1_NAME));
    assertEquals(3, tsFileWithWALResource.getStartTime(DEVICE2_NAME));
    assertEquals(15, tsFileWithWALResource.getEndTime(DEVICE2_NAME));
    // check file existence
    assertTrue(new File(FILE_WITH_WAL_NAME).exists());
    assertTrue(new File(FILE_WITH_WAL_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    // endregion

    // region check file without wal
    // check file content
    reader = new TsFileSequenceReader(FILE_WITHOUT_WAL_NAME);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s1", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s2", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s1", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s2", true));
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

  @Test
  public void testRecoverOldWalWithEmptyTsFile() throws Exception {
    walBuffer.close();
    bufferClosed = true;
    // old version of wal is generated by prepareCheckpointAndWALFileForSnapshot() in v1.3
    String oldWalPathStr = this.getClass().getClassLoader().getResource("oldwal").getFile();
    File oldWalFileDir = new File(oldWalPathStr);
    FileUtils.copyDir(oldWalFileDir, new File(WAL_NODE_FOLDER));
    WALRecoverManager.getInstance().clear();
    recoverFromOldWalAndCheck(false);
  }

  @Test
  public void testRecoverOldWalWithBrokenTsFile() throws Exception {
    walBuffer.close();
    bufferClosed = true;
    // old version of wal is generated by prepareCheckpointAndWALFileForSnapshot() in v1.3
    String oldWalPathStr = this.getClass().getClassLoader().getResource("oldwal").getFile();
    File oldWalFileDir = new File(oldWalPathStr);
    FileUtils.copyDir(oldWalFileDir, new File(WAL_NODE_FOLDER));
    WALRecoverManager.getInstance().clear();
    recoverFromOldWalAndCheck(true);
  }

  private void recoverFromOldWalAndCheck(boolean withBrokenTsFile) throws Exception {
    // prepare tsFiles
    List<WALRecoverListener> recoverListeners = new ArrayList<>();

    // prepare file with wal
    File fileWithWALDir = new File(FILE_WITH_WAL_NAME).getParentFile();
    Files.createDirectories(fileWithWALDir.toPath());
    File fileWithWAL = new File(fileWithWALDir, "1723544967972-1-0-0.tsfile");
    if (withBrokenTsFile) {
      // copy a broken TsFileV3
      String oldWalPathStr = this.getClass().getClassLoader().getResource("oldwal").getFile();
      File oldWalFileDir = new File(oldWalPathStr);
      FileUtils.copyFile(new File(oldWalFileDir, "1723544967972-1-0-0"), fileWithWAL);
    } else {
      // create an empty tsfile
      Files.createFile(fileWithWAL.toPath());
    }
    tsFileWithWALResource = new TsFileResource(fileWithWAL);
    UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileWithWALResource, true, performer -> assertFalse(performer.canWrite()));
    recoverManager.addRecoverPerformer(recoverPerformer);
    recoverListeners.add(recoverPerformer.getRecoverListener());
    // recover
    recoverManager.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));
    recoverManager.recover();
    // check recover listeners
    try {
      for (WALRecoverListener recoverListener : recoverListeners) {
        assertEquals(WALRecoverListener.Status.SUCCESS, recoverListener.waitForResult());
      }
    } catch (NullPointerException e) {
      // ignore
    }
    // region check file with wal
    // check file content
    TsFileSequenceReader reader = new TsFileSequenceReader(fileWithWAL.getPath());
    List<ChunkMetadata> chunkMetadataList =
        reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s1", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s2", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s1", true));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path(DEVICE2_NAME, "s2", true));
    assertNotNull(chunkMetadataList);
    assertEquals(1, chunkMetadataList.size());
    Chunk chunk = reader.readMemChunk(chunkMetadataList.get(0));
    assertEquals(15, chunk.getChunkStatistic().getEndTime());
    reader.close();
    // check .resource file in memory
    assertEquals(4, tsFileWithWALResource.getStartTime(DEVICE2_NAME));
    assertEquals(15, tsFileWithWALResource.getEndTime(DEVICE2_NAME));
    // check file existence
    assertTrue(fileWithWAL.exists());
    assertTrue(new File(fileWithWAL.getPath().concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    // endregion

  }

  private InsertRowNode getInsertRowNode(String devicePath, long time)
      throws MetadataException, QueryProcessException {
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE};
    Object[] columns = new Object[] {1.0f, 1.0d};
    PartialPath path = new PartialPath(devicePath);
    String[] measurements = new String[] {"s1", "s2"};
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""), path, false, measurements, dataTypes, time, columns, false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.FLOAT),
          new MeasurementSchema("s2", TSDataType.DOUBLE)
        });
    return insertRowNode;
  }

  private InsertRowsNode getInsertRowsNode(String devicePath, long time)
      throws MetadataException, QueryProcessException {
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE};
    Object[] columns = new Object[] {1.0f, 1.0d};
    PartialPath path = new PartialPath(devicePath);
    String[] measurements = new String[] {"s1", "s2"};
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""), path, false, measurements, dataTypes, time, columns, false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.FLOAT),
          new MeasurementSchema("s2", TSDataType.DOUBLE)
        });
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.addOneInsertRowNode(insertRowNode, 0);
    insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""), path, false, measurements, dataTypes, time + 10, columns, false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.FLOAT),
          new MeasurementSchema("s2", TSDataType.DOUBLE)
        });
    insertRowsNode.addOneInsertRowNode(insertRowNode, 1);
    return insertRowsNode;
  }

  private InsertTabletNode getInsertTabletNode(String devicePath) throws IllegalPathException {
    long[] times = new long[] {110L, 111L, 112L, 113L};
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.BOOLEAN);
    dataTypes.add(TSDataType.TEXT);

    Object[] columns = new Object[6];
    columns[0] = new double[times.length];
    columns[1] = new float[times.length];
    columns[2] = new long[times.length];
    columns[3] = new int[times.length];
    columns[4] = new boolean[times.length];
    columns[5] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r, TSFileConfig.STRING_CHARSET);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            true,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes.toArray(new TSDataType[0]),
            times,
            bitMaps,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });
    return insertTabletNode;
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
            tsFileWithWALResource, true, performer -> assertFalse(performer.canWrite()));
    recoverManager.addRecoverPerformer(recoverPerformer);
    recoverListeners.add(recoverPerformer.getRecoverListener());

    // prepare file without wal
    File fileWithoutWAL = new File(FILE_WITHOUT_WAL_NAME);
    generateCrashedFile(fileWithoutWAL);
    tsFileWithoutWALResource = new TsFileResource(fileWithoutWAL);
    recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileWithoutWALResource, true, performer -> assertFalse(performer.canWrite()));
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
      writer.writeRecord(
          new TSRecord(DEVICE1_NAME, 1)
              .addTuple(new IntDataPoint("s1", 1))
              .addTuple(new LongDataPoint("s2", 1)));
      writer.writeRecord(
          new TSRecord(DEVICE1_NAME, 2)
              .addTuple(new IntDataPoint("s1", 2))
              .addTuple(new LongDataPoint("s2", 2)));
      writer.writeRecord(
          new TSRecord(DEVICE2_NAME, 3)
              .addTuple(new FloatDataPoint("s1", 3))
              .addTuple(new DoubleDataPoint("s2", 3)));
      writer.flush();
      try (FileChannel channel = new FileInputStream(tsFile).getChannel()) {
        truncateSize = channel.size();
      }
      writer.writeRecord(
          new TSRecord(DEVICE2_NAME, 4)
              .addTuple(new FloatDataPoint("s1", 4))
              .addTuple(new DoubleDataPoint("s2", 4)));
      writer.flush();
      try (FileChannel channel = new FileInputStream(tsFile).getChannel()) {
        truncateSize = (truncateSize + channel.size()) / 2;
      }
    }
    try (FileChannel channel = new FileOutputStream(tsFile, true).getChannel()) {
      channel.truncate(truncateSize);
    }
  }
}
