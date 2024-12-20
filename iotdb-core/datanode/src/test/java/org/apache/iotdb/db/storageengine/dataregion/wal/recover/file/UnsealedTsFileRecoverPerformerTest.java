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
package org.apache.iotdb.db.storageengine.dataregion.wal.recover.file;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UnsealedTsFileRecoverPerformerTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String SG_NAME = "root.recover_sg";
  private static final IDeviceID DEVICE1_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d1"));
  private static final IDeviceID DEVICE2_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d2"));
  private static final String FILE_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 0, 1);
  private TsFileResource tsFileResource;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(new File(FILE_NAME).getParent());
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    if (tsFileResource != null) {
      tsFileResource.close();
    }
    EnvironmentUtils.cleanDir(new File(FILE_NAME).getParent());
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRedoInsertPlan() throws Exception {
    // generate crashed .tsfile
    File file = new File(FILE_NAME);
    generateCrashedFile(file);
    assertTrue(file.exists());
    assertFalse(new File(FILE_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    // generate InsertRowPlan
    long time = 4;
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE};
    Object[] columns = new Object[] {1.0f, 1.0d};
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE2_NAME),
            false,
            new String[] {"s1", "s2"},
            dataTypes,
            time,
            columns,
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.FLOAT),
          new MeasurementSchema("s2", TSDataType.DOUBLE)
        });
    int fakeMemTableId = 1;
    WALEntry walEntry = new WALInfoEntry(fakeMemTableId, insertRowNode);
    // recover
    tsFileResource = new TsFileResource(file);
    // vsg processor is used to test IdTable, don't test IdTable here
    try (UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileResource, true, performer -> assertFalse(performer.canWrite()))) {
      recoverPerformer.startRecovery();
      assertTrue(recoverPerformer.hasCrashed());
      assertTrue(recoverPerformer.canWrite());
      assertEquals(3, tsFileResource.getEndTime(DEVICE2_NAME));

      recoverPerformer.redoLog(walEntry);

      recoverPerformer.endRecovery();
    }
    // check file content
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
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
    assertEquals(4, chunk.getChunkStatistic().getEndTime());
    reader.close();
    // check .resource file in memory
    assertEquals(1, tsFileResource.getStartTime(DEVICE1_NAME));
    assertEquals(2, tsFileResource.getEndTime(DEVICE1_NAME));
    assertEquals(3, tsFileResource.getStartTime(DEVICE2_NAME));
    assertEquals(4, tsFileResource.getEndTime(DEVICE2_NAME));
    // check file existence
    assertTrue(file.exists());
    assertTrue(new File(FILE_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
  }

  @Test
  public void testRedoRelationalInsertPlan() throws Exception {

    // fake table schema
    final TsTable testTable1 = new TsTable("table1");
    testTable1.addColumnSchema(
        new MeasurementColumnSchema("m1", TSDataType.INT32, TSEncoding.RLE, CompressionType.GZIP));
    DataNodeTableCache.getInstance().preUpdateTable(SG_NAME, testTable1);
    DataNodeTableCache.getInstance().commitUpdateTable(SG_NAME, "table1");
    // generate crashed .tsfile
    File file = new File(FILE_NAME);
    Files.createDirectories(file.getParentFile().toPath());
    Files.createFile(file.toPath());
    // generate insertTabletNode
    RelationalInsertTabletNode insertTabletNode = StatementTestUtils.genInsertTabletNode(10, 0);
    int fakeMemTableId = 1;
    WALEntry walEntry = new WALInfoEntry(fakeMemTableId, insertTabletNode);
    // recover
    tsFileResource = new TsFileResource(file);
    try (UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileResource, true, performer -> assertFalse(performer.canWrite()))) {
      recoverPerformer.startRecovery();
      assertTrue(recoverPerformer.hasCrashed());
      assertTrue(recoverPerformer.canWrite());
      recoverPerformer.redoLog(walEntry);
      recoverPerformer.endRecovery();
    }
    // check file content
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
    TsFileMetadata metadata = reader.readFileMetadata();
    Map<String, TableSchema> tableSchemaMap = reader.readFileMetadata().getTableSchemaMap();
    assertEquals(1, tableSchemaMap.size());
    assertTrue(tableSchemaMap.containsKey("table1"));
    reader.close();
  }

  @Test
  public void testRedoDeletePlan() throws Exception {
    // generate crashed .tsfile
    File file = new File(FILE_NAME);
    generateCrashedFile(file);
    assertTrue(file.exists());
    assertFalse(new File(FILE_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    assertFalse(ModificationFile.getExclusiveMods(new File(FILE_NAME)).exists());
    // generate InsertRowPlan
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId("0"),
            Collections.singletonList(new MeasurementPath(DEVICE2_NAME, "**")),
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    int fakeMemTableId = 1;
    WALEntry walEntry = new WALInfoEntry(fakeMemTableId, deleteDataNode);
    // recover
    tsFileResource = new TsFileResource(file);
    // vsg processor is used to test IdTable, don't test IdTable here
    try (UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileResource, true, performer -> assertFalse(performer.canWrite()))) {
      recoverPerformer.startRecovery();
      assertTrue(recoverPerformer.hasCrashed());
      assertTrue(recoverPerformer.canWrite());
      assertEquals(3, tsFileResource.getEndTime(DEVICE2_NAME));

      recoverPerformer.redoLog(walEntry);

      recoverPerformer.endRecovery();
    }
    // check file content
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
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
    assertEquals(3, chunk.getChunkStatistic().getEndTime());
    reader.close();
    // check .resource file in memory
    assertEquals(1, tsFileResource.getStartTime(DEVICE1_NAME));
    assertEquals(2, tsFileResource.getEndTime(DEVICE1_NAME));
    assertEquals(3, tsFileResource.getStartTime(DEVICE2_NAME));
    assertEquals(3, tsFileResource.getEndTime(DEVICE2_NAME));
    // check file existence
    assertTrue(file.exists());
    assertTrue(new File(FILE_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    assertTrue(ModificationFile.getExclusiveMods(new File(FILE_NAME)).exists());
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

  /**
   * Recover WALEntry that only contains InsertRowNode/InsertTabletNode with null values. This type
   * of node will be generated when inserting mismatched type data.
   */
  @Test
  public void testRecoverNullInsertRowPlan() throws Exception {
    // generate crashed .tsfile
    File file = new File(FILE_NAME);
    generateCrashedFile(file);
    assertTrue(file.exists());
    assertFalse(new File(FILE_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    assertFalse(ModificationFile.getCompactionMods(new File(FILE_NAME)).exists());
    // generate InsertRowNode with null
    long time = 4;
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode 1"),
            new PartialPath(DEVICE2_NAME),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            time,
            new Integer[] {1},
            false);
    insertRowNode.markFailedMeasurement(0);

    // generate InsertTabletNode with null
    time = 5;
    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("plannode 2"),
            new PartialPath(DEVICE2_NAME),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            null,
            new long[] {time},
            null,
            new Integer[] {1},
            1);
    insertTabletNode.markFailedMeasurement(0);

    int fakeMemTableId = 1;
    WALEntry walEntry1 = new WALInfoEntry(fakeMemTableId++, insertRowNode);
    WALEntry walEntry2 = new WALInfoEntry(fakeMemTableId, insertTabletNode);
    // recover
    tsFileResource = new TsFileResource(file);
    // vsg processor is used to test IdTable, don't test IdTable here
    try (UnsealedTsFileRecoverPerformer recoverPerformer =
        new UnsealedTsFileRecoverPerformer(
            tsFileResource, true, performer -> assertFalse(performer.canWrite()))) {
      recoverPerformer.startRecovery();
      assertTrue(recoverPerformer.hasCrashed());
      assertTrue(recoverPerformer.canWrite());
      recoverPerformer.redoLog(walEntry1);
      recoverPerformer.redoLog(walEntry2);
      recoverPerformer.endRecovery();
    }
  }

  @Test
  public void testRecoverDuplicate()
      throws IllegalPathException,
          IOException,
          WriteProcessException,
          DataRegionException,
          WALRecoverException {
    // generate crashed .tsfile
    File file = new File(FILE_NAME);
    generateCrashedFile(file);
    assertTrue(file.exists());
    assertFalse(new File(FILE_NAME.concat(TsFileResource.RESOURCE_SUFFIX)).exists());
    assertFalse(ModificationFile.getExclusiveMods(new File(FILE_NAME)).exists());
    tsFileResource = new TsFileResource(file);

    int fakeMemTableId = 1;

    IMemTable memTable = new PrimitiveMemTable();
    memTable.setDatabaseAndDataRegionId(SG_NAME, "0");
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
    memTable.write(DEVICE1_NAME, schemaList, 1, new Object[] {100000});
    WALEntry duplicateMemTableSnapshotWalEntry = new WALInfoEntry(fakeMemTableId++, memTable);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("plannode 1"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT32},
            2,
            new Integer[] {20},
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT32)});
    WALEntry duplicateWalEntry = new WALInfoEntry(fakeMemTableId++, insertRowNode);

    InsertRowNode insertRowNode2 =
        new InsertRowNode(
            new PlanNodeId("plannode 2"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT32},
            10,
            new Integer[] {10},
            false);
    insertRowNode2.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT32)});

    WALEntry normalWalEntry = new WALInfoEntry(fakeMemTableId++, insertRowNode2);

    try (UnsealedTsFileRecoverPerformer performer =
        new UnsealedTsFileRecoverPerformer(tsFileResource, true, p -> assertFalse(p.canWrite()))) {
      performer.startRecovery();
      performer.redoLog(duplicateMemTableSnapshotWalEntry);
      performer.redoLog(duplicateWalEntry);
      performer.redoLog(normalWalEntry);
      performer.endRecovery();
      performer.getTsFileResource();
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME)) {
      List<ChunkMetadata> chunkMetadataList =
          reader.getChunkMetadataList(new Path(DEVICE1_NAME, "s1", true));
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        ChunkReader chunkReader = new ChunkReader(chunk);
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          while (batchData.hasCurrent()) {
            Assert.assertEquals((int) batchData.currentTime(), batchData.currentValue());
            batchData.next();
          }
        }
      }
    }
  }
}
