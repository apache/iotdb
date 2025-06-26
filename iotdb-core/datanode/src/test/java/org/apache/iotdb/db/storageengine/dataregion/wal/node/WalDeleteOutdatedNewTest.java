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

package org.apache.iotdb.db.storageengine.dataregion.wal.node;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;

public class WalDeleteOutdatedNewTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier1 = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory1 =
      TestConstant.BASE_OUTPUT_PATH.concat("sequence/root.test_sg/1/2910/");
  private static final String databasePath = "root.test_sg";
  private static final String devicePath = databasePath + ".test_d";
  private static final String dataRegionId = "1";
  private WALMode prevMode;
  private String prevConsensus;
  private WALNode walNode1;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory1);
    prevMode = config.getWalMode();
    prevConsensus = config.getDataRegionConsensusProtocolClass();
    config.setWalMode(WALMode.SYNC);
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    walNode1 = new WALNode(identifier1, logDirectory1);
    DataRegion dataRegion = new DataRegionTest.DummyDataRegion(logDirectory1, databasePath);
    dataRegion.updatePartitionFileVersion(2911, 0);
    StorageEngine.getInstance().setDataRegion(new DataRegionId(1), dataRegion);
  }

  @After
  public void tearDown() throws Exception {
    walNode1.close();
    config.setWalMode(prevMode);
    config.setDataRegionConsensusProtocolClass(prevConsensus);
    EnvironmentUtils.cleanDir(logDirectory1);
    StorageEngine.getInstance().reset();
  }

  /**
   * The simulation here is to write the last file, because serialization and disk flushing
   * operations are asynchronous, so you have to wait until all the entries are processed to get the
   * correct result, when WalEntry is not consumed to get memTableIdsOfWal, the result is not
   * accurate, so when the actual deletion of expired wal files, Don't read the last wal file.
   */
  @Test
  public void test01() throws IllegalPathException {
    IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable1, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));

    IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 4));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 5));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 6));

    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    Map<Long, Set<Long>> memTableIdsOfWal = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(1, memTableIdsOfWal.size());
    Assert.assertEquals(2, memTableIdsOfWal.get(0L).size());
    Assert.assertEquals(1, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);
    walNode1.close();
  }

  /**
   * Ensure that the memtableIds maintained by each wal file are accurate:<br>
   * 1. _0-1-1.wal:memTable0、memTable1 <br>
   * 2. roll wal file <br>
   * 3. _1-6-1.wal: memTable1 <br>
   * 4. wait until all walEntry consumed
   */
  @Test
  public void test02() throws IllegalPathException {
    IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable1, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));

    IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 4));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 5));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 6));

    walNode1.rollWALFile();
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 7));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 8));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 9));
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    Map<Long, Set<Long>> memTableIdsOfWal = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(2, memTableIdsOfWal.size());
    Assert.assertEquals(1, memTableIdsOfWal.get(1L).size());
    Assert.assertEquals(2, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);
  }

  /**
   * Ensure that files that can be cleaned can be deleted: <br>
   * 1. _0-0-1.wal: memTable0 、 memTable1 <br>
   * 2. roll wal file <br>
   * 3. _1-1-1.wal: memTable1 <br>
   * 4. wait until all walEntry consumed <br>
   * 5. memTable0 flush, memTable1 flush <br>
   * 6. delete outdated wal files
   */
  @Test
  public void test03() throws IllegalPathException {

    IMemTable memTable0 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable0, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));

    IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable1, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.rollWALFile();
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));

    Map<Long, Set<Long>> memTableIdsOfWal = walNode1.getWALBuffer().getMemTableIdsOfWal();
    walNode1.onMemTableFlushed(memTable0);
    walNode1.onMemTableFlushed(memTable1);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    // before deleted
    Assert.assertEquals(2, memTableIdsOfWal.size());
    Assert.assertEquals(2, memTableIdsOfWal.get(0L).size());
    File[] files = WALFileUtils.listAllWALFiles(new File(logDirectory1));
    Assert.assertEquals(2, files.length);

    walNode1.deleteOutdatedFiles();
    Map<Long, Set<Long>> memTableIdsOfWalAfter = walNode1.getWALBuffer().getMemTableIdsOfWal();

    // after deleted
    Assert.assertEquals(0, memTableIdsOfWalAfter.size());
    File[] filesAfter = WALFileUtils.listAllWALFiles(new File(logDirectory1));
    Assert.assertEquals(1, filesAfter.length);
  }

  /**
   * Ensure that files that can be cleaned can be deleted: <br>
   * 1. _0-0-1.wal: memTable0 <br>
   * 2. roll wal file <br>
   * 3. _1-1-0.wal: memTable1 <br>
   * 4. roll wal file <br>
   * 5. _2-1-1.wal: memTable1 <br>
   * 6. wait until all walEntry consumed <br>
   * 7. memTable0 flush, memTable1 flush, memTable2 flush <br>
   * 6. delete outdated wal files
   */
  @Test
  public void test04() throws IllegalPathException {
    IMemTable memTable0 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable0, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.rollWALFile();

    IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable1, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.rollWALFile();

    IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));
    walNode1.onMemTableFlushed(memTable2);
    walNode1.onMemTableFlushed(memTable0);
    walNode1.onMemTableFlushed(memTable1);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());

    Map<Long, Set<Long>> memTableIdsOfWal = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(3, memTableIdsOfWal.size());
    Assert.assertEquals(3, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);

    walNode1.deleteOutdatedFiles();
    Map<Long, Set<Long>> memTableIdsOfWalAfter = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(0, memTableIdsOfWalAfter.size());
    Assert.assertEquals(1, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);
  }

  /**
   * Ensure that the flushed wal related to memtable cannot be deleted: <br>
   * 1. _0-0-1.wal: memTable0 <br>
   * 2. roll wal file <br>
   * 3. _1-1-1.wal: memTable0 <br>
   * 4. roll wal file <br>
   * 5. _2-1-1.wal: memTable0 <br>
   * 6. roll wal file <br>
   * 7. _2-1-1.wal: memTable0 <br>
   * 8. wait until all walEntry consumed <br>
   * 9. delete outdated wal files
   */
  @Test
  public void test06() throws IllegalPathException {
    IMemTable memTable0 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable0, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.rollWALFile();
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.rollWALFile();
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));
    walNode1.rollWALFile();
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 4));
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());

    Map<Long, Set<Long>> memTableIdsOfWal = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(4, memTableIdsOfWal.size());
    Assert.assertEquals(4, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);

    walNode1.deleteOutdatedFiles();
    Map<Long, Set<Long>> memTableIdsOfWalAfter = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(4, memTableIdsOfWalAfter.size());
    Assert.assertEquals(4, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);
  }

  /**
   * Ensure that files that can be cleaned can be deleted: <br>
   * 1. _0-0-1.wal: memTable0 <br>
   * 2. roll wal file <br>
   * 3. _1-1-0.wal: memTable1、memTable2 <br>
   * 4. roll wal file <br>
   * 5. _2-1-0.wal: memTable2 <br>
   * 6. roll wal file <br>
   * 7. _3-1-0.wal: memTable3 <br>
   * 8. roll wal file <br>
   * 9. _4-1-0.wal: memTable3 <br>
   * 10. wait until all walEntry consumed <br>
   * 11. memTable1 flush, memTable2 flush, memTable3 flush <br>
   * 12. delete outdated wal files
   */
  @Test
  public void test07() throws IllegalPathException {
    IMemTable memTable0 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable0, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.rollWALFile();

    IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable1, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));

    IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.rollWALFile();
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.rollWALFile();
    IMemTable memTable3 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable3, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable3.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.rollWALFile();
    walNode1.log(
        memTable3.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.onMemTableFlushed(memTable1);
    walNode1.onMemTableFlushed(memTable2);
    walNode1.onMemTableFlushed(memTable3);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());

    Map<Long, Set<Long>> memTableIdsOfWal = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(5, memTableIdsOfWal.size());
    Assert.assertEquals(5, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);

    walNode1.deleteOutdatedFiles();
    Map<Long, Set<Long>> memTableIdsOfWalAfter = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(2, memTableIdsOfWalAfter.size());
    Assert.assertEquals(2, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);

    walNode1.onMemTableFlushed(memTable0);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    walNode1.deleteOutdatedFiles();
    Map<Long, Set<Long>> memTableIdsOfWalAfterAfter = walNode1.getWALBuffer().getMemTableIdsOfWal();
    Assert.assertEquals(0, memTableIdsOfWalAfterAfter.size());
    Assert.assertEquals(1, WALFileUtils.listAllWALFiles(new File(logDirectory1)).length);
  }

  /**
   * Ensure that files that can be cleaned can be deleted: <br>
   * 1. _0-0-1.wal: memTable0 <br>
   * 2. roll wal file <br>
   * 3. _1-1-0.wal: memTable1<br>
   * 4. memTable1 flush <br>
   * 5. roll wal file <br>
   * 6. _2-1-0.wal: memTable2 <br>
   * 7. wait until all walEntry consumed <br>
   * 8. delete outdated wal files
   */
  @Test
  public void test08() throws IllegalPathException {
    IMemTable memTable0 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable0, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.rollWALFile();

    ConsensusReqReader.ReqIterator itr1 = walNode1.getReqIterator(1);
    Assert.assertFalse(itr1.hasNext());

    IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable1, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable1.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), -1));
    walNode1.onMemTableFlushed(memTable1);
    walNode1.rollWALFile();

    ConsensusReqReader.ReqIterator itr2 = walNode1.getReqIterator(1);
    Assert.assertTrue(itr2.hasNext());

    IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());

    ConsensusReqReader.ReqIterator itr3 = walNode1.getReqIterator(1);
    Assert.assertTrue(itr3.hasNext());
    walNode1.deleteOutdatedFiles();

    ConsensusReqReader.ReqIterator itr4 = walNode1.getReqIterator(1);
    Assert.assertFalse(itr4.hasNext());
    walNode1.rollWALFile();
    Assert.assertTrue(itr4.hasNext());
  }

  /**
   * Ensure that files that can be cleaned can be deleted: <br>
   * 1. _0-0-1.wal: memTable0 <br>
   * 2. roll wal file <br>
   * 3. _2-1-0.wal: memTable2 <br>
   * 4. wait until all walEntry consumed <br>
   * 5. delete outdated wal files
   */
  @Test
  public void test09() throws IllegalPathException {
    IMemTable memTable0 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable0, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable0.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 1));
    walNode1.rollWALFile();

    IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable2, logDirectory1 + "/" + "fake.tsfile");
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 2));
    walNode1.log(
        memTable2.getMemTableId(),
        generateInsertRowNode(devicePath, System.currentTimeMillis(), 3));
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());

    ConsensusReqReader.ReqIterator itr3 = walNode1.getReqIterator(1);
    Assert.assertFalse(itr3.hasNext());
  }

  public static InsertRowNode generateInsertRowNode(String devicePath, long time, long searchIndex)
      throws IllegalPathException {
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0d;
    columns[1] = 2f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);
    MeasurementSchema[] schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema("s" + (i + 1), dataTypes[i]);
    }
    node.setMeasurementSchemas(schemas);
    node.setSearchIndex(searchIndex);
    return node;
  }
}
