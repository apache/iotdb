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
package org.apache.iotdb.db.wal.node;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.iot.wal.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.exception.MemTablePinException;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.db.wal.utils.WALPipeHandler;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WALPipeHandlerTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  private static final String devicePath = "root.test_sg.test_d";
  private WALMode prevMode;
  private boolean prevIsClusterMode;
  private WALNode walNode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    prevMode = config.getWalMode();
    prevIsClusterMode = config.isClusterMode();
    config.setWalMode(WALMode.SYNC);
    config.setClusterMode(true);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    config.setWalMode(prevMode);
    config.setClusterMode(prevIsClusterMode);
    EnvironmentUtils.cleanDir(logDirectory);
  }

  @Test(expected = MemTablePinException.class)
  public void pinDeletedMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    walNode.onMemTableFlushed(memTable);
    // pin flushed memTable
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    handler.pinMemTable();
  }

  @Test
  public void pinMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    handler.pinMemTable();
    walNode.onMemTableFlushed(memTable);
    // roll wal file
    walNode.rollWALFile();
    walNode.rollWALFile();
    // find node1
    ConsensusReqReader.ReqIterator itr = walNode.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
    // try to delete flushed but pinned memTable
    walNode.deleteOutdatedFiles();
    // try to find node1
    itr = walNode.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
  }

  @Test(expected = MemTablePinException.class)
  public void unpinDeletedMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    walNode.onMemTableFlushed(memTable);
    // pin flushed memTable
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    handler.unpinMemTable();
  }

  @Test
  public void unpinFlushedMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    // pin twice
    handler.pinMemTable();
    handler.pinMemTable();
    walNode.onMemTableFlushed(memTable);
    // unpin 1
    CheckpointManager checkpointManager = walNode.getCheckpointManager();
    handler.unpinMemTable();
    MemTableInfo oldestMemTableInfo = checkpointManager.getOldestMemTableInfo();
    assertEquals(memTable.getMemTableId(), oldestMemTableInfo.getMemTableId());
    assertNull(oldestMemTableInfo.getMemTable());
    assertTrue(oldestMemTableInfo.isPinned());
    // unpin 2
    handler.unpinMemTable();
    assertNull(checkpointManager.getOldestMemTableInfo());
  }

  @Test
  public void unpinMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    handler.pinMemTable();
    walNode.onMemTableFlushed(memTable);
    // roll wal file
    walNode.rollWALFile();
    walNode.rollWALFile();
    // find node1
    ConsensusReqReader.ReqIterator itr = walNode.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
    // unpin flushed memTable
    handler.unpinMemTable();
    // try to delete flushed but pinned memTable
    walNode.deleteOutdatedFiles();
    // try to find node1
    itr = walNode.getReqIterator(1);
    assertFalse(itr.hasNext());
  }

  @Test
  public void getUnFlushedValue() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    handler.pinMemTable();
    walNode.onMemTableFlushed(memTable);
    assertEquals(node1, handler.getValue());
  }

  @Test
  public void getFlushedValue() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALPipeHandler handler = flushListener.getWalPipeHandler();
    handler.pinMemTable();
    walNode.onMemTableFlushed(memTable);
    // wait until wal flushed
    while (!walNode.isAllWALEntriesConsumed()) {
      Thread.sleep(50);
    }
    assertEquals(node1, handler.getValue());
  }

  private InsertRowNode getInsertRowNode(String devicePath, long time) throws IllegalPathException {
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
    columns[5] = new Binary("hh" + 0);

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
    return node;
  }
}
