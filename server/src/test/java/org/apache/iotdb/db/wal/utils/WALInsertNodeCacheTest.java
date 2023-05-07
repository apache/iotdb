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
package org.apache.iotdb.db.wal.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.node.WALNode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WALInsertNodeCacheTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  private static final String devicePath = "root.test_sg.test_d";
  private static final WALInsertNodeCache cache = WALInsertNodeCache.getInstance();
  private WALMode prevMode;
  private boolean prevIsClusterMode;
  private WALNode walNode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    cache.clear();
    prevMode = config.getWalMode();
    prevIsClusterMode = config.isClusterMode();
    config.setWalMode(WALMode.SYNC);
    config.setClusterMode(true);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    cache.clear();
    config.setWalMode(prevMode);
    config.setClusterMode(prevIsClusterMode);
    EnvironmentUtils.cleanDir(logDirectory);
  }

  @Test
  public void testLoadUnsealedWALFile() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    WALEntryPosition position = flushListener.getWalPipeHandler().getWalEntryPosition();
    // wait until wal flushed
    while (!walNode.isAllWALEntriesConsumed() || !position.canRead()) {
      Thread.sleep(50);
    }
    // load by cache
    assertEquals(node1, cache.get(position));
  }

  @Test
  public void testBatchLoad() throws Exception {
    // write memTable1
    IMemTable memTable1 = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable1, logDirectory + "/" + "fake1.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener1 = walNode.log(memTable1.getMemTableId(), node1);
    WALEntryPosition position1 = flushListener1.getWalPipeHandler().getWalEntryPosition();
    InsertRowNode node2 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(2);
    WALFlushListener flushListener2 = walNode.log(memTable1.getMemTableId(), node2);
    WALEntryPosition position2 = flushListener2.getWalPipeHandler().getWalEntryPosition();
    // write memTable2
    IMemTable memTable2 = new PrimitiveMemTable();
    walNode.onMemTableCreated(memTable2, logDirectory + "/" + "fake2.tsfile");
    InsertRowNode node3 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(3);
    WALFlushListener flushListener3 = walNode.log(memTable2.getMemTableId(), node3);
    WALEntryPosition position3 = flushListener3.getWalPipeHandler().getWalEntryPosition();
    // wait until wal flushed
    walNode.rollWALFile();
    // check batch load memTable1
    cache.addMemTable(memTable1.getMemTableId());
    assertEquals(node1, cache.get(position1));
    assertTrue(cache.contains(position1));
    assertTrue(cache.contains(position2));
    assertFalse(cache.contains(position3));
    // check batch load none
    cache.removeMemTable(memTable1.getMemTableId());
    cache.clear();
    assertEquals(node1, cache.get(position1));
    assertTrue(cache.contains(position1));
    assertFalse(cache.contains(position2));
    assertFalse(cache.contains(position3));
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
