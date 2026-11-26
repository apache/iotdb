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
package org.apache.iotdb.db.storageengine.dataregion.wal.buffer;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/** Tests that WAL file number statistics correctly account for all WAL files. */
public class WALBufferFileNumTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory =
      TestConstant.BASE_OUTPUT_PATH.concat("wal-file-num-test");
  private static final String devicePath = "root.test_sg.test_d";

  private WALMode prevMode;
  private String prevConsensus;
  private long prevWalFileSizeThreshold;
  private WALNode walNode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    prevMode = config.getWalMode();
    prevConsensus = config.getDataRegionConsensusProtocolClass();
    prevWalFileSizeThreshold = config.getWalFileSizeThresholdInByte();
    config.setWalMode(WALMode.SYNC);
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    // use a small threshold so that writes can trigger auto-roll
    config.setWalFileSizeThresholdInByte(2 * 1024 * 1024);
  }

  @After
  public void tearDown() throws Exception {
    if (walNode != null) {
      walNode.close();
    }
    config.setWalMode(prevMode);
    config.setDataRegionConsensusProtocolClass(prevConsensus);
    config.setWalFileSizeThresholdInByte(prevWalFileSizeThreshold);
    EnvironmentUtils.cleanDir(logDirectory);
  }

  /** Verify that the initial WAL file writer created in the constructor is counted in fileNum. */
  @Test
  public void testInitialFileNumAfterConstruction() throws Exception {
    walNode = new WALNode(identifier, logDirectory);
    // after construction on a fresh directory, there should be exactly 1 WAL file
    // (the currentWALFileWriter created in the constructor)
    assertEquals(1, walNode.getFileNum());
    // verify disk agrees
    File[] walFilesOnDisk = WALFileUtils.listAllWALFiles(new File(logDirectory));
    assertEquals(1, walFilesOnDisk.length);
  }

  /**
   * Verify that fileNum stays correct after rolling the WAL file. After one roll, there should be 2
   * files: the original (now closed) and the new writer.
   */
  @Test
  public void testFileNumAfterRoll() throws Exception {
    walNode = new WALNode(identifier, logDirectory);
    assertEquals(1, walNode.getFileNum());

    // write some data then roll
    walNode.log(
        0,
        getInsertTabletNode(devicePath, new long[] {1}),
        Collections.singletonList(new int[] {0, 1}));
    walNode.rollWALFile();
    Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed());

    // after one roll: 1 closed file + 1 new open file = 2
    assertEquals(2, walNode.getFileNum());
    File[] walFilesOnDisk = WALFileUtils.listAllWALFiles(new File(logDirectory));
    assertEquals(2, walFilesOnDisk.length);
  }

  /**
   * Verify that fileNum stays correct after multiple rolls. After N rolls, there should be N+1
   * files on disk and fileNum should match.
   */
  @Test
  public void testFileNumAfterMultipleRolls() throws Exception {
    walNode = new WALNode(identifier, logDirectory);
    assertEquals(1, walNode.getFileNum());

    int rollCount = 3;
    for (int i = 0; i < rollCount; i++) {
      walNode.log(
          0,
          getInsertTabletNode(devicePath, new long[] {i + 1}),
          Collections.singletonList(new int[] {0, 1}));
      walNode.rollWALFile();
      Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed());
    }

    // rollCount closed files + 1 current open file
    long expectedFileNum = rollCount + 1;
    assertEquals(expectedFileNum, walNode.getFileNum());
    File[] walFilesOnDisk = WALFileUtils.listAllWALFiles(new File(logDirectory));
    assertEquals(expectedFileNum, walFilesOnDisk.length);
  }

  /**
   * Verify that WALManager's totalFileNum is consistent with the per-node fileNum for a single WAL
   * node.
   */
  @Test
  public void testTotalFileNumInWALManager() throws Exception {
    long totalFileNumBefore = WALManager.getInstance().getTotalFileNum();
    walNode = new WALNode(identifier, logDirectory);

    // after construction, totalFileNum should increase by 1
    assertEquals(totalFileNumBefore + 1, WALManager.getInstance().getTotalFileNum());

    // roll once
    walNode.log(
        0,
        getInsertTabletNode(devicePath, new long[] {1}),
        Collections.singletonList(new int[] {0, 1}));
    walNode.rollWALFile();
    Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed());

    // after one roll, totalFileNum should increase by 1 more
    assertEquals(totalFileNumBefore + 2, WALManager.getInstance().getTotalFileNum());
  }

  private InsertTabletNode getInsertTabletNode(String devicePath, long[] times)
      throws IllegalPathException {
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.TEXT};
    String[] measurements = new String[] {"s1"};
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.TEXT)};

    Object[] columns = new Object[1];
    Binary[] binaryValues = new Binary[times.length];
    for (int i = 0; i < times.length; i++) {
      binaryValues[i] = new Binary("test" + times[i], TSFileConfig.STRING_CHARSET);
    }
    columns[0] = binaryValues;

    BitMap[] bitMaps = new BitMap[1];
    bitMaps[0] = new BitMap(times.length);

    InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            measurements,
            dataTypes,
            times,
            bitMaps,
            columns,
            times.length);
    node.setMeasurementSchemas(schemas);
    return node;
  }
}
