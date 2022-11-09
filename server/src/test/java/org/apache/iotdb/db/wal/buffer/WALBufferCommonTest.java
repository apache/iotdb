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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.wal.io.WALReader;
import org.apache.iotdb.db.wal.utils.WALFileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class WALBufferCommonTest {
  protected static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected static final String identifier = String.valueOf(Integer.MAX_VALUE);
  protected static final boolean preIsClusterMode = config.isClusterMode();
  protected static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  protected static final String devicePath = "root.test_sg.test_d";
  protected IWALBuffer walBuffer;

  @Before
  public void setUp() throws Exception {
    walBuffer = new WALBuffer(identifier, logDirectory);
    config.setClusterMode(true);
  }

  @After
  public void tearDown() throws Exception {
    walBuffer.close();
    config.setClusterMode(preIsClusterMode);
  }

  @Test
  public void testConcurrentWrite() throws Exception {
    // start write threads to write concurrently
    int threadsNum = 3;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    Set<InsertRowNode> expectedInsertRowNodes = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < threadsNum; ++i) {
      int memTableId = i;
      Callable<Void> writeTask =
          () -> {
            try {
              writeInsertRowNode(memTableId, expectedInsertRowNodes);
            } catch (IllegalPathException e) {
              fail();
            }
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
    while (!walBuffer.isAllWALEntriesConsumed()) {
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    // check .wal files
    File[] walFiles = WALFileUtils.listAllWALFiles(new File(logDirectory));
    Set<InsertRowNode> actualInsertRowNodes = new HashSet<>();
    if (walFiles != null) {
      for (File walFile : walFiles) {
        try (WALReader walReader = new WALReader(walFile)) {
          while (walReader.hasNext()) {
            actualInsertRowNodes.add((InsertRowNode) walReader.next().getValue());
          }
        }
      }
    }
    assertEquals(expectedInsertRowNodes, actualInsertRowNodes);
  }

  private void writeInsertRowNode(int memTableId, Set<InsertRowNode> expectedInsertRowNodes)
      throws IllegalPathException, QueryProcessException {
    for (int i = 0; i < 100; ++i) {
      InsertRowNode insertRowNode = getInsertRowNode(devicePath + memTableId, i);
      expectedInsertRowNodes.add(insertRowNode);

      WALEntry walEntry = new WALInfoEntry(memTableId, insertRowNode);
      walBuffer.write(walEntry);
    }
  }

  private InsertRowNode getInsertRowNode(String devicePath, long time)
      throws IllegalPathException, QueryProcessException {
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

  @Test
  public void testHugeWrite() throws Exception {
    // use small buffer (only 32 bytes) to simulate huge write request
    int prevWalBufferSize = config.getWalBufferSize();
    config.setWalBufferSize(32);
    try {
      testConcurrentWrite();
    } finally {
      config.setWalBufferSize(prevWalBufferSize);
    }
  }
}
