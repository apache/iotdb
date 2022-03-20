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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.wal.io.WALReader;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

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

import static org.junit.Assert.*;

public abstract class WALBufferCommonTest {
  protected static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected static final String identifier = "1";
  protected static final String logDirectory = "wal-test";
  protected static final String devicePath = "root.test_sg.test_d";
  protected IWALBufferForTest walBuffer;

  @Test
  public void testConcurrentWrite() throws Exception {
    walBuffer = new WALBuffer(identifier, logDirectory);
    // start write threads to write concurrently
    int threadsNum = 3;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    Set<InsertRowPlan> expectedInsertRowPlans = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < threadsNum; ++i) {
      int memTableId = i;
      Callable<Void> writeTask =
          () -> {
            try {
              writeInsertRowPlan(memTableId, expectedInsertRowPlans);
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
    while (!walBuffer.isAllWALEditConsumed()) {
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    // close wal buffer
    walBuffer.close();
    // check .wal files
    File[] walFiles = new File(logDirectory).listFiles(WALWriter::walFilenameFilter);
    Set<InsertRowPlan> actualInsertRowPlans = new HashSet<>();
    if (walFiles != null) {
      for (File walFile : walFiles) {
        try (WALReader walReader = new WALReader(walFile)) {
          while (walReader.hasNext()) {
            actualInsertRowPlans.add((InsertRowPlan) walReader.next().getValue());
          }
        }
      }
    }
    assertEquals(expectedInsertRowPlans, actualInsertRowPlans);
  }

  private void writeInsertRowPlan(int memTableId, Set<InsertRowPlan> expectedInsertRowPlans)
      throws IllegalPathException {
    for (int i = 0; i < 100; ++i) {
      InsertRowPlan insertRowPlan = getInsertRowPlan(devicePath + memTableId, i);
      expectedInsertRowPlans.add(insertRowPlan);

      WALEdit walEdit = new WALEdit(memTableId, insertRowPlan);
      walBuffer.write(walEdit);
    }
  }

  private InsertRowPlan getInsertRowPlan(String devicePath, long time) throws IllegalPathException {
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath(devicePath),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
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
