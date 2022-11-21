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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.wal.exception.WALException;
import org.apache.iotdb.db.wal.io.ILogWriter;
import org.apache.iotdb.db.wal.io.WALFileTest;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.db.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.wal.utils.WALFileStatus;
import org.apache.iotdb.db.wal.utils.WALFileUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WalCheckerTest {
  private static final String DEVICE_ID = "root.test_sg.test_d";

  @Test
  public void testNoDir() {
    WalChecker checker = new WalChecker("no such dir");
    boolean caught = false;
    try {
      checker.doCheck();
    } catch (WALException e) {
      caught = true;
    }
    assertTrue(caught);
  }

  @Test
  public void testEmpty() throws IOException, WALException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("root"));
    tempRoot.mkdir();

    try {
      WalChecker checker = new WalChecker(tempRoot.getAbsolutePath());
      assertTrue(checker.doCheck().isEmpty());
    } finally {
      FileUtils.deleteDirectory(tempRoot);
    }
  }

  @Test
  public void testNormalCheck() throws IOException, WALException, IllegalPathException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("wal"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File walNodeDir = new File(tempRoot, String.valueOf(i));
        walNodeDir.mkdir();

        File walFile =
            new File(
                walNodeDir, WALFileUtils.getLogFileName(i, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
        int fakeMemTableId = 1;
        List<WALEntry> walEntries = new ArrayList<>();
        walEntries.add(new WALInfoEntry(fakeMemTableId, WALFileTest.getInsertRowNode(DEVICE_ID)));
        walEntries.add(
            new WALInfoEntry(fakeMemTableId, WALFileTest.getInsertTabletNode(DEVICE_ID)));
        walEntries.add(new WALInfoEntry(fakeMemTableId, WALFileTest.getDeleteDataNode(DEVICE_ID)));
        int size = 0;
        for (WALEntry walEntry : walEntries) {
          size += walEntry.serializedSize();
        }
        WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
        for (WALEntry walEntry : walEntries) {
          walEntry.serialize(buffer);
        }
        try (ILogWriter walWriter = new WALWriter(walFile)) {
          walWriter.write(buffer.getBuffer());
        }
      }

      WalChecker checker = new WalChecker(tempRoot.getAbsolutePath());
      assertTrue(checker.doCheck().isEmpty());
    } finally {
      FileUtils.deleteDirectory(tempRoot);
    }
  }

  @Test
  public void testAbnormalCheck() throws IOException, WALException, IllegalPathException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("wal"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File walNodeDir = new File(tempRoot, String.valueOf(i));
        walNodeDir.mkdir();

        File walFile =
            new File(
                walNodeDir, WALFileUtils.getLogFileName(i, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
        int fakeMemTableId = 1;
        List<WALEntry> walEntries = new ArrayList<>();
        walEntries.add(new WALInfoEntry(fakeMemTableId, WALFileTest.getInsertRowNode(DEVICE_ID)));
        walEntries.add(
            new WALInfoEntry(fakeMemTableId, WALFileTest.getInsertTabletNode(DEVICE_ID)));
        walEntries.add(new WALInfoEntry(fakeMemTableId, WALFileTest.getDeleteDataNode(DEVICE_ID)));
        int size = 0;
        for (WALEntry walEntry : walEntries) {
          size += walEntry.serializedSize();
        }
        WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
        for (WALEntry walEntry : walEntries) {
          walEntry.serialize(buffer);
        }
        try (ILogWriter walWriter = new WALWriter(walFile)) {
          walWriter.write(buffer.getBuffer());
          if (i == 0) {
            ByteBuffer errorBuffer = ByteBuffer.allocate(2);
            errorBuffer.put((byte) 3);
            walWriter.write(errorBuffer);
          }
        }
      }

      WalChecker checker = new WalChecker(tempRoot.getAbsolutePath());
      assertEquals(1, checker.doCheck().size());
    } finally {
      FileUtils.deleteDirectory(tempRoot);
    }
  }

  @Test
  public void testOneDamagedCheck() throws IOException, WALException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("wal"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File walNodeDir = new File(tempRoot, String.valueOf(i));
        walNodeDir.mkdir();

        File walFile =
            new File(
                walNodeDir, WALFileUtils.getLogFileName(i, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));

        FileOutputStream fileOutputStream = new FileOutputStream(walFile);
        try {
          fileOutputStream.write(i);
        } finally {
          fileOutputStream.close();
        }
      }

      WalChecker checker = new WalChecker(tempRoot.getAbsolutePath());
      assertEquals(5, checker.doCheck().size());
    } finally {
      FileUtils.deleteDirectory(tempRoot);
    }
  }
}
