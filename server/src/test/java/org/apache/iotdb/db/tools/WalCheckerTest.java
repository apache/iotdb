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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.SystemCheckException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.wal.buffer.WALEdit;
import org.apache.iotdb.db.wal.io.ILogWriter;
import org.apache.iotdb.db.wal.io.WALFileTest;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.db.wal.utils.WALByteBufferForTest;

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
    } catch (SystemCheckException e) {
      caught = true;
    }
    assertTrue(caught);
  }

  @Test
  public void testEmpty() throws IOException, SystemCheckException {
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
  public void testNormalCheck() throws IOException, SystemCheckException, IllegalPathException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("wal"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File walNodeDir = new File(tempRoot, String.valueOf(i));
        walNodeDir.mkdir();

        File walFile =
            new File(walNodeDir, WALWriter.FILE_PREFIX + i + IoTDBConstant.WAL_FILE_SUFFIX);
        int fakeMemTableId = 1;
        List<WALEdit> walEdits = new ArrayList<>();
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertRowPlan(DEVICE_ID)));
        walEdits.add(
            new WALEdit(fakeMemTableId, WALFileTest.getInsertRowsOfOneDevicePlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertRowsPlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertTabletPlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertMultiTabletPlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getDeletePlan(DEVICE_ID)));
        int size = 0;
        for (WALEdit walEdit : walEdits) {
          size += walEdit.serializedSize();
        }
        WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
        for (WALEdit walEdit : walEdits) {
          walEdit.serialize(buffer);
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
  public void testAbnormalCheck() throws IOException, SystemCheckException, IllegalPathException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("wal"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File walNodeDir = new File(tempRoot, String.valueOf(i));
        walNodeDir.mkdir();

        File walFile = new File(walNodeDir, "_" + i + IoTDBConstant.WAL_FILE_SUFFIX);
        int fakeMemTableId = 1;
        List<WALEdit> walEdits = new ArrayList<>();
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertRowPlan(DEVICE_ID)));
        walEdits.add(
            new WALEdit(fakeMemTableId, WALFileTest.getInsertRowsOfOneDevicePlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertRowsPlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertTabletPlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getInsertMultiTabletPlan(DEVICE_ID)));
        walEdits.add(new WALEdit(fakeMemTableId, WALFileTest.getDeletePlan(DEVICE_ID)));
        int size = 0;
        for (WALEdit walEdit : walEdits) {
          size += walEdit.serializedSize();
        }
        WALByteBufferForTest buffer = new WALByteBufferForTest(ByteBuffer.allocate(size));
        for (WALEdit walEdit : walEdits) {
          walEdit.serialize(buffer);
        }
        try (ILogWriter walWriter = new WALWriter(walFile)) {
          walWriter.write(buffer.getBuffer());
          if (i == 0) {
            ByteBuffer errorBuffer = ByteBuffer.allocate(4);
            errorBuffer.putInt(1);
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
  public void testOneDamagedCheck() throws IOException, SystemCheckException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("wal"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File walNodeDir = new File(tempRoot, String.valueOf(i));
        walNodeDir.mkdir();

        File walFile = new File(walNodeDir, "_" + i + IoTDBConstant.WAL_FILE_SUFFIX);

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
