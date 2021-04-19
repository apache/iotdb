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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.SystemCheckException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode.WAL_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WalCheckerTest {

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
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("root"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File subDir = new File(tempRoot, "storage_group" + i);
        subDir.mkdir();
        LogWriter logWriter =
            new LogWriter(
                subDir.getPath() + File.separator + WAL_FILE_NAME,
                IoTDBDescriptor.getInstance().getConfig().getForceWalPeriodInMs() == 0);

        ByteBuffer binaryPlans = ByteBuffer.allocate(64 * 1024);
        String deviceId = "device1";
        String[] measurements = new String[] {"s1", "s2", "s3"};
        TSDataType[] types =
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64, TSDataType.INT64};
        String[] values = new String[] {"5", "6", "7"};
        for (int j = 0; j < 10; j++) {
          new InsertRowPlan(new PartialPath(deviceId), j, measurements, types, values)
              .serialize(binaryPlans);
        }
        binaryPlans.flip();
        logWriter.write(binaryPlans);
        logWriter.force();

        logWriter.close();
      }

      WalChecker checker = new WalChecker(tempRoot.getAbsolutePath());
      assertTrue(checker.doCheck().isEmpty());
    } finally {
      FileUtils.deleteDirectory(tempRoot);
    }
  }

  @Test
  public void testAbnormalCheck() throws IOException, SystemCheckException, IllegalPathException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("root"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File subDir = new File(tempRoot, "storage_group" + i);
        subDir.mkdir();
        LogWriter logWriter =
            new LogWriter(
                subDir.getPath() + File.separator + WAL_FILE_NAME,
                IoTDBDescriptor.getInstance().getConfig().getForceWalPeriodInMs() == 0);

        ByteBuffer binaryPlans = ByteBuffer.allocate(64 * 1024);
        String deviceId = "device1";
        String[] measurements = new String[] {"s1", "s2", "s3"};
        TSDataType[] types =
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64, TSDataType.INT64};
        String[] values = new String[] {"5", "6", "7"};
        for (int j = 0; j < 10; j++) {
          new InsertRowPlan(new PartialPath(deviceId), j, measurements, types, values)
              .serialize(binaryPlans);
        }
        if (i > 2) {
          binaryPlans.put("not a wal".getBytes());
        }
        logWriter.write(binaryPlans);
        logWriter.force();

        logWriter.close();
      }

      WalChecker checker = new WalChecker(tempRoot.getAbsolutePath());
      assertEquals(2, checker.doCheck().size());
    } finally {
      FileUtils.deleteDirectory(tempRoot);
    }
  }

  @Test
  public void testOneDamagedCheck() throws IOException, SystemCheckException {
    File tempRoot = new File(TestConstant.BASE_OUTPUT_PATH.concat("root"));
    tempRoot.mkdir();

    try {
      for (int i = 0; i < 5; i++) {
        File subDir = new File(tempRoot, "storage_group" + i);
        subDir.mkdir();

        FileOutputStream fileOutputStream = new FileOutputStream(new File(subDir, WAL_FILE_NAME));
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
