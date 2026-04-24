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
package org.apache.iotdb.db.storageengine.dataregion.wal.encrypt;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.BrokenWALFileException;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.timecho.iotdb.commons.secret.SecretKey;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class WALEncryptTest {
  private final File walFile =
      new File(
          TestConstant.BASE_OUTPUT_PATH.concat(
              WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX)));

  private final String compressionDir =
      TestConstant.OUTPUT_DATA_DIR.concat(
          File.separator + "wal-compression" + File.separator + "root.__audit-1");

  CompressionType originCompressionType =
      IoTDBDescriptor.getInstance().getConfig().getWALCompressionAlgorithm();

  private final String devicePath = "root.sg.d1";

  @Before
  public void setUp()
      throws IOException, NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
    if (walFile.exists()) {
      FileUtils.delete(walFile);
    }
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(CompressionType.LZ4);
  }

  @After
  public void tearDown()
      throws IOException, NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
    if (walFile.exists()) {
      FileUtils.delete(walFile);
    }
    if (new File(compressionDir).exists()) {
      FileUtils.forceDelete(new File(compressionDir));
    }
    IoTDBDescriptor.getInstance().getConfig().setWALCompressionAlgorithm(originCompressionType);
  }

  public void testWALReader()
      throws IOException, QueryProcessException, IllegalPathException, InterruptedException {
    File dir = new File(compressionDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    WALBuffer walBuffer = new WALBuffer("root.__audit-1", compressionDir);
    List<WALEntry> entryList = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      InsertRowNode node = WALTestUtils.getInsertRowNode(devicePath, i);
      WALEntry entry = new WALInfoEntry(0, node);
      walBuffer.write(entry);
      entryList.add(entry);
    }
    long sleepTime = 0;
    while (!walBuffer.isAllWALEntriesConsumed()) {
      Thread.sleep(100);
      sleepTime += 100;
      if (sleepTime > 10_000) {
        Assert.fail("It has been too long for all entries to be consumed");
      }
    }
    walBuffer.close();

    File[] walFiles = WALFileUtils.listAllWALFiles(new File(compressionDir));
    Assert.assertNotNull(walFiles);
    walFiles =
        Arrays.stream(walFiles)
            .sorted(
                Comparator.comparingInt(
                    f -> {
                      return Integer.parseInt(f.getName().substring(1, f.getName().indexOf("-")));
                    }))
            .collect(Collectors.toList())
            .toArray(new File[0]);
    List<WALEntry> readWALEntryList = new ArrayList<>();
    for (File file : walFiles) {
      try (WALReader reader = new WALReader(file)) {
        while (reader.hasNext()) {
          readWALEntryList.add(reader.next());
        }
      }
    }
    Assert.assertEquals(entryList, readWALEntryList);

    int i = 0;
    for (File file : walFiles) {
      try (WALByteBufReader reader = new WALByteBufReader(file)) {
        while (reader.hasNext()) {
          ByteBuffer buffer = reader.next();
          Assert.assertEquals(entryList.get(i++).serializedSize(), buffer.array().length);
        }
      } catch (BrokenWALFileException e) {
        break;
      }
    }
  }

  @Test
  public void testEncrypted()
      throws IOException, QueryProcessException, IllegalPathException, InterruptedException {
    String prevEncryptType = TSFileDescriptor.getInstance().getConfig().getEncryptType();
    byte[] prevEncryptKey = TSFileDescriptor.getInstance().getConfig().getEncryptKey();
    int walBufferSize = IoTDBDescriptor.getInstance().getConfig().getWalBufferSize();
    long walFileSizeThresholdInByte =
        IoTDBDescriptor.getInstance().getConfig().getWalFileSizeThresholdInByte();
    String secretKey = SecretKey.getInstance().getSecretKey();
    String hardwareCode = SecretKey.getInstance().getHardwareCode();
    try {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setEncryptType("com.timecho.iotdb.commons.encrypt.AES128.AES128");
      TSFileDescriptor.getInstance()
          .getConfig()
          .setEncryptKey(new byte[] {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4});
      IoTDBDescriptor.getInstance().getConfig().setWalBufferSize(100000);
      IoTDBDescriptor.getInstance().getConfig().setWalFileSizeThresholdInByte(200000);
      SecretKey.getInstance().setSecretKey("1234567890");
      SecretKey.getInstance().setHardwareCode("TestHardwareCode");
      SecretKey.getInstance().initEncryptProps("com.timecho.iotdb.commons.encrypt.AES128.AES128");
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setTSFileDBToEncryptMap(
              new ConcurrentHashMap<>(
                  Collections.singletonMap(
                      "root.__audit",
                      new EncryptParameter(
                          SecretKey.getInstance().getEncryptType(),
                          SecretKey.getInstance().getRealSecretKey()))));

      testWALReader();
    } finally {
      TSFileDescriptor.getInstance().getConfig().setEncryptType(prevEncryptType);
      TSFileDescriptor.getInstance().getConfig().setEncryptKey(prevEncryptKey);
      IoTDBDescriptor.getInstance().getConfig().setWalBufferSize(walBufferSize);
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setWalFileSizeThresholdInByte(walFileSizeThresholdInByte);
      SecretKey.getInstance().setSecretKey(secretKey);
      SecretKey.getInstance().setHardwareCode(hardwareCode);
    }
  }
}
