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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.FileUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;

import static org.apache.iotdb.db.engine.compaction.CompactionFileInfo.INFO_SEPARATOR;

public class CompactionFileInfoUT {

  @Test
  public void testGetInfoFromFilePath() {
    String firstPath =
        "sequence"
            + File.separator
            + "root.test.sg"
            + File.separator
            + "0"
            + File.separator
            + "0"
            + File.separator
            + "1-1-0-0.tsfile";
    CompactionFileInfo firstInfo = CompactionFileInfo.getFileInfoFromFilePath(firstPath);
    Assert.assertEquals(firstInfo.getFilename(), "1-1-0-0.tsfile");
    Assert.assertEquals(firstInfo.getLogicalStorageGroupName(), "root.test.sg");
    Assert.assertEquals(firstInfo.getTimePartitionId(), "0");
    Assert.assertEquals(firstInfo.getVirtualStorageGroupId(), "0");
    Assert.assertTrue(firstInfo.isSequence());

    String secondPath =
        "unsequence"
            + File.separator
            + "root.test.sg"
            + File.separator
            + "0"
            + File.separator
            + "426"
            + File.separator
            + "999-3-24-12.tsfile";

    CompactionFileInfo secondInfo = CompactionFileInfo.getFileInfoFromFilePath(secondPath);
    Assert.assertEquals(secondInfo.getFilename(), "999-3-24-12.tsfile");
    Assert.assertEquals(secondInfo.getLogicalStorageGroupName(), "root.test.sg");
    Assert.assertEquals(secondInfo.getTimePartitionId(), "426");
    Assert.assertEquals(secondInfo.getVirtualStorageGroupId(), "0");
    Assert.assertFalse(secondInfo.isSequence());

    String illegalPath =
        "root.test.sg"
            + File.separator
            + "0"
            + File.separator
            + "426"
            + File.separator
            + "999-3-24-12.tsfile";
    try {
      CompactionFileInfo.getFileInfoFromFilePath(illegalPath);
      Assert.fail();
    } catch (RuntimeException e) {

    }
  }

  @Test
  public void testGetInfoFromInfoString() {
    String[] firstInfoArray = new String[] {"root.test.sg", "0", "0", "true", "1-1-0-0.tsfile"};
    String firstInfoString = String.join(INFO_SEPARATOR, firstInfoArray);
    CompactionFileInfo firstInfo = CompactionFileInfo.getFileInfoFromInfoString(firstInfoString);
    Assert.assertEquals(firstInfo.getFilename(), "1-1-0-0.tsfile");
    Assert.assertEquals(firstInfo.getTimePartitionId(), "0");
    Assert.assertEquals(firstInfo.getVirtualStorageGroupId(), "0");
    Assert.assertEquals(firstInfo.getLogicalStorageGroupName(), "root.test.sg");
    Assert.assertTrue(firstInfo.isSequence());

    String[] secondInfoArray =
        new String[] {"root.test.sg", "0", "425", "false", "666-888-222-131.tsfile"};
    String secondInfoString = String.join(INFO_SEPARATOR, secondInfoArray);
    CompactionFileInfo secondInfo = CompactionFileInfo.getFileInfoFromInfoString(secondInfoString);
    Assert.assertEquals(secondInfo.getFilename(), "666-888-222-131.tsfile");
    Assert.assertEquals(secondInfo.getTimePartitionId(), "425");
    Assert.assertEquals(secondInfo.getVirtualStorageGroupId(), "0");
    Assert.assertEquals(secondInfo.getLogicalStorageGroupName(), "root.test.sg");
    Assert.assertFalse(secondInfo.isSequence());

    String[] illegalInfoArray = new String[] {"0", "425", "false", "666-888-222-131.tsfile"};
    String illegalInfoString = String.join(INFO_SEPARATOR, illegalInfoArray);

    try {
      CompactionFileInfo.getFileInfoFromInfoString(illegalInfoString);
      Assert.fail();
    } catch (RuntimeException e) {

    }
  }

  @Test
  public void testGetInfoFromFileFromSingleDir() throws IOException {
    String firstPath =
        "sequence"
            + File.separator
            + "root.test.sg"
            + File.separator
            + "0"
            + File.separator
            + "0"
            + File.separator
            + "100-10-5-1.tsfile";
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    File file = new File(dataDirs[0], firstPath);

    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }

    try {
      CompactionFileInfo info = CompactionFileInfo.getFileInfoFromFilePath(firstPath);
      Assert.assertNull(info.getFileFromDataDirs());
      if (!file.getParentFile().exists()) {
        Assert.assertTrue(file.getParentFile().mkdirs());
      }
      Assert.assertTrue(file.createNewFile());
      Assert.assertTrue(Files.isSameFile(file.toPath(), info.getFileFromDataDirs().toPath()));
    } finally {
      file.delete();
    }
  }

  @Test
  public void testGetInfoFromFileFromMultiDirs() throws Exception {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    String[] originDataDirs = config.getDataDirs();
    Class configClass = config.getClass();
    Field dataDirField = configClass.getDeclaredField("dataDirs");
    dataDirField.setAccessible(true);
    dataDirField.set(
        config,
        new String[] {"target" + File.separator + "data1", "target" + File.separator + "data2"});
    String filePath =
        "sequence"
            + File.separator
            + "root.test.sg"
            + File.separator
            + "0"
            + File.separator
            + "0"
            + File.separator
            + "100-10-5-1.tsfile";
    File testFile = new File("target" + File.separator + "data2", filePath);
    try {
      CompactionFileInfo info = CompactionFileInfo.getFileInfoFromFilePath(filePath);
      if (!testFile.getParentFile().exists()) {
        Assert.assertTrue(testFile.getParentFile().mkdirs());
      }
      if (testFile.exists()) {
        Assert.assertTrue(testFile.delete());
      }
      Assert.assertNull(info.getFileFromDataDirs());
      Assert.assertTrue(testFile.createNewFile());
      Assert.assertTrue(Files.isSameFile(testFile.toPath(), info.getFileFromDataDirs().toPath()));
    } finally {
      dataDirField.set(config, originDataDirs);
      Files.deleteIfExists(testFile.toPath());
      FileUtils.deleteDirectory(new File("target" + File.separator + "data2"));
    }
  }
}
