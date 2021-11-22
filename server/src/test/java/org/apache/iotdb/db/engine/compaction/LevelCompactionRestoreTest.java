/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;

public class LevelCompactionRestoreTest {
  static String dataDir = TestConstant.SEQUENCE_DATA_DIR;
  LevelCompactionTsFileManagementForTest tsFileManagementForTest;

  @Before
  public void setUp() {
    File dataDirectory = new File(dataDir);
    if (!dataDirectory.exists()) {
      Assert.assertTrue(dataDirectory.mkdirs());
    }
    tsFileManagementForTest =
        new LevelCompactionTsFileManagementForTest("root.compactionTest", "0", dataDir);
  }

  @After
  public void tearDown() throws IOException {
    File dataDirectory = new File(dataDir);
    if (dataDirectory.exists()) {
      Files.delete(dataDirectory.toPath());
    }
    tsFileManagementForTest = null;
  }

  @Test
  public void test() {}

  private static class LevelCompactionTsFileManagementForTest
      extends LevelCompactionTsFileManagement {

    public boolean shouldThrowExceptionInDLFIL = false;
    public boolean shouldThrowExceptionInDLFID = false;

    public LevelCompactionTsFileManagementForTest(
        String storageGroupName, String virtualStorageGroupId, String storageGroupDir) {
      super(storageGroupName, virtualStorageGroupId, storageGroupDir);
    }

    @Override
    protected void deleteLevelFilesInList(
        long timePartitionId,
        Collection<TsFileResource> mergeTsFiles,
        int level,
        boolean sequence) {
      if (shouldThrowExceptionInDLFIL) {
        throw new RuntimeException("test");
      }
    }

    @Override
    protected void deleteLevelFile(TsFileResource seqFile) {
      seqFile.writeLock();
      try {
        ChunkCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
        FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
        seqFile.setDeleted(true);
        seqFile.delete();
      } catch (IOException ignored) {
      } finally {
        seqFile.writeUnlock();
      }
    }

    @Override
    protected void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
      int count = 0;
      int size = mergeTsFiles.size();
      for (TsFileResource mergeFile : mergeTsFiles) {
        deleteLevelFile(mergeFile);
        count++;
        if (shouldThrowExceptionInDLFID && count > size / 2) {
          throw new RuntimeException("test");
        }
      }
    }
  }
}
