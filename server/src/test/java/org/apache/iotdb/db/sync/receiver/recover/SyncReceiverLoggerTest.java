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
package org.apache.iotdb.db.sync.receiver.recover;

import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SyncReceiverLoggerTest {

  private ISyncReceiverLogger receiverLogger;
  private String dataDir;

  @Before
  public void setUp() throws DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    dataDir =
        new File(DirectoryManager.getInstance().getNextFolderForSequenceFile())
            .getParentFile()
            .getAbsolutePath();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSyncReceiverLogger() throws IOException {
    receiverLogger =
        new SyncReceiverLogger(new File(getReceiverFolderFile(), SyncConstant.SYNC_LOG_NAME));
    Set<String> deletedFileNames = new HashSet<>();
    Set<String> deletedFileNamesTest = new HashSet<>();
    receiverLogger.startSyncDeletedFilesName();
    for (int i = 0; i < 200; i++) {
      receiverLogger.finishSyncDeletedFileName(new File(getReceiverFolderFile(), "deleted" + i));
      deletedFileNames.add(new File(getReceiverFolderFile(), "deleted" + i).getAbsolutePath());
    }
    Set<String> toBeSyncedFiles = new HashSet<>();
    Set<String> toBeSyncedFilesTest = new HashSet<>();
    receiverLogger.startSyncTsFiles();
    for (int i = 0; i < 200; i++) {
      receiverLogger.finishSyncTsfile(new File(getReceiverFolderFile(), "new" + i));
      toBeSyncedFiles.add(new File(getReceiverFolderFile(), "new" + i).getAbsolutePath());
    }
    receiverLogger.close();
    int count = 0;
    int mode = 0;
    try (BufferedReader br =
        new BufferedReader(
            new FileReader(new File(getReceiverFolderFile(), SyncConstant.SYNC_LOG_NAME)))) {
      String line;
      while ((line = br.readLine()) != null) {
        count++;
        if (line.equals(SyncReceiverLogger.SYNC_DELETED_FILE_NAME_START)) {
          mode = -1;
        } else if (line.equals(SyncReceiverLogger.SYNC_TSFILE_START)) {
          mode = 1;
        } else {
          if (mode == -1) {
            deletedFileNamesTest.add(line);
          } else if (mode == 1) {
            toBeSyncedFilesTest.add(line);
          }
        }
      }
    }
    assertEquals(402, count);
    assertEquals(deletedFileNames.size(), deletedFileNamesTest.size());
    assertEquals(toBeSyncedFiles.size(), toBeSyncedFilesTest.size());
    assertTrue(deletedFileNames.containsAll(deletedFileNamesTest));
    assertTrue(toBeSyncedFiles.containsAll(toBeSyncedFilesTest));
  }

  private File getReceiverFolderFile() {
    return new File(
        dataDir
            + File.separatorChar
            + SyncConstant.SYNC_RECEIVER
            + File.separatorChar
            + "127.0.0.1_5555");
  }
}
