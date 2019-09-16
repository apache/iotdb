/**
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
package org.apache.iotdb.db.sync.sender.recover;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.sender.conf.SyncConstant;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.sender.manage.SyncFileManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncSenderLogAnalyzerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncSenderLogAnalyzerTest.class);
  private SyncSenderLogger senderLogger;
  private SyncSenderLogAnalyzer senderLogAnalyzer;
  private SyncFileManager manager = SyncFileManager.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private String dataDir;

  @Before
  public void setUp()
      throws IOException, InterruptedException, StartupException, DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    dataDir = new File(DirectoryManager.getInstance().getNextFolderForSequenceFile())
        .getParentFile().getAbsolutePath();
    config.update(dataDir);
    senderLogger = new SyncSenderLogger(
        new File(config.getSenderFolderPath(), SyncConstant.SYNC_LOG_NAME));
    senderLogAnalyzer = new SyncSenderLogAnalyzer(config.getSenderFolderPath());
  }

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void recover() throws IOException {
    Map<String, Set<File>> allFileList = new HashMap<>();

    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList.put(String.valueOf(i), new HashSet<>());
        }
        String rand = r.nextInt(10000) + TSFILE_SUFFIX;
        String fileName = FilePathUtils.regularizePath(dataDir) + IoTDBConstant.SEQUENCE_FLODER_NAME
            + File.separator + i
            + File.separator + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          LOGGER.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          LOGGER.error("Can not create new file {}", file.getPath());
        }
      }
    }
    manager.getValidFiles(dataDir);
    assert isEmpty(manager.getLastLocalFilesMap());
    senderLogger.startSyncTsFiles();
    for(Set<File> newTsFiles:allFileList.values()){
      for(File file: newTsFiles){
        senderLogger.finishSyncTsfile(file);
      }
    }
    senderLogger.close();

    // recover log
    senderLogAnalyzer.recover();
    manager.getValidFiles(dataDir);
    assert !isEmpty(manager.getLastLocalFilesMap());
    Map<String, Set<File>> lastFilesMap = manager.getLastLocalFilesMap();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      assert lastFilesMap.containsKey(entry.getKey());
      assert lastFilesMap.get(entry.getKey()).size() == entry.getValue().size();
      assert lastFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }

    // delete some files
    assert !new File(config.getSenderFolderPath(), SyncConstant.SYNC_LOG_NAME).exists();
    senderLogger = new SyncSenderLogger(
        new File(config.getSenderFolderPath(), SyncConstant.SYNC_LOG_NAME));
    manager.getValidFiles(dataDir);
    assert !isEmpty(manager.getLastLocalFilesMap());
    senderLogger.startSyncDeletedFilesName();
    for(Set<File> newTsFiles:allFileList.values()){
      for(File file: newTsFiles){
        senderLogger.finishSyncDeletedFileName(file);
      }
    }
    senderLogger.close();
    // recover log
    senderLogAnalyzer.recover();
    manager.getValidFiles(dataDir);
    assert isEmpty(manager.getLastLocalFilesMap());
    assert isEmpty(manager.getDeletedFilesMap());
    Map<String, Set<File>> toBeSyncedFilesMap = manager.getToBeSyncedFilesMap();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      assert toBeSyncedFilesMap.containsKey(entry.getKey());
      assert toBeSyncedFilesMap.get(entry.getKey()).size() == entry.getValue().size();
      assert toBeSyncedFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }
  }

  private boolean isEmpty(Map<String, Set<File>> sendingFileList) {
    for (Entry<String, Set<File>> entry : sendingFileList.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        return false;
      }
    }
    return true;
  }

}