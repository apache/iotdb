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
package org.apache.iotdb.db.sync.sender.manage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
import org.apache.iotdb.db.sync.sender.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncFileManagerTest {

  private static final Logger logger = LoggerFactory.getLogger(SyncFileManagerTest.class);
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
  }

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testGetValidFiles() throws IOException {
    Map<String, Set<File>> allFileList = new HashMap<>();

    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList.put(String.valueOf(i), new HashSet<>());
        }
        String rand = String.valueOf(r.nextInt(10000));
        String fileName = FilePathUtils.regularizePath(dataDir) + IoTDBConstant.SEQUENCE_FLODER_NAME
            + File.separator + i
            + File.separator + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    Map<String, Set<File>> lastFileMap;
    Map<String, Set<File>> curFileMap;
    Map<String, Set<File>> deletedFilesMap;
    Map<String, Set<File>> toBeSyncedFilesMap;

    // lastFileList is empty
    manager.getValidFiles(dataDir);
    assert isEmpty(manager.getLastLocalFilesMap());

    updateLastLocalFiles(allFileList);

    manager.getValidFiles(dataDir);
    lastFileMap = manager.getLastLocalFilesMap();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      assert lastFileMap.containsKey(entry.getKey());
      assert lastFileMap.get(entry.getKey()).containsAll(entry.getValue());
    }

    // add some files
    Map<String, Set<File>> correctToBeSyncedFiles = new HashMap<>();
    r = new Random(1);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList.put(String.valueOf(i), new HashSet<>());
        }
        correctToBeSyncedFiles.putIfAbsent(String.valueOf(i), new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            FilePathUtils.regularizePath(dataDir) + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator + i
                + File.separator + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).add(file);
        correctToBeSyncedFiles.get(String.valueOf(i)).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    manager.getValidFiles(dataDir);
    lastFileMap = manager.getLastLocalFilesMap();
    curFileMap = manager.getCurrentSealedLocalFilesMap();
    deletedFilesMap = manager.getDeletedFilesMap();
    toBeSyncedFilesMap = manager.getToBeSyncedFilesMap();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      assert curFileMap.containsKey(entry.getKey());
      assert curFileMap.get(entry.getKey()).containsAll(entry.getValue());
    }
    for (Entry<String, Set<File>> entry : correctToBeSyncedFiles.entrySet()) {
      assert toBeSyncedFilesMap.containsKey(entry.getKey());
      assert toBeSyncedFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }
    updateLastLocalFiles(allFileList);
    manager.getValidFiles(dataDir);
    lastFileMap = manager.getLastLocalFilesMap();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      assert lastFileMap.containsKey(entry.getKey());
      assert lastFileMap.get(entry.getKey()).containsAll(entry.getValue());
    }

    // add some files and delete some files
    correctToBeSyncedFiles.clear();
    r = new Random(2);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList.put(String.valueOf(i), new HashSet<>());
        }
        correctToBeSyncedFiles.putIfAbsent(String.valueOf(i), new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            FilePathUtils.regularizePath(dataDir) + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator + i
                + File.separator + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).add(file);
        correctToBeSyncedFiles.get(String.valueOf(i)).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    int count = 0;
    Map<String, Set<File>> correctDeleteFile = new HashMap<>();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      correctDeleteFile.put(entry.getKey(), new HashSet<>());
      for (File file : entry.getValue()) {
        count++;
        if (count % 3 == 0 && lastFileMap.get(entry.getKey()).contains(file)) {
          correctDeleteFile.get(entry.getKey()).add(file);
        }
      }
    }
    for (Entry<String, Set<File>> entry : correctDeleteFile.entrySet()) {
      for (File file : entry.getValue()) {
        file.delete();
        new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).delete();
        allFileList.get(entry.getKey()).remove(file);
      }
    }
    manager.getValidFiles(dataDir);
    lastFileMap = manager.getLastLocalFilesMap();
    curFileMap = manager.getCurrentSealedLocalFilesMap();
    deletedFilesMap = manager.getDeletedFilesMap();
    toBeSyncedFilesMap = manager.getToBeSyncedFilesMap();
    for (Entry<String, Set<File>> entry : allFileList.entrySet()) {
      assert curFileMap.containsKey(entry.getKey());
      assert curFileMap.get(entry.getKey()).containsAll(entry.getValue());
    }
    for (Entry<String, Set<File>> entry : correctDeleteFile.entrySet()) {
      assert deletedFilesMap.containsKey(entry.getKey());
      assert deletedFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }
    for (Entry<String, Set<File>> entry : correctToBeSyncedFiles.entrySet()) {
      assert toBeSyncedFilesMap.containsKey(entry.getKey());
      assert toBeSyncedFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }

    // add some invalid files
    r = new Random(3);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList.put(String.valueOf(i), new HashSet<>());
        }
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            FilePathUtils.regularizePath(dataDir) + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator + i
                + File.separator + rand;
        File file = new File(fileName);
        allFileList.get(String.valueOf(i)).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    manager.getValidFiles(dataDir);
    lastFileMap = manager.getLastLocalFilesMap();
    curFileMap = manager.getCurrentSealedLocalFilesMap();
    deletedFilesMap = manager.getDeletedFilesMap();
    toBeSyncedFilesMap = manager.getToBeSyncedFilesMap();
    for (Entry<String, Set<File>> entry : curFileMap.entrySet()) {
      assert allFileList.containsKey(entry.getKey());
      assert allFileList.get(entry.getKey()).size() != entry.getValue().size();
      assert allFileList.get(entry.getKey()).containsAll(entry.getValue());
    }
    for (Entry<String, Set<File>> entry : correctDeleteFile.entrySet()) {
      assert deletedFilesMap.containsKey(entry.getKey());
      assert deletedFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }
    for (Entry<String, Set<File>> entry : correctToBeSyncedFiles.entrySet()) {
      assert toBeSyncedFilesMap.containsKey(entry.getKey());
      assert toBeSyncedFilesMap.get(entry.getKey()).containsAll(entry.getValue());
    }
  }

  private void updateLastLocalFiles(Map<String, Set<File>> lastLocalFilesMap) {
    try (BufferedWriter bw = new BufferedWriter(
        new FileWriter(new File(config.getLastFileInfoPath())))) {
      for (Set<File> currentLocalFiles : lastLocalFilesMap.values()) {
        for (File file : currentLocalFiles) {
          bw.write(file.getAbsolutePath());
          bw.newLine();
        }
        bw.flush();
      }
    } catch (IOException e) {
      logger.error("Can not clear sync log {}", config.getLastFileInfoPath(), e);
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