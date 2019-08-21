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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.sync.sender.conf.SyncSenderDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncFileManager implements ISyncFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncFileManager.class);

  private Map<String, Set<File>> currentSealedLocalFilesMap;

  private Map<String, Set<File>> lastLocalFilesMap;

  private Map<String, Set<File>> deletedFilesMap;

  private Map<String, Set<File>> toBeSyncedFilesMap;

  private SyncFileManager() {

  }

  public static final SyncFileManager getInstance() {
    return SyncFileManagerHolder.INSTANCE;
  }

  @Override
  public void getCurrentLocalFiles(String dataDir) {
    LOGGER.info("Start to get current local files in data folder {}", dataDir);
    // get all files in data dir sequence folder
    Map<String, Set<File>> currentAllLocalFiles = new HashMap<>();
    File[] allSGFolders = new File(
        dataDir + File.separatorChar + IoTDBConstant.SEQUENCE_FLODER_NAME)
        .listFiles();
    for (File sgFolder : allSGFolders) {
      currentAllLocalFiles.putIfAbsent(sgFolder.getName(), new HashSet<>());
      Arrays.stream(sgFolder.listFiles()).forEach(file -> currentAllLocalFiles.get(sgFolder.getName())
          .add(new File(sgFolder.getAbsolutePath(), file.getName())));
    }

    // get sealed tsfiles
    currentSealedLocalFilesMap = new HashMap<>();
    for (Entry<String, Set<File>> entry : currentAllLocalFiles.entrySet()) {
      String sgName = entry.getKey();
      currentSealedLocalFilesMap.putIfAbsent(sgName, new HashSet<>());
      for (File file : entry.getValue()) {
        if (file.getName().endsWith(ModificationFile.FILE_SUFFIX) || file.getName()
            .endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          continue;
        }
        if (new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists() && !new File(
            file.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists()) {
          currentSealedLocalFilesMap.get(sgName).add(file);
        }
      }
    }
  }

  @Override
  public void getLastLocalFiles(File lastLocalFileInfo) throws IOException {
    LOGGER.info("Start to get last local files from last local file info {}",
        lastLocalFileInfo.getAbsoluteFile());
    lastLocalFilesMap = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(lastLocalFileInfo))) {
      String fileName;
      while ((fileName = reader.readLine()) != null) {
        String sgName = new File(fileName).getParent();
        lastLocalFilesMap.putIfAbsent(sgName, new HashSet<>());
        lastLocalFilesMap.get(sgName).add(new File(fileName));
      }
    }
  }

  @Override
  public void getValidFiles(String dataDir) throws IOException {
    getCurrentLocalFiles(dataDir);
    getLastLocalFiles(new File(SyncSenderDescriptor.getInstance().getConfig().getLastFileInfo()));
    toBeSyncedFilesMap = new HashMap<>();
    deletedFilesMap = new HashMap<>();
    for(Entry<String, Set<File>> entry: currentSealedLocalFilesMap.entrySet()){
      String sgName = entry.getKey();
      toBeSyncedFilesMap.putIfAbsent(sgName, new HashSet<>());
      deletedFilesMap.putIfAbsent(sgName, new HashSet<>());
      for(File newFile:currentSealedLocalFilesMap.get(sgName)){
        if(!lastLocalFilesMap.get(sgName).contains(newFile)){
          toBeSyncedFilesMap.get(sgName).add(newFile);
        }
      }
      for(File oldFile:lastLocalFilesMap.get(sgName)){
        if(!currentSealedLocalFilesMap.get(sgName).contains(oldFile)){
          deletedFilesMap.get(sgName).add(oldFile);
        }
      }
    }
  }

  @Override
  public void updateLastLocalFiles(File lastLocalFile, Set<String> localFiles) {

  }

  public Map<String, Set<File>> getCurrentSealedLocalFilesMap() {
    return currentSealedLocalFilesMap;
  }

  public Map<String, Set<File>> getLastLocalFilesMap() {
    return lastLocalFilesMap;
  }

  public Map<String, Set<File>> getDeletedFilesMap() {
    return deletedFilesMap;
  }

  public Map<String, Set<File>> getToBeSyncedFilesMap() {
    return toBeSyncedFilesMap;
  }

  private static class SyncFileManagerHolder {

    private static final SyncFileManager INSTANCE = new SyncFileManager();

    private SyncFileManagerHolder() {

    }
  }
}
