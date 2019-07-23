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
package org.apache.iotdb.db.sync.sender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.sync.conf.Constans;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncFileManager is used to pick up those tsfiles need to sync.
 */
public class SyncFileManager {

  private static final Logger logger = LoggerFactory.getLogger(SyncFileManager.class);

  /**
   * Files that need to be synchronized
   **/
  private Map<String, Set<String>> validAllFiles = new HashMap<>();

  /**
   * All tsfiles in last synchronization process
   **/
  private Set<String> lastLocalFiles = new HashSet<>();

  /**
   * All tsfiles in data directory
   **/
  private Map<String, Set<String>> currentLocalFiles = new HashMap<>();

  private SyncSenderConfig syncConfig = SyncSenderDescriptor.getInstance().getConfig();

  private IoTDBConfig systemConfig = IoTDBDescriptor.getInstance().getConfig();

  private static final String RESTORE_SUFFIX = ".restore";

  private SyncFileManager() {
  }

  public static final SyncFileManager getInstance() {
    return FileManagerHolder.INSTANCE;
  }

  /**
   * Initialize SyncFileManager.
   */
  public void init() throws IOException {
    validAllFiles.clear();
    lastLocalFiles.clear();
    currentLocalFiles.clear();
    getLastLocalFileList(syncConfig.getLastFileInfo());
    getCurrentLocalFileList(systemConfig.getDataDirs());
    getValidFileList();
  }

  /**
   * get files that needs to be synchronized
   */
  public void getValidFileList() {
    for (Entry<String, Set<String>> entry : currentLocalFiles.entrySet()) {
      for (String path : entry.getValue()) {
        if (!lastLocalFiles.contains(path)) {
          validAllFiles.get(entry.getKey()).add(path);
        }
      }
    }
    logger.info("Acquire list of valid files.");
    for (Entry<String, Set<String>> entry : validAllFiles.entrySet()) {
      for (String path : entry.getValue()) {
        currentLocalFiles.get(entry.getKey()).remove(path);
      }
    }
  }

  /**
   * get last local file list.
   *
   * @param path path
   */
  public void getLastLocalFileList(String path) throws IOException {
    Set<String> fileList = new HashSet<>();
    File file = new File(path);
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        throw new IOException("Cannot get last local file list", e);
      }
    } else {
      try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
        String fileName;
        while ((fileName = bf.readLine()) != null) {
          fileList.add(fileName);
        }
      } catch (IOException e) {
        logger.error("Cannot get last local file list when reading file {}.",
            syncConfig.getLastFileInfo());
        throw new IOException(e);
      }
    }
    lastLocalFiles = fileList;
  }

  /**
   * get current local file list.
   *
   * @param paths paths in String[] structure
   */
  public void getCurrentLocalFileList(String[] paths) {
    for (String path : paths) {
      if (!new File(path).exists()) {
        continue;
      }
      File[] listFiles = new File(path).listFiles();
      for (File storageGroup : listFiles) {
        if (!storageGroup.isDirectory() || storageGroup.getName().equals(Constans.SYNC_CLIENT)) {
          continue;
        }
        getStorageGroupFiles(storageGroup);
      }
    }
  }

  private void getStorageGroupFiles(File storageGroup) {
    if (!currentLocalFiles.containsKey(storageGroup.getName())) {
      currentLocalFiles.put(storageGroup.getName(), new HashSet<>());
    }
    if (!validAllFiles.containsKey(storageGroup.getName())) {
      validAllFiles.put(storageGroup.getName(), new HashSet<>());
    }
    File[] files = storageGroup.listFiles();
    for (File file : files) {
      if (!file.getPath().endsWith(RESTORE_SUFFIX) && !new File(
          file.getPath() + RESTORE_SUFFIX).exists()) {
        currentLocalFiles.get(storageGroup.getName()).add(file.getPath());
      }
    }
  }

  /**
   * backup current local file information.
   *
   * @param backupFile backup file path
   */
  public void backupNowLocalFileInfo(String backupFile) {
    try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(backupFile))) {
      for (Entry<String, Set<String>> entry : currentLocalFiles.entrySet()) {
        for (String file : entry.getValue()) {
          bufferedWriter.write(file + "\n");
        }
      }
    } catch (IOException e) {
      logger.error("Cannot back up current local file info", e);
    }
  }

  public Map<String, Set<String>> getValidAllFiles() {
    return validAllFiles;
  }

  public Set<String> getLastLocalFiles() {
    return lastLocalFiles;
  }

  public Map<String, Set<String>> getCurrentLocalFiles() {
    return currentLocalFiles;
  }

  public void setCurrentLocalFiles(Map<String, Set<String>> newNowLocalFiles) {
    currentLocalFiles = newNowLocalFiles;
  }

  private static class FileManagerHolder {

    private static final SyncFileManager INSTANCE = new SyncFileManager();
  }
}