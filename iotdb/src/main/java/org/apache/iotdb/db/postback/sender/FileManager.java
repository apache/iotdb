/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.postback.sender;

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
import org.apache.iotdb.db.postback.conf.PostBackSenderConfig;
import org.apache.iotdb.db.postback.conf.PostBackSenderDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class is to pick up which files need to postback.
 *
 * @author lta
 */
public class FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);
  private Map<String, Set<String>> sendingFiles = new HashMap<>();
  private Set<String> lastLocalFiles = new HashSet<>();
  private Map<String, Set<String>> currentLocalFiles = new HashMap<>();
  private PostBackSenderConfig postbackConfig = PostBackSenderDescriptor.getInstance().getConfig();
  private IoTDBConfig tsfileConfig = IoTDBDescriptor.getInstance().getConfig();

  private FileManager() {
  }

  public static final FileManager getInstance() {
    return FileManagerHolder.INSTANCE;
  }

  /**
   * initialize FileManager.
   */
  public void init() {
    sendingFiles.clear();
    lastLocalFiles.clear();
    currentLocalFiles.clear();
    getLastLocalFileList(postbackConfig.getLastFileInfo());
    getCurrentLocalFileList(tsfileConfig.getBufferWriteDirs());
    getSendingFileList();
  }

  /**
   * get sending file list.
   */
  public void getSendingFileList() {
    for (Entry<String, Set<String>> entry : currentLocalFiles.entrySet()) {
      for (String path : entry.getValue()) {
        if (!lastLocalFiles.contains(path)) {
          sendingFiles.get(entry.getKey()).add(path);
        }
      }
    }
    LOGGER.info("IoTDB sender : Sender has got list of sending files.");
    for (Entry<String, Set<String>> entry : sendingFiles.entrySet()) {
      for (String path : entry.getValue()) {
        LOGGER.info(path);
        currentLocalFiles.get(entry.getKey()).remove(path);
      }
    }
  }

  /**
   * get last local file list.
   *
   * @param path path
   */
  public void getLastLocalFileList(String path) {
    Set<String> fileList = new HashSet<>();
    File file = new File(path);
    if (!file.exists()) {
      try {
        if (!file.createNewFile()) {
          LOGGER.error("IoTDB post back sender: cannot create file {}", file.getAbsoluteFile());
        }
      } catch (IOException e) {
        LOGGER.error("IoTDB post back sender: cannot get last local file list because {}",
            e.getMessage());
      }
    } else {
      try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
        String fileName;
        while ((fileName = bf.readLine()) != null) {
          fileList.add(fileName);
        }
      } catch (IOException e) {
        LOGGER.error(
            "IoTDB post back sender: cannot get last local file list when reading file {} "
                + "because {}.",
            postbackConfig.getLastFileInfo(), e.getMessage());
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
        if (storageGroup.isDirectory() && !storageGroup.getName().equals("postback")) {
          if (!currentLocalFiles.containsKey(storageGroup.getName())) {
            currentLocalFiles.put(storageGroup.getName(), new HashSet<String>());
          }
          if (!sendingFiles.containsKey(storageGroup.getName())) {
            sendingFiles.put(storageGroup.getName(), new HashSet<String>());
          }
          File[] files = storageGroup.listFiles();
          for (File file : files) {
            if (!file.getAbsolutePath().endsWith(".restore") && !new File(
                file.getAbsolutePath() + ".restore").exists()) {
              currentLocalFiles.get(storageGroup.getName()).add(file.getAbsolutePath());
            }
          }
        }
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
      LOGGER.error("IoTDB post back sender: cannot back up now local file info because {}", e);
    }
  }

  public Map<String, Set<String>> getSendingFiles() {
    return sendingFiles;
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

    private static final FileManager INSTANCE = new FileManager();
  }
}