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
package org.apache.iotdb.db.sync.sender.recover;

import org.apache.iotdb.db.sync.conf.SyncConstant;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SyncSenderLogAnalyzer implements ISyncSenderLogAnalyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncSenderLogAnalyzer.class);
  private String senderPath;
  private File currentLocalFile;
  private File lastLocalFile;
  private File syncLogFile;

  public SyncSenderLogAnalyzer(String senderPath) {
    this.senderPath = senderPath;
    this.currentLocalFile = new File(senderPath, SyncConstant.CURRENT_LOCAL_FILE_NAME);
    this.lastLocalFile = new File(senderPath, SyncConstant.LAST_LOCAL_FILE_NAME);
    this.syncLogFile = new File(senderPath, SyncConstant.SYNC_LOG_NAME);
  }

  @Override
  public void recover() throws IOException {
    if (currentLocalFile.exists() && !lastLocalFile.exists()) {
      FileUtils.moveFile(currentLocalFile, lastLocalFile);
    } else {
      Set<String> lastLocalFiles = new HashSet<>();
      Set<String> deletedFiles = new HashSet<>();
      Set<String> newFiles = new HashSet<>();
      loadLastLocalFiles(lastLocalFiles);
      loadLogger(deletedFiles, newFiles);
      lastLocalFiles.removeAll(deletedFiles);
      lastLocalFiles.addAll(newFiles);
      updateLastLocalFile(lastLocalFiles);
    }
    FileUtils.deleteDirectory(new File(senderPath, SyncConstant.DATA_SNAPSHOT_NAME));
    syncLogFile.delete();
  }

  @Override
  public void loadLastLocalFiles(Set<String> lastLocalFiles) {
    if (!lastLocalFile.exists()) {
      LOGGER.info("last local file {} doesn't exist.", lastLocalFile.getAbsolutePath());
      return;
    }
    try (BufferedReader br = new BufferedReader(new FileReader(lastLocalFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        lastLocalFiles.add(line);
      }
    } catch (IOException e) {
      LOGGER.error(
          "Can not load last local file list from file {}", lastLocalFile.getAbsoluteFile(), e);
    }
  }

  @Override
  public void loadLogger(Set<String> deletedFiles, Set<String> newFiles) {
    if (!syncLogFile.exists()) {
      LOGGER.info("log file {} doesn't exist.", syncLogFile.getAbsolutePath());
      return;
    }
    try (BufferedReader br = new BufferedReader(new FileReader(syncLogFile))) {
      String line;
      int mode = 0;
      while ((line = br.readLine()) != null) {
        if (line.equals(SyncSenderLogger.SYNC_DELETED_FILE_NAME_START)) {
          mode = -1;
        } else if (line.equals(SyncSenderLogger.SYNC_TSFILE_START)) {
          mode = 1;
        } else {
          if (mode == -1) {
            deletedFiles.add(line);
          } else if (mode == 1) {
            newFiles.add(line);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error(
          "Can not load last local file list from file {}", lastLocalFile.getAbsoluteFile(), e);
    }
  }

  @Override
  public void updateLastLocalFile(Set<String> currentLocalFiles) throws IOException {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(currentLocalFile))) {
      for (String line : currentLocalFiles) {
        bw.write(line);
        bw.newLine();
      }
      bw.flush();
    } catch (IOException e) {
      LOGGER.error("Can not clear sync log {}", syncLogFile.getAbsoluteFile(), e);
    }
    lastLocalFile.delete();
    FileUtils.moveFile(currentLocalFile, lastLocalFile);
  }
}
