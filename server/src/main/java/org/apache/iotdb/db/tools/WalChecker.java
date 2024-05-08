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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.buffer.WALEntryType;
import org.apache.iotdb.db.wal.exception.WALException;
import org.apache.iotdb.db.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** WalChecker verifies that whether all insert ahead logs in the WAL folder are recognizable. */
public class WalChecker {

  private static final Logger logger = LoggerFactory.getLogger(WalChecker.class);

  /** the root dir of wals, which should have wal directories of databases as its children. */
  private String walFolder;

  public WalChecker(String walFolder) {
    this.walFolder = walFolder;
  }

  /**
   * check the root wal dir and find the damaged files
   *
   * @return a list of damaged files.
   * @throws WALException if the root wal dir does not exist.
   */
  public List<File> doCheck() throws WALException {
    File walFolderFile = SystemFileFactory.INSTANCE.getFile(walFolder);
    logger.info("Checking folder: {}", walFolderFile.getAbsolutePath());
    if (!walFolderFile.exists() || !walFolderFile.isDirectory()) {
      throw new WALException(walFolder);
    }

    File[] walNodeFolders = walFolderFile.listFiles(File::isDirectory);
    if (walNodeFolders == null || walNodeFolders.length == 0) {
      logger.info("No sub-directories under the given directory, check ends");
      return Collections.emptyList();
    }

    List<File> failedFiles = new ArrayList<>();
    for (int dirIndex = 0; dirIndex < walNodeFolders.length; dirIndex++) {
      File walNodeFolder = walNodeFolders[dirIndex];
      logger.info("Checking the No.{} directory {}", dirIndex, walNodeFolder.getName());
      File[] walFiles = WALFileUtils.listAllWALFiles(walNodeFolder);
      if (walFiles == null) {
        continue;
      }
      for (File walFile : walFiles) {
        if (!checkFile(walFile)) {
          failedFiles.add(walFile);
        }
      }
    }
    return failedFiles;
  }

  private boolean checkFile(File walFile) {
    try (DataInputStream logStream =
        new DataInputStream(new BufferedInputStream(new FileInputStream(walFile)))) {
      while (logStream.available() > 0) {
        WALEntry walEntry = WALEntry.deserialize(logStream);
        if (walEntry.getType() == WALEntryType.WAL_FILE_INFO_END_MARKER) {
          return true;
        }
      }
    } catch (FileNotFoundException e) {
      logger.debug("Wal file doesn't exist, skipping");
      return true;
    } catch (IOException | IllegalPathException e) {
      logger.error("{} fails the check because", walFile, e);
      return false;
    }
    return true;
  }

  // a temporary method which should be in the integrated self-check module in the future
  public static void report(List<File> failedFiles) {
    if (failedFiles.isEmpty()) {
      logger.info("Check finished. There is no damaged file");
    } else {
      logger.error("There are {} failed files. They are {}", failedFiles.size(), failedFiles);
    }
  }

  /** @param args walRootDirectory */
  public static void main(String[] args) throws WALException {
    if (args.length < 1) {
      logger.error("No enough args: require the walRootDirectory");
      return;
    }

    WalChecker checker = new WalChecker(args[0]);
    List<File> files = checker.doCheck();
    report(files);
  }
}
