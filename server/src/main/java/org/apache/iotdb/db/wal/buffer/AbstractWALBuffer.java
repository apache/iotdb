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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.db.wal.utils.WALFileStatus;
import org.apache.iotdb.db.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public abstract class AbstractWALBuffer implements IWALBuffer {
  private static final Logger logger = LoggerFactory.getLogger(AbstractWALBuffer.class);

  /** WALNode identifier of this buffer */
  protected final String identifier;
  /** directory to store .wal files */
  protected final String logDirectory;
  /** current wal file version id */
  protected volatile long currentWALFileVersion;
  /** current search index */
  protected volatile long currentSearchIndex;
  /** current wal file log writer */
  protected volatile WALWriter currentWALFileWriter;

  protected AbstractWALBuffer(
      String identifier, String logDirectory, long startFileVersion, long startSearchIndex)
      throws FileNotFoundException {
    this.identifier = identifier;
    this.logDirectory = logDirectory;
    File logDirFile = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!logDirFile.exists() && logDirFile.mkdirs()) {
      logger.info("Create folder {} for wal node-{}'s buffer.", logDirectory, identifier);
    }
    currentSearchIndex = startSearchIndex;
    currentWALFileWriter =
        new WALWriter(
            SystemFileFactory.INSTANCE.getFile(
                logDirectory,
                WALFileUtils.getLogFileName(
                    startFileVersion, currentSearchIndex, WALFileStatus.CONTAINS_SEARCH_INDEX)));
    currentWALFileVersion = startFileVersion;
  }

  @Override
  public long getCurrentWALFileVersion() {
    return currentWALFileVersion;
  }

  @Override
  public long getCurrentWALFileSize() {
    return currentWALFileWriter.size();
  }

  /** Notice: only called by syncBufferThread and old log writer will be closed by this function. */
  protected void rollLogWriter(long searchIndex, WALFileStatus fileStatus) throws IOException {
    // close file
    File currentFile = currentWALFileWriter.getLogFile();
    String currentName = currentFile.getName();
    currentWALFileWriter.close();
    WALManager.getInstance().addTotalDiskUsage(currentWALFileWriter.size());
    WALManager.getInstance().addTotalFileNum(1);
    if (WALFileUtils.parseStatusCode(currentName) != fileStatus) {
      String targetName =
          WALFileUtils.getLogFileName(
              WALFileUtils.parseVersionId(currentName),
              WALFileUtils.parseStartSearchIndex(currentName),
              fileStatus);
      if (!currentFile.renameTo(SystemFileFactory.INSTANCE.getFile(logDirectory, targetName))) {
        logger.error("Fail to rename file {} to {}", currentName, targetName);
      }
    }
    // roll file
    long nextFileVersion = currentWALFileVersion + 1;
    File nextLogFile =
        SystemFileFactory.INSTANCE.getFile(
            logDirectory,
            WALFileUtils.getLogFileName(
                nextFileVersion, searchIndex, WALFileStatus.CONTAINS_SEARCH_INDEX));
    currentWALFileWriter = new WALWriter(nextLogFile);
    currentWALFileVersion = nextFileVersion;
    logger.debug("Open new wal file {} for wal node-{}'s buffer.", nextLogFile, identifier);
  }

  @Override
  public long getCurrentSearchIndex() {
    return currentSearchIndex;
  }
}
