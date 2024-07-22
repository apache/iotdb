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

package org.apache.iotdb.db.storageengine.dataregion.wal.buffer;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

public abstract class AbstractWALBuffer implements IWALBuffer {
  private static final Logger logger = LoggerFactory.getLogger(AbstractWALBuffer.class);

  // WALNode identifier of this buffer
  protected final String identifier;
  // directory to store .wal files
  protected final String logDirectory;
  // disk usage of this node‘s wal files
  protected long diskUsage = 0;
  // number of this node‘s wal files
  protected long fileNum = 0;
  // current wal file version id
  protected volatile long currentWALFileVersion;
  // current search index
  protected volatile long currentSearchIndex;

  // current wal file log writer
  // it's safe to use volatile here to make this reference thread-safe.
  @SuppressWarnings("squid:S3077")
  protected volatile WALWriter currentWALFileWriter;

  protected AbstractWALBuffer(
      String identifier, String logDirectory, long startFileVersion, long startSearchIndex)
      throws IOException {
    this.identifier = identifier;
    this.logDirectory = logDirectory;
    File logDirFile = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!logDirFile.exists() && logDirFile.mkdirs()) {
      logger.info("Create folder {} for wal node-{}'s buffer.", logDirectory, identifier);
    }
    // update info
    File[] walFiles = WALFileUtils.listAllWALFiles(logDirFile);
    addDiskUsage(Arrays.stream(walFiles).mapToLong(File::length).sum());
    addFileNum(walFiles.length);
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
  public long getCurrentWALOriginalFileSize() {
    return currentWALFileWriter.originalSize();
  }

  /**
   * Notice: only called by syncBufferThread and old log writer will be closed by this function.
   *
   * @return last wal file
   * @throws IOException If failing to close or open the log writer
   */
  protected File rollLogWriter(long searchIndex, WALFileStatus fileStatus) throws IOException {
    // close file
    currentWALFileWriter.close();
    addDiskUsage(currentWALFileWriter.size());
    addFileNum(1);
    File lastFile = currentWALFileWriter.getLogFile();
    String lastName = lastFile.getName();
    if (WALFileUtils.parseStatusCode(lastName) != fileStatus) {
      String targetName =
          WALFileUtils.getLogFileName(
              WALFileUtils.parseVersionId(lastName),
              WALFileUtils.parseStartSearchIndex(lastName),
              fileStatus);
      File targetFile = SystemFileFactory.INSTANCE.getFile(logDirectory, targetName);
      Files.move(
          lastFile.toPath(),
          targetFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
      lastFile = targetFile;
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
    return lastFile;
  }

  public long getDiskUsage() {
    return diskUsage;
  }

  public void addDiskUsage(long size) {
    diskUsage += size;
    WALManager.getInstance().addTotalDiskUsage(size);
  }

  public void subtractDiskUsage(long size) {
    diskUsage -= size;
    WALManager.getInstance().subtractTotalDiskUsage(size);
  }

  public long getFileNum() {
    return fileNum;
  }

  public void addFileNum(long num) {
    fileNum += num;
    WALManager.getInstance().addTotalFileNum(num);
  }

  public void subtractFileNum(long num) {
    fileNum -= num;
    WALManager.getInstance().subtractTotalFileNum(num);
  }

  @Override
  public long getCurrentSearchIndex() {
    return currentSearchIndex;
  }
}
