/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InplaceCompactionRecoverTask extends InplaceCompactionTask {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractCrossSpaceCompactionRecoverTask.class);
  private File logFile;

  public InplaceCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileResourceList selectedSeqTsFileResourceList,
      TsFileResourceList selectedUnSeqTsFileResourceList,
      File logFile,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartitionId,
        storageGroupDir,
        null,
        null,
        null,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        currentTaskNum);
    this.logFile = logFile;
  }

  @Override
  public void doCompaction() throws IOException {
    taskName = fullStorageGroupName + "-" + System.currentTimeMillis();
    logger.info(
        "{} a CleanLastCrossSpaceCompactionTask {} starts...", fullStorageGroupName, taskName);
    cleanLastCrossSpaceCompactionInfo(logFile);
    for (TsFileResource seqFile : selectedSeqTsFileResourceList) {
      ModificationFile.getCompactionMods(seqFile).remove();
    }
  }

  public void cleanLastCrossSpaceCompactionInfo(File logFile) throws IOException {
    if (!logFile.exists()) {
      logger.info("{} no merge.log, cross space compaction clean ends.", taskName);
      return;
    }
    long startTime = System.currentTimeMillis();
    if (mergeLogCrashed(logFile)) {
      reuseLastTargetFiles(logFile);
    }
    cleanUp();

    if (logger.isInfoEnabled()) {
      logger.info(
          "{} cross space compaction clean ends after {}ms.",
          taskName,
          (System.currentTimeMillis() - startTime));
    }
  }

  private boolean mergeLogCrashed(File logFile) throws IOException {
    FileChannel fileChannel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ);
    long totalSize = fileChannel.size();
    ByteBuffer magicStringBytes =
        ByteBuffer.allocate(InplaceCompactionLogger.MAGIC_STRING.getBytes().length);
    fileChannel.read(
        magicStringBytes, totalSize - InplaceCompactionLogger.MAGIC_STRING.getBytes().length);
    magicStringBytes.flip();
    if (!InplaceCompactionLogger.MAGIC_STRING.equals(new String(magicStringBytes.array()))) {
      return false;
    }
    magicStringBytes.clear();
    fileChannel.read(magicStringBytes, 0);
    magicStringBytes.flip();
    fileChannel.close();
    return InplaceCompactionLogger.MAGIC_STRING.equals(new String(magicStringBytes.array()));
  }

  void reuseLastTargetFiles(File logFile) throws IOException {
    FileReader fr = new FileReader(logFile);
    BufferedReader br = new BufferedReader(fr);
    String line;
    int isTargetFile = 0;
    List<File> mergeTmpFile = new ArrayList<>();
    while ((line = br.readLine()) != null) {
      switch (line) {
        case InplaceCompactionLogger.STR_TARGET_FILES:
          isTargetFile = 1;
          break;
        case InplaceCompactionLogger.STR_SEQ_FILES:
        case InplaceCompactionLogger.STR_UNSEQ_FILES:
          isTargetFile = 2;
          break;
        default:
          progress(isTargetFile, line, mergeTmpFile, logFile);
          break;
      }
    }
    br.close();
    fr.close();
  }

  void moveTargetFile(File file, String logFilePath) throws IOException {
    boolean result;
    if (file.exists()) {
      // if the file that ends with ".merge", we need to rename it
      result = file.renameTo(new File(file.getAbsolutePath().replace(MERGE_SUFFIX, "")));
    } else {
      // check whether the file has been changed.
      // if the file before and after the change does not exist, an exception exists
      result = new File(file.getAbsolutePath().replace(MERGE_SUFFIX, "")).exists();
    }
    if (!result) {
      // if one of the files that ends with ".merge" is abnormal, it cannot be restored.
      // in the method of cleanup(), all temporary files would be deleted.
      throw new IOException(
          String.format(
              "The last target file '%s' cannot be reused in %s .",
              file.getAbsolutePath(), logFilePath));
    }
  }

  void deleteOldFile(File oldFile) throws IOException {
    if (!oldFile.delete()) {
      throw new IOException(
          String.format(
              "Failed to delete old files from last merge result:%s", oldFile.getAbsolutePath()));
    }
  }

  void progress(int isTargetFile, String fileName, List<File> mergeTmpFile, File logFile)
      throws IOException {
    TsFileIdentifier tsFileIdentifier;
    if (isTargetFile == 1) {
      tsFileIdentifier = TsFileIdentifier.getFileIdentifierFromInfoString(fileName);
      // Get all ".merge" files
      mergeTmpFile.add(tsFileIdentifier.getFileFromDataDirs());
    } else if (isTargetFile == 2) {
      // move ".merge" to ".tsfile"
      for (File file : mergeTmpFile) {
        moveTargetFile(file, logFile.getAbsolutePath());
      }
      mergeTmpFile.clear();
      tsFileIdentifier = TsFileIdentifier.getFileIdentifierFromInfoString(fileName);
      // clear seqFiles and unseqFiles that have been merged
      deleteOldFile(tsFileIdentifier.getFileFromDataDirs());
    }
    // The first line is the magic string, just skip
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof InplaceCompactionRecoverTask) {
      return logFile.equals(((InplaceCompactionRecoverTask) other).logFile);
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return logFile.exists();
  }
}
