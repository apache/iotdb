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
 *
 */
package org.apache.iotdb.db.newsync.sender.recovery;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipeData;
import org.apache.iotdb.db.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class TsFilePipeLog {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeLog.class);

  private final String pipeDir;
  private final String tsFileDir;
  private final String pipeLogDir;

  private DataOutputStream historyOutputStream;

  private BlockingDeque<Long> realTimePipeLogStartNumber;
  private DataOutputStream realTimeOutputStream;
  private long currentPipeLogSize;

  private BufferedWriter removeSerialNumberWriter;
  private long currentRemoveLogSize;

  public TsFilePipeLog(TsFilePipe tsFilePipe) {
    pipeDir = SenderConf.getPipeDir(tsFilePipe);
    tsFileDir = new File(pipeDir, SenderConf.tsFileDirName).getPath();
    pipeLogDir = new File(pipeDir, SenderConf.pipeLogDirName).getPath();
  }

  /** make hard link for tsfile * */
  public File addHistoryTsFile(File tsFile, long modsOffset) throws IOException {
    File mods = new File(tsFile.getPath() + ModificationFile.FILE_SUFFIX);

    if (mods.exists()) {
      File modsHardLink = createHardLink(mods);
      if (modsOffset != 0L) {
        serializeModsOffset(
            new File(modsHardLink.getPath() + SenderConf.modsOffsetFileSuffix), modsOffset);
      }
    } else if (modsOffset != 0L) {
      logger.warn(
          String.format(
              "Can not find %s mods to create hard link. The mods offset is %d.",
              mods.getPath(), modsOffset));
    }
    addTsFileResource(tsFile);
    return addRealTimeTsFile(tsFile);
  }

  private void serializeModsOffset(File modsOffsetFile, long modsOffset) {
    try {
      createFile(modsOffsetFile);
      BufferedWriter bw = new BufferedWriter(new FileWriter(modsOffsetFile));
      bw.write(String.valueOf(modsOffset));
      bw.flush();
      bw.close();
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Serialize mods offset in %s error. The mods offset is %d",
              modsOffsetFile.getPath(), modsOffset));
    }
  }

  public File addRealTimeTsFile(File tsFile) throws IOException {
    return createHardLink(tsFile);
  }

  public void addTsFileResource(File tsFile) throws IOException {
    File tsFileResource = new File(tsFile.getPath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      createHardLink(tsFileResource);
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Record tsfile resource %s on disk error, make a empty to close it.",
              tsFileResource.getPath()));
      createFile(new File(tsFileDir, getRelativeFilePath(tsFileResource)));
    }
  }

  private File createHardLink(File file) throws IOException {
    File link = new File(tsFileDir, getRelativeFilePath(file));
    if (!link.getParentFile().exists()) {
      link.getParentFile().mkdirs();
    }

    Path sourcePath = FileSystems.getDefault().getPath(file.getAbsolutePath());
    Path linkPath = FileSystems.getDefault().getPath(link.getAbsolutePath());
    Files.createLink(linkPath, sourcePath);
    return link;
  }

  private String getRelativeFilePath(File file) {
    StringBuilder builder = new StringBuilder(file.getName());
    while (!file.getName().equals(IoTDBConstant.SEQUENCE_FLODER_NAME)
        && !file.getName().equals(IoTDBConstant.UNSEQUENCE_FLODER_NAME)) {
      file = file.getParentFile();
      builder = new StringBuilder(file.getName()).append(File.separator).append(builder);
    }
    return builder.toString();
  }

  /** add pipe log data */
  public void addHistoryPipeData(TsFilePipeData pipeData) throws IOException {
    getHistoryOutputStream();
    pipeData.serialize(historyOutputStream);
  }

  private void getHistoryOutputStream() throws IOException {
    if (historyOutputStream != null) {
      return;
    }

    // recover history pipe log
    File logDir = new File(pipeLogDir);
    logDir.mkdirs();
    File historyPipeLog = new File(pipeLogDir, SenderConf.historyPipeLogName);
    createFile(historyPipeLog);
    historyOutputStream = new DataOutputStream(new FileOutputStream(historyPipeLog, true));
  }

  public synchronized void addRealTimePipeData(TsFilePipeData pipeData) throws IOException {
    getRealTimeOutputStream(pipeData.getSerialNumber());
    currentPipeLogSize += pipeData.serialize(realTimeOutputStream);
  }

  private void getRealTimeOutputStream(long serialNumber) throws IOException {
    if (realTimeOutputStream == null) {
      // recover real time pipe log
      realTimePipeLogStartNumber = new LinkedBlockingDeque<>();
      File logDir = new File(pipeLogDir);
      List<Long> startNumbers = new ArrayList<>();

      logDir.mkdirs();
      for (File file : logDir.listFiles())
        if (file.getName().endsWith(SenderConf.realTimePipeLogNameSuffix)) {
          startNumbers.add(SenderConf.getSerialNumberFromPipeLogName(file.getName()));
        }
      if (startNumbers.size() != 0) {
        Collections.sort(startNumbers);
        for (Long startTime : startNumbers) {
          realTimePipeLogStartNumber.offer(startTime);
        }
        File writingPipeLog =
            new File(
                pipeLogDir,
                SenderConf.getRealTimePipeLogName(startNumbers.get(startNumbers.size() - 1)));
        realTimeOutputStream = new DataOutputStream(new FileOutputStream(writingPipeLog, true));
        currentPipeLogSize = writingPipeLog.length();
      } else {
        moveToNextPipeLog(serialNumber);
      }
    }

    if (currentPipeLogSize > SenderConf.defaultPipeLogSizeInByte) {
      moveToNextPipeLog(serialNumber);
    }
  }

  private void moveToNextPipeLog(long startSerialNumber) throws IOException {
    if (realTimeOutputStream != null) {
      realTimeOutputStream.close();
    }
    File newPipeLog = new File(pipeLogDir, SenderConf.getRealTimePipeLogName(startSerialNumber));
    createFile(newPipeLog);

    realTimeOutputStream = new DataOutputStream(new FileOutputStream(newPipeLog));
    realTimePipeLogStartNumber.offer(startSerialNumber);
    currentPipeLogSize = 0;
  }

  /** remove pipe log data */
  public void removePipeData(long serialNumber) throws IOException {
    serializeRemoveSerialNumber(serialNumber);

    if (serialNumber >= 0) {
      if (historyOutputStream != null) {
        removeHistoryPipeLog();
      }
      if (realTimePipeLogStartNumber.size() < 2) {
        long pipeLogStartNumber;
        while (true) {
          pipeLogStartNumber = realTimePipeLogStartNumber.poll();
          if (!realTimePipeLogStartNumber.isEmpty()
              && realTimePipeLogStartNumber.peek() < serialNumber) {
            removeRealTimePipeLog(pipeLogStartNumber);
          } else {
            break;
          }
        }
        realTimePipeLogStartNumber.addFirst(pipeLogStartNumber);
      }
    }
  }

  private void removeHistoryPipeLog() throws IOException {
    historyOutputStream.close();
    historyOutputStream = null;
    File historyPipeLog = new File(pipeLogDir, SenderConf.historyPipeLogName);
    try {
      Files.delete(historyPipeLog.toPath());
    } catch (NoSuchFileException e) {
      logger.warn(
          String.format("delete history pipe log in %s error, %s", historyPipeLog.getPath(), e));
    }
  }

  private void removeRealTimePipeLog(long serialNumber) throws IOException {
    File realTimePipeLog = new File(pipeLogDir, SenderConf.getRealTimePipeLogName(serialNumber));
    removeTsFile(realTimePipeLog);
    try {
      Files.delete(realTimePipeLog.toPath());
    } catch (NoSuchFileException e) {
      logger.warn(
          String.format("delete real time pipe log in %s error, %s", realTimePipeLog.getPath(), e));
    }
  }

  private void removeTsFile(File realTimePipeLog) {
    try {
      List<TsFilePipeData> pipeData = TsFilePipeLogAnalyzer.parseFile(realTimePipeLog);
      List<File> tsFiles;
      for (TsFilePipeData data : pipeData)
        if (data.isTsFile()) {
          tsFiles = data.getTsFiles();
          for (File file : tsFiles) {
            Files.deleteIfExists(file.toPath());
          }
        }
    } catch (IOException e) {
      logger.warn(
          String.format(
              "Can not parse pipe log %s, the tsfiles in this pipe log will not be deleted, because %s",
              realTimePipeLog.getPath(), e));
    }
  }

  private void serializeRemoveSerialNumber(long serialNumber) throws IOException {
    if (removeSerialNumberWriter == null) {
      removeSerialNumberWriter =
          new BufferedWriter(
              new FileWriter(new File(pipeLogDir, SenderConf.removeSerialNumberLogName)));
      currentRemoveLogSize = 0;
    }
    removeSerialNumberWriter.write(String.valueOf(serialNumber));
    removeSerialNumberWriter.newLine();
    removeSerialNumberWriter.flush();
    currentRemoveLogSize += Long.BYTES;
    if (currentRemoveLogSize >= SenderConf.defaultPipeLogSizeInByte) {
      removeSerialNumberWriter.close();
      removeSerialNumberWriter = null;
    }
  }

  public void finishCollect() {
    try {
      if (createFile(new File(pipeDir, SenderConf.pipeCollectFinishLockName))) {
        logger.info(String.format("Create finish collecting Lock file in %s.", pipeDir));
      }
    } catch (IOException e) {
      logger.warn(String.format("Can not make lock file in %s, because %s", pipeDir, e));
    }
  }

  private boolean createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    return file.createNewFile();
  }

  public void clear() throws IOException {
    if (historyOutputStream != null) {
      removeHistoryPipeLog();
    }
    if (realTimeOutputStream != null) {
      realTimeOutputStream.close();
      realTimeOutputStream = null;
    }
    if (removeSerialNumberWriter != null) {
      removeSerialNumberWriter.close();
      removeSerialNumberWriter = null;
    }

    realTimePipeLogStartNumber = null;
    File pipeDir = new File(this.pipeDir);
    if (pipeDir.exists()) {
      FileUtils.deleteDirectory(pipeDir);
    }
  }
}
