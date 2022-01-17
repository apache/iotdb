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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TsFilePipeLog {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeLog.class);

  private final String pipeDir;
  private final String tsFileDir;
  private final String pipeLogDir;

  private DataOutputStream historyOutputStream;

  private BlockingQueue<Long> realTimePipeLogStartNumber;
  private DataOutputStream realTimeOutputStream;
  private long currentPipeLogSize;

  public TsFilePipeLog(TsFilePipe tsFilePipe) {
    pipeDir = SenderConf.getPipeDir(tsFilePipe);
    tsFileDir = new File(pipeDir, SenderConf.tsFileDirName).getPath();
    pipeLogDir = new File(pipeDir, SenderConf.pipeLogDirName).getPath();
  }

  /** make hard link for tsfile * */
  public File addHistoryTsFile(File tsFile) throws IOException {
    File mods = new File(tsFile.getPath() + ModificationFile.FILE_SUFFIX);

    if (mods.exists()) {
      createHardLink(mods);
    }
    return addRealTimeTsFile(tsFile);
  }

  public File addRealTimeTsFile(File tsFile) throws IOException {
    File link = createHardLink(tsFile);
    File resource = new File(tsFile.getPath() + TsFileResource.RESOURCE_SUFFIX);

    if (resource.exists()) {
      createHardLink(resource);
    } else {
      logger.warn(String.format("Can not find resource %s.", resource.getPath()));
    }
    return link;
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

  /** tsfile pipe data log */
  public void addHistoryTsFilePipeData(TsFilePipeData pipeData) throws IOException {
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
    historyOutputStream = new DataOutputStream(new FileOutputStream(historyPipeLog));
  }

  public void addRealTimeTsFilePipeData(TsFilePipeData pipeData) {
    getRealTimeOutputStream();
  }

  private void getRealTimeOutputStream() throws IOException {
    if (realTimeOutputStream != null) {
      return;
    }

    // recover real time pipe log
    realTimePipeLogStartNumber = new LinkedBlockingDeque<>();
    File logDir = new File(pipeLogDir);
    List<Long> startTimes = new ArrayList<>();

    logDir.mkdirs();
    for (File file : logDir.listFiles())
      if (file.getName().endsWith(SenderConf.realTimePipeLogNameSuffix)) {
        startTimes.add(SenderConf.getSerialNumberFromPipeLogName(file.getName()));
      }
    if (startTimes.size() != 0) {
      Collections.sort(startTimes);
      for (Long startTime : startTimes) {
        realTimePipeLogStartNumber.offer(startTime);
      }
      File writingPipeLog =
          new File(SenderConf.getRealTimePipeLogName(startTimes.get(startTimes.size() - 1)));
      realTimeOutputStream = new DataOutputStream(new FileOutputStream(writingPipeLog));
      currentPipeLogSize = writingPipeLog.length();
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

  public void clear() {}
}
