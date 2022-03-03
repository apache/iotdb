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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class TsFilePipeLogAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(TsFilePipeLogAnalyzer.class);

  private final String pipeDir;
  private final String pipeLogDir;

  private BlockingDeque<PipeData> pipeDataDeque;
  private long removeSerialNumber;

  public TsFilePipeLogAnalyzer(TsFilePipe pipe) {
    pipeDir = SyncPathUtil.getSenderPipeDir(pipe.getName(), pipe.getCreateTime());
    pipeLogDir = new File(pipeDir, SyncConstant.PIPE_LOG_DIR_NAME).getPath();
  }

  public BlockingDeque<PipeData> recover() {
    pipeDataDeque = new LinkedBlockingDeque<>();
    removeSerialNumber = Long.MIN_VALUE;

    if (!new File(pipeDir).exists()) {
      return pipeDataDeque;
    }

    deserializeRemoveSerialNumber();
    recoverHistoryData();
    recoverRealTimeData();

    return pipeDataDeque;
  }

  private void deserializeRemoveSerialNumber() {
    File removeSerialNumberLog = new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME);
    if (!removeSerialNumberLog.exists()) {
      return;
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(removeSerialNumberLog));
      long lineIndex = 0;
      String readLine;
      while ((readLine = br.readLine()) != null) {
        lineIndex += 1;
        try {
          removeSerialNumber = Long.parseLong(readLine);
        } catch (NumberFormatException e) {
          logger.warn(String.format("Can not parse Long in line %s, skip it.", lineIndex));
        }
      }
      br.close();
    } catch (IOException e) {
      logger.warn(
          String.format(
              "deserialize remove serial number error, remove serial number has been set to %d, because %s",
              removeSerialNumber, e));
    }
  }

  private void recoverHistoryData() {
    File historyPipeLog = new File(pipeLogDir, SyncConstant.HISTORY_PIPE_LOG_NAME);
    if (!historyPipeLog.exists()) {
      return;
    }

    if (removeSerialNumber < 0) {
      try {
        List<PipeData> historyPipeData = parseFile(historyPipeLog);
        for (PipeData data : historyPipeData)
          if (data.getSerialNumber() > removeSerialNumber) {
            pipeDataDeque.offer(data);
          }
      } catch (IOException e) {
        logger.error(
            String.format(
                "Can not parse history pipe log %s, because %s", historyPipeLog.getPath(), e));
      }
    } else {
      try {
        Files.delete(historyPipeLog.toPath());
      } catch (IOException e) {
        logger.warn(
            String.format(
                "Can not delete history pipe log %s, because %s", historyPipeLog.getPath(), e));
      }
    }
  }

  private void recoverRealTimeData() {
    File pipeLogDir = new File(this.pipeLogDir);
    if (!pipeLogDir.exists()) {
      return;
    }

    List<Long> startNumbers = new ArrayList<>();
    for (File file : pipeLogDir.listFiles())
      if (file.getName().endsWith(SyncConstant.PIPE_LOG_NAME_SUFFIX)) {
        startNumbers.add(SyncConstant.getSerialNumberFromPipeLogName(file.getName()));
      }
    if (startNumbers.size() != 0) {
      Collections.sort(startNumbers);
      for (Long startNumber : startNumbers) {
        File realTimePipeLog = new File(this.pipeLogDir, SyncConstant.getPipeLogName(startNumber));
        try {
          List<PipeData> realTimeData = parseFile(realTimePipeLog);
          for (PipeData data : realTimeData)
            if (data.getSerialNumber() > removeSerialNumber) {
              pipeDataDeque.offer(data);
            }
        } catch (IOException e) {
          logger.error(
              String.format(
                  "Can not parse real time pipe log %s, because %s", realTimePipeLog.getPath(), e));
        }
      }
    }
  }

  public boolean isCollectFinished() {
    return new File(pipeDir, SyncConstant.FINISH_COLLECT_LOCK_NAME).exists();
  }

  public static List<PipeData> parseFile(File file) throws IOException {
    List<PipeData> pipeData = new ArrayList<>();
    try (DataInputStream inputStream = new DataInputStream(new FileInputStream(file))) {
      while (true) {
        pipeData.add(PipeData.deserialize(inputStream));
      }
    } catch (EOFException e) {
      logger.info(String.format("Finish parsing pipeLog %s.", file.getPath()));
    } catch (IllegalPathException e) {
      logger.error(String.format("Parsing pipeLog %s error, because %s", file.getPath(), e));
      throw new IOException(e);
    }

    return pipeData;
  }
}
