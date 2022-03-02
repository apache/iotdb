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
package org.apache.iotdb.db.newsync.receiver.recovery;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ReceiverLogAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverLogAnalyzer.class);
  private static boolean pipeServerEnable =
      false; // record recovery result of receiver server status
  private static Map<String, Map<String, PipeInfo>> pipeInfoMap =
      new HashMap<>(); // record recovery result of receiver server status

  public static void scan() throws StartupException {
    logger.info("Start to recover all sync state for sync receiver.");
    pipeInfoMap = new HashMap<>();
    pipeServerEnable = false;
    File logFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_LOG_NAME);
    try (BufferedReader loadReader = new BufferedReader(new FileReader(logFile))) {
      String line;
      int lineNum = 0;
      while ((line = loadReader.readLine()) != null) {
        lineNum++;
        try {
          analyzeLog(line);
        } catch (Exception e) {
          logger.error("Receiver log recovery error: log file parse error at line " + lineNum);
          logger.error(e.getMessage());
          throw new StartupException(
              ServiceType.RECEIVER_SERVICE.getName(), "log file recover error at line " + lineNum);
        }
      }
    } catch (IOException e) {
      logger.info("Receiver log file not found");
    }
  }

  public static boolean isPipeServerEnable() {
    return pipeServerEnable;
  }

  public static Map<String, Map<String, PipeInfo>> getPipeInfoMap() {
    return pipeInfoMap;
  }

  /**
   * parse log line and load result
   *
   * @param logLine log line
   */
  private static void analyzeLog(String logLine) {
    if (logLine.equals("on")) {
      pipeServerEnable = true;
    } else if (logLine.equals("off")) {
      pipeServerEnable = false;
    } else {
      String[] items = logLine.split(",");
      String pipeName = items[0];
      String remoteIp = items[1];
      PipeStatus status = PipeStatus.valueOf(items[2]);
      if (status == PipeStatus.RUNNING) {
        if (!pipeInfoMap.containsKey(pipeName)) {
          pipeInfoMap.put(pipeName, new HashMap<>());
        }
        if (items.length == 4) {
          // create
          pipeInfoMap
              .get(pipeName)
              .put(remoteIp, new PipeInfo(pipeName, remoteIp, status, Long.parseLong(items[3])));
        } else {
          pipeInfoMap.get(pipeName).get(remoteIp).setStatus(status);
        }
      } else {
        pipeInfoMap.get(pipeName).get(remoteIp).setStatus(status);
      }
    }
  }
}
