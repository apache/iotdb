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

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.newsync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReceiverLogAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverLogAnalyzer.class);
  // record recovery result of receiver server status
  private static boolean pipeServerEnable = false;
  private static Map<String, Map<String, PipeInfo>> pipeInfoMap = new ConcurrentHashMap<>();
  private static Map<String, List<PipeMessage>> pipeMessageMap = new ConcurrentHashMap<>();

  public static void scan() throws StartupException {
    logger.info("Start to recover all sync state for sync receiver.");
    pipeInfoMap = new ConcurrentHashMap<>();
    pipeMessageMap = new ConcurrentHashMap<>();
    pipeServerEnable = false;
    File serviceLogFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_LOG_NAME);
    try (BufferedReader loadReader = new BufferedReader(new FileReader(serviceLogFile))) {
      String line;
      int lineNum = 0;
      while ((line = loadReader.readLine()) != null) {
        lineNum++;
        try {
          analyzeServiceLog(line);
        } catch (Exception e) {
          logger.error(
              "Receiver service log recovery error: log file parse error at line " + lineNum);
          logger.error(e.getMessage());
          throw new StartupException(
              ServiceType.RECEIVER_SERVICE.getName(),
              "Receiver service log file recover error at line " + lineNum);
        }
      }
    } catch (IOException e) {
      logger.info("Receiver service log file not found");
    }
    File msgLogFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_MSG_LOG_NAME);
    try (BufferedReader loadReader = new BufferedReader(new FileReader(msgLogFile))) {
      String line;
      int lineNum = 0;
      while ((line = loadReader.readLine()) != null) {
        lineNum++;
        try {
          analyzeMsgLog(line);
        } catch (Exception e) {
          logger.error("Receiver msg log recovery error: log file parse error at line " + lineNum);
          logger.error(e.getMessage());
          throw new StartupException(
              ServiceType.RECEIVER_SERVICE.getName(),
              "Receiver msg log file recover error at line " + lineNum);
        }
      }
    } catch (IOException e) {
      logger.info("Receiver msg log file not found");
    }
  }

  public static boolean isPipeServerEnable() {
    return pipeServerEnable;
  }

  public static Map<String, Map<String, PipeInfo>> getPipeInfoMap() {
    return pipeInfoMap;
  }

  public static Map<String, List<PipeMessage>> getPipeMessageMap() {
    return pipeMessageMap;
  }

  /**
   * parse service log line and load result
   *
   * @param logLine log line
   */
  private static void analyzeServiceLog(String logLine) {
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

  /**
   * parse message log line and load result
   *
   * @param logLine log line
   */
  private static void analyzeMsgLog(String logLine) {
    String[] items = logLine.split(",");
    String pipeIdentifier = items[0];
    if (items.length == 3) {
      // write
      PipeMessage message = new PipeMessage(PipeMessage.MsgType.valueOf(items[1]), items[2]);
      pipeMessageMap.computeIfAbsent(pipeIdentifier, i -> new ArrayList<>()).add(message);
    } else {
      // read
      pipeMessageMap.remove(pipeIdentifier);
    }
  }
}
