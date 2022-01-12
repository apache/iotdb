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
package org.apache.iotdb.db.newsync.receiver.recover;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ReceiverLogAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(ReceiverLogAnalyzer.class);

  public static Map<String, Map<String, PipeInfo>> recover() {
    logger.info("Start to recover all sync state for sync receiver.");
    Map<String, Map<String, PipeInfo>> res = new HashMap<>();
    String syncSystemDir = IoTDBDescriptor.getInstance().getConfig().getSyncDir();
    File logFile = new File(syncSystemDir, SyncConstant.RECEIVER_LOG_NAME);
    try (BufferedReader loadReader = new BufferedReader(new FileReader(logFile))) {
      String line;
      int lineNum = 0;
      while ((line = loadReader.readLine()) != null) {
        lineNum++;
        try {
          analyzeLog(line, res);
        } catch (Exception e) {
          logger.error("Receiver log recovery error: log file parse error at line " + lineNum);
          return null;
        }
      }
    } catch (IOException e) {
      logger.error("Receiver log recovery error: log file not found");
      return null;
    }
    return res;
  }

  /**
   * parse log line and load result into map
   *
   * @param logLine log line
   * @param map map
   */
  private static void analyzeLog(String logLine, Map<String, Map<String, PipeInfo>> map) {
    String[] items = logLine.split(",");
    String pipeName = items[0];
    String remoteIp = items[1];
    PipeStatus status = PipeStatus.valueOf(items[2]);
    if (status == PipeStatus.RUNNING) {
      if (!map.containsKey(pipeName)) {
        map.put(pipeName, new HashMap<>());
      }
      if (items.length == 4) {
        // create
        map.get(pipeName)
            .put(remoteIp, new PipeInfo(pipeName, remoteIp, status, Long.parseLong(items[3])));
      } else {
        map.get(pipeName).get(remoteIp).setStatus(status);
      }
    } else {
      map.get(pipeName).get(remoteIp).setStatus(status);
    }
  }
}
