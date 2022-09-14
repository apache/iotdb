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
package org.apache.iotdb.db.sync.common.persistence;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.sync.sender.pipe.PipeInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncLogReader {
  private static final Logger logger = LoggerFactory.getLogger(SyncLogReader.class);
  // <pipeFolderName, pipeMsg>
  private Map<String, List<PipeMessage>> pipeMessageMap = new ConcurrentHashMap<>();
  // <pipeSinkName, PipeSink>
  private Map<String, PipeSink> pipeSinks = new ConcurrentHashMap<>();
  private List<PipeInfo> pipes = new ArrayList<>();
  private PipeInfo runningPipe;

  public void recover() throws StartupException {
    logger.info("Start to recover all sync state for sync.");
    this.pipeMessageMap = new ConcurrentHashMap<>();
    this.pipeSinks = new ConcurrentHashMap<>();
    this.pipes = new ArrayList<>();
    File serviceLogFile = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_LOG_NAME);
    try (BufferedReader br = new BufferedReader(new FileReader(serviceLogFile))) {
      recoverPipe(br);
    } catch (IOException e) {
      logger.warn("Sync service log file not found");
    }
    File msgLogFile = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_MSG_LOG_NAME);
    try (BufferedReader loadReader = new BufferedReader(new FileReader(msgLogFile))) {
      String line;
      int lineNum = 0;
      while ((line = loadReader.readLine()) != null) {
        lineNum++;
        try {
          analyzeMsgLog(line);
        } catch (Exception e) {
          logger.error("Sync msg log recovery error: log file parse error at line " + lineNum);
          logger.error(e.getMessage());
          throw new StartupException(
              ServiceType.SYNC_SERVICE.getName(),
              "Sync msg log file recover error at line " + lineNum);
        }
      }
    } catch (IOException e) {
      logger.info("Sync msg log file not found");
    }
  }

  public Map<String, List<PipeMessage>> getPipeMessageMap() {
    return pipeMessageMap;
  }

  public Map<String, PipeSink> getAllPipeSinks() {
    return pipeSinks;
  }

  public List<PipeInfo> getAllPipeInfos() {
    return pipes;
  }

  public PipeInfo getRunningPipeInfo() {
    return runningPipe;
  }

  private void recoverPipe(BufferedReader br) throws IOException {
    int lineNumber =
        0; // line index shown in sender log starts from 1, so lineNumber starts from 0.
    String readLine = "";
    String[] parseStrings;

    try {
      while ((readLine = br.readLine()) != null) {
        lineNumber += 1;
        parseStrings = readLine.split(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);

        StatementType type = StatementType.valueOf(parseStrings[0]);

        switch (type) {
          case CREATE_PIPESINK:
            readLine = br.readLine();
            lineNumber += 1;
            CreatePipeSinkStatement createPipeSinkStatement =
                CreatePipeSinkStatement.parseString(readLine);
            pipeSinks.put(
                createPipeSinkStatement.getPipeSinkName(),
                SyncPipeUtil.parseCreatePipeSinkStatement(createPipeSinkStatement));
            break;
          case DROP_PIPESINK:
            pipeSinks.remove(parseStrings[1]);
            break;
          case CREATE_PIPE:
            readLine = br.readLine();
            lineNumber += 1;
            CreatePipeStatement createPipeStatement = CreatePipeStatement.parseString(readLine);
            runningPipe =
                SyncPipeUtil.parseCreatePipePlanAsPipeInfo(
                    createPipeStatement,
                    pipeSinks.get(createPipeStatement.getPipeSinkName()),
                    Long.parseLong(parseStrings[1]));
            pipes.add(runningPipe);
            break;
          case STOP_PIPE:
            runningPipe.stop();
            break;
          case START_PIPE:
            runningPipe.start();
            break;
          case DROP_PIPE:
            runningPipe.drop();
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("Can not recognize type %s.", type.name()));
        }
      }
    } catch (Exception e) {
      throw new IOException(
          String.format("Recover error in line %d : %s, because %s", lineNumber, readLine, e));
    }
  }

  /**
   * parse message log line and load result
   *
   * @param logLine log line
   */
  private void analyzeMsgLog(String logLine) {
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
