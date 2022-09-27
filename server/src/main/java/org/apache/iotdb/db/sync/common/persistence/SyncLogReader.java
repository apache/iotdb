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
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncLogReader {
  private static final Logger logger = LoggerFactory.getLogger(SyncLogReader.class);
  // <pipeSinkName, PipeSink>
  private Map<String, PipeSink> pipeSinks = new ConcurrentHashMap<>();
  private Map<String, Map<Long, PipeInfo>> pipes = new ConcurrentHashMap<>();
  private PipeInfo runningPipe;

  public void recover() throws StartupException {
    logger.info("Start to recover all sync state for sync.");
    File serviceLogFile = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_LOG_NAME);
    try (BufferedReader br = new BufferedReader(new FileReader(serviceLogFile))) {
      recoverPipe(br);
    } catch (IOException e) {
      logger.warn("Sync service log file not found");
    }
  }

  public Map<String, PipeSink> getAllPipeSinks() {
    return pipeSinks;
  }

  public Map<String, Map<Long, PipeInfo>> getAllPipeInfos() {
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
            pipes
                .computeIfAbsent(runningPipe.getPipeName(), i -> new ConcurrentHashMap<>())
                .computeIfAbsent(runningPipe.getCreateTime(), i -> runningPipe);
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
}
