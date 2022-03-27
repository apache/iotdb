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

import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;
import org.apache.iotdb.db.newsync.sender.service.SenderService;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SenderLogAnalyzer {
  private File senderLog;
  private BufferedReader br;

  private Map<String, PipeSink> pipeSinks;
  private List<Pipe> pipes;

  private Pipe runningPipe;
  private Pipe.PipeStatus runningPipeStatus;
  private String runningMsg;

  public SenderLogAnalyzer() throws IOException {
    senderLog = new File(SyncPathUtil.getSysDir(), SyncConstant.SENDER_LOG_NAME);
    if (!senderLog.exists()) {
      senderLog.createNewFile();
    }

    this.pipeSinks = new HashMap<>();
    this.pipes = new ArrayList<>();
    this.runningMsg = "";
  }

  public void recover() throws IOException {
    br = new BufferedReader(new FileReader(senderLog));

    int lineNumber = 0; // start from 1
    String readLine = "";
    String[] parseStrings;

    try {
      while ((readLine = br.readLine()) != null) {
        lineNumber += 1;
        parseStrings = readLine.split(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
        Operator.OperatorType type = Operator.OperatorType.valueOf(parseStrings[0]);

        switch (type) {
          case CREATE_PIPESINK:
            readLine = br.readLine();
            lineNumber += 1;
            CreatePipeSinkPlan pipeSinkPlan = CreatePipeSinkPlan.parseString(readLine);
            pipeSinks.put(
                pipeSinkPlan.getPipeSinkName(),
                SenderService.getInstance().parseCreatePipeSinkPlan(pipeSinkPlan));
            break;
          case DROP_PIPESINK:
            pipeSinks.remove(parseStrings[1]);
            break;
          case CREATE_PIPE:
            readLine = br.readLine();
            lineNumber += 1;
            CreatePipePlan pipePlan = CreatePipePlan.parseString(readLine);
            runningPipe =
                SenderService.getInstance()
                    .parseCreatePipePlan(
                        pipePlan,
                        pipeSinks.get(pipePlan.getPipeSinkName()),
                        Long.parseLong(parseStrings[1]));
            pipes.add(runningPipe);
            runningPipeStatus = runningPipe.getStatus();
            runningPipe.stop();
            runningMsg = "";
            break;
          case STOP_PIPE: // ignore status check
            runningPipeStatus = Pipe.PipeStatus.STOP;
            appendMsg(parseStrings);
            break;
          case START_PIPE:
            runningPipeStatus = Pipe.PipeStatus.RUNNING;
            appendMsg(parseStrings);
            break;
          case DROP_PIPE:
            runningPipeStatus = Pipe.PipeStatus.DROP;
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

    if (pipes.size() > 0) {
      try {
        switch (runningPipeStatus) {
          case RUNNING:
            runningPipe.start();
            break;
          case STOP:
            runningPipe.stop();
            break;
          case DROP:
            runningPipe.drop();
            break;
          default:
            throw new IOException(
                String.format("Can not recognize running pipe status %s.", runningPipeStatus));
        }
      } catch (PipeException e) {
        throw new IOException(e.getMessage());
      }
    }

    br.close();
  }

  private void appendMsg(String[] parseStrings) {
    if (parseStrings.length == 3) {
      if (runningMsg.length() > 0) {
        runningMsg += System.lineSeparator();
      }
      runningMsg += parseStrings[2];
    }
  }

  public Map<String, PipeSink> getRecoveryAllPipeSinks() {
    return pipeSinks;
  }

  public List<Pipe> getRecoveryAllPipes() {
    return pipes;
  }

  public Pipe getRecoveryRunningPipe() {
    return runningPipe;
  }

  public String getRecoveryRunningMsg() {
    return runningMsg;
  }
}
