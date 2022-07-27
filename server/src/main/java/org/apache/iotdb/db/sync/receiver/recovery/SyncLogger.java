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
package org.apache.iotdb.db.sync.receiver.recovery;

import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.commons.sync.SyncPathUtil;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SyncLogger {
  // record pipe meta info
  private BufferedWriter pipeWriter;
  // record message for running pipe
  private BufferedWriter msgWriter;

  public void getBufferedWriter() throws IOException {
    if (pipeWriter == null || msgWriter == null) {
      File logFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_LOG_NAME);
      File msgFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_MSG_LOG_NAME);
      if (!logFile.getParentFile().exists()) {
        logFile.getParentFile().mkdirs();
      }
      pipeWriter = new BufferedWriter(new FileWriter(logFile, true));
      msgWriter = new BufferedWriter(new FileWriter(msgFile, true));
    }
  }

  public void startPipeServer() throws IOException {
    getBufferedWriter();
    pipeWriter.write(Operator.OperatorType.START_PIPE_SERVER.name());
    pipeWriter.newLine();
    pipeWriter.flush();
  }

  public void stopPipeServer() throws IOException {
    getBufferedWriter();
    pipeWriter.write(Operator.OperatorType.STOP_PIPE_SERVER.name());
    pipeWriter.newLine();
    pipeWriter.flush();
  }

  public synchronized void addPipeSink(CreatePipeSinkPlan plan) throws IOException {
    getBufferedWriter();
    pipeWriter.write(Operator.OperatorType.CREATE_PIPESINK.name());
    pipeWriter.newLine();
    pipeWriter.write(plan.toString());
    pipeWriter.newLine();
    pipeWriter.flush();
  }

  public synchronized void dropPipeSink(String pipeSinkName) throws IOException {
    getBufferedWriter();
    pipeWriter.write(Operator.OperatorType.DROP_PIPESINK.name());
    pipeWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeWriter.write(pipeSinkName);
    pipeWriter.newLine();
    pipeWriter.flush();
  }

  public synchronized void addPipe(CreatePipePlan plan, long pipeCreateTime) throws IOException {
    getBufferedWriter();
    pipeWriter.write(Operator.OperatorType.CREATE_PIPE.name());
    pipeWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeWriter.write(String.valueOf(pipeCreateTime));
    pipeWriter.newLine();
    pipeWriter.write(plan.toString());
    pipeWriter.newLine();
    pipeWriter.flush();
  }

  public synchronized void operatePipe(String pipeName, Operator.OperatorType type)
      throws IOException {
    getBufferedWriter();
    pipeWriter.write(type.name());
    pipeWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeWriter.write(pipeName);
    pipeWriter.newLine();
    pipeWriter.flush();
  }

  public void writePipeMsg(String pipeIdentifier, PipeMessage pipeMessage) throws IOException {
    getBufferedWriter();
    msgWriter.write(
        String.format("%s,%s,%s", pipeIdentifier, pipeMessage.getType(), pipeMessage.getMsg()));
    msgWriter.newLine();
    msgWriter.flush();
  }

  public void comsumePipeMsg(String pipeIdentifier) throws IOException {
    getBufferedWriter();
    msgWriter.write(String.format("%s,read", pipeIdentifier));
    msgWriter.newLine();
    msgWriter.flush();
  }

  public void close() throws IOException {
    if (pipeWriter != null) {
      pipeWriter.close();
      pipeWriter = null;
    }
    if (msgWriter != null) {
      msgWriter.close();
      msgWriter = null;
    }
  }
}
