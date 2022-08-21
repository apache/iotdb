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

/**
 * SyncLogger is used to manage the persistent information in the sync module. Persistent
 * information can be recovered on reboot via {@linkplain SyncLogReader}.
 */
public class SyncLogWriter {
  // record pipe meta info
  private BufferedWriter pipeInfoWriter;
  // record pipe message
  private BufferedWriter pipeMsgWriter;

  private SyncLogWriter() {}

  public void getBufferedWriter() throws IOException {
    if (pipeInfoWriter == null || pipeMsgWriter == null) {
      File logFile = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_LOG_NAME);
      File msgFile = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_MSG_LOG_NAME);
      if (!logFile.getParentFile().exists()) {
        logFile.getParentFile().mkdirs();
      }
      pipeInfoWriter = new BufferedWriter(new FileWriter(logFile, true));
      pipeMsgWriter = new BufferedWriter(new FileWriter(msgFile, true));
    }
  }

  public synchronized void addPipeSink(CreatePipeSinkPlan plan) throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(Operator.OperatorType.CREATE_PIPESINK.name());
    pipeInfoWriter.newLine();
    pipeInfoWriter.write(plan.toString());
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public synchronized void dropPipeSink(String pipeSinkName) throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(Operator.OperatorType.DROP_PIPESINK.name());
    pipeInfoWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeInfoWriter.write(pipeSinkName);
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public synchronized void addPipe(CreatePipePlan plan, long pipeCreateTime) throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(Operator.OperatorType.CREATE_PIPE.name());
    pipeInfoWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeInfoWriter.write(String.valueOf(pipeCreateTime));
    pipeInfoWriter.newLine();
    pipeInfoWriter.write(plan.toString());
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public synchronized void operatePipe(String pipeName, Operator.OperatorType type)
      throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(type.name());
    pipeInfoWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeInfoWriter.write(pipeName);
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public void writePipeMsg(String pipeIdentifier, PipeMessage pipeMessage) throws IOException {
    getBufferedWriter();
    pipeMsgWriter.write(
        String.format("%s,%s,%s", pipeIdentifier, pipeMessage.getType(), pipeMessage.getMsg()));
    pipeMsgWriter.newLine();
    pipeMsgWriter.flush();
  }

  public void comsumePipeMsg(String pipeIdentifier) throws IOException {
    getBufferedWriter();
    pipeMsgWriter.write(String.format("%s,read", pipeIdentifier));
    pipeMsgWriter.newLine();
    pipeMsgWriter.flush();
  }

  public void close() throws IOException {
    if (pipeInfoWriter != null) {
      pipeInfoWriter.close();
      pipeInfoWriter = null;
    }
    if (pipeMsgWriter != null) {
      pipeMsgWriter.close();
      pipeMsgWriter = null;
    }
  }

  private static class SyncLoggerHolder {
    private static final SyncLogWriter INSTANCE = new SyncLogWriter();

    private SyncLoggerHolder() {
      // empty constructor
    }
  }

  public static SyncLogWriter getInstance() {
    return SyncLogWriter.SyncLoggerHolder.INSTANCE;
  }
}
