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

import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeStatement;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;

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

  private SyncLogWriter() {}

  public void getBufferedWriter() throws IOException {
    if (pipeInfoWriter == null) {
      File logFile = new File(SyncPathUtil.getSysDir(), SyncConstant.SYNC_LOG_NAME);
      if (!logFile.getParentFile().exists()) {
        logFile.getParentFile().mkdirs();
      }
      pipeInfoWriter = new BufferedWriter(new FileWriter(logFile, true));
    }
  }

  // TODO(sync): delete this in new-standalone version
  public synchronized void addPipeSink(CreatePipeSinkPlan plan) throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(Operator.OperatorType.CREATE_PIPESINK.name());
    pipeInfoWriter.newLine();
    pipeInfoWriter.write(plan.toString());
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public synchronized void addPipeSink(CreatePipeSinkStatement createPipeSinkStatement)
      throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(createPipeSinkStatement.getType().name());
    pipeInfoWriter.newLine();
    pipeInfoWriter.write(createPipeSinkStatement.toString());
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

  // TODO(sync): delete this in new-standalone version
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

  public synchronized void addPipe(CreatePipeStatement createPipeStatement, long pipeCreateTime)
      throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(createPipeStatement.getType().name());
    pipeInfoWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeInfoWriter.write(String.valueOf(pipeCreateTime));
    pipeInfoWriter.newLine();
    pipeInfoWriter.write(createPipeStatement.toString());
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public synchronized void operatePipe(String pipeName, SyncOperation syncOperation)
      throws IOException {
    getBufferedWriter();
    pipeInfoWriter.write(syncOperation.name());
    pipeInfoWriter.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
    pipeInfoWriter.write(pipeName);
    pipeInfoWriter.newLine();
    pipeInfoWriter.flush();
  }

  public void close() throws IOException {
    if (pipeInfoWriter != null) {
      pipeInfoWriter.close();
      pipeInfoWriter = null;
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
