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
package org.apache.iotdb.db.sync.sender.recovery;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SenderLogger {
  private static final Logger logger = LoggerFactory.getLogger(SenderLogger.class);

  private BufferedWriter bw;

  public SenderLogger() {}

  private void getBufferedWriter() {
    try {
      if (bw != null) {
        return;
      }

      File senderLog = new File(SyncPathUtil.getSysDir(), SyncConstant.SENDER_LOG_NAME);
      if (!senderLog.exists()) {
        if (!senderLog.getParentFile().exists()) {
          senderLog.getParentFile().mkdirs();
        }
        senderLog.createNewFile();
      }

      bw = new BufferedWriter(new FileWriter(senderLog, true));
    } catch (IOException e) {
      logger.warn("Can not init sender logger.", e);
    }
  }

  public synchronized void addPipeSink(CreatePipeSinkPlan plan) {
    getBufferedWriter();
    try {
      bw.write(Operator.OperatorType.CREATE_PIPESINK.name());
      bw.newLine();
      bw.write(plan.toString());
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      logger.warn(String.format("Can not record add pipeSink %s.", plan.getPipeSinkName()), e);
    }
  }

  public synchronized void dropPipeSink(String pipeSinkName) {
    getBufferedWriter();
    try {
      bw.write(Operator.OperatorType.DROP_PIPESINK.name());
      bw.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
      bw.write(pipeSinkName);
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      logger.warn(String.format("Can not record drop pipeSink %s.", pipeSinkName), e);
    }
  }

  public synchronized void addPipe(CreatePipePlan plan, long pipeCreateTime) {
    getBufferedWriter();
    try {
      bw.write(Operator.OperatorType.CREATE_PIPE.name());
      bw.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
      bw.write(String.valueOf(pipeCreateTime));
      bw.newLine();
      bw.write(plan.toString());
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      logger.warn(String.format("Can not record add pipe %s.", plan.getPipeName()), e);
    }
  }

  public synchronized void operatePipe(String pipeName, Operator.OperatorType type) {
    getBufferedWriter();
    try {
      bw.write(type.name());
      bw.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
      bw.write(pipeName);
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      logger.warn(String.format("Can not record %s %s.", type.name(), pipeName), e);
    }
  }

  public synchronized void recordMsg(String pipeName, Operator.OperatorType type, String msg) {
    getBufferedWriter();
    try {
      bw.write(type.name());
      bw.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
      bw.write(pipeName);
      bw.write(SyncConstant.SENDER_LOG_SPLIT_CHARACTER);
      bw.write(msg);
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      logger.warn(String.format("Record msg %s error.", msg), e);
    }
  }

  public synchronized void close() {
    try {
      if (bw != null) {
        bw.close();
      }
    } catch (IOException e) {
      logger.warn("Can not close sender log.", e);
    }
  }
}
