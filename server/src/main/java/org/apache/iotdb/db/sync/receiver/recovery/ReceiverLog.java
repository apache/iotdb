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
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ReceiverLog {
  private BufferedWriter pipeServerWriter;
  private BufferedWriter msgWriter;

  public void init() throws IOException {
    File logFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_LOG_NAME);
    File msgFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_MSG_LOG_NAME);
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }
    pipeServerWriter = new BufferedWriter(new FileWriter(logFile, true));
    msgWriter = new BufferedWriter(new FileWriter(msgFile, true));
  }

  public void startPipeServer() throws IOException {
    if (pipeServerWriter == null) {
      init();
    }
    pipeServerWriter.write("on");
    pipeServerWriter.newLine();
    pipeServerWriter.flush();
  }

  public void stopPipeServer() throws IOException {
    if (pipeServerWriter == null) {
      init();
    }
    pipeServerWriter.write("off");
    pipeServerWriter.newLine();
    pipeServerWriter.flush();
  }

  public void createPipe(String pipeName, String remoteIp, long time) throws IOException {
    writeLog(pipeName, remoteIp, time);
  }

  public void startPipe(String pipeName, String remoteIp, long time) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.RUNNING, time);
  }

  public void stopPipe(String pipeName, String remoteIp, long time) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.STOP, time);
  }

  public void dropPipe(String pipeName, String remoteIp, long time) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.DROP, time);
  }

  public void writePipeMsg(String pipeIdentifier, PipeMessage pipeMessage) throws IOException {
    if (msgWriter == null) {
      init();
    }
    msgWriter.write(
        String.format("%s,%s,%s", pipeIdentifier, pipeMessage.getType(), pipeMessage.getMsg()));
    msgWriter.newLine();
    msgWriter.flush();
  }

  public void comsumePipeMsg(String pipeIdentifier) throws IOException {
    if (msgWriter == null) {
      init();
    }
    msgWriter.write(String.format("%s,read", pipeIdentifier));
    msgWriter.newLine();
    msgWriter.flush();
  }

  private void writeLog(String pipeName, String remoteIp, PipeStatus status, long time)
      throws IOException {
    if (pipeServerWriter == null) {
      init();
    }
    pipeServerWriter.write(String.format("%s,%s,%d,%s", pipeName, remoteIp, time, status));
    pipeServerWriter.newLine();
    pipeServerWriter.flush();
  }

  private void writeLog(String pipeName, String remoteIp, long time) throws IOException {
    if (pipeServerWriter == null) {
      init();
    }
    pipeServerWriter.write(String.format("%s,%s,%d", pipeName, remoteIp, time));
    pipeServerWriter.newLine();
    pipeServerWriter.flush();
  }

  public void clean() throws IOException {
    File logFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_LOG_NAME);
    File msgFile = new File(SyncPathUtil.getSysDir(), SyncConstant.RECEIVER_MSG_LOG_NAME);
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }
    pipeServerWriter = new BufferedWriter(new FileWriter(logFile, false));
    msgWriter = new BufferedWriter(new FileWriter(msgFile, false));
  }

  public void close() throws IOException {
    if (pipeServerWriter != null) {
      pipeServerWriter.close();
      pipeServerWriter = null;
    }
    if (msgWriter != null) {
      msgWriter.close();
      msgWriter = null;
    }
  }
}
