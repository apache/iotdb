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

import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;

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
    writeLog(pipeName, remoteIp, PipeStatus.RUNNING, time);
  }

  public void startPipe(String pipeName, String remoteIp) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.RUNNING);
  }

  public void stopPipe(String pipeName, String remoteIp) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.STOP);
  }

  public void dropPipe(String pipeName, String remoteIp) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.DROP);
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

  public void readPipeMsg(String pipeIdentifier) throws IOException {
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
    pipeServerWriter.write(String.format("%s,%s,%s,%d", pipeName, remoteIp, status, time));
    pipeServerWriter.newLine();
    pipeServerWriter.flush();
  }

  private void writeLog(String pipeName, String remoteIp, PipeStatus status) throws IOException {
    if (pipeServerWriter == null) {
      init();
    }
    pipeServerWriter.write(String.format("%s,%s,%s", pipeName, remoteIp, status));
    pipeServerWriter.newLine();
    pipeServerWriter.flush();
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
