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
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ReceiverLog {
  private BufferedWriter bw;

  public ReceiverLog() throws IOException {
    String syncSystemDir = IoTDBDescriptor.getInstance().getConfig().getSyncDir();
    // TODO: set new sync dir
    File logFile = new File(syncSystemDir, SyncConstant.RECEIVER_LOG_NAME);
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }
    bw = new BufferedWriter(new FileWriter(logFile, true));
  }

  public void startPipeServer() throws IOException {
    bw.write("on");
    bw.newLine();
    bw.flush();
  }

  public void stopPipeServer() throws IOException {
    bw.write("off");
    bw.newLine();
    bw.flush();
  }

  public void createPipe(String pipeName, String remoteIp, long time) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.RUNNING, time);
  }

  public void startPipe(String pipeName, String remoteIp) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.RUNNING);
  }

  public void stopPipe(String pipeName, String remoteIp) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.PAUSE);
  }

  public void dropPipe(String pipeName, String remoteIp) throws IOException {
    writeLog(pipeName, remoteIp, PipeStatus.DROP);
  }

  private void writeLog(String pipeName, String remoteIp, PipeStatus status, long time)
      throws IOException {
    bw.write(String.format("%s,%s,%s,%d", pipeName, remoteIp, status, time));
    bw.newLine();
    bw.flush();
  }

  private void writeLog(String pipeName, String remoteIp, PipeStatus status) throws IOException {
    bw.write(String.format("%s,%s,%s", pipeName, remoteIp, status));
    bw.newLine();
    bw.flush();
  }
}
