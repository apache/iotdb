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
package org.apache.iotdb.commons.sync.persistence;

import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * SyncLogger is used to manage the persistent information in the sync module. Persistent
 * information can be recovered on reboot via {@linkplain SyncLogReader}.
 */
public class SyncLogWriter implements AutoCloseable {
  // record pipe meta info
  private OutputStream outputStream;
  private final File dir;
  private final String fileName;

  public SyncLogWriter(File dir) {
    this.dir = dir;
    this.fileName = SyncConstant.SYNC_LOG_NAME;
  }

  public SyncLogWriter(File dir, String fileName) {
    this.dir = dir;
    this.fileName = fileName;
  }

  public void initOutputStream() throws IOException {
    if (outputStream == null) {
      File logFile = new File(dir, fileName);
      if (!logFile.getParentFile().exists()) {
        logFile.getParentFile().mkdirs();
      }
      outputStream = new FileOutputStream(logFile, true);
    }
  }

  public synchronized void addPipeSink(PipeSink pipeSink) throws IOException {
    initOutputStream();
    ReadWriteIOUtils.write((byte) SyncOperation.CREATE_PIPESINK.ordinal(), outputStream);
    pipeSink.serialize(outputStream);
  }

  public synchronized void dropPipeSink(String pipeSinkName) throws IOException {
    initOutputStream();
    ReadWriteIOUtils.write((byte) SyncOperation.DROP_PIPESINK.ordinal(), outputStream);
    ReadWriteIOUtils.write(pipeSinkName, outputStream);
  }

  public synchronized void addPipe(PipeInfo pipeInfo) throws IOException {
    initOutputStream();
    ReadWriteIOUtils.write((byte) SyncOperation.CREATE_PIPE.ordinal(), outputStream);
    pipeInfo.serialize(outputStream);
  }

  public synchronized void operatePipe(String pipeName, SyncOperation syncOperation)
      throws IOException {
    initOutputStream();
    ReadWriteIOUtils.write((byte) syncOperation.ordinal(), outputStream);
    ReadWriteIOUtils.write(pipeName, outputStream);
  }

  @Override
  public void close() throws IOException {
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }
}
