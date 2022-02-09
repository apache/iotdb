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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class TriggerLogWriter implements AutoCloseable {

  private final ByteBuffer logBuffer;
  private final File logFile;
  private final ILogWriter logWriter;

  public TriggerLogWriter(String logFilePath) throws IOException {
    logBuffer = ByteBuffer.allocate(IoTDBDescriptor.getInstance().getConfig().getMlogBufferSize());
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    logWriter = new LogWriter(logFile, false);
  }

  public synchronized void write(PhysicalPlan plan) throws IOException {
    try {
      plan.serialize(logBuffer);
      logWriter.write(logBuffer);
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Current trigger management operation plan is too large to write into buffer, please increase tlog_buffer_size.",
          e);
    } finally {
      logBuffer.clear();
    }
  }

  @Override
  public void close() throws IOException {
    logWriter.close();
  }

  public void deleteLogFile() throws IOException {
    FileUtils.forceDelete(logFile);
  }
}
