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
package org.apache.iotdb.db.engine.cq;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.utils.writelog.LogWriter;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class CQLogWriter implements AutoCloseable {

  private static final String TOO_LARGE_RECORD_EXCEPTION =
      "Current CQ management operation plan is too large to write into buffer, please increase cqlog_buffer_size.";

  private final ByteBuffer logBuffer;
  private final LogWriter logWriter;

  public CQLogWriter(String logFilePath) throws IOException {
    logBuffer = ByteBuffer.allocate(IoTDBDescriptor.getInstance().getConfig().getTlogBufferSize());
    File logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    logWriter = new LogWriter(logFile, true);
  }

  public synchronized void createContinuousQuery(
      CreateContinuousQueryPlan createContinuousQueryPlan) throws IOException {
    try {
      createContinuousQueryPlan.serialize(logBuffer);
      logWriter.write(logBuffer);
    } catch (BufferOverflowException e) {
      throw new IOException(TOO_LARGE_RECORD_EXCEPTION, e);
    } finally {
      logBuffer.clear();
    }
  }

  public synchronized void dropContinuousQuery(DropContinuousQueryPlan dropContinuousQueryPlan)
      throws IOException {
    try {
      dropContinuousQueryPlan.serialize(logBuffer);
      logWriter.write(logBuffer);
    } catch (BufferOverflowException e) {
      throw new IOException(TOO_LARGE_RECORD_EXCEPTION, e);
    } finally {
      logBuffer.clear();
    }
  }

  @Override
  public void close() throws IOException {
    logWriter.close();
  }
}
