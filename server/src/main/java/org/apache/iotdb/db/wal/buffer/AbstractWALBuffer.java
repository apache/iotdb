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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.wal.io.ILogWriter;
import org.apache.iotdb.db.wal.io.WALWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractWALBuffer implements IWALBuffer {
  private static final Logger logger = LoggerFactory.getLogger(AbstractWALBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /** use size limit to control WALEntry number in each file */
  protected static final long FILE_SIZE_THRESHOLD = config.getWalFileSizeThresholdInByte();

  /** WALNode identifier of this buffer */
  protected final String identifier;
  /** directory to store .wal files */
  protected final String logDirectory;
  /** current wal file version id */
  protected final AtomicInteger currentWALFileVersion = new AtomicInteger();
  /** current wal file log writer */
  protected volatile ILogWriter currentWALFileWriter;

  public AbstractWALBuffer(String identifier, String logDirectory) throws FileNotFoundException {
    this.identifier = identifier;
    this.logDirectory = logDirectory;
    File logDirFile = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!logDirFile.exists() && logDirFile.mkdirs()) {
      logger.info("create folder {} for wal buffer-{}.", logDirectory, identifier);
    }
    currentWALFileWriter =
        new WALWriter(
            SystemFileFactory.INSTANCE.getFile(
                logDirectory, WALWriter.getLogFileName(currentWALFileVersion.get())));
  }

  @Override
  public int getCurrentWALFileVersion() {
    return currentWALFileVersion.get();
  }

  /** Notice: only called by syncBufferThread and old log writer will be closed by this function. */
  protected boolean tryRollingLogWriter() throws IOException {
    if (currentWALFileWriter.size() < FILE_SIZE_THRESHOLD) {
      return false;
    }
    currentWALFileWriter.close();
    File nextLogFile =
        SystemFileFactory.INSTANCE.getFile(
            logDirectory, WALWriter.getLogFileName(currentWALFileVersion.incrementAndGet()));
    currentWALFileWriter = new WALWriter(nextLogFile);
    return true;
  }
}
