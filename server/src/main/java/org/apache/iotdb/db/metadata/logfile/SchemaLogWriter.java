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

package org.apache.iotdb.db.metadata.logfile;

import org.apache.iotdb.commons.file.SystemFileFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class SchemaLogWriter<T> implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLogWriter.class);

  private static final int INITIALIZED_BUFFER_SIZE = 8192;

  private final File logFile;

  private final ByteArrayOutputStream logBufferStream =
      new ByteArrayOutputStream(INITIALIZED_BUFFER_SIZE);
  private final ByteBuffer logLengthBuffer = ByteBuffer.allocate(Integer.BYTES);
  private final FileOutputStream fileOutputStream;

  private final ISerializer<T> serializer;

  private final boolean forceEachWrite;

  private boolean hasSynced = true;

  public SchemaLogWriter(
      String schemaDir, String logFileName, ISerializer<T> serializer, boolean forceEachWrite)
      throws IOException {
    File dir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!dir.exists()) {
      if (dir.mkdirs()) {
        LOGGER.info("create schema folder {}.", dir);
      } else {
        LOGGER.warn("create schema folder {} failed.", dir);
      }
    }

    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    fileOutputStream = new FileOutputStream(logFile, true);
    this.serializer = serializer;

    this.forceEachWrite = forceEachWrite;
  }

  public synchronized void write(T schemaPlan) throws IOException {
    hasSynced = false;
    // serialize plan to binary data
    serializer.serialize(schemaPlan, logBufferStream);
    // write the length of plan data
    logLengthBuffer.putInt(logBufferStream.size());
    fileOutputStream.write(logLengthBuffer.array());
    // write the plan data
    logBufferStream.writeTo(fileOutputStream);
    // clear buffer
    logLengthBuffer.clear();
    logBufferStream.reset();

    if (forceEachWrite) {
      syncBufferToDisk();
    }
  }

  public synchronized void force() throws IOException {
    if (hasSynced) {
      return;
    }
    hasSynced = true;
    fileOutputStream.getFD().sync();
  }

  private void syncBufferToDisk() throws IOException {
    fileOutputStream.getFD().sync();
    hasSynced = true;
  }

  public synchronized void clear() throws IOException {
    logBufferStream.reset();
    fileOutputStream.close();

    if (logFile != null && logFile.exists()) {
      Files.delete(logFile.toPath());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    logBufferStream.reset();
    fileOutputStream.close();
  }
}
