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
package org.apache.iotdb.lsm.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.NoSuchElementException;

public class WALReader implements IWALReader {
  private static final Logger logger = LoggerFactory.getLogger(WALReader.class);
  private final File logFile;
  private final WALRecord prototype;
  private final DataInputStream logStream;
  private WALRecord nextRecord;
  private boolean fileCorrupted = false;

  public WALReader(File logFile, WALRecord prototype) throws IOException {
    this.logFile = logFile;
    this.logStream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile.toPath())));
    this.prototype = prototype;
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }

  @Override
  public boolean hasNext() {
    if (nextRecord != null) {
      return true;
    }
    try {
      if (fileCorrupted) {
        return false;
      }
      int logSize = logStream.readInt();
      if (logSize <= 0) {
        return false;
      }
      nextRecord = prototype.clone();
      nextRecord.deserialize(logStream);
    } catch (EOFException e) {
      logger.info("");
      return false;
    } catch (IOException e) {
      logger.warn("");
      fileCorrupted = true;
      return false;
    }
    return true;
  }

  @Override
  public WALRecord next() {
    if (nextRecord == null) {
      throw new NoSuchElementException();
    }
    WALRecord walRecord = nextRecord;
    nextRecord = null;
    return walRecord;
  }

  @Override
  public String toString() {
    return "WALReader{" + "logFile=" + logFile + '}';
  }
}
