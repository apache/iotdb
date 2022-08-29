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
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.buffer.WALEntryType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This reader returns {@link WALEntry} directly, the usage of WALReader is like {@link Iterator}.
 */
public class WALReader implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(WALReader.class);
  /** 1/10 of .wal file size as buffer size */
  private static final int STREAM_BUFFER_SIZE =
      (int) IoTDBDescriptor.getInstance().getConfig().getWalFileSizeThresholdInByte() / 10;

  private final File logFile;
  private final boolean fileMayCorrupt;
  private final DataInputStream logStream;
  private WALEntry nextEntry;
  private boolean fileCorrupted = false;

  public WALReader(File logFile) throws IOException {
    this(logFile, false);
  }

  public WALReader(File logFile, boolean fileMayCorrupt) throws IOException {
    this.logFile = logFile;
    this.fileMayCorrupt = fileMayCorrupt;
    this.logStream =
        new DataInputStream(
            new BufferedInputStream(Files.newInputStream(logFile.toPath()), STREAM_BUFFER_SIZE));
  }

  /** Like {@link Iterator#hasNext()} */
  public boolean hasNext() {
    if (nextEntry != null) {
      return true;
    }
    // read WALEntries from log stream
    try {
      if (fileCorrupted) {
        return false;
      }
      nextEntry = WALEntry.deserialize(logStream);
      if (nextEntry.getType() == WALEntryType.WAL_FILE_INFO_END_MARKER) {
        nextEntry = null;
        return false;
      }
    } catch (IllegalPathException e) {
      fileCorrupted = true;
      logger.warn(
          "WALEntry of wal file {} contains illegal path, skip illegal WALEntries.", logFile, e);
    } catch (Exception e) {
      fileCorrupted = true;
      // log only when file should be complete
      if (!fileMayCorrupt) {
        logger.warn("Fail to read WALEntry from wal file {}, skip broken WALEntries.", logFile, e);
      }
    }

    return nextEntry != null;
  }

  /** Like {@link Iterator#next()} */
  public WALEntry next() {
    if (nextEntry == null) {
      throw new NoSuchElementException();
    }
    WALEntry next = nextEntry;
    nextEntry = null;
    return next;
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }
}
