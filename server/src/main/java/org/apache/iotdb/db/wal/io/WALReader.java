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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.wal.buffer.WALEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The usage of WALReader is like {@link Iterator}, which aims to control the memory usage of
 * reader.
 */
public class WALReader implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(WALReader.class);
  /** 10MB as default memory limit */
  private static final int MEMORY_LIMIT_IN_BYTES = 10 * 1024 * 1024;

  private final File logFile;
  private final DataInputStream logStream;
  private final List<WALEntry> walEntries;

  private Iterator<WALEntry> itr = null;
  private boolean fileCorrupted = false;

  public WALReader(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    this.logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.walEntries = new LinkedList<>();
  }

  /** Like {@link Iterator#hasNext()} */
  public boolean hasNext() {
    if (itr != null && itr.hasNext()) {
      return true;
    }
    // read WALEntries from log stream
    try {
      if (fileCorrupted || logStream.available() <= 0) {
        return false;
      }
      walEntries.clear();
      int totalSize = 0;
      while (totalSize < MEMORY_LIMIT_IN_BYTES) {
        int availableBytes = logStream.available();
        WALEntry walEntry = WALEntry.deserialize(logStream);

        walEntries.add(walEntry);

        totalSize += availableBytes - logStream.available();
      }
    } catch (EOFException e) {
      // reach end of wal file
      fileCorrupted = true;
    } catch (IllegalPathException e) {
      fileCorrupted = true;
      logger.warn(
          "WALEntry of wal file {} contains illegal path, skip illegal WALEntries.", logFile, e);
    } catch (Exception e) {
      fileCorrupted = true;
      logger.warn("Fail to read WALEntry from wal file {}, skip broken WALEntries.", logFile, e);
    }

    if (walEntries.size() != 0) {
      itr = walEntries.iterator();
      return true;
    }
    return false;
  }

  /** Like {@link Iterator#next()} */
  public WALEntry next() {
    if (itr == null) {
      throw new NoSuchElementException();
    }
    return itr.next();
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }
}
