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
import org.apache.iotdb.db.wal.buffer.WALEdit;

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
  private final List<WALEdit> walEdits;

  private Iterator<WALEdit> itr = null;
  private boolean fileCorrupted = false;

  public WALReader(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    this.logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.walEdits = new LinkedList<>();
  }

  /** Like {@link Iterator#hasNext()} */
  public boolean hasNext() {
    if (itr != null && itr.hasNext()) {
      return true;
    }
    // read WALEdits from log stream
    try {
      if (fileCorrupted || logStream.available() <= 0) {
        return false;
      }
      walEdits.clear();
      int totalSize = 0;
      while (totalSize < MEMORY_LIMIT_IN_BYTES) {
        int availableBytes = logStream.available();
        WALEdit walEdit = WALEdit.deserialize(logStream);

        walEdits.add(walEdit);

        totalSize += availableBytes - logStream.available();
      }
    } catch (EOFException e) {
      fileCorrupted = true;
      logger.info("Reach end of wal file {}.", logFile, e);
    } catch (IllegalPathException e) {
      fileCorrupted = true;
      logger.error("WALEdit of wal file {} contains illegal path.", logFile, e);
    } catch (IOException e) {
      fileCorrupted = true;
      logger.error("Fail to read WALEdit from wal file {}.", logFile, e);
    }

    if (walEdits.size() != 0) {
      itr = walEdits.iterator();
      return true;
    }
    return false;
  }

  /** Like {@link Iterator#next()} */
  public WALEdit next() {
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
