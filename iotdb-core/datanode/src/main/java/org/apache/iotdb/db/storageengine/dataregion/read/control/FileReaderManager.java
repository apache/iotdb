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

package org.apache.iotdb.db.storageengine.dataregion.read.control;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/**
 * {@link FileReaderManager} is a singleton, which is used to manage all file readers(opened file
 * streams) to ensure that each file is opened at most once.
 */
public class FileReaderManager {

  private static final Logger logger = LoggerFactory.getLogger(FileReaderManager.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  /** max number of file streams being cached, must be lower than 65535. */
  private static final int MAX_CACHED_FILE_SIZE = 30000;

  /**
   * When number of file streams reached MAX_CACHED_FILE_SIZE, then we will print a warning log each
   * PRINT_INTERVAL.
   */
  private static final int PRINT_INTERVAL = 10000;

  /**
   * the key of closedFileReaderMap is the file path and the value of closedFileReaderMap is the
   * corresponding reader.
   */
  private Map<String, TsFileSequenceReader> closedFileReaderMap;

  /**
   * the key of unclosedFileReaderMap is the file path and the value of unclosedFileReaderMap is the
   * corresponding reader.
   */
  private Map<String, TsFileSequenceReader> unclosedFileReaderMap;

  /**
   * the key of closedFileReaderMap is the file path and the value of closedFileReaderMap is the
   * file's reference count.
   */
  private Map<String, AtomicInteger> closedReferenceMap;

  /**
   * the key of unclosedFileReaderMap is the file path and the value of unclosedFileReaderMap is the
   * file's reference count.
   */
  private Map<String, AtomicInteger> unclosedReferenceMap;

  private FileReaderManager() {
    closedFileReaderMap = new ConcurrentHashMap<>();
    unclosedFileReaderMap = new ConcurrentHashMap<>();
    closedReferenceMap = new ConcurrentHashMap<>();
    unclosedReferenceMap = new ConcurrentHashMap<>();
  }

  public static FileReaderManager getInstance() {
    return FileReaderManagerHelper.INSTANCE;
  }

  public synchronized void closeFileAndRemoveReader(String filePath) throws IOException {
    closedReferenceMap.remove(filePath);
    TsFileSequenceReader reader = closedFileReaderMap.remove(filePath);
    if (reader != null) {
      reader.close();
    }
    unclosedReferenceMap.remove(filePath);
    reader = unclosedFileReaderMap.remove(filePath);
    if (reader != null) {
      reader.close();
    }
  }

  /**
   * Get the reader of the file(tsfile or unseq tsfile) indicated by filePath. If the reader already
   * exists, just get it from closedFileReaderMap or unclosedFileReaderMap depending on isClosing .
   * Otherwise a new reader will be created and cached.
   *
   * @param filePath the path of the file, of which the reader is desired.
   * @param isClosed whether the corresponding file still receives insertions or not.
   * @return the reader of the file specified by filePath.
   * @throws IOException when reader cannot be created.
   */
  @SuppressWarnings("squid:S2095")
  public synchronized TsFileSequenceReader get(String filePath, boolean isClosed)
      throws IOException {
    return get(filePath, isClosed, null);
  }

  /**
   * Get the reader of the file(tsfile or unseq tsfile) indicated by filePath. If the reader already
   * exists, just get it from closedFileReaderMap or unclosedFileReaderMap depending on isClosing .
   * Otherwise a new reader will be created and cached.
   *
   * @param filePath the path of the file, of which the reader is desired.
   * @param isClosed whether the corresponding file still receives insertions or not.
   * @param ioSizeRecorder can be null
   * @return the reader of the file specified by filePath.
   * @throws IOException when reader cannot be created.
   */
  @SuppressWarnings("squid:S2095")
  public synchronized TsFileSequenceReader get(
      String filePath, boolean isClosed, LongConsumer ioSizeRecorder) throws IOException {

    Map<String, TsFileSequenceReader> readerMap =
        !isClosed ? unclosedFileReaderMap : closedFileReaderMap;
    if (!readerMap.containsKey(filePath)) {
      int currentOpenedReaderCount = readerMap.size();
      if (currentOpenedReaderCount >= MAX_CACHED_FILE_SIZE
          && (currentOpenedReaderCount % PRINT_INTERVAL == 0)) {
        logger.warn("Query has opened {} files !", readerMap.size());
      }

      TsFileSequenceReader tsFileReader = null;
      // check if the file is old version
      if (!isClosed) {
        tsFileReader = new UnClosedTsFileReader(filePath, ioSizeRecorder);
      } else {
        // already do the version check in TsFileSequenceReader's constructor
        tsFileReader = new TsFileSequenceReader(filePath, ioSizeRecorder);
      }
      readerMap.put(filePath, tsFileReader);
      return tsFileReader;
    }

    return readerMap.get(filePath);
  }

  /**
   * Increase the reference count of the reader specified by filePath. Only when the reference count
   * of a reader equals zero, the reader can be closed and removed.
   */
  public void increaseFileReaderReference(TsFileResource tsFile, boolean isClosed) {
    tsFile.readLock();
    synchronized (this) {
      if (!isClosed) {
        unclosedReferenceMap
            .computeIfAbsent(tsFile.getTsFilePath(), k -> new AtomicInteger())
            .getAndIncrement();
      } else {
        closedReferenceMap
            .computeIfAbsent(tsFile.getTsFilePath(), k -> new AtomicInteger())
            .getAndIncrement();
      }
    }
  }

  /**
   * Decrease the reference count of the reader specified by filePath. This method is latch-free.
   * Only when the reference count of a reader equals zero, the reader can be closed and removed.
   */
  public void decreaseFileReaderReference(TsFileResource tsFile, boolean isClosed) {
    synchronized (this) {
      if (!isClosed && unclosedReferenceMap.containsKey(tsFile.getTsFilePath())) {
        if (unclosedReferenceMap.get(tsFile.getTsFilePath()).decrementAndGet() == 0) {
          closeUnUsedReaderAndRemoveRef(tsFile.getTsFilePath(), false);
        }
      } else if (closedReferenceMap.containsKey(tsFile.getTsFilePath())
          && (closedReferenceMap.get(tsFile.getTsFilePath()).decrementAndGet() == 0)) {
        closeUnUsedReaderAndRemoveRef(tsFile.getTsFilePath(), true);
      }
    }
    tsFile.readUnlock();
  }

  private void closeUnUsedReaderAndRemoveRef(String tsFilePath, boolean isClosed) {
    Map<String, TsFileSequenceReader> readerMap =
        isClosed ? closedFileReaderMap : unclosedFileReaderMap;
    Map<String, AtomicInteger> refMap = isClosed ? closedReferenceMap : unclosedReferenceMap;
    synchronized (this) {
      // check ref num again
      if (refMap.get(tsFilePath).get() != 0) {
        return;
      }

      TsFileSequenceReader reader = readerMap.get(tsFilePath);
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("Can not close TsFileSequenceReader {} !", reader.getFileName(), e);
        }
      }
      readerMap.remove(tsFilePath);
      refMap.remove(tsFilePath);
      if (resourceLogger.isDebugEnabled()) {
        resourceLogger.debug("{} TsFileReader is closed because of no reference.", tsFilePath);
      }
    }
  }

  /**
   * Only for <code>EnvironmentUtils.cleanEnv</code> method. To make sure that unit tests and
   * integration tests will not conflict with each other.
   *
   * @throws IOException if failed to close file handlers, IOException will be thrown
   */
  public synchronized void closeAndRemoveAllOpenedReaders() throws IOException {
    Iterator<Map.Entry<String, TsFileSequenceReader>> iterator =
        closedFileReaderMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, TsFileSequenceReader> entry = iterator.next();
      entry.getValue().close();
      if (resourceLogger.isDebugEnabled()) {
        resourceLogger.debug("{} closedTsFileReader is closed.", entry.getKey());
      }
      closedReferenceMap.remove(entry.getKey());
      iterator.remove();
    }
    iterator = unclosedFileReaderMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, TsFileSequenceReader> entry = iterator.next();
      entry.getValue().close();
      if (resourceLogger.isDebugEnabled()) {
        resourceLogger.debug("{} unclosedTsFileReader is closed.", entry.getKey());
      }
      unclosedReferenceMap.remove(entry.getKey());
      iterator.remove();
    }
  }

  /** This method is only for unit tests. */
  public synchronized boolean contains(TsFileResource tsFile, boolean isClosed) {
    return (isClosed && closedFileReaderMap.containsKey(tsFile.getTsFilePath()))
        || (!isClosed && unclosedFileReaderMap.containsKey(tsFile.getTsFilePath()));
  }

  @TestOnly
  public Map<String, TsFileSequenceReader> getClosedFileReaderMap() {
    return closedFileReaderMap;
  }

  @TestOnly
  public Map<String, TsFileSequenceReader> getUnclosedFileReaderMap() {
    return unclosedFileReaderMap;
  }

  private static class FileReaderManagerHelper {

    private static final FileReaderManager INSTANCE = new FileReaderManager();

    private FileReaderManagerHelper() {}
  }
}
