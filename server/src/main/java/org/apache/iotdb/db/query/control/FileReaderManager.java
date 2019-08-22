/**
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
package org.apache.iotdb.db.query.control;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.utils.QueryTrace;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileReaderManager is a singleton, which is used to manage
 * all file readers(opened file streams) to ensure that each file is opened at most once.
 */
public class FileReaderManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(FileReaderManager.class);
  private static final Logger qlogger = LoggerFactory.getLogger(QueryTrace.class);

  /**
   * max number of file streams being cached, must be lower than 65535.
   */
  private static final int MAX_CACHED_FILE_SIZE = 30000;

  /**
   * the key of closedFileReaderMap is the file path and the value of closedFileReaderMap
   * is the corresponding reader.
   */
  private ConcurrentHashMap<String, TsFileSequenceReader> closedFileReaderMap;
  /**
   * the key of unclosedFileReaderMap is the file path and the value of unclosedFileReaderMap
   * is the corresponding reader.
   */
  private ConcurrentHashMap<String, TsFileSequenceReader> unclosedFileReaderMap;

  /**
   * the key of closedFileReaderMap is the file path and the value of closedFileReaderMap
   * is the file's reference count.
   */
  private ConcurrentHashMap<String, AtomicInteger> closedReferenceMap;
  /**
   * the key of unclosedFileReaderMap is the file path and the value of unclosedFileReaderMap
   * is the file's reference count.
   */
  private ConcurrentHashMap<String, AtomicInteger> unclosedReferenceMap;

  private ScheduledExecutorService executorService;

  private FileReaderManager() {
    closedFileReaderMap = new ConcurrentHashMap<>();
    unclosedFileReaderMap = new ConcurrentHashMap<>();
    closedReferenceMap = new ConcurrentHashMap<>();
    unclosedReferenceMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "opended-files-manager");

    clearUnUsedFilesInFixTime();
  }

  public static FileReaderManager getInstance() {
    return FileReaderManagerHelper.INSTANCE;
  }

  private void clearUnUsedFilesInFixTime() {

    long examinePeriod = IoTDBDescriptor.getInstance().getConfig().getCacheFileReaderClearPeriod();

    executorService.scheduleAtFixedRate(() -> {
      synchronized (this) {
        clearMap(closedFileReaderMap, closedReferenceMap);
        clearMap(unclosedFileReaderMap, unclosedReferenceMap);
      }
    }, 0, examinePeriod, TimeUnit.MILLISECONDS);
  }

  private void clearMap(Map<String, TsFileSequenceReader> readerMap,
      Map<String, AtomicInteger> refMap) {
    for (Map.Entry<String, TsFileSequenceReader> entry : readerMap.entrySet()) {
      TsFileSequenceReader reader = entry.getValue();
      AtomicInteger refAtom = refMap.get(entry.getKey());

      if (refAtom != null && refAtom.get() == 0) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("Can not close TsFileSequenceReader {} !", reader.getFileName(), e);
        }
        readerMap.remove(entry.getKey());
        refMap.remove(entry.getKey());
      }
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
  public synchronized TsFileSequenceReader get(String filePath, boolean isClosed)
      throws IOException {

    Map<String, TsFileSequenceReader> readerMap = !isClosed ? unclosedFileReaderMap
        : closedFileReaderMap;
    if (!readerMap.containsKey(filePath)) {
      if (qlogger.isInfoEnabled()) {
        String isClosedStr = isClosed ? "closed" : "unclosed";
        qlogger.info("phase 3 or 4: isReaderMapMiss = 1 for {} file {}", isClosedStr, filePath);
      }

      if (readerMap.size() >= MAX_CACHED_FILE_SIZE) {
        logger.warn("Query has opened {} files !", readerMap.size());
      }

      TsFileSequenceReader tsFileReader = !isClosed ? new UnClosedTsFileReader(filePath)
          : new TsFileSequenceReader(filePath);

      readerMap.put(filePath, tsFileReader);
      return tsFileReader;
    }

    if (qlogger.isInfoEnabled()) {
      String isClosedStr = isClosed ? "closed" : "unclosed";
      qlogger.info("phase 3 or 4: isReaderMapMiss = 0 for {} file {}", isClosedStr, filePath);
    }

    return readerMap.get(filePath);
  }

  /**
   * Increase the reference count of the reader specified by filePath. Only when the reference count
   * of a reader equals zero, the reader can be closed and removed.
   */
  public synchronized void increaseFileReaderReference(String filePath, boolean isClosed) {
    // TODO : this should be called in get()
    if (!isClosed) {
      unclosedReferenceMap.computeIfAbsent(filePath, k -> new AtomicInteger()).getAndIncrement();
    } else {
      closedReferenceMap.computeIfAbsent(filePath, k -> new AtomicInteger()).getAndIncrement();
    }
  }

  /**
   * Decrease the reference count of the reader specified by filePath. This method is latch-free.
   * Only when the reference count of a reader equals zero, the reader can be closed and removed.
   */
  public synchronized void decreaseFileReaderReference(String filePath, boolean isClosed) {
    if (!isClosed && unclosedReferenceMap.containsKey(filePath)) {
      unclosedReferenceMap.get(filePath).getAndDecrement();
    } else if (closedReferenceMap.containsKey(filePath)){
      closedReferenceMap.get(filePath).getAndDecrement();
    }
  }

  /**
   * This method is used when the given file path is deleted.
   */
  public synchronized void closeFileAndRemoveReader(String filePath)
      throws IOException {
    if (unclosedFileReaderMap.containsKey(filePath)) {
      unclosedReferenceMap.remove(filePath);
      unclosedFileReaderMap.get(filePath).close();
      unclosedFileReaderMap.remove(filePath);
    }
    if (closedFileReaderMap.containsKey(filePath)) {
      closedReferenceMap.remove(filePath);
      closedFileReaderMap.get(filePath).close();
      closedFileReaderMap.remove(filePath);
    }
  }

  /**
   * Only for <code>EnvironmentUtils.cleanEnv</code> method. To make sure that unit tests and
   * integration tests will not conflict with each other.
   */
  public synchronized void closeAndRemoveAllOpenedReaders() throws IOException {
    for (Map.Entry<String, TsFileSequenceReader> entry : closedFileReaderMap.entrySet()) {
      entry.getValue().close();
      closedReferenceMap.remove(entry.getKey());
      closedFileReaderMap.remove(entry.getKey());
    }
    for (Map.Entry<String, TsFileSequenceReader> entry : unclosedFileReaderMap.entrySet()) {
      entry.getValue().close();
      unclosedReferenceMap.remove(entry.getKey());
      unclosedFileReaderMap.remove(entry.getKey());
    }
  }

  /**
   * This method is only for unit tests.
   */
  public synchronized boolean contains(String filePath, boolean isClosed) {
    return (isClosed && closedFileReaderMap.containsKey(filePath))
        || (!isClosed && unclosedFileReaderMap.containsKey(filePath));
  }

  @Override
  public void start() {
    // Do nothing
  }

  @Override
  public void stop() {
    if (executorService == null || executorService.isShutdown()) {
      return;
    }

    executorService.shutdown();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("StatMonitor timing service could not be shutdown.", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_READER_MANAGER_SERVICE;
  }

  private static class FileReaderManagerHelper {

    private static final FileReaderManager INSTANCE = new FileReaderManager();

    private FileReaderManagerHelper() {
    }
  }
}