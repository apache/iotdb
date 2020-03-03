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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FileReaderManager is a singleton, which is used to manage
 * all file readers(opened file streams) to ensure that each file is opened at most once.
 */
public class FileReaderManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(FileReaderManager.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  /**
   * max number of file streams being cached, must be lower than 65535.
   */
  private static final int MAX_CACHED_FILE_SIZE = 30000;

  /**
   * the key of closedFileReaderMap is the file path and the value of closedFileReaderMap
   * is the corresponding reader.
   */
  private Map<TsFileResource, TsFileSequenceReader> closedFileReaderMap;
  /**
   * the key of unclosedFileReaderMap is the file path and the value of unclosedFileReaderMap
   * is the corresponding reader.
   */
  private Map<TsFileResource, TsFileSequenceReader> unclosedFileReaderMap;

  /**
   * the key of closedFileReaderMap is the file path and the value of closedFileReaderMap
   * is the file's reference count.
   */
  private Map<TsFileResource, AtomicInteger> closedReferenceMap;
  /**
   * the key of unclosedFileReaderMap is the file path and the value of unclosedFileReaderMap
   * is the file's reference count.
   */
  private Map<TsFileResource, AtomicInteger> unclosedReferenceMap;

  private ScheduledExecutorService executorService;

  private FileReaderManager() {
    closedFileReaderMap = new ConcurrentHashMap<>();
    unclosedFileReaderMap = new ConcurrentHashMap<>();
    closedReferenceMap = new ConcurrentHashMap<>();
    unclosedReferenceMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "open-files-manager");

    clearUnUsedFilesInFixTime();
  }

  public static FileReaderManager getInstance() {
    return FileReaderManagerHelper.INSTANCE;
  }

  public synchronized void closeFileAndRemoveReader(TsFileResource seqFile) throws IOException {
    closedReferenceMap.remove(seqFile);
    TsFileSequenceReader reader = closedFileReaderMap.remove(seqFile);
    if (reader != null) {
      reader.close();
    }
    unclosedReferenceMap.remove(seqFile);
    reader = unclosedFileReaderMap.remove(seqFile);
    if (reader != null) {
      reader.close();
    }
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

  private void clearMap(Map<TsFileResource, TsFileSequenceReader> readerMap,
      Map<TsFileResource, AtomicInteger> refMap) {
    Iterator<Map.Entry<TsFileResource, TsFileSequenceReader>> iterator = readerMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TsFileResource, TsFileSequenceReader> entry = iterator.next();
      TsFileSequenceReader reader = entry.getValue();
      AtomicInteger refAtom = refMap.get(entry.getKey());

      if (refAtom != null && refAtom.get() == 0) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("Can not close TsFileSequenceReader {} !", reader.getFileName(), e);
        }
        iterator.remove();
        refMap.remove(entry.getKey());
        if (resourceLogger.isDebugEnabled()) {
          resourceLogger.debug("{} TsFileReader is closed because of no reference.", entry.getValue().getFileName());
        }
      }
    }
  }

  /**
   * Get the reader of the file(tsfile or unseq tsfile) indicated by filePath. If the reader already
   * exists, just get it from closedFileReaderMap or unclosedFileReaderMap depending on isClosing .
   * Otherwise a new reader will be created and cached.
   *
   * @param tsFile the path of the file, of which the reader is desired.
   * @param isClosed whether the corresponding file still receives insertions or not.
   * @return the reader of the file specified by filePath.
   * @throws IOException when reader cannot be created.
   */
  public synchronized TsFileSequenceReader get(TsFileResource tsFile, boolean isClosed)
      throws IOException {

    Map<TsFileResource, TsFileSequenceReader> readerMap = !isClosed ? unclosedFileReaderMap
        : closedFileReaderMap;
    if (!readerMap.containsKey(tsFile)) {

      if (readerMap.size() >= MAX_CACHED_FILE_SIZE) {
        logger.warn("Query has opened {} files !", readerMap.size());
      }

      TsFileSequenceReader tsFileReader = !isClosed ? new UnClosedTsFileReader(tsFile.getFile().getPath())
          : new TsFileSequenceReader(tsFile.getFile().getPath());

      readerMap.put(tsFile, tsFileReader);
      return tsFileReader;
    }

    return readerMap.get(tsFile);
  }


  /**
   * Increase the reference count of the reader specified by filePath. Only when the reference count
   * of a reader equals zero, the reader can be closed and removed.
   */
  void increaseFileReaderReference(TsFileResource tsFile, boolean isClosed) {
    // TODO : this should be called in get()
    tsFile.getWriteQueryLock().readLock().lock();
    synchronized (this) {
      if (!isClosed) {
        unclosedReferenceMap.computeIfAbsent(tsFile, k -> new AtomicInteger()).getAndIncrement();
      } else {
        closedReferenceMap.computeIfAbsent(tsFile, k -> new AtomicInteger()).getAndIncrement();
      }
    }
  }

  /**
   * Decrease the reference count of the reader specified by filePath. This method is latch-free.
   * Only when the reference count of a reader equals zero, the reader can be closed and removed.
   */
  void decreaseFileReaderReference(TsFileResource tsFile, boolean isClosed) {
    synchronized (this) {
      if (!isClosed && unclosedReferenceMap.containsKey(tsFile)) {
        unclosedReferenceMap.get(tsFile).decrementAndGet();
      } else if (closedReferenceMap.containsKey(tsFile)){
        closedReferenceMap.get(tsFile).decrementAndGet();
      }
    }
    tsFile.getWriteQueryLock().readLock().unlock();
  }

  /**
   * Only for <code>EnvironmentUtils.cleanEnv</code> method. To make sure that unit tests and
   * integration tests will not conflict with each other.
   */
  public synchronized void closeAndRemoveAllOpenedReaders() throws IOException {
    Iterator<Map.Entry<TsFileResource, TsFileSequenceReader>> iterator = closedFileReaderMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TsFileResource, TsFileSequenceReader> entry = iterator.next();
      entry.getValue().close();
      if (resourceLogger.isDebugEnabled()) {
        resourceLogger.debug("{} closedTsFileReader is closed.", entry.getValue().getFileName());
      }
      closedReferenceMap.remove(entry.getKey());
      iterator.remove();
    }
    iterator = unclosedFileReaderMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TsFileResource, TsFileSequenceReader> entry = iterator.next();
      entry.getValue().close();
      if (resourceLogger.isDebugEnabled()) {
        resourceLogger.debug("{} unclosedTsFileReader is closed.", entry.getValue().getFileName());
      }
      unclosedReferenceMap.remove(entry.getKey());
      iterator.remove();
    }
  }

  /**
   * This method is only for unit tests.
   */
  public synchronized boolean contains(TsFileResource tsFile, boolean isClosed) {
    return (isClosed && closedFileReaderMap.containsKey(tsFile))
        || (!isClosed && unclosedFileReaderMap.containsKey(tsFile));
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