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
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileReaderManager is a singleton, which is used to manage
 * all file readers(opened file streams) to ensure that each file is opened at most once.
 */
public class FileReaderManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileReaderManager.class);

  /**
   * max number of file streams being cached, must be lower than 65535.
   */
  private static final int MAX_CACHED_FILE_SIZE = 30000;

  /**
   * the key of fileReaderMap is the file path and the value of fileReaderMap is
   * the corresponding reader.
   */
  private ConcurrentHashMap<String, TsFileSequenceReader> fileReaderMap;

  /**
   * the key of fileReaderMap is the file path and the value of fileReaderMap is the file's
   * reference count.
   */
  private ConcurrentHashMap<String, AtomicInteger> referenceMap;

  private ScheduledExecutorService executorService;

  private FileReaderManager() {
    fileReaderMap = new ConcurrentHashMap<>();
    referenceMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        "opended-files-manager");

    clearUnUsedFilesInFixTime();
  }

  public static FileReaderManager getInstance() {
    return FileReaderManagerHelper.INSTANCE;
  }

  private void clearUnUsedFilesInFixTime() {

    long examinePeriod = IoTDBDescriptor.getInstance().getConfig().cacheFileReaderClearPeriod;

    executorService.scheduleAtFixedRate(() -> {
      synchronized (this) {
        for (Map.Entry<String, TsFileSequenceReader> entry : fileReaderMap.entrySet()) {
          TsFileSequenceReader reader = entry.getValue();
          int referenceNum = referenceMap.get(entry.getKey()).get();

          if (referenceNum == 0) {
            try {
              reader.close();
            } catch (IOException e) {
              LOGGER.error("Can not close TsFileSequenceReader {} !", reader.getFileName());
            }
            fileReaderMap.remove(entry.getKey());
            referenceMap.remove(entry.getKey());
          }
        }
      }
    }, 0, examinePeriod, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the reader of the file(tsfile or unseq tsfile) indicated by filePath. If the reader already
   * exists, just get it from fileReaderMap. Otherwise a new reader will be created.
   * @param filePath the path of the file, of which the reader is desired.
   * @param isUnClosed whether the corresponding file still receives insertions or not.
   * @return the reader of the file specified by filePath.
   * @throws IOException when reader cannot be created.
   */
  public synchronized TsFileSequenceReader get(String filePath, boolean isUnClosed)
      throws IOException {

    if (!fileReaderMap.containsKey(filePath)) {

      if (fileReaderMap.size() >= MAX_CACHED_FILE_SIZE) {
        LOGGER.warn("Query has opened {} files !", fileReaderMap.size());
      }

      TsFileSequenceReader tsFileReader = isUnClosed ? new UnClosedTsFileReader(filePath)
          : new TsFileSequenceReader(filePath);

      fileReaderMap.put(filePath, tsFileReader);
      return tsFileReader;
    }

    return fileReaderMap.get(filePath);
  }

  /**
   * Increase the reference count of the reader specified by filePath. Only when the reference count
   * of a reader equals zero, the reader can be closed and removed.
   */
  public synchronized void increaseFileReaderReference(String filePath) {
    referenceMap.computeIfAbsent(filePath, k -> new AtomicInteger()).getAndIncrement();
  }

  /**
   * Decrease the reference count of the reader specified by filePath. This method is latch-free.
   * Only when the reference count of a reader equals zero, the reader can be closed and removed.
   */
  public synchronized void decreaseFileReaderReference(String filePath) {
    referenceMap.get(filePath).getAndDecrement();
  }

  /**
   * This method is used when the given file path is deleted.
   */
  public synchronized void closeFileAndRemoveReader(String filePath) throws IOException {
    if (fileReaderMap.containsKey(filePath)) {
      referenceMap.remove(filePath);
      fileReaderMap.get(filePath).close();
      fileReaderMap.remove(filePath);
    }
  }

  /**
   * Only for <code>EnvironmentUtils.cleanEnv</code> method. To make sure that unit tests
   * and integration tests will not conflict with each other.
   */
  public synchronized void closeAndRemoveAllOpenedReaders() throws IOException {
    for (Map.Entry<String, TsFileSequenceReader> entry : fileReaderMap.entrySet()) {
      entry.getValue().close();
      referenceMap.remove(entry.getKey());
      fileReaderMap.remove(entry.getKey());
    }
  }

  /**
   * This method is only for unit tests.
   */
  public synchronized boolean contains(String filePath) {
    return fileReaderMap.containsKey(filePath);
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
      LOGGER.error("StatMonitor timing service could not be shutdown.", e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_READER_MANAGER_SERVICE;
  }

  private static class FileReaderManagerHelper {
    private static final FileReaderManager INSTANCE = new FileReaderManager();
  }
}