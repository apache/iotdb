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

package org.apache.iotdb.db.pipe.receiver.load;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.runtime.PipePeriodicalJobExecutor;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AutoLoadTsFileService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AutoLoadTsFileService.class);

  private static final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static String[] LOAD_ACTIVE_LISTENING_DIRS = new String[0];
  private static String LOAD_ACTIVE_LISTENING_FAIL_DIR = "";

  private static final Long LOAD_ACTIVE_LISTENING_CHECK_INTERVAL_SECONDS =
      IOTDB_CONFIG.getLoadActiveListeningCheckIntervalSeconds();
  private static final Integer LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM =
      Math.min(
          IOTDB_CONFIG.getLoadActiveListeningMaxThreadNum(),
          Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

  // the maximum number of times a thread attempts to retrieve an element
  // end the current thread after exceeding the maximum number of times
  private static final int RETRY_MAX_NUM = 60;
  // max length of waitingLoadTsFileQueue
  private static final int QUEUE_MAX_NUM = 200;
  // the rest space size of waitingLoadTsFileQueue
  private int QUEUE_REST_NUM = QUEUE_MAX_NUM;

  // whether the load tsfile thread enable
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  // whether the tsfileSet and resourceOrModsSet get enough file
  private final AtomicBoolean isFileListFull = new AtomicBoolean(false);

  private static final String RESOURCE = ".resource";
  private static final String MODS = ".mods";

  private static final Set<String> tsfileSet = new HashSet<>();
  private static final Set<String> resourceOrModsSet = new HashSet<>();

  private static final PipePeriodicalJobExecutor checkTsFilePeriodicalJobExecutor =
      new PipePeriodicalJobExecutor();
  private static WrappedThreadPoolExecutor loadTsFileExecutor;

  private static final LinkedHashSet<String> waitingLoadTsFileQueue =
      new LinkedHashSet<>(QUEUE_MAX_NUM);

  private static final Lock lock = new ReentrantLock();

  @Override
  public void start() throws StartupException {
    registerPeriodicalJob(this::monitoringTsFile);
    checkTsFilePeriodicalJobExecutor.start();
    isShutdown.set(false);
  }

  @Override
  public void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);
    checkTsFilePeriodicalJobExecutor.stop();
    loadTsFileExecutor.shutdown();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.AUTO_LOAD_TSFILE_SERVICE;
  }

  private void registerPeriodicalJob(Runnable periodicalJob) {
    checkTsFilePeriodicalJobExecutor.register(
        "AutoLoadTsFileService#loadTsFiles",
        periodicalJob,
        AutoLoadTsFileService.LOAD_ACTIVE_LISTENING_CHECK_INTERVAL_SECONDS);
  }

  // initial directory
  private void initializeConfiguration() {
    try {
      LOAD_ACTIVE_LISTENING_DIRS = IOTDB_CONFIG.getLoadActiveListeningDirs();
      LOAD_ACTIVE_LISTENING_FAIL_DIR = IOTDB_CONFIG.getLoadActiveListeningFailDir();
      for (String listenerFileDir : LOAD_ACTIVE_LISTENING_DIRS) {
        createDirectoriesIfNotExists(listenerFileDir);
      }
      createDirectoriesIfNotExists(LOAD_ACTIVE_LISTENING_FAIL_DIR);
    } catch (Exception e) {
      LOGGER.warn("failed to init file folder because all disks of folders are full.", e);
    }
  }

  // create a directory that does not exist in the configuration file
  private void createDirectoriesIfNotExists(String path) throws StartupException {
    File file = new File(path);
    boolean isNeeded = (file.exists() && file.isDirectory()) || file.mkdirs();
    if (isShutdown.get()) return;
    if (isNeeded) {
      LOGGER.info("successfully init file folder: {}", path);
    } else {
      LOGGER.warn("failed to init file folder");
      throw new StartupException("failed to init file folder");
    }
  }

  // move the file that failed to load to the directory of the failed loading configuration
  private void removeToFailDir(String filePath) {
    Path sourcePath = Paths.get(filePath);
    Path targetPath = Paths.get(LOAD_ACTIVE_LISTENING_FAIL_DIR);
    try {
      Files.move(
          sourcePath,
          targetPath.resolve(sourcePath.getFileName()),
          StandardCopyOption.REPLACE_EXISTING);
      LOGGER.info(
          "moved the file {} to fail directory: {}", filePath, LOAD_ACTIVE_LISTENING_FAIL_DIR);
    } catch (IOException e) {
      LOGGER.error(
          "failed to move file {} to fail directory: {}",
          filePath,
          LOAD_ACTIVE_LISTENING_FAIL_DIR,
          e);
    }
  }

  private boolean isEnableTheActiveListening() {
    if (!IOTDB_CONFIG.getLoadActiveListeningEnable()) {
      isShutdown.set(true);
      if (loadTsFileExecutor != null) {
        loadTsFileExecutor.shutdown();
      }
      return false;
    }
    return true;
  }

  // enable file monitoring
  private void monitoringTsFile() {
    try {
      // if not enable the active listening, skip
      if (!isEnableTheActiveListening()) {
        return;
      }
      if (loadTsFileExecutor == null) {
        loadTsFileExecutor =
            (WrappedThreadPoolExecutor) newCachedThreadPool(ThreadName.LOAD_TSFILE.name());
      }
      initializeConfiguration();
      // only select the rest num in queue
      QUEUE_REST_NUM = QUEUE_MAX_NUM - waitingLoadTsFileQueue.size();
      for (String listenerFileDir : LOAD_ACTIVE_LISTENING_DIRS) {
        File listenerDir = new File(listenerFileDir);
        filterFile(listenerDir);
        addNoResourceOrModsToSet();
        // have got the enough file
        if (isFileListFull.get()) {
          isFileListFull.set(false);
          break;
        }
      }
      if (!tsfileSet.isEmpty()) {
        addAllPathWithLock();
        tsfileSet.clear();
        resourceOrModsSet.clear();
      }
      // adjust the thread pool core size
      adjustThreadPoolSize();
    } catch (Exception e) {
      LOGGER.warn("Failed to start monitoring", e);
    }
  }

  // load tsfile
  private void processTsFile() {
    // the number of attempts to obtain elements
    // terminate the current thread after more than five attempts
    int attemptGetElementFromQueueCounter = 0;
    while (!isShutdown.get()) {
      String absolutePath = getAndRemovePathWithLock();
      // if not get element, then block 1s and skip
      // when reached the RETRY_MAX_NUM, terminal the thread
      if (StringUtils.isEmpty(absolutePath)) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (attemptGetElementFromQueueCounter++ == RETRY_MAX_NUM) {
          break;
        }
        continue;
      } else {
        attemptGetElementFromQueueCounter = 0;
      }

      try {
        TSStatus result = receiver.loadTsFile(absolutePath);
        if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          LOGGER.info("successfully load tsfile {}: {}", absolutePath, result);
        } else {
          LOGGER.warn(
              "load tsfile {} failed, remove to {}: {}",
              absolutePath,
              LOAD_ACTIVE_LISTENING_FAIL_DIR,
              result);
          removeToFailDir(absolutePath);
        }
      } catch (FileNotFoundException e) {
        LOGGER.warn("file {} is not found", absolutePath);
        removePathWithLock(absolutePath);
      } catch (Exception e) {
        LOGGER.warn("load tsfile {} failed", absolutePath);
        if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("memory")) {
          LOGGER.warn(
              "rejecting file {} due to memory constraints, will retry later.", absolutePath);
          addPathWithLock(absolutePath);
        }
      }
    }
  }

  // take an element from a set and remove
  private String getAndRemovePathWithLock() {
    String absolutePath = "";
    // take an element from a set and remove
    try {
      lock.lock();
      if (waitingLoadTsFileQueue.iterator().hasNext()) {
        absolutePath = waitingLoadTsFileQueue.iterator().next();
        waitingLoadTsFileQueue.remove(absolutePath);
      }
    } finally {
      lock.unlock();
    }
    return absolutePath;
  }

  private void addAllPathWithLock() {
    lock.lock();
    try {
      waitingLoadTsFileQueue.addAll(tsfileSet);
    } finally {
      lock.unlock();
    }
  }

  private void addPathWithLock(String absolutePath) {
    lock.lock();
    try {
      waitingLoadTsFileQueue.add(absolutePath);
    } finally {
      lock.unlock();
    }
  }

  private void removePathWithLock(String absolutePath) {
    try {
      lock.lock();
      waitingLoadTsFileQueue.remove(absolutePath);
    } finally {
      lock.unlock();
    }
  }

  private void adjustThreadPoolSize() {
    int currentSize = waitingLoadTsFileQueue.size();
    // get the target size
    // if the queue size gt the LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM, thread core is
    // LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM
    // if the queue size lte the LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM, thread core is queue size
    int targetPoolSize =
        currentSize > LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM
            ? LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM
            : currentSize;
    if (loadTsFileExecutor.getCorePoolSize() != targetPoolSize) {
      loadTsFileExecutor.setCorePoolSize(targetPoolSize);
    }
    // calculate how many threads need to be loaded
    int addThreadNum =
        Math.max(
            Math.min(LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM, currentSize)
                - loadTsFileExecutor.getActiveCount(),
            0);
    for (int i = 0; i < addThreadNum; i++) {
      loadTsFileExecutor.execute(this::processTsFile);
    }
  }

  private void addNoResourceOrModsToSet() {
    for (final String filePath : resourceOrModsSet) {
      final String tsfilePath =
          filePath.endsWith(RESOURCE)
              ? filePath.substring(0, filePath.length() - RESOURCE.length())
              : filePath.substring(0, filePath.length() - MODS.length());
      tsfileSet.add(tsfilePath);
    }
  }

  // select the file to load
  private void filterFile(File file) {
    File[] files = file.listFiles();
    if (files != null) {
      for (File loadFile : files) {
        if (loadFile.isDirectory()) {
          filterFile(loadFile);
        } else {
          if (loadFile.getName().endsWith(RESOURCE) || loadFile.getName().endsWith(MODS)) {
            resourceOrModsSet.add(loadFile.getAbsolutePath());
          } else {
            tsfileSet.add(loadFile.getAbsolutePath());
          }
          if (tsfileSet.size() + resourceOrModsSet.size() >= QUEUE_REST_NUM) {
            isFileListFull.set(true);
            return;
          }
        }
      }
    }
  }

  // there is no suitable method in IoTDBThreadPoolFactory
  private static ExecutorService newCachedThreadPool(String poolName) {
    return new WrappedThreadPoolExecutor(
        LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM,
        LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM,
        0L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new IoTThreadFactory(poolName),
        poolName);
  }

  private static class Holder {

    private static final AutoLoadTsFileService INSTANCE = new AutoLoadTsFileService();

    private Holder() {}
  }

  public static AutoLoadTsFileService getInstance() {
    return Holder.INSTANCE;
  }
}
