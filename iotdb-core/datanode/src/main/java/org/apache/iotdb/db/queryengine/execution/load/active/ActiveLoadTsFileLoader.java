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

package org.apache.iotdb.db.queryengine.execution.load.active;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ActiveLoadTsFileLoader {

  private final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadTsFileLoader.class);

  private final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final AtomicReference<String[]> LOAD_ACTIVE_LISTENING_DIRS = new AtomicReference<>();
  private final AtomicReference<String> LOAD_ACTIVE_LISTENING_FAIL_DIR = new AtomicReference<>();

  private final Integer LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM =
      Math.min(
          IOTDB_CONFIG.getLoadActiveListeningMaxThreadNum(),
          Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

  private final String RESOURCE = ".resource";
  private final String MODS = ".mods";

  private final AtomicReference<WrappedThreadPoolExecutor> activeLoadTsFileExecutor =
      new AtomicReference<>();

  private final Set<String> waitingLoadTsFileQueue =
      Collections.synchronizedSet(new LinkedHashSet<>());

  // enable file monitoring
  private void monitoringTsFile() {
    try {
      // if not enable the active listening, skip
      if (!isEnableTheActiveListening()) {
        return;
      }

      if (activeLoadTsFileExecutor.get() == null) {
        synchronized (activeLoadTsFileExecutor) {
          if (activeLoadTsFileExecutor.get() == null) {
            activeLoadTsFileExecutor.set(
                (WrappedThreadPoolExecutor)
                    newCachedThreadPool(ThreadName.ACTIVE_LOAD_TSFILE_LOADER.name()));
          }
        }
      }

      initializeConfiguration();

      // only select the rest num in queue
      for (String listenerFileDir : LOAD_ACTIVE_LISTENING_DIRS.get()) {
        // max length of waitingLoadTsFileQueue
        int queueMaxNum = 200;
        // the rest space size of waitingLoadTsFileQueue
        int queueRestNum = queueMaxNum - waitingLoadTsFileQueue.size();
        Set<String> tsfileSet = filterFile(listenerFileDir, queueRestNum);
        if (!tsfileSet.isEmpty()) {
          waitingLoadTsFileQueue.addAll(tsfileSet);
        }
      }

      // adjust the thread pool core size
      adjustLoadTsFileExecutorThreadPoolSize();
    } catch (Exception e) {
      LOGGER.warn("Failed to start monitoring", e);
    }
  }

  private boolean isEnableTheActiveListening() {
    if (!IOTDB_CONFIG.getLoadActiveListeningEnable()) {
      if (activeLoadTsFileExecutor.get() != null) {
        synchronized (activeLoadTsFileExecutor) {
          if (activeLoadTsFileExecutor.get() != null) {
            activeLoadTsFileExecutor.get().shutdown();
            activeLoadTsFileExecutor.set(null);
          }
        }
      }
      return false;
    }
    return true;
  }

  // there is no suitable method in IoTDBThreadPoolFactory
  private ExecutorService newCachedThreadPool(String poolName) {
    return new WrappedThreadPoolExecutor(
        LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM,
        LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM,
        0L,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new IoTThreadFactory(poolName),
        poolName);
  }

  // initial directory
  private void initializeConfiguration() {
    try {
      getLoadActiveListeningFolders();
      getLoadActiveListeningFailFolder();

      for (int i = 0; i < LOAD_ACTIVE_LISTENING_DIRS.get().length; i++) {
        createDirectoriesIfNotExists(LOAD_ACTIVE_LISTENING_DIRS.get()[i]);
      }

      createDirectoriesIfNotExists(LOAD_ACTIVE_LISTENING_FAIL_DIR.get());
    } catch (Exception e) {
      LOGGER.warn("failed to init file folder because all disks of folders are full.", e);
    }
  }

  // get the LOAD_ACTIVE_LISTENING_DIRS
  private void getLoadActiveListeningFolders() {
    if (!Arrays.equals(
        IOTDB_CONFIG.getLoadActiveListeningDirs(), LOAD_ACTIVE_LISTENING_DIRS.get())) {
      synchronized (this) {
        if (IOTDB_CONFIG.getLoadActiveListeningDirs() != LOAD_ACTIVE_LISTENING_DIRS.get()) {
          LOAD_ACTIVE_LISTENING_DIRS.set(IOTDB_CONFIG.getLoadActiveListeningDirs());
        }
      }
    }
  }

  // get the LOAD_ACTIVE_LISTENING_FAIL_DIR
  private void getLoadActiveListeningFailFolder() {
    if (!Objects.equals(
        IOTDB_CONFIG.getLoadActiveListeningFailDir(), LOAD_ACTIVE_LISTENING_FAIL_DIR.get())) {
      synchronized (this) {
        if (!Objects.equals(
            IOTDB_CONFIG.getLoadActiveListeningFailDir(), LOAD_ACTIVE_LISTENING_FAIL_DIR.get())) {
          LOAD_ACTIVE_LISTENING_FAIL_DIR.set(IOTDB_CONFIG.getLoadActiveListeningFailDir());
        }
      }
    }
  }

  // create a directory that does not exist in the configuration file
  private void createDirectoriesIfNotExists(final String dirPath) {
    File file = new File(dirPath);
    try {
      FileUtils.forceMkdir(file);
    } catch (IOException e) {
      LOGGER.warn("failed to create directory {}", dirPath, e);
    }
  }

  // select the file to load, max select num is queueRestNum
  private Set<String> filterFile(final String dirPath, int queueRestNum) {
    File scanDir = new File(dirPath);
    Set<String> tsfileSet = new HashSet<>();
    if (queueRestNum <= 0) {
      return tsfileSet;
    }

    try {
      tsfileSet =
          FileUtils.streamFiles(scanDir, true, (String[]) null)
              .map(
                  loadFile -> {
                    if (loadFile.getName().endsWith(RESOURCE)
                        || loadFile.getName().endsWith(MODS)) {
                      return getResourceOrModsPath(loadFile.getAbsolutePath());
                    }
                    return loadFile.getAbsolutePath();
                  })
              .limit(queueRestNum)
              .collect(Collectors.toSet());
    } catch (IOException e) {
      LOGGER.warn("failed to scan director {}", scanDir.getAbsolutePath(), e);
    }
    return tsfileSet;
  }

  private String getResourceOrModsPath(final String filePath) {
    return filePath.endsWith(RESOURCE)
        ? filePath.substring(0, filePath.length() - RESOURCE.length())
        : filePath.substring(0, filePath.length() - MODS.length());
  }

  private void adjustLoadTsFileExecutorThreadPoolSize() {
    int currentSize = waitingLoadTsFileQueue.size();
    // get the target size
    // if the queue size gt the LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM, thread core is
    // LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM
    // if the queue size lte the LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM, thread core is queue size
    int targetPoolSize =
        currentSize > LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM
            ? LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM
            : currentSize;
    if (activeLoadTsFileExecutor.get().getCorePoolSize() != targetPoolSize) {
      activeLoadTsFileExecutor.get().setCorePoolSize(targetPoolSize);
    }

    // calculate how many threads need to be loaded
    int addThreadNum =
        Math.max(
            Math.min(LOAD_ACTIVE_LISTENING_MAX_THREAD_NUM, currentSize)
                - activeLoadTsFileExecutor.get().getActiveCount(),
            0);
    for (int i = 0; i < addThreadNum; i++) {
      activeLoadTsFileExecutor.get().execute(this::processTsFile);
    }
  }

  // auto load tsfile
  private void processTsFile() {
    // the number of attempts to obtain elements
    // terminate the current thread after more than five attempts
    int attemptGetElementFromQueueCounter = 0;
    // the maximum number of times a thread attempts to retrieve an element
    // end the current thread after exceeding the maximum number of times
    // the thread survival time is twice the polling interval
    long maxRetryNum = IOTDB_CONFIG.getLoadActiveListeningCheckIntervalSeconds() << 1;
    while (true) {
      String absolutePath = getAndRemovePathWithLock();
      // if not get element, then block 1s and skip
      // when reached the RETRY_MAX_NUM, terminal the thread
      if (StringUtils.isEmpty(absolutePath)) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (attemptGetElementFromQueueCounter++ == maxRetryNum) {
          break;
        }
        continue;
      } else {
        attemptGetElementFromQueueCounter = 0;
      }

      try {
        TSStatus result = loadTsFile(absolutePath);
        if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          LOGGER.info("successfully auto load tsfile {}: {}", absolutePath, result);
        } else {
          handleLoadFailure(absolutePath, result);
        }
      } catch (FileNotFoundException e) {
        handleFileNotFoundException(absolutePath);
      } catch (Exception e) {
        handleOtherException(absolutePath, e);
      }
    }
  }

  // take an element from a set and remove
  private synchronized String getAndRemovePathWithLock() {
    String absolutePath = "";
    // take an element from a set and remove
    if (waitingLoadTsFileQueue.iterator().hasNext()) {
      absolutePath = waitingLoadTsFileQueue.iterator().next();
      waitingLoadTsFileQueue.remove(absolutePath);
    }
    return absolutePath;
  }

  public TSStatus loadTsFile(final String fileAbsolutePath) throws FileNotFoundException {
    final LoadTsFileStatement statement = new LoadTsFileStatement(fileAbsolutePath);

    statement.setDeleteAfterLoad(true);
    statement.setVerifySchema(true);
    statement.setAutoCreateDatabase(false);

    return executeStatement(statement);
  }

  private TSStatus executeStatement(Statement statement) {
    if (statement == null) {
      return RpcUtils.getStatus(
          TSStatusCode.PIPE_TRANSFER_EXECUTE_STATEMENT_ERROR, "Execute null statement.");
    }

    statement = new PipeEnrichedStatement(statement);

    final ExecutionResult result =
        Coordinator.getInstance()
            .executeForTreeModel(
                statement,
                SessionManager.getInstance().requestQueryId(),
                new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault()),
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance(),
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    return result.status;
  }

  // handler load failed, remove the tsfile from waitingLoadTsFileQueue
  // remove tsfile, resource and mods from LOAD_ACTIVE_LISTENING_DIRS
  private void handleLoadFailure(String absolutePath, TSStatus result) {
    LOGGER.warn(
        "auto load tsfile {} failed, remove to {}: {}",
        absolutePath,
        LOAD_ACTIVE_LISTENING_FAIL_DIR,
        result);
    removeFileAndResourceAndModsToFailDir(absolutePath);
  }

  // handler file notfound, remove the tsfile from waitingLoadTsFileQueue
  // remove tsfile, resource and mods from LOAD_ACTIVE_LISTENING_DIRS
  private void handleFileNotFoundException(String absolutePath) {
    LOGGER.warn("file {} is not found", absolutePath);
    removeFileAndResourceAndModsToFailDir(absolutePath);
  }

  // handler other exception, remove the tsfile from waitingLoadTsFileQueue
  // remove tsfile, resource and mods from LOAD_ACTIVE_LISTENING_DIRS
  private void handleOtherException(String absolutePath, Exception e) {
    LOGGER.warn("auto load tsfile {} failed", absolutePath);
    if (e.getMessage() != null && e.getMessage().contains("memory")) {
      LOGGER.warn("rejecting file {} due to memory constraints, will retry later.", absolutePath);
      waitingLoadTsFileQueue.add(absolutePath);
    } else {
      removeFileAndResourceAndModsToFailDir(absolutePath);
    }
  }

  // remove tsfile with resource and mods
  private void removeFileAndResourceAndModsToFailDir(String filePath) {
    removeToFailDir(filePath);
    removeToFailDir(filePath + RESOURCE);
    removeToFailDir(filePath + MODS);
  }

  private void removeToFailDir(final String filePath) {
    File sourceFile = new File(filePath);
    // prevent the resource or mods not exist
    if (!sourceFile.exists()) {
      return;
    }

    File targetDir = new File(LOAD_ACTIVE_LISTENING_FAIL_DIR.get());
    try {
      moveFileWithMD5Check(sourceFile, targetDir);
    } catch (IOException e) {
      LOGGER.error(
          "failed to move file {} to fail directory: {}",
          filePath,
          LOAD_ACTIVE_LISTENING_FAIL_DIR,
          e);
    }
  }

  // move the file that load failed to fail director
  private void moveFileWithMD5Check(File sourceFile, File targetDir) throws IOException {
    String sourceFileName = sourceFile.getName();
    File targetFile = new File(targetDir, sourceFileName);

    // whether there a file with the same name in the target folder
    if (targetFile.exists()) {
      // whether there a file with the same "md5" in the target folder
      if (isHasSameMD5(sourceFile, targetFile)) {
        FileUtils.forceDelete(sourceFile);
        LOGGER.info(
            "already has file with the same name and content, deleted file {}", sourceFileName);
      } else {
        renameWithMD5(sourceFile, targetDir);
      }
    } else {
      FileUtils.moveFileToDirectory(sourceFile, targetDir, true);
      LOGGER.info(
          "moved the file {} to fail directory: {}",
          sourceFile.getName(),
          targetDir.getAbsolutePath());
    }
  }

  // whether the file1 and file2 have the same md5
  private boolean isHasSameMD5(File file1, File file2) throws IOException {
    try (InputStream is1 = Files.newInputStream(file1.toPath());
        InputStream is2 = Files.newInputStream(file2.toPath())) {
      return DigestUtils.md5Hex(is1).equals(DigestUtils.md5Hex(is2));
    }
  }

  // rename the sourceFile with {filename + "-" + md5 + suffix}
  private void renameWithMD5(File sourceFile, File targetDir) throws IOException {
    String sourceFileMD5 = DigestUtils.md5Hex(Files.newInputStream(sourceFile.toPath()));
    String baseName = FilenameUtils.getBaseName(sourceFile.getName());
    String newFileName =
        baseName + "-" + sourceFileMD5.substring(0, 16) + TsFileConstant.TSFILE_SUFFIX;

    File newFile = new File(targetDir, newFileName);
    FileUtils.moveFile(sourceFile, newFile, StandardCopyOption.REPLACE_EXISTING);
    LOGGER.info(
        "already has file with the same name, renamed file {} to {}",
        sourceFile.getName(),
        newFileName);
  }
}
