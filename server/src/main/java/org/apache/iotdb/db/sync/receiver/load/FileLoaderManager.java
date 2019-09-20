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
package org.apache.iotdb.db.sync.receiver.load;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to manage all FileLoader.
 */
public class FileLoaderManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoaderManager.class);

  private static final int WAIT_TIMEOUT = 2000;

  private ConcurrentHashMap<String, IFileLoader> fileLoaderMap;

  private ExecutorService loadTaskRunnerPool;

  private FileLoaderManager() {
  }

  public static FileLoaderManager getInstance() {
    return FileLoaderManagerHolder.INSTANCE;
  }

  public void addFileLoader(String senderName, IFileLoader fileLoader){
    fileLoaderMap.put(senderName, fileLoader);
  }

  public void removeFileLoader(String senderName){
    fileLoaderMap.remove(senderName);
  }

  public IFileLoader getFileLoader(String senderName) {
    return fileLoaderMap.get(senderName);
  }

  public boolean containsFileLoader(String senderName){
    return fileLoaderMap.containsKey(senderName);
  }

  public void addLoadTaskRunner(Runnable taskRunner){
    loadTaskRunnerPool.submit(taskRunner);
  }

  public void start() {
    if (fileLoaderMap == null) {
      fileLoaderMap = new ConcurrentHashMap<>();
    }
    if (loadTaskRunnerPool == null) {
      loadTaskRunnerPool = IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.LOAD_TSFILE.getName());
    }
  }

  public void stop() {
    fileLoaderMap = null;
    loadTaskRunnerPool.shutdownNow();
    int totalWaitTime = WAIT_TIMEOUT;
    while (!loadTaskRunnerPool.isTerminated()) {
      try {
        if (!loadTaskRunnerPool.awaitTermination(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
          LOGGER.info("File load manager thread pool doesn't exit after {}ms.",
              +totalWaitTime);
        }
        totalWaitTime += WAIT_TIMEOUT;
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while waiting file load manager thread pool to exit. ", e);
      }
    }
    loadTaskRunnerPool = null;
  }

  private static class FileLoaderManagerHolder {

    private static final FileLoaderManager INSTANCE = new FileLoaderManager();
  }
}
