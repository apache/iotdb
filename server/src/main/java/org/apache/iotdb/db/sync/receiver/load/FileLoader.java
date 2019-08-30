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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import org.apache.iotdb.db.sync.sender.conf.Constans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLoader implements IFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

  private static final int WAIT_TIME = 1000;

  private String syncFolderPath;

  private String senderName;

  private ArrayDeque<LoadTask> queue = new ArrayDeque<>();

  private LoadLogger loadLog;

  private LoadType curType = LoadType.NONE;

  private volatile boolean endSync = false;

  private FileLoader(String senderName, String syncFolderPath) throws IOException {
    this.senderName = senderName;
    this.syncFolderPath = syncFolderPath;
    this.loadLog = new LoadLogger(new File(syncFolderPath, Constans.LOAD_LOG_NAME));
    FileLoaderManager.getInstance().addFileLoader(senderName, this);
    FileLoaderManager.getInstance().addLoadTaskRunner(loadTaskRunner);
  }

  public static FileLoader createFileLoader(String senderName, String syncFolderPath)
      throws IOException {
    return new FileLoader(senderName, syncFolderPath);
  }

  private Runnable loadTaskRunner = () -> {
    try {
      while (true) {
        if (endSync) {
          cleanUp();
          break;
        }
        if (queue.isEmpty()) {
          synchronized (queue) {
            if (queue.isEmpty()) {
              queue.wait(WAIT_TIME);
            }
          }
        }
        if (!queue.isEmpty()) {
          handleLoadTask(queue.poll());
        }
      }
    } catch (InterruptedException | IOException e) {
      LOGGER.error("Can not handle load task", e);
    }
  };

  @Override
  public void addDeletedFileName(File deletedFile) {
    synchronized (queue) {
      queue.add(new LoadTask(deletedFile, LoadType.DELETE));
      queue.notify();
    }
  }

  @Override
  public void addTsfile(File tsfile) {
    synchronized (queue) {
      queue.add(new LoadTask(tsfile, LoadType.ADD));
      queue.notify();
    }
  }

  @Override
  public void endSync() {
    this.endSync = true;
  }

  @Override
  public void handleLoadTask(LoadTask task) throws IOException {
    switch (task.type) {
      case ADD:
        loadDeletedFile(task.file);
        break;
      case DELETE:
        loadNewTsfile(task.file);
        break;
      default:
        LOGGER.error("Wrong load task type {}", task.type);
    }
  }

  private void loadDeletedFile(File file) throws IOException {
    if (curType != LoadType.DELETE) {
      loadLog.startLoadDeletedFiles();
      curType = LoadType.DELETE;
    }
    // TODO load deleted file
    loadLog.finishLoadDeletedFile(file);
  }

  private void loadNewTsfile(File file) throws IOException {
    if (curType != LoadType.ADD) {
      loadLog.startLoadTsFiles();
      curType = LoadType.ADD;
    }
    // TODO load new tsfile
    loadLog.finishLoadTsfile(file);
  }


  @Override
  public void cleanUp() {
    try {
      loadLog.close();
      new File(syncFolderPath, Constans.SYNC_LOG_NAME).deleteOnExit();
      new File(syncFolderPath, Constans.LOAD_LOG_NAME).deleteOnExit();
      new File(syncFolderPath, Constans.SYNC_END).deleteOnExit();
      FileLoaderManager.getInstance().removeFileLoader(senderName);
    } catch (IOException e) {
      LOGGER.error("Can not clean up sync resource.", e);
    }
  }

  class LoadTask {

    private File file;
    private LoadType type;

    LoadTask(File file, LoadType type) {
      this.file = file;
      this.type = type;
    }
  }

  private enum LoadType {
    DELETE, ADD, NONE
  }
}
