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
import java.util.ArrayDeque;
import org.apache.iotdb.db.sync.sender.conf.Constans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLoader implements IFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

  private static final int WAIT_TIME = 1000;

  private File syncEndFile;

  private String senderName;

  private ArrayDeque<LoadTask> queue = new ArrayDeque<>();

  private volatile boolean endSync = false;

  private FileLoader(String senderName, String syncFolderPath) {
    this.senderName = senderName;
    this.syncEndFile = new File(syncFolderPath, Constans.SYNC_END);
    FileLoaderManager.getInstance().addFileLoader(senderName, this);
    FileLoaderManager.getInstance().addLoadTaskRunner(loadTaskRunner);
  }

  public static FileLoader createFileLoader(String senderName, String syncFolderPath) {
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
    } catch (InterruptedException e) {
      LOGGER.info("Close the run load task thread.");
    }
  };

  @Override
  public void addDeletedFileName(String sgName, File deletedFile) {
    synchronized (queue) {
      queue.add(new LoadTask(sgName, deletedFile, LoadType.DELETE));
      queue.notify();
    }
  }

  @Override
  public void addTsfile(String sgName, File tsfile) {
    synchronized (queue) {
      queue.add(new LoadTask(sgName, tsfile, LoadType.ADD));
      queue.notify();
    }
  }

  @Override
  public void endSync() {
    this.endSync = true;
  }

  @Override
  public void handleLoadTask(LoadTask task) {

  }

  @Override
  public void cleanUp() {
    FileLoaderManager.getInstance().removeFileLoader(senderName);
  }

  class LoadTask {

    String sgName;
    File file;
    LoadType type;

    LoadTask(String sgName, File file, LoadType type) {
      this.sgName = sgName;
      this.file = file;
      this.type = type;
    }
  }

  private enum LoadType {
    DELETE, ADD;
  }
}
