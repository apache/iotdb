/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.task;

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class DataRegionTaskManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataRegionTaskManager.class);
  private static final String TASKS_DIR_NAME = "tasks";
  private static final String TASK_FILE_SUFFIX = ".tsk";

  private final DataRegion dataRegion;
  private final AtomicLong lastestTaskId = new AtomicLong(0);
  private final File tasksDir;

  public DataRegionTaskManager(DataRegion dataRegion) {
    this.dataRegion = dataRegion;
    this.tasksDir = new File(dataRegion.getDataRegionSysDir() + File.separator + TASKS_DIR_NAME);
  }

  public void recover() {
    tasksDir.mkdirs();
    File[] files = tasksDir.listFiles((File dir, String name) -> name.endsWith(TASK_FILE_SUFFIX));
    if (files == null) {
      return;
    }

    Arrays.sort(
        files,
        (f1, f2) -> {
          String fileName1 = f1.getName();
          int suffixIndex1 = fileName1.indexOf(".");
          long taskId1 = Long.parseLong(fileName1.substring(0, suffixIndex1));

          String fileName2 = f2.getName();
          int suffixIndex2 = fileName2.indexOf(".");
          long taskId2 = Long.parseLong(fileName1.substring(0, suffixIndex2));

          return Long.compare(taskId1, taskId2);
        });

    for (File file : files) {
      String fileName = file.getName();
      int suffixIndex = fileName.indexOf(".");
      long taskId = Long.parseLong(fileName.substring(0, suffixIndex));
      lastestTaskId.getAndUpdate(l -> Math.max(l, taskId));

      try (FileInputStream fis = new FileInputStream(file);
          BufferedInputStream bufferedInputStream = new BufferedInputStream(fis)) {
        DataRegionTask task = DataRegionTask.createFrom(bufferedInputStream, taskId, dataRegion);
        task.run();
      } catch (IOException e) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Cannot recover task from file {}", file.getAbsolutePath(), e);
        }
      } finally {
        file.delete();
      }
    }
  }

  private void persistTask(DataRegionTask task) throws IOException {
    File taskFile = new File(tasksDir, task.getTaskId() + ".tsk");
    try (FileOutputStream fos = new FileOutputStream(taskFile);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fos)) {
      task.serialize(bufferedOutputStream);
    }
  }

  private void removeTask(DataRegionTask task) throws IOException {
    File taskFile = new File(tasksDir, task.getTaskId() + ".tsk");
    taskFile.delete();
  }

  public void submitAndRun(DataRegionTask dataRegionTask) throws IOException {
    dataRegionTask.setTaskId(lastestTaskId.getAndIncrement());
    persistTask(dataRegionTask);
    dataRegionTask.run();
    removeTask(dataRegionTask);
  }
}
