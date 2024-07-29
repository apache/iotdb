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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.writelog.FileTimeIndexCacheWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileTimeIndexCacheRecorder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTimeIndexCacheRecorder.class);

  private final static int VERSION = 0;

  protected final static String FILE_NAME = "FileTimeIndexCache_" + VERSION;

  private final ScheduledExecutorService recordFileIndexThread;

  private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

  private final Map<Integer, Map<Long, FileTimeIndexCacheWriter>> writerMap = new HashMap<>();

  private FileTimeIndexCacheRecorder() {
    recordFileIndexThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.FILE_TIMEINDEX_RECORD.getName());
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        recordFileIndexThread, this::executeTasks, 1, 1, TimeUnit.SECONDS);
  }

  private void executeTasks() {
    Runnable task;
    while ((task = taskQueue.poll()) != null) {
      recordFileIndexThread.submit(task);
    }
  }

  public void submitTask(File dataRegionSysDir, TsFileResource tsFileResource) {
    TsFileID tsFileID = tsFileResource.getTsFileID();
    int dataRegionId = tsFileID.regionId;
    long partitionId = tsFileID.timePartitionId;

    FileTimeIndexCacheWriter writer =
        writerMap
            .computeIfAbsent(dataRegionId, k -> new HashMap<>())
            .computeIfAbsent(
                partitionId,
                k -> {
                  File partitionDir =
                      SystemFileFactory.INSTANCE.getFile(
                          dataRegionSysDir, String.valueOf(partitionId));
                  File logFile =
                      SystemFileFactory.INSTANCE.getFile(partitionDir, FILE_NAME);
                  try {
                    if (!partitionDir.exists() && !partitionDir.mkdirs()) {
                      LOGGER.warn(
                          "Partition directory has existed，filePath:{}", partitionDir.getAbsolutePath());
                    }
                    if (!logFile.createNewFile()) {
                      LOGGER.warn(
                          "Partition log file has existed，filePath:{}", logFile.getAbsolutePath());
                    }
                    return new FileTimeIndexCacheWriter(logFile, false);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
    taskQueue.offer(
        () -> {
          try {
            writer.write(tsFileResource.serializeFileTimeIndexToByteBuffer());
            writer.force();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  public static FileTimeIndexCacheRecorder getInstance() {
    return FileTimeIndexCacheRecorder.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final FileTimeIndexCacheRecorder INSTANCE = new FileTimeIndexCacheRecorder();
  }
}
