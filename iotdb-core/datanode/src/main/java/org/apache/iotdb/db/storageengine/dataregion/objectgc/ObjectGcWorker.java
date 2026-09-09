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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/** Per-DataRegion task that replays the object-GC journal off the write lock. */
public class ObjectGcWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectGcWorker.class);

  private final String databaseName;
  private final String dataRegionId;
  private final ObjectGcJournal journal;
  private final Executor executor;
  private final AtomicInteger workInProgress = new AtomicInteger();
  private volatile boolean stopped;

  public ObjectGcWorker(
      String databaseName, String dataRegionId, ObjectGcJournal journal, Executor executor) {
    this.databaseName = databaseName;
    this.dataRegionId = dataRegionId;
    this.journal = journal;
    this.executor = executor;
  }

  public void signal() {
    if (stopped || workInProgress.getAndIncrement() != 0) {
      return;
    }
    try {
      executor.execute(this);
    } catch (RejectedExecutionException e) {
      workInProgress.decrementAndGet();
      throw e;
    }
  }

  public void stop() {
    stopped = true;
  }

  @Override
  public void run() {
    LOGGER.info(
        StorageEngineMessages.LOG_START_OBJECT_GC_WORKER_FOR_DATA_REGION_ARG_ARG_A61D2B19,
        databaseName,
        dataRegionId);
    try {
      int missed = 1;
      while (!stopped) {
        drain();
        missed = workInProgress.addAndGet(-missed);
        if (missed == 0) {
          return;
        }
      }
    } catch (Throwable t) {
      LOGGER.warn(
          StorageEngineMessages
              .LOG_FAILED_TO_PROCESS_OBJECT_GC_RECORD_TYPE_ARG_FOR_DATA_REGION_ARG_ARG_8C49E9B9,
          "worker",
          databaseName,
          dataRegionId,
          t);
    } finally {
      workInProgress.set(0);
      LOGGER.info(
          StorageEngineMessages.LOG_STOP_OBJECT_GC_WORKER_FOR_DATA_REGION_ARG_ARG_EFE4BDF6,
          databaseName,
          dataRegionId);
    }
  }

  private void drain() {
    try (ObjectGcJournalReader reader = new ObjectGcJournalReader(journal)) {
      ObjectGcRecord record;
      while (!stopped && (record = reader.next()) != null) {
        try {
          process(record);
          journal.checkpoint(record.getSeq(), record.getEndOffset());
        } catch (Exception e) {
          LOGGER.warn(
              StorageEngineMessages
                  .LOG_FAILED_TO_PROCESS_OBJECT_GC_RECORD_TYPE_ARG_FOR_DATA_REGION_ARG_ARG_8C49E9B9,
              record.getType(),
              databaseName,
              dataRegionId,
              e);
          break;
        }
      }
    } catch (IOException e) {
      LOGGER.warn(
          StorageEngineMessages
              .LOG_FAILED_TO_PROCESS_OBJECT_GC_RECORD_TYPE_ARG_FOR_DATA_REGION_ARG_ARG_8C49E9B9,
          "read",
          databaseName,
          dataRegionId,
          e);
    }
  }

  private void process(ObjectGcRecord record) {
    if (record.getType() == ObjectGcRecord.TYPE_DROP_TABLE) {
      ObjectDirectoryScanner.dropTableDirs(
          record.getDropTableDirs(),
          LOGGER,
          StorageEngineMessages.LOG_FAILED_TO_DELETE_OBJECT_FILE_ARG_DURING_OBJECT_GC_74AE0729);
      return;
    }
    if (record.getType() == ObjectGcRecord.TYPE_SCAN) {
      ObjectDirectoryScanner.scanAndDelete(
          databaseName,
          Integer.parseInt(dataRegionId),
          record,
          LOGGER,
          StorageEngineMessages.LOG_FAILED_TO_DELETE_OBJECT_FILE_ARG_DURING_OBJECT_GC_74AE0729);
    }
  }
}
