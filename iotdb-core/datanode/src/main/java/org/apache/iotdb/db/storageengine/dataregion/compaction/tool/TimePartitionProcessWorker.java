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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TimePartitionProcessWorker {
  private final List<TimePartitionProcessTask> workerTaskList;
  private final List<OverlapStatistic> workerResults;

  public TimePartitionProcessWorker() {
    workerTaskList = new ArrayList<>();
    workerResults = new ArrayList<>();
  }

  public void addTask(TimePartitionProcessTask task) {
    workerTaskList.add(task);
  }

  public void run(CountDownLatch latch) {
    new Thread(
            () -> {
              SequenceFileSubTaskThreadExecutor fileProcessTaskExecutor =
                  new SequenceFileSubTaskThreadExecutor(OverlapStatisticTool.subTaskNum);
              while (!workerTaskList.isEmpty()) {
                TimePartitionProcessTask task = workerTaskList.remove(0);
                OverlapStatistic partialRet = null;
                try {
                  partialRet = task.processTimePartition(fileProcessTaskExecutor);
                } catch (InterruptedException e) {
                  fileProcessTaskExecutor.shutdown();
                  Thread.currentThread().interrupt();
                  return;
                }
                workerResults.add(partialRet);
              }
              latch.countDown();
              fileProcessTaskExecutor.shutdown();
            })
        .start();
  }

  public List<OverlapStatistic> getWorkerResults() {
    return workerResults;
  }
}
