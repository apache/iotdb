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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.worker.CompactionWorkerType;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiWorkerTypeCompactionTaskQueues {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Map<CompactionWorkerType, List<CompactionTaskQueue>> workerQueueMap =
      new ConcurrentHashMap<>();
  private final List<CompactionTaskQueue> queues = new ArrayList<>();

  private final CompactionTaskQueue candidateNormalCompactionTaskQueue =
      new CompactionTaskQueue(
          config.getCandidateCompactionTaskQueueSize(), new DefaultCompactionTaskComparatorImpl());
  private final CompactionTaskQueue candidateLightweightCompactionTaskQueue =
      new CompactionTaskQueue(
          config.getCandidateCompactionTaskQueueSize(), new DefaultCompactionTaskComparatorImpl());

  public MultiWorkerTypeCompactionTaskQueues() {
    queues.add(candidateNormalCompactionTaskQueue);
    queues.add(candidateLightweightCompactionTaskQueue);
    registerPollLastHook(
        AbstractCompactionTask::resetCompactionCandidateStatusForAllSourceFiles,
        AbstractCompactionTask::handleTaskCleanup);
    workerQueueMap.put(
        CompactionWorkerType.NORMAL_TASK_WORKER,
        Arrays.asList(candidateLightweightCompactionTaskQueue, candidateNormalCompactionTaskQueue));
    workerQueueMap.put(
        CompactionWorkerType.LIGHTWEIGHT_TASK_WORKER,
        Collections.singletonList(candidateLightweightCompactionTaskQueue));
  }

  private void registerPollLastHook(
      FixedPriorityBlockingQueue.PollLastHook<AbstractCompactionTask>... pollLastHooks) {
    for (FixedPriorityBlockingQueue.PollLastHook pollLastHook : pollLastHooks) {
      for (CompactionTaskQueue queue : queues) {
        queue.regsitPollLastHook(pollLastHook);
      }
    }
  }

  public AbstractCompactionTask tryTakeCompactionTask(CompactionWorkerType workerType)
      throws InterruptedException {
    List<CompactionTaskQueue> compactionTaskQueues = workerQueueMap.get(workerType);
    if (compactionTaskQueues.size() == 1) {
      return compactionTaskQueues.get(0).take();
    }
    AbstractCompactionTask task = null;
    for (CompactionTaskQueue taskQueue : compactionTaskQueues) {
      task = taskQueue.tryTake();
      if (task != null) {
        break;
      }
    }
    return task;
  }

  public void clear() {
    queues.forEach(CompactionTaskQueue::clear);
  }

  public int size() {
    return queues.stream().mapToInt(CompactionTaskQueue::size).sum();
  }

  public int getMaxSize() {
    return queues.stream().mapToInt(CompactionTaskQueue::getMaxSize).sum();
  }

  public boolean contains(AbstractCompactionTask compactionTask) {
    return queues.stream().anyMatch(queue -> queue.contains(compactionTask));
  }

  public int getMaxSize(CompactionWorkerType type) {
    return workerQueueMap.get(type).stream().mapToInt(CompactionTaskQueue::getMaxSize).sum();
  }

  public int size(CompactionWorkerType type) {
    return workerQueueMap.get(type).stream().mapToInt(CompactionTaskQueue::size).sum();
  }

  public void put(AbstractCompactionTask compactionTask) throws InterruptedException {
    if (compactionTask.getCompactionRewriteFileSize()
        > config.getSmallCompactionTaskFileSizeInBytes()) {
      candidateNormalCompactionTaskQueue.put(compactionTask);
    } else {
      candidateLightweightCompactionTaskQueue.put(compactionTask);
    }
  }

  public List<AbstractCompactionTask> getAllElementAsList() {
    List<AbstractCompactionTask> allElements =
        new ArrayList<>(config.getCandidateCompactionTaskQueueSize() * queues.size());
    queues.forEach(queue -> allElements.addAll(queue.getAllElementAsList()));
    return allElements;
  }
}
