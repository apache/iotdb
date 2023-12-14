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

package org.apache.iotdb.db.queryengine.load.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class LoadTsFileSplitterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileSplitterManager.class);
  private static final int LOAD_TSFILE_SPLIT_THREAD_COUNT =
      Runtime.getRuntime().availableProcessors();
  private static final long SPLIT_WORKER_ALIVE_TIME_IN_MILLISECONDS = 1000L;

  private final LinkedBlockingQueue<LoadSingleTsFileStateMachine> tsFileQueue;
  private final ExecutorService splitWorkerExecutorService;
  private final ImmutableList<TsFileSplitWorker> splitWorkers;

  LoadTsFileSplitterManager() {
    this.tsFileQueue = new LinkedBlockingQueue<>();
    this.splitWorkerExecutorService =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            LOAD_TSFILE_SPLIT_THREAD_COUNT, LoadTsFileSplitterManager.class.getSimpleName());
    this.splitWorkers =
        IntStream.range(0, LOAD_TSFILE_SPLIT_THREAD_COUNT)
            .mapToObj(i -> new TsFileSplitWorker(i, tsFileQueue))
            .collect(ImmutableList.toImmutableList());
  }

  public void submitTsFileStateMachines(Collection<LoadSingleTsFileStateMachine> stateMachines) {
    tsFileQueue.addAll(stateMachines);
    startAllWorker();
  }

  private void startAllWorker() {
    for (int i = 0; i < LOAD_TSFILE_SPLIT_THREAD_COUNT; i++) {
      if (splitWorkers.get(i).submitSelfToExecutorServiceIfNotWorking(splitWorkerExecutorService)) {
        LOGGER.info("TsFile split worker {} starts.", i);
      }
    }
  }

  private static class TsFileSplitWorker implements Runnable {
    private final int workerId;
    private final LinkedBlockingQueue<LoadSingleTsFileStateMachine> tsFileQueue;

    private final Set<TEndPoint> visitedDataNodeEndPointSet;
    private final AtomicBoolean isWorking;

    TsFileSplitWorker(int workerId, LinkedBlockingQueue<LoadSingleTsFileStateMachine> tsFileQueue) {
      this.workerId = workerId;
      this.tsFileQueue = tsFileQueue;

      this.visitedDataNodeEndPointSet = new HashSet<>();
      this.isWorking = new AtomicBoolean(false);
    }

    public boolean submitSelfToExecutorServiceIfNotWorking(ExecutorService executorService) {
      return true;
    }

    @Override
    public void run() {
      while (isWorking.get()) {
        LoadSingleTsFileStateMachine stateMachine;
        try {
          stateMachine =
              tsFileQueue.poll(SPLIT_WORKER_ALIVE_TIME_IN_MILLISECONDS, TimeUnit.MILLISECONDS);
          isWorking.set(stateMachine != null);
          if (stateMachine == null) {
            continue;
          }
        } catch (InterruptedException e) {
          LOGGER.warn("TsFile split worker {} is interrupted.", workerId, e);
          isWorking.set(false);
          continue;
        }

        // handle TsFile state machine
        // TODO: toString
        switch (stateMachine.getState()) {
          case UNVISITED:
          default:
        }
      }
      LOGGER.info("TsFile split worker {} exits.", workerId);
    }
  }
}
