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
 *
 */
package org.apache.iotdb.db.newsync.receiver.collector;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.queue.BufferedPipeDataQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** scan sync receiver folder and load pipeData into IoTDB */
public class Collector {

  private static final Logger logger = LoggerFactory.getLogger(Collector.class);
  private static final int WAIT_TIMEOUT = 2000;
  private ExecutorService executorService;
  private Map<String, Future> taskFutures;

  public Collector() {
    taskFutures = new ConcurrentHashMap<>();
  }

  public void startCollect() {
    this.executorService =
        IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
  }

  public void stopCollect() {
    for (Future f : taskFutures.values()) {
      f.cancel(true);
    }
    if (executorService != null) {
      executorService.shutdownNow();
      int totalWaitTime = WAIT_TIMEOUT;
      while (!executorService.isTerminated()) {
        try {
          if (!executorService.awaitTermination(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
            logger.info(
                "{} thread pool doesn't exit after {}ms.",
                ThreadName.SYNC_RECEIVER_COLLECTOR.getName(),
                totalWaitTime);
          }
          totalWaitTime += WAIT_TIMEOUT;
        } catch (InterruptedException e) {
          logger.error(
              "Interrupted while waiting {} thread pool to exit. ",
              ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
          Thread.currentThread().interrupt();
        }
      }
      executorService = null;
    }
  }

  public void startPipe(String pipeName, String remoteIp, long createTime) {
    String dir = SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime);
    ScanTask task = new ScanTask(dir);
    taskFutures.put(dir, executorService.submit(task));
  }

  public void stopPipe(String pipeName, String remoteIp, long createTime) {
    String dir = SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime);
    taskFutures.get(dir).cancel(true);
    taskFutures.remove(dir);
  }

  private class ScanTask implements Runnable {
    private final BufferedPipeDataQueue pipeDataQueue;

    private ScanTask(String pipeLogDir) {
      pipeDataQueue = BufferedPipeDataQueue.getInstance(pipeLogDir);
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        PipeData pipeData = null;
        try {
          pipeData = pipeDataQueue.take();
          logger.info(
              "Start load pipeData with serialize number {} and type {}",
              pipeData.getSerialNumber(),
              pipeData.getType());
          pipeData.createLoader().load();
          pipeDataQueue.commit();
        } catch (InterruptedException e) {
          logger.warn("Be interrupted when waiting for pipe data, because {}", e.getMessage());
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          // TODO: how to response error message to sender?
          // TODO: should drop this pipe?
          if (pipeData != null) {
            logger.error(
                "Cannot load pipeData with serialize number {} and type {}, because {}",
                pipeData.getSerialNumber(),
                pipeData.getType(),
                e.getMessage());
          } else {
            logger.error("Cannot load pipeData because {}", e.getMessage());
          }
          break;
        }
      }
    }
  }
}
