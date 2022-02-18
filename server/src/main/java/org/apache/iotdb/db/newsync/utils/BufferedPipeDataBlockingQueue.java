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
package org.apache.iotdb.db.newsync.utils;

import org.apache.iotdb.db.newsync.pipedata.PipeData;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BufferedPipeDataBlockingQueue {
  // TODO: delete mock if unnecessary
  private static BufferedPipeDataBlockingQueue mock;
  private String fileName;
  private BlockingQueue<PipeData> pipeDataQueue;
  private int currentIndex;
  private boolean end;

  public BufferedPipeDataBlockingQueue(String fileName, int blockingQueueCapacity) {
    this.fileName = fileName;
    this.currentIndex = 0;
    this.end = false;
    pipeDataQueue = new ArrayBlockingQueue<>(blockingQueueCapacity);
  }

  public String getFileName() {
    return fileName;
  }

  public int getAndIncreaseIndex() {
    return currentIndex++;
  }

  public boolean offer(PipeData pipeData) {
    return pipeDataQueue.offer(pipeData);
  }

  public PipeData take() throws InterruptedException {
    return pipeDataQueue.take();
  }

  public void end() {
    this.end = true;
  }

  public boolean isEnd() {
    return end && pipeDataQueue.isEmpty();
  }

  // TODO: delete mock if unnecessary
  public static BufferedPipeDataBlockingQueue getMock() {
    if (mock == null) {
      String mockPath = SyncPathUtil.getReceiverPipeLogDir("mock", "127.0.0.1", 0);
      File file = new File(mockPath);
      if (!file.exists()) {
        file.mkdirs();
      }
      mock = new BufferedPipeDataBlockingQueue(mockPath + File.separator + "100", 100);
    }
    return mock;
  }
}
