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
package org.apache.iotdb.db.newsync.pipedata.queue;

import org.apache.iotdb.commons.utils.TestOnly;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipeDataQueueFactory {

  // TODO: try use weakReference to avoid memory leak
  /** pipe log dir name -> BufferedPipeDataQueue in this dir */
  private static final Map<String, BufferedPipeDataQueue> bufferedPipeDataQueueMap =
      new ConcurrentHashMap<>();
  /**
   * get or create BufferedPipeDataQueue identified by key
   *
   * @param pipeLogDir using path of pipe-log dir as key
   * @return BufferedPipeDataQueue
   */
  public static BufferedPipeDataQueue getBufferedPipeDataQueue(String pipeLogDir) {
    return bufferedPipeDataQueueMap.computeIfAbsent(
        pipeLogDir, i -> new BufferedPipeDataQueue(pipeLogDir));
  }

  public static void removeBufferedPipeDataQueue(String pipeLogDir) {
    BufferedPipeDataQueue queue = bufferedPipeDataQueueMap.remove(pipeLogDir);
    if (queue != null) {
      queue.clear();
    }
  }

  @TestOnly
  public static void clear() {
    for (PipeDataQueue pipeDataQueue : bufferedPipeDataQueueMap.values()) {
      pipeDataQueue.clear();
    }
  }
}
