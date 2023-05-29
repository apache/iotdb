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

package org.apache.iotdb.db.pipe.config;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.File;

// TODO: make these parameters configurable
// TODO: make all pipe related parameters in one place
// TODO: set the default value of the parameters in IoTDBDescriptor
// TODO: move it to common module?
public class PipeConfig {

  /**
   * The name of the directory that stores the tsfiles temporarily hold or generated by the pipe
   * module. The directory is located in the data directory of IoTDB.
   */
  public static final String PIPE_TSFILE_DIR_NAME = "pipe";

  private final int defaultRingBufferSize = 65536;

  private final int matcherCacheSize = 1024;

  private final int realtimeCollectorPendingQueueCapacity = 65536;

  // this should be less than or equals to realtimeCollectorPendingQueueCapacity
  private final int realtimeCollectorPendingQueueTabletLimit =
      realtimeCollectorPendingQueueCapacity / 2;

  private final int readFileBufferSize = 8388608;

  private final long pendingQueueMaxBlockingTimeMs = 1000;

  public int getDefaultRingBufferSize() {
    return defaultRingBufferSize;
  }

  public int getMatcherCacheSize() {
    return matcherCacheSize;
  }

  public int getRealtimeCollectorPendingQueueCapacity() {
    return realtimeCollectorPendingQueueCapacity;
  }

  public int getRealtimeCollectorPendingQueueTabletLimit() {
    return realtimeCollectorPendingQueueTabletLimit;
  }

  public String getReceiveFileDir() {
    return IoTDBDescriptor.getInstance().getConfig().getSystemDir()
        + File.separator
        + "pipe"; // TODO: replace with resource manager
  }

  public int getReadFileBufferSize() {
    return readFileBufferSize;
  }

  public long getPendingQueueMaxBlockingTimeMs() {
    return pendingQueueMaxBlockingTimeMs;
  }

  /////////////////////////////// Singleton ///////////////////////////////

  private PipeConfig() {}

  public static PipeConfig getInstance() {
    return PipeConfigHolder.INSTANCE;
  }

  private static class PipeConfigHolder {
    private static final PipeConfig INSTANCE = new PipeConfig();
  }
}
