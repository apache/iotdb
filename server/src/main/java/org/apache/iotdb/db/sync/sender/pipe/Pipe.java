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
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.sender.manager.ISyncManager;
import org.apache.iotdb.db.sync.transport.client.ISyncClient;
import org.apache.iotdb.db.sync.transport.client.SenderManager;

/**
 * Pipe is the abstract of a sync task, and a data source for {@linkplain ISyncClient}. When the
 * pipe is started, it will collect history data and real time data from IoTDB continuously, and
 * record it. A {@linkplain ISyncClient} can take PipeData from a pipe, just like take data from a
 * BlockingQueue.
 */
public interface Pipe {
  /**
   * Start this pipe, so it can collect data from IoTDB continuously.
   *
   * @throws PipeException This pipe is dropped or some inside error happens.
   */
  void start() throws PipeException;

  /**
   * Stop this pipe, but it will not stop collecting data from IoTDB.
   *
   * @throws PipeException This pipe is dropped or some inside error happens.
   */
  void stop() throws PipeException;

  /**
   * Drop this pipe, delete all information about this pipe on disk.
   *
   * @throws PipeException Some inside error happens(such as IOException about disk).
   */
  void drop() throws PipeException;

  /**
   * Close this pipe, stop collecting data from IoTDB, but do not delete information about this pipe
   * on disk. Used for {@linkplain SyncService#shutdown(long)}. Do not change the status of this
   * pipe.
   *
   * @throws PipeException Some inside error happens(such as IOException about disk).
   */
  void close() throws PipeException;

  /**
   * Get the name of this pipe.
   *
   * @return The name of this pipe.
   */
  String getName();

  /**
   * Get the {@linkplain PipeSink} of this pipe.
   *
   * @return The {@linkplain PipeSink} of this pipe.
   */
  PipeSink getPipeSink();

  /**
   * Get the creation time of this pipe with unit of {@linkplain
   * IoTDBConfig}.getTimestampPrecision() .
   *
   * @return A time with unit of {@linkplain IoTDBConfig#getTimestampPrecision()}.
   */
  long getCreateTime();

  /**
   * Get the {@linkplain PipeStatus} of this pipe. When a pipe is created, the status should be
   * {@linkplain PipeStatus#STOP}
   *
   * @return The Status of this pipe.
   */
  PipeStatus getStatus();

  /**
   * Used for {@linkplain ISyncClient} to take one {@linkplain PipeData} from this pipe. If there is
   * no new data in this pipe, the method will block the thread until there is a new one.
   *
   * @param dataRegionId string of {@linkplain org.apache.iotdb.commons.consensus.DataRegionId}
   * @return A {@linkplain PipeData}.
   * @throws InterruptedException Be Interrupted when waiting for new {@linkplain PipeData}.
   */
  PipeData take(String dataRegionId) throws InterruptedException;

  /**
   * Used for {@linkplain ISyncClient} to commit all {@linkplain PipeData}s which are taken but not
   * be committed yet.
   *
   * @param dataRegionId string of {@linkplain org.apache.iotdb.commons.consensus.DataRegionId}
   */
  void commit(String dataRegionId);

  /**
   * Get {@linkplain ISyncManager} by dataRegionId. If ISyncManager does not exist, it will be
   * created automatically.
   *
   * @param dataRegionId string of {@linkplain org.apache.iotdb.commons.consensus.DataRegionId}
   * @return ISyncManager
   */
  ISyncManager getOrCreateSyncManager(String dataRegionId);

  void unregisterDataRegion(String dataRegionId);

  /**
   * If false, no need to collect realtime file
   *
   * @return whether finish collect history file.
   */
  boolean isHistoryCollectFinished();

  @TestOnly
  SenderManager getSenderManager();

  PipeInfo getPipeInfo();
}
