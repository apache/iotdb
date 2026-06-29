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

package org.apache.iotdb.confignode.manager.pipe.coordinator.runtime;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.heartbeat.PipeHeartbeat;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.heartbeat.PipeHeartbeatScheduler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class PipeRuntimeCoordinator implements IClusterStatusSubscriber {

  // shared thread pool in the runtime package
  private static final AtomicReference<ExecutorService> procedureSubmitterHolder =
      new AtomicReference<>();
  private final ExecutorService procedureSubmitter;

  private final PipeLeaderChangeHandler pipeLeaderChangeHandler;
  private final PipeMetaSyncer pipeMetaSyncer;
  private final PipeHeartbeatScheduler pipeHeartbeatScheduler;

  public PipeRuntimeCoordinator(final ConfigManager configManager) {
    if (procedureSubmitterHolder.get() == null) {
      synchronized (PipeRuntimeCoordinator.class) {
        if (procedureSubmitterHolder.get() == null) {
          procedureSubmitterHolder.set(
              IoTDBThreadPoolFactory.newSingleThreadExecutor(
                  ThreadName.PIPE_RUNTIME_PROCEDURE_SUBMITTER.getName()));
        }
      }
    }
    procedureSubmitter = procedureSubmitterHolder.get();

    pipeLeaderChangeHandler = new PipeLeaderChangeHandler(configManager);
    pipeMetaSyncer = new PipeMetaSyncer(configManager);
    pipeHeartbeatScheduler = new PipeHeartbeatScheduler(configManager);
  }

  public ExecutorService getProcedureSubmitter() {
    return procedureSubmitter;
  }

  public synchronized void onConfigRegionGroupLeaderChanged() {
    pipeLeaderChangeHandler.onConfigRegionGroupLeaderChanged();
  }

  @Override
  public synchronized void onConsensusGroupStatisticsChanged(
      final ConsensusGroupStatisticsChangeEvent event) {
    pipeLeaderChangeHandler.onConsensusGroupStatisticsChanged(event);
  }

  public void startPipeMetaSync() {
    pipeMetaSyncer.start();
  }

  public void stopPipeMetaSync() {
    pipeMetaSyncer.stop();
  }

  public void startPipeHeartbeat() {
    pipeHeartbeatScheduler.start();
  }

  public void stopPipeHeartbeat() {
    pipeHeartbeatScheduler.stop();
  }

  public void parseHeartbeat(
      final int dataNodeId,
      final List<ByteBuffer> pipeMetaByteBufferListFromDataNode,
      /* @Nullable */ final List<Boolean> pipeCompletedListFromAgent,
      /* @Nullable */ final List<Long> pipeRemainingEventCountListFromAgent,
      /* @Nullable */ final List<Double> pipeRemainingTimeListFromAgent) {
    pipeHeartbeatScheduler.parseHeartbeat(
        dataNodeId,
        new PipeHeartbeat(
            pipeMetaByteBufferListFromDataNode,
            pipeCompletedListFromAgent,
            pipeRemainingEventCountListFromAgent,
            pipeRemainingTimeListFromAgent));
  }
}
