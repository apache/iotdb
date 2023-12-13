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

package org.apache.iotdb.db.pipe.commit;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.metric.PipeEventCommitMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipeEventCommitManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitManager.class);

  // key: pipeName_dataRegionId
  private final Map<String, PipeEventCommitter> eventCommitterMap = new ConcurrentHashMap<>();

  public void register(String pipeName, int dataRegionId, String pipePluginName) {
    if (pipeName == null || pipePluginName == null) {
      return;
    }

    final String committerKey = generateCommitterKey(pipeName, dataRegionId);
    if (eventCommitterMap.containsKey(committerKey)) {
      LOGGER.warn(
          "Pipe with same name is already registered on this data region, overwriting: {}",
          committerKey);
    }
    PipeEventCommitter eventCommitter = new PipeEventCommitter(pipeName, dataRegionId);
    eventCommitterMap.put(committerKey, eventCommitter);
    PipeEventCommitMetrics.getInstance().register(eventCommitter, committerKey);
    LOGGER.info("Pipe committer registered for pipe on data region: {}", committerKey);
  }

  public void deregister(String pipeName, int dataRegionId) {
    final String committerKey = generateCommitterKey(pipeName, dataRegionId);
    eventCommitterMap.remove(committerKey);
    PipeEventCommitMetrics.getInstance().deregister(committerKey);
    LOGGER.info("Pipe committer deregistered for pipe on data region: {}", committerKey);
  }

  /**
   * Assign a commit id and a key for commit. Make sure {@code EnrichedEvent.pipeName} is set before
   * calling this.
   */
  public void enrichWithCommitterKeyAndCommitId(EnrichedEvent event, int dataRegionId) {
    if (event == null || event instanceof PipeHeartbeatEvent || event.getPipeName() == null) {
      return;
    }

    final String committerKey = generateCommitterKey(event.getPipeName(), dataRegionId);
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);
    if (committer == null) {
      return;
    }
    event.setCommitterKeyAndCommitId(committerKey, committer.generateCommitId());
  }

  public void commit(EnrichedEvent event, String committerKey) {
    if (event == null
        || event instanceof PipeHeartbeatEvent
        || event.getCommitId() <= EnrichedEvent.NO_COMMIT_ID
        || committerKey == null) {
      return;
    }

    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);
    if (committer == null) {
      return;
    }
    committer.commit(event);
  }

  private static String generateCommitterKey(String pipeName, int dataRegionId) {
    return String.format("%s_%s", pipeName, dataRegionId);
  }

  private PipeEventCommitManager() {
    // Do nothing but make it private.
  }

  private static class PipeEventCommitManagerHolder {
    private static final PipeEventCommitManager INSTANCE = new PipeEventCommitManager();
  }

  public static PipeEventCommitManager getInstance() {
    return PipeEventCommitManagerHolder.INSTANCE;
  }
}
