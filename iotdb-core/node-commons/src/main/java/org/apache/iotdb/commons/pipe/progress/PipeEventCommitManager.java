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

package org.apache.iotdb.commons.pipe.progress;

import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.metric.PipeEventCommitMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PipeEventCommitManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitManager.class);

  // key: pipeName_regionId
  private final Map<String, PipeEventCommitter> eventCommitterMap = new ConcurrentHashMap<>();

  private Consumer<PipeTaskRuntimeEnvironment> commitRateMarker;

  public void register(
      final String pipeName,
      final long creationTime,
      final int regionId,
      final String pipePluginName) {
    if (pipeName == null || pipePluginName == null) {
      return;
    }

    final String committerKey = generateCommitterKey(pipeName, creationTime, regionId);
    if (eventCommitterMap.containsKey(committerKey)) {
      LOGGER.warn(
          "Pipe with same name is already registered on this region, overwriting: {}",
          committerKey);
    }
    final PipeEventCommitter eventCommitter =
        new PipeEventCommitter(pipeName, creationTime, regionId);
    eventCommitterMap.put(committerKey, eventCommitter);
    PipeEventCommitMetrics.getInstance().register(eventCommitter, committerKey);
    LOGGER.info("Pipe committer registered for pipe on region: {}", committerKey);
  }

  public void deregister(final String pipeName, final long creationTime, final int regionId) {
    final String committerKey = generateCommitterKey(pipeName, creationTime, regionId);
    eventCommitterMap.remove(committerKey);
    PipeEventCommitMetrics.getInstance().deregister(committerKey);
    LOGGER.info("Pipe committer deregistered for pipe on region: {}", committerKey);
  }

  /**
   * Assign a commit id and a key for commit. Make sure {@code EnrichedEvent.pipeName} is set before
   * calling this.
   */
  public void enrichWithCommitterKeyAndCommitId(
      final EnrichedEvent event, final long creationTime, final int regionId) {
    if (event == null || event.getPipeName() == null || !event.needToCommit()) {
      return;
    }

    final String committerKey = generateCommitterKey(event.getPipeName(), creationTime, regionId);
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);
    if (committer == null) {
      return;
    }
    event.setCommitterKeyAndCommitId(committerKey, committer.generateCommitId());
  }

  public void commit(final EnrichedEvent event, final String committerKey) {
    if (committerKey == null
        || event == null
        || !event.needToCommit()
        || event.getCommitId() <= EnrichedEvent.NO_COMMIT_ID) {
      return;
    }
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);

    if (committer == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "missing PipeEventCommitter({}) when commit event: {}, stack trace: {}",
            committerKey,
            event.coreReportMessage(),
            Thread.currentThread().getStackTrace());
      }
      return;
    }

    committer.commit(event);
    if (Objects.nonNull(commitRateMarker)) {
      try {
        commitRateMarker.accept(
            new PipeTaskRuntimeEnvironment(
                committer.getPipeName(), committer.getCreationTime(), committer.getRegionId()));
      } catch (Exception e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Failed to mark commit rate for pipe: {}, stack trace: {}",
              committerKey,
              Thread.currentThread().getStackTrace());
        }
      }
    }
  }

  private static String generateCommitterKey(
      final String pipeName, final long creationTime, final int regionId) {
    return String.format("%s_%s_%s", pipeName, regionId, creationTime);
  }

  public void setCommitRateMarker(final Consumer<PipeTaskRuntimeEnvironment> commitRateMarker) {
    this.commitRateMarker = commitRateMarker;
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
