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

package org.apache.iotdb.commons.pipe.agent.task.progress;

import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.metric.PipeEventCommitMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class PipeEventCommitManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitManager.class);

  private volatile PipeTaskAgent taskAgent;
  private final Map<CommitterKey, PipeEventCommitter> eventCommitterMap = new ConcurrentHashMap<>();

  // the restartTimes in the committer key is always -1
  private final Map<CommitterKey, Integer> eventCommitterRestartTimesMap =
      new ConcurrentHashMap<>();

  private BiConsumer<String, Boolean> commitRateMarker;

  public void register(
      final String pipeName,
      final long creationTime,
      final int regionId,
      final String pipePluginName) {
    if (pipeName == null || pipePluginName == null) {
      return;
    }

    final CommitterKey committerKey = generateCommitterKey(pipeName, creationTime, regionId);
    if (eventCommitterMap.containsKey(committerKey)) {
      LOGGER.warn(
          "Pipe with same name is already registered on this region, overwriting: {}",
          committerKey);
    }

    final PipeEventCommitter eventCommitter = new PipeEventCommitter(committerKey);
    eventCommitterMap.put(committerKey, eventCommitter);

    PipeEventCommitMetrics.getInstance().register(eventCommitter, committerKey.stringify());
    LOGGER.info("Pipe committer registered for pipe on region: {}", committerKey);
  }

  public void deregister(final String pipeName, final long creationTime, final int regionId) {
    final CommitterKey committerKey = generateCommitterKey(pipeName, creationTime, regionId);
    eventCommitterMap.remove(committerKey);
    eventCommitterRestartTimesMap.compute(
        generateCommitterRestartTimesKey(pipeName, creationTime, regionId),
        (k, v) -> Objects.nonNull(v) ? v + 1 : 0);

    PipeEventCommitMetrics.getInstance().deregister(committerKey.stringify());
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

    final CommitterKey committerKey =
        generateCommitterKey(event.getPipeName(), creationTime, regionId);
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);
    if (committer == null) {
      return;
    }
    event.setCommitterKeyAndCommitId(committerKey, committer.generateCommitId());
  }

  public void commit(final EnrichedEvent event, final CommitterKey committerKey) {
    if (event == null
        || !event.needToCommit()
        || Objects.isNull(event.getPipeName())
        || event.getCreationTime() == 0) {
      return;
    }
    if (Objects.nonNull(commitRateMarker)) {
      try {
        commitRateMarker.accept(
            taskAgent.getPipeNameWithCreationTime(event.getPipeName(), event.getCreationTime()),
            event.isDataRegionEvent());
      } catch (final Exception e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Failed to mark commit rate for pipe task: {}, stack trace: {}",
              committerKey,
              Thread.currentThread().getStackTrace());
        }
      }
    }
    if (committerKey == null || event.getCommitId() <= EnrichedEvent.NO_COMMIT_ID) {
      return;
    }
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);

    if (committer == null) {
      final int currentRestartTimes =
          eventCommitterRestartTimesMap.computeIfAbsent(
              generateCommitterRestartTimesKey(committerKey), k -> 0);
      if (LOGGER.isDebugEnabled()) {
        if (committerKey.getRestartTimes() < currentRestartTimes) {
          LOGGER.debug(
              "stale PipeEventCommitter({}) when commit event: {}, current restart times {}",
              committerKey,
              event.coreReportMessage(),
              currentRestartTimes);
        } else {
          LOGGER.debug(
              "missing PipeEventCommitter({}) when commit event: {}, stack trace: {}",
              committerKey,
              event.coreReportMessage(),
              Thread.currentThread().getStackTrace());
        }
      }
      return;
    }

    committer.commit(event);
  }

  private CommitterKey generateCommitterKey(
      final String pipeName, final long creationTime, final int regionId) {
    return taskAgent.getCommitterKey(
        pipeName,
        creationTime,
        regionId,
        eventCommitterRestartTimesMap.computeIfAbsent(
            generateCommitterRestartTimesKey(pipeName, creationTime, regionId), k -> 0));
  }

  private static CommitterKey generateCommitterRestartTimesKey(
      final String pipeName, final long creationTime, final int regionId) {
    return new CommitterKey(pipeName, creationTime, regionId);
  }

  private static CommitterKey generateCommitterRestartTimesKey(final CommitterKey committerKey) {
    return new CommitterKey(
        committerKey.getPipeName(), committerKey.getCreationTime(), committerKey.getRegionId());
  }

  public void setTaskAgent(final PipeTaskAgent taskAgent) {
    this.taskAgent = taskAgent;
  }

  public void setCommitRateMarker(final BiConsumer<String, Boolean> commitRateMarker) {
    this.commitRateMarker = commitRateMarker;
  }

  public long getGivenConsensusPipeCommitId(
      final String consensusPipeName, final long creationTime, final int consensusGroupId) {
    final CommitterKey committerKey =
        generateCommitterKey(consensusPipeName, creationTime, consensusGroupId);
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);
    if (committer == null) {
      return 0;
    }
    return committer.getCurrentCommitId();
  }

  //////////////////////////// singleton ////////////////////////////

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
