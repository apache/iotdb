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

  // key: pipeName_regionId
  private final Map<String, PipeEventCommitter> eventCommitterMap = new ConcurrentHashMap<>();

  private BiConsumer<String, Boolean> commitRateMarker;

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
    if (event == null
        || !event.needToCommit()
        || Objects.isNull(event.getPipeName())
        || event.getCreationTime() == 0) {
      return;
    }
    if (Objects.nonNull(commitRateMarker)) {
      try {
        commitRateMarker.accept(
            event.getPipeName() + '_' + event.getCreationTime(), event.isDataRegionEvent());
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
  }

  private static String generateCommitterKey(
      final String pipeName, final long creationTime, final int regionId) {
    return String.format("%s_%s_%s", pipeName, regionId, creationTime);
  }

  /**
   * Parses the region ID from the given committer key.
   *
   * <p>The committer key is expected to be in the format of {@code
   * <pipeName>_<regionId>_<creationTime>}, where {@code pipeName} may contain underscores. This
   * method extracts the region ID, which is assumed to be an integer between the second-to-last and
   * the last underscore in the string.
   *
   * @param committerKey the committer key to parse.
   * @return the parsed region ID as an {@code int}, or {@code -1} if the committer key format is
   *     invalid or if the region ID is not a valid integer.
   */
  public static int parseRegionIdFromCommitterKey(final String committerKey) {
    try {
      final int lastUnderscoreIndex = committerKey.lastIndexOf('_');
      final int secondLastUnderscoreIndex = committerKey.lastIndexOf('_', lastUnderscoreIndex - 1);

      if (lastUnderscoreIndex == -1 || secondLastUnderscoreIndex == -1) {
        return -1; // invalid format
      }

      final String regionIdStr =
          committerKey.substring(secondLastUnderscoreIndex + 1, lastUnderscoreIndex);
      return Integer.parseInt(regionIdStr); // convert region id to integer
    } catch (final NumberFormatException e) {
      return -1; // return -1 if the region id is not a valid integer
    }
  }

  public void setCommitRateMarker(final BiConsumer<String, Boolean> commitRateMarker) {
    this.commitRateMarker = commitRateMarker;
  }

  private PipeEventCommitManager() {
    // Do nothing but make it private.
  }

  public long getGivenConsensusPipeCommitId(String committerKey) {
    final PipeEventCommitter committer = eventCommitterMap.get(committerKey);
    if (committer == null) {
      return 0;
    }
    return committer.getCurrentCommitId();
  }

  private static class PipeEventCommitManagerHolder {
    private static final PipeEventCommitManager INSTANCE = new PipeEventCommitManager();
  }

  public static PipeEventCommitManager getInstance() {
    return PipeEventCommitManagerHolder.INSTANCE;
  }
}
