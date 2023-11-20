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

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeEventCommitManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitManager.class);

  private final Set<String> committableConnectors;

  private final Map<String, PipeEventCommitter> eventCommitterMap = new ConcurrentHashMap<>();

  public void register(String pipeName, int dataRegionId, String pipePluginName) {
    if (!committableConnectors.contains(pipePluginName) || pipeName == null) {
      return;
    }

    final String key = generateKey(pipeName, dataRegionId);
    if (eventCommitterMap.containsKey(key)) {
      LOGGER.warn("Pipe with same name is already registered, overwriting: {}", key);
    }
    eventCommitterMap.put(key, new PipeEventCommitter());
    LOGGER.info("Pipe committer registered for pipe: {}", key);
  }

  public void deregister(String pipeName, int dataRegionId) {
    eventCommitterMap.remove(generateKey(pipeName, dataRegionId));
  }

  private String generateKey(String pipeName, int dataRegionId) {
    return String.format("%s_%s", pipeName, dataRegionId);
  }

  /**
   * Assign a commit id and a key for commit. Make sure {@code EnrichedEvent.pipeName} is set before
   * calling this.
   */
  public synchronized void enrichWithCommitIdAndCommitterKey(
      EnrichedEvent event, int dataRegionId) {
    if (event == null || event instanceof PipeHeartbeatEvent || event.getPipeName() == null) {
      return;
    }

    final String key = generateKey(event.getPipeName(), dataRegionId);
    if (!eventCommitterMap.containsKey(key)) {
      return;
    }

    event.setCommitId(eventCommitterMap.get(key).generateCommitId());
    event.setCommitterKey(key);
  }

  public void commit(EnrichedEvent event, String committerKey) {
    if (event == null || event instanceof PipeHeartbeatEvent) {
      return;
    }

    final long commitId = event.getCommitId();
    if (committerKey == null
        || !eventCommitterMap.containsKey(committerKey)
        || commitId <= EnrichedEvent.NO_COMMIT_ID) {
      return;
    }

    eventCommitterMap.get(committerKey).commit(event);
  }

  private PipeEventCommitManager() {
    committableConnectors =
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName(),
                    BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName(),
                    BuiltinPipePlugin.IOTDB_LEGACY_PIPE_SINK.getPipePluginName(),
                    BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName())));
  }

  private static class PipeEventCommitManagerHolder {
    private static final PipeEventCommitManager INSTANCE = new PipeEventCommitManager();
  }

  public static PipeEventCommitManager getInstance() {
    return PipeEventCommitManagerHolder.INSTANCE;
  }
}
