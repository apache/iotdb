package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.pipe.task.PipeTask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeSchemaRegionStatusListener {

  private final Map<SchemaRegionId, SchemaRegionStatus> id2StatusMap = new ConcurrentHashMap<>();

  public void notifyLeaderReady(SchemaRegionId schemaRegionId) {
    id2StatusMap.computeIfAbsent(schemaRegionId, k -> new SchemaRegionStatus()).notifyLeaderReady();
  }

  public void notifyLeaderUnavailable(SchemaRegionId schemaRegionId) {
    id2StatusMap
        .computeIfAbsent(schemaRegionId, k -> new SchemaRegionStatus())
        .notifyLeaderUnavailable();
  }

  public boolean isLeaderReady(SchemaRegionId schemaRegionId) {
    return id2StatusMap
        .computeIfAbsent(schemaRegionId, k -> new SchemaRegionStatus())
        .isLeaderReady();
  }

  private static class SchemaRegionStatus {

    private final AtomicBoolean isLeaderReady = new AtomicBoolean(false);

    /**
     * Get leader ready state, DO NOT use consensus layer's leader ready flag because
     * SimpleConsensus' ready flag is always {@code true}. Note that this flag has nothing to do
     * with listening and a {@link PipeTask} starts only iff the current node is a leader and ready.
     *
     * @return {@code true} iff the current node is a leader and ready
     */
    private boolean isLeaderReady() {
      return isLeaderReady.get();
    }

    // Leader ready flag has the following effect
    // 1. The linked list starts serving only after leader gets ready
    // 2. Config pipe task is only created after leader gets ready
    private void notifyLeaderReady() {
      isLeaderReady.set(true);
    }

    private void notifyLeaderUnavailable() {
      isLeaderReady.set(false);
    }
  }
}
