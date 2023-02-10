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

package org.apache.iotdb.consensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.Utils;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

@ThreadSafe
public interface IStateMachine {

  interface Registry extends Function<ConsensusGroupId, IStateMachine> {}

  void start();

  void stop();

  default boolean isReadOnly() {
    return false;
  }

  /**
   * apply a deserialized write-request from user.
   *
   * @param request deserialized request
   */
  TSStatus write(IConsensusRequest request);

  /**
   * deserialize IConsensusRequest.
   *
   * @param request write request
   * @return deserialized request
   */
  IConsensusRequest deserializeRequest(IConsensusRequest request);

  /**
   * read local data and return.
   *
   * @param request read request
   */
  DataSet read(IConsensusRequest request);

  /**
   * Take a snapshot of current statemachine. All files are required to be stored under snapshotDir,
   * which is a subdirectory of the StorageDir in Consensus
   *
   * @param snapshotDir required storage dir
   * @return true if snapshot is successfully taken
   */
  boolean takeSnapshot(File snapshotDir);

  /**
   * Take a snapshot of current statemachine. Snapshot.log will be stored under snapshotDir, The
   * data of the snapshot will be stored under `data folder/snapshot/snapshotId`.
   *
   * @param snapshotDir required storage dir
   * @param snapshotTmpId temporary id of the snapshot
   * @param snapshotId the id of the snapshot
   * @return true if snapshot is successfully taken
   */
  default boolean takeSnapshot(File snapshotDir, String snapshotTmpId, String snapshotId) {
    return takeSnapshot(snapshotDir);
  }

  /**
   * Load the latest snapshot from given dir.
   *
   * @param latestSnapshotRootDir dir where the latest snapshot sits
   */
  void loadSnapshot(File latestSnapshotRootDir);

  /**
   * given a snapshot dir, ask statemachine to provide all snapshot files. By default, it will list
   * all files recursively under latestSnapshotDir
   *
   * <p>DataRegion may take snapshot at a different disk and only store a log file containing file
   * paths. So statemachine is required to read the log file and provide the real snapshot file
   * paths.
   *
   * @param latestSnapshotRootDir dir where the latest snapshot sits
   * @return List of real snapshot files.
   */
  default List<Path> getSnapshotFiles(File latestSnapshotRootDir) {
    return Utils.listAllRegularFilesRecursively(latestSnapshotRootDir);
  }

  /**
   * To guarantee the statemachine replication property, when {@link #write(IConsensusRequest)}
   * failed in this statemachine, Upper consensus implementation like RatisConsensus may choose to
   * retry the operation until it succeed.
   */
  interface RetryPolicy {
    /** whether we should retry according to the last write result. */
    default boolean shouldRetry(TSStatus writeResult) {
      return false;
    }

    /**
     * Use the latest write result to update final write result.
     *
     * @param previousResult previous write result
     * @param retryResult latest write result
     * @return the aggregated result upon current retry
     */
    default TSStatus updateResult(TSStatus previousResult, TSStatus retryResult) {
      return retryResult;
    }

    /**
     * sleep time before the next retry.
     *
     * @return time in millis
     */
    default long getSleepTime() {
      return 100;
    }
  }

  /** An optional API for event notifications. */
  interface EventApi {
    /**
     * Notify the {@link IStateMachine} that a new leader has been elected. Note that the new leader
     * can possibly be this server.
     *
     * @param groupId The id of this consensus group.
     * @param newLeaderId The id of the new leader node.
     */
    default void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
      // do nothing default
    }

    /**
     * Notify the {@link IStateMachine} a configuration change. This method will be invoked when a
     * newConfiguration is processed.
     *
     * @param term term of the current log entry
     * @param index index which is being updated
     * @param newConfiguration new configuration
     */
    default void notifyConfigurationChanged(long term, long index, List<Peer> newConfiguration) {
      // do nothing default
    }
  }

  /**
   * Get the {@link IStateMachine.EventApi} object.
   *
   * <p>If this {@link IStateMachine} chooses to support the optional {@link
   * IStateMachine.EventApi}, it may either implement {@link IStateMachine.EventApi} directly or
   * override this method to return an {@link IStateMachine.EventApi} object.
   *
   * @return The {@link IStateMachine.EventApi} object.
   */
  default IStateMachine.EventApi event() {
    return (IStateMachine.EventApi) this;
  }

  /**
   * Since Ratis 2.4.1, RatisConsensus allows statemachine to customize its own snapshot storage.
   * Currently only DataRegionStateMachine will use this interface.
   *
   * @return statemachine snapshot root
   */
  default File getSnapshotRoot() {
    return null;
  }
}
