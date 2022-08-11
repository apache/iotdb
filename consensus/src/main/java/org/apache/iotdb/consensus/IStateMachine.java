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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
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
   * apply a write-request from user
   *
   * @param IConsensusRequest write request
   */
  TSStatus write(IConsensusRequest IConsensusRequest);

  /**
   * read local data and return
   *
   * @param IConsensusRequest read request
   */
  DataSet read(IConsensusRequest IConsensusRequest);

  /**
   * Take a snapshot of current statemachine. All files are required to be stored under snapshotDir,
   * which is a subdirectory of the StorageDir in Consensus
   *
   * @param snapshotDir required storage dir
   * @return true if snapshot is successfully taken
   */
  boolean takeSnapshot(File snapshotDir);

  /**
   * Load the latest snapshot from given dir
   *
   * @param latestSnapshotRootDir dir where the latest snapshot sits
   */
  void loadSnapshot(File latestSnapshotRootDir);

  /**
   * given a snapshot dir, ask statemachine to provide all snapshot files.
   *
   * <p>DataRegion may take snapshot at a different disk and only store a log file containing file
   * paths. So statemachine is required to read the log file and provide the real snapshot file
   * paths.
   *
   * @param latestSnapshotRootDir dir where the latest snapshot sits
   * @return List of real snapshot files. If the returned list is null, consensus implementations
   *     will visit and add all files under this give latestSnapshotRootDir.
   */
  default List<File> getSnapshotFiles(File latestSnapshotRootDir) {
    return null;
  }

  /** An optional API for event notifications. */
  interface EventApi {
    /**
     * Notify the {@link IStateMachine} that a new leader has been elected. Note that the new leader
     * can possibly be this server.
     *
     * @param groupId The id of this consensus group.
     * @param newLeader The id of the new leader.
     */
    default void notifyLeaderChanged(ConsensusGroupId groupId, TEndPoint newLeader) {}

    /**
     * Notify the {@link IStateMachine} a configuration change. This method will be invoked when a
     * newConfiguration is processed.
     *
     * @param term term of the current log entry
     * @param index index which is being updated
     * @param newConfiguration new configuration
     */
    default void notifyConfigurationChanged(long term, long index, List<Peer> newConfiguration) {}
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
}
