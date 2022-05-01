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

package org.apache.iotdb.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.SnapshotMeta;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.function.Function;

public interface IStateMachine {

  interface Registry extends Function<ConsensusGroupId, IStateMachine> {}

  void start();

  void stop();

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
   * IConsensus will periodically take the snapshot on both log and statemachine Data
   *
   * @param metadata the metadata IConsensus want IStateMachine to preserve. NOTICE: the more
   *     updated snapshot will have lexicographically larger metadata. This property should be
   *     guaranteed by every IConsensus implementation. IStateMachine can use the metadata to sort
   *     or label snapshot. e.g, metadata is byteBuffer("123_456"), the statemachine can create a
   *     directory ${snapshotDir}/123_456/ and store all files under this directory
   * @param snapshotDir the root dir of snapshot files
   * @return true if snapshot successfully taken
   */
  boolean takeSnapshot(ByteBuffer metadata, File snapshotDir);

  /**
   * When recover from crash / leader installSnapshot to follower, this method is called.
   * IStateMachine is required to find the latest snapshot in snapshotDir.
   *
   * @param snapshotDir the root dir of snapshot files
   * @return latest snapshot info (metadata + snapshot files)
   */
  SnapshotMeta getLatestSnapshot(File snapshotDir);

  /**
   * When recover from crash / follower installSnapshot from leader, this method is called.
   * IStateMachine is required to load the given snapshot.
   *
   * @param latest is the latest snapshot given
   */
  void loadSnapshot(SnapshotMeta latest);

  /**
   * IConsensus will periodically clean up old snapshots. This method is called to inform
   * IStateMachine to remove out-dated snapshot.
   *
   * @param snapshotDir the root dir of snapshot files
   */
  void cleanUpOldSnapshots(File snapshotDir);
}
