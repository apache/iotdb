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
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.util.function.Function;

@ThreadSafe
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
   * Take a snapshot of current statemachine. All files are required to be stored under snapshotDir,
   * which is a sub-directory of the StorageDir in Consensus
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
}
