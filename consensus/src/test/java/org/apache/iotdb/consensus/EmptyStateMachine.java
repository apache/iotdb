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
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import java.io.File;

public class EmptyStateMachine implements IStateMachine, IStateMachine.EventApi {

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest IConsensusRequest) {
    return new TSStatus(0);
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    return request;
  }

  @Override
  public DataSet read(IConsensusRequest IConsensusRequest) {
    return null;
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return false;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {}
}
