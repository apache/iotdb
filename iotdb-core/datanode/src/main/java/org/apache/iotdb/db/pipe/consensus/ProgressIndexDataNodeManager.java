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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ProgressIndexManager;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProgressIndexDataNodeManager implements ProgressIndexManager {
  private final Map<ConsensusGroupId, ProgressIndex> groupId2MaxProgressIndex;

  public ProgressIndexDataNodeManager() {
    this.groupId2MaxProgressIndex = new ConcurrentHashMap<>();

    recoverMaxProgressIndexFromDataRegion();
  }

  private void recoverMaxProgressIndexFromDataRegion() {
    StorageEngine.getInstance()
        .getAllDataRegionIds()
        .forEach(
            dataRegionId -> {
              final TsFileManager tsFileManager =
                  StorageEngine.getInstance().getDataRegion(dataRegionId).getTsFileManager();

              ProgressIndex maxProgressIndex = MinimumProgressIndex.INSTANCE;
              tsFileManager.getTsFileList(true).stream()
                  .map(TsFileResource::getMaxProgressIndex)
                  .forEach(maxProgressIndex::updateToMinimumEqualOrIsAfterProgressIndex);
              tsFileManager.getTsFileList(false).stream()
                  .map(TsFileResource::getMaxProgressIndex)
                  .forEach(maxProgressIndex::updateToMinimumEqualOrIsAfterProgressIndex);
              groupId2MaxProgressIndex
                  .computeIfAbsent(dataRegionId, o -> MinimumProgressIndex.INSTANCE)
                  .updateToMinimumEqualOrIsAfterProgressIndex(maxProgressIndex);
            });

    // TODO: update deletion progress index
  }

  @Override
  public ProgressIndex getProgressIndex(ConsensusPipeName consensusPipeName) {
    return PipeAgent.task()
        .getPipeTaskProgressIndex(
            consensusPipeName.toString(), consensusPipeName.getConsensusGroupId().getId());
  }

  @Override
  public ProgressIndex assignProgressIndex(ConsensusGroupId consensusGroupId) {
    final ProgressIndex progressIndex =
        PipeAgent.runtime().assignSimpleProgressIndexForPipeConsensus();
    groupId2MaxProgressIndex
        .computeIfAbsent(consensusGroupId, o -> MinimumProgressIndex.INSTANCE)
        .updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
    return progressIndex;
  }

  @Override
  public ProgressIndex getMaxAssignedProgressIndex(ConsensusGroupId consensusGroupId) {
    return groupId2MaxProgressIndex.getOrDefault(consensusGroupId, MinimumProgressIndex.INSTANCE);
  }
}
