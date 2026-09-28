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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ProgressWALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

final class ConsensusSubscriptionWalRetention {

  private ConsensusSubscriptionWalRetention() {}

  static String generateRetentionId(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return consumerGroupId + "\u0000" + topicName + "\u0000" + regionId;
  }

  static long computeCommittedRetainedMinVersionId(
      final ConsensusReqReader consensusReqReader,
      final ConsensusGroupId consensusGroupId,
      final RegionProgress committedRegionProgress) {
    if (!(consensusReqReader instanceof WALNode)) {
      return 0L;
    }

    final WALNode walNode = (WALNode) consensusReqReader;
    final long currentWalVersion = walNode.getCurrentWALFileVersion();
    final File[] walFiles = WALFileUtils.listAllWALFiles(walNode.getLogDirectory());
    if (Objects.isNull(walFiles) || walFiles.length == 0) {
      return Math.max(0L, currentWalVersion);
    }

    WALFileUtils.ascSortByVersionId(walFiles);
    for (final File walFile : walFiles) {
      final long versionId = WALFileUtils.parseVersionId(walFile.getName());
      if (versionId >= currentWalVersion) {
        return Math.max(0L, currentWalVersion);
      }
      if (ProgressWALIterator.isHeaderOnlyWalFile(walFile)) {
        continue;
      }

      try (final ProgressWALReader reader = new ProgressWALReader(walFile)) {
        if (!ConsensusPrefetchingQueue.WalFileCommitRequirement.fromMetadata(
                consensusGroupId.toString(), reader.getMetaData())
            .isCoveredBy(committedRegionProgress)) {
          return versionId;
        }
      } catch (final IOException e) {
        return versionId;
      }
    }
    return Math.max(0L, currentWalVersion);
  }

  static void registerDetached(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final IoTConsensusServerImpl serverImpl,
      final SubscriptionWalRetentionPolicy retentionPolicy,
      final ConsensusSubscriptionCommitManager commitManager) {
    serverImpl.registerDetachedSubscriptionRetention(
        generateRetentionId(consumerGroupId, topicName, regionId),
        retentionPolicy,
        () ->
            computeCommittedRetainedMinVersionId(
                serverImpl.getConsensusReqReader(),
                regionId,
                commitManager.getCommittedRegionProgress(consumerGroupId, topicName, regionId)));
  }

  static void unregisterDetached(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final IoTConsensusServerImpl serverImpl) {
    serverImpl.deregisterDetachedSubscriptionRetention(
        generateRetentionId(consumerGroupId, topicName, regionId));
  }
}
