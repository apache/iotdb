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
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

final class ConsensusSubscriptionWalRetention {

  private ConsensusSubscriptionWalRetention() {}

  static String generateRetentionId(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return consumerGroupId + "\u0000" + topicName + "\u0000" + regionId;
  }

  static boolean isProgressMonotonic(
      final RegionProgress previousProgress, final RegionProgress currentProgress) {
    if (Objects.isNull(previousProgress)) {
      return true;
    }
    if (Objects.isNull(currentProgress)) {
      return false;
    }
    for (final Map.Entry<WriterId, WriterProgress> entry :
        previousProgress.getWriterPositions().entrySet()) {
      final WriterProgress previous = entry.getValue();
      final WriterProgress current = currentProgress.getWriterPositions().get(entry.getKey());
      if (Objects.isNull(current)
          || current.getPhysicalTime() < previous.getPhysicalTime()
          || (current.getPhysicalTime() == previous.getPhysicalTime()
              && current.getLocalSeq() < previous.getLocalSeq())) {
        return false;
      }
    }
    return true;
  }

  private static final class RetainedWalVersionCalculator {

    private final ConsensusGroupId consensusGroupId;
    private ConsensusReqReader lastConsensusReqReader;
    private RegionProgress lastCommittedRegionProgress;
    private long lastCoveredWalVersionId = -1L;

    private RetainedWalVersionCalculator(final ConsensusGroupId consensusGroupId) {
      this.consensusGroupId = consensusGroupId;
    }

    private synchronized long compute(
        final ConsensusReqReader consensusReqReader, final RegionProgress committedRegionProgress) {
      if (lastConsensusReqReader != consensusReqReader
          || !isProgressMonotonic(lastCommittedRegionProgress, committedRegionProgress)) {
        lastCoveredWalVersionId = -1L;
      }
      lastConsensusReqReader = consensusReqReader;
      lastCommittedRegionProgress = committedRegionProgress;
      if (!(consensusReqReader instanceof WALNode)) {
        return 0L;
      }

      final WALNode walNode = (WALNode) consensusReqReader;
      final long currentWalVersion = walNode.getCurrentWALFileVersion();
      if (currentWalVersion <= lastCoveredWalVersionId) {
        lastCoveredWalVersionId = -1L;
      }
      final File[] walFiles = walNode.getSortedWalFilesSnapshot();
      if (Objects.isNull(walFiles) || walFiles.length == 0) {
        return Math.max(0L, currentWalVersion);
      }

      for (final File walFile : walFiles) {
        final long versionId = WALFileUtils.parseVersionId(walFile.getName());
        if (versionId >= currentWalVersion) {
          return Math.max(0L, currentWalVersion);
        }
        if (versionId <= lastCoveredWalVersionId
            || ProgressWALIterator.isHeaderOnlyWalFile(walFile)) {
          continue;
        }

        try (final ProgressWALReader reader = new ProgressWALReader(walFile)) {
          if (!ConsensusPrefetchingQueue.WalFileCommitRequirement.fromMetadata(
                  consensusGroupId.toString(), reader.getMetaData())
              .isCoveredBy(committedRegionProgress)) {
            return versionId;
          }
          lastCoveredWalVersionId = versionId;
        } catch (final IOException e) {
          return versionId;
        }
      }
      return Math.max(0L, currentWalVersion);
    }
  }

  static LongSupplier createCommittedRetainedMinVersionIdSupplier(
      final ConsensusGroupId consensusGroupId,
      final Supplier<ConsensusReqReader> consensusReqReaderSupplier,
      final Supplier<RegionProgress> committedRegionProgressSupplier) {
    final RetainedWalVersionCalculator calculator =
        new RetainedWalVersionCalculator(consensusGroupId);
    return () ->
        calculator.compute(consensusReqReaderSupplier.get(), committedRegionProgressSupplier.get());
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
        createCommittedRetainedMinVersionIdSupplier(
            regionId,
            serverImpl::getConsensusReqReader,
            () -> commitManager.getCommittedRegionProgress(consumerGroupId, topicName, regionId)));
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
