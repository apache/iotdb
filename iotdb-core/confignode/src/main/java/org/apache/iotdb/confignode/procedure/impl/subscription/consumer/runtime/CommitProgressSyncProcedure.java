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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.CommitProgressHandleMetaChangePlan;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TPullCommitProgressResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Periodically pulls commit progress from all DataNodes and persists the merged result to
 * ConfigNode consensus.
 */
public class CommitProgressSyncProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommitProgressSyncProcedure.class);

  private static final long MIN_EXECUTION_INTERVAL_MS =
      PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 / 2;
  private static final AtomicLong LAST_EXECUTION_TIME = new AtomicLong(0);

  public CommitProgressSyncProcedure() {
    super();
  }

  @Override
  protected AtomicReference<SubscriptionInfo> acquireLockInternal(
      ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return configNodeProcedureEnv
        .getConfigManager()
        .getSubscriptionManager()
        .getSubscriptionCoordinator()
        .tryLock();
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    if (System.currentTimeMillis() - LAST_EXECUTION_TIME.get() < MIN_EXECUTION_INTERVAL_MS) {
      subscriptionInfo = null;
      LOGGER.info(
          "CommitProgressSyncProcedure: acquireLock, skip the procedure due to the last execution time {}",
          LAST_EXECUTION_TIME.get());
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    return super.acquireLock(configNodeProcedureEnv);
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.SYNC_COMMIT_PROGRESS;
  }

  @Override
  public boolean executeFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("CommitProgressSyncProcedure: executeFromValidate");
    LAST_EXECUTION_TIME.set(System.currentTimeMillis());
    return true;
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CommitProgressSyncProcedure: executeFromOperateOnConfigNodes");

    // 1. Pull commit progress from all DataNodes
    final Map<Integer, TPullCommitProgressResp> respMap = env.pullCommitProgressFromDataNodes();

    // 2. Merge all DataNode responses with existing progress using Math::max
    final Map<String, RegionProgress> mergedRegionProgress =
        deserializeRegionProgressMap(
            subscriptionInfo.get().getCommitProgressKeeper().getAllRegionProgress());

    for (Map.Entry<Integer, TPullCommitProgressResp> entry : respMap.entrySet()) {
      final TPullCommitProgressResp resp = entry.getValue();
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to pull commit progress from DataNode {}, status: {}",
            entry.getKey(),
            resp.getStatus());
        continue;
      }
      if (resp.isSetCommitRegionProgress()) {
        for (final Map.Entry<String, ByteBuffer> progressEntry :
            resp.getCommitRegionProgress().entrySet()) {
          final RegionProgress incomingProgress =
              deserializeRegionProgress(progressEntry.getKey(), progressEntry.getValue());
          if (Objects.nonNull(incomingProgress)) {
            mergedRegionProgress.merge(
                progressEntry.getKey(),
                incomingProgress,
                CommitProgressSyncProcedure::mergeRegionProgress);
          }
        }
      }
    }

    // 3. Write the merged progress to consensus
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  new CommitProgressHandleMetaChangePlan(
                      serializeRegionProgressMap(mergedRegionProgress)));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CommitProgressSyncProcedure: executeFromOperateOnDataNodes (no-op)");
    // No need to push back to DataNodes
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("CommitProgressSyncProcedure: rollbackFromValidate");
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CommitProgressSyncProcedure: rollbackFromOperateOnConfigNodes");
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CommitProgressSyncProcedure: rollbackFromOperateOnDataNodes");
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.COMMIT_PROGRESS_SYNC_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CommitProgressSyncProcedure;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  private static Map<String, RegionProgress> deserializeRegionProgressMap(
      final Map<String, ByteBuffer> serializedRegionProgressMap) {
    final Map<String, RegionProgress> result = new HashMap<>();
    for (final Map.Entry<String, ByteBuffer> entry : serializedRegionProgressMap.entrySet()) {
      final RegionProgress regionProgress =
          deserializeRegionProgress(entry.getKey(), entry.getValue());
      if (Objects.nonNull(regionProgress)) {
        result.put(entry.getKey(), regionProgress);
      }
    }
    return result;
  }

  private static Map<String, ByteBuffer> serializeRegionProgressMap(
      final Map<String, RegionProgress> regionProgressMap) {
    final Map<String, ByteBuffer> result = new HashMap<>();
    for (final Map.Entry<String, RegionProgress> entry : regionProgressMap.entrySet()) {
      final ByteBuffer serialized = serializeRegionProgress(entry.getValue());
      if (Objects.nonNull(serialized)) {
        result.put(entry.getKey(), serialized);
      }
    }
    return result;
  }

  private static RegionProgress deserializeRegionProgress(
      final String key, final ByteBuffer buffer) {
    if (Objects.isNull(buffer)) {
      return null;
    }
    final ByteBuffer duplicate = buffer.slice();
    try {
      return RegionProgress.deserialize(duplicate);
    } catch (final RuntimeException e) {
      LOGGER.warn(
          "CommitProgressSyncProcedure: failed to deserialize region progress, key={}, summary={}",
          key,
          summarizeRegionProgressPayload(buffer),
          e);
      throw e;
    }
  }

  private static String summarizeRegionProgressPayload(final ByteBuffer buffer) {
    if (Objects.isNull(buffer)) {
      return "null";
    }
    final int position = buffer.position();
    final int limit = buffer.limit();
    final int capacity = buffer.capacity();
    final ByteBuffer duplicate = buffer.slice();
    final int remaining = duplicate.remaining();
    final String firstIntSummary;
    if (remaining >= Integer.BYTES) {
      final int firstInt = duplicate.getInt();
      firstIntSummary = firstInt + "(0x" + String.format("%08x", firstInt) + ")";
      duplicate.position(0);
    } else {
      firstIntSummary = "n/a";
    }
    final int sampleLength = Math.min(16, remaining);
    final byte[] sample = new byte[sampleLength];
    duplicate.get(sample, 0, sampleLength);
    return "pos="
        + position
        + ", limit="
        + limit
        + ", capacity="
        + capacity
        + ", remaining="
        + remaining
        + ", firstInt="
        + firstIntSummary
        + ", firstBytes="
        + bytesToHex(sample);
  }

  private static String bytesToHex(final byte[] bytes) {
    if (Objects.isNull(bytes) || bytes.length == 0) {
      return "<empty>";
    }
    final StringBuilder builder = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
  }

  private static ByteBuffer serializeRegionProgress(final RegionProgress regionProgress) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos)) {
      regionProgress.serialize(dos);
      dos.flush();
      return ByteBuffer.wrap(baos.toByteArray()).asReadOnlyBuffer();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize region progress " + regionProgress, e);
    }
  }

  private static RegionProgress mergeRegionProgress(
      final RegionProgress left, final RegionProgress right) {
    final Map<WriterId, WriterProgress> merged = new LinkedHashMap<>(left.getWriterPositions());
    for (final Map.Entry<WriterId, WriterProgress> entry : right.getWriterPositions().entrySet()) {
      merged.merge(
          entry.getKey(),
          entry.getValue(),
          (oldProgress, newProgress) ->
              compareWriterProgress(newProgress, oldProgress) > 0 ? newProgress : oldProgress);
    }
    return new RegionProgress(merged);
  }

  private static int compareWriterProgress(
      final WriterProgress leftProgress, final WriterProgress rightProgress) {
    int cmp = Long.compare(leftProgress.getPhysicalTime(), rightProgress.getPhysicalTime());
    if (cmp != 0) {
      return cmp;
    }
    return Long.compare(leftProgress.getLocalSeq(), rightProgress.getLocalSeq());
  }
}
