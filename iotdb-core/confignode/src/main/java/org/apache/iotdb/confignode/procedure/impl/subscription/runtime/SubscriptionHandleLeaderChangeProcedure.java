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

package org.apache.iotdb.confignode.procedure.impl.subscription.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.CommitProgressHandleMetaChangePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TPullCommitProgressResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
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
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles subscription runtime leader changes. The first version focuses on pulling the latest
 * commit progress during leader migration so the new runtime owner starts from a fresher frontier.
 */
public class SubscriptionHandleLeaderChangeProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionHandleLeaderChangeProcedure.class);

  private Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToOldAndNewLeaderPairMap =
      new HashMap<>();
  private long runtimeVersion;

  public SubscriptionHandleLeaderChangeProcedure() {
    super();
  }

  public SubscriptionHandleLeaderChangeProcedure(
      final Map<TConsensusGroupId, Pair<Integer, Integer>> regionGroupToOldAndNewLeaderPairMap,
      final long runtimeVersion) {
    super();
    this.regionGroupToOldAndNewLeaderPairMap = regionGroupToOldAndNewLeaderPairMap;
    this.runtimeVersion = runtimeVersion;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.HANDLE_LEADER_CHANGE;
  }

  @Override
  public boolean executeFromValidate(final ConfigNodeProcedureEnv env) {
    LOGGER.info("SubscriptionHandleLeaderChangeProcedure: executeFromValidate");
    if (regionGroupToOldAndNewLeaderPairMap.isEmpty()) {
      return false;
    }
    for (final TopicMeta topicMeta : subscriptionInfo.get().getAllTopicMeta()) {
      if (topicMeta.getConfig().isConsensusMode()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void executeFromOperateOnConfigNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("SubscriptionHandleLeaderChangeProcedure: executeFromOperateOnConfigNodes");

    final Map<Integer, TPullCommitProgressResp> respMap = env.pullCommitProgressFromDataNodes();
    final Map<String, RegionProgress> mergedRegionProgress =
        deserializeRegionProgressMap(
            subscriptionInfo.get().getCommitProgressKeeper().getAllRegionProgress());

    for (final Map.Entry<Integer, TPullCommitProgressResp> entry : respMap.entrySet()) {
      final TPullCommitProgressResp resp = entry.getValue();
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "SubscriptionHandleLeaderChangeProcedure: failed to pull commit progress from DataNode {}, status: {}",
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
                SubscriptionHandleLeaderChangeProcedure::mergeRegionProgress);
          }
        }
      }
    }

    final TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  new CommitProgressHandleMetaChangePlan(
                      serializeRegionProgressMap(mergedRegionProgress)));
    } catch (final ConsensusException e) {
      LOGGER.warn(
          "SubscriptionHandleLeaderChangeProcedure: failed in the write API executing the consensus layer due to: ",
          e);
      throw new SubscriptionException(e.getMessage());
    }

    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("SubscriptionHandleLeaderChangeProcedure: executeFromOperateOnDataNodes");

    final Map<Integer, TPushTopicMetaResp> topicRespMap = pushTopicMetaToDataNodes(env);
    topicRespMap.forEach(
        (dataNodeId, resp) -> {
          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                "SubscriptionHandleLeaderChangeProcedure: ignored failed topic meta push to DataNode {}, status: {}",
                dataNodeId,
                resp.getStatus());
          }
        });

    final Map<Integer, TPushConsumerGroupMetaResp> consumerGroupRespMap =
        pushConsumerGroupMetaToDataNodes(env);
    consumerGroupRespMap.forEach(
        (dataNodeId, resp) -> {
          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                "SubscriptionHandleLeaderChangeProcedure: ignored failed consumer group meta push to DataNode {}, status: {}",
                dataNodeId,
                resp.getStatus());
          }
        });

    final Map<TConsensusGroupId, Pair<Integer, Integer>> runtimeLeaderPairMap =
        regionGroupToOldAndNewLeaderPairMap.entrySet().stream()
            .filter(entry -> entry.getValue().getRight() >= 0)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (!runtimeLeaderPairMap.isEmpty()) {
      final Set<Integer> readableDataNodeIds = getReadableDataNodeIds(env);
      final Map<Integer, TSStatus> runtimeRespMap =
          env.pushSubscriptionRuntimeStatesToDataNodes(runtimeLeaderPairMap, runtimeVersion);
      final String runtimePushError =
          collectRequiredRuntimePushFailures(readableDataNodeIds, runtimeRespMap);
      if (!runtimePushError.isEmpty()) {
        throw new SubscriptionException(
            String.format(
                "Failed to push subscription runtime state to readable DataNodes during leader change, details: %s",
                runtimePushError));
      }
      runtimeRespMap.forEach(
          (dataNodeId, status) -> {
            if (!readableDataNodeIds.contains(dataNodeId)
                && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              LOGGER.warn(
                  "SubscriptionHandleLeaderChangeProcedure: ignored failed subscription runtime push to unreadable DataNode {}, status: {}",
                  dataNodeId,
                  status);
            }
          });
    }
  }

  @Override
  public void rollbackFromValidate(final ConfigNodeProcedureEnv env) {
    LOGGER.info("SubscriptionHandleLeaderChangeProcedure: rollbackFromValidate");
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(final ConfigNodeProcedureEnv env) {
    LOGGER.info("SubscriptionHandleLeaderChangeProcedure: rollbackFromOperateOnConfigNodes");
  }

  @Override
  public void rollbackFromOperateOnDataNodes(final ConfigNodeProcedureEnv env) {
    LOGGER.info("SubscriptionHandleLeaderChangeProcedure: rollbackFromOperateOnDataNodes");
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.SUBSCRIPTION_HANDLE_LEADER_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(runtimeVersion, stream);
    ReadWriteIOUtils.write(regionGroupToOldAndNewLeaderPairMap.size(), stream);
    for (final Map.Entry<TConsensusGroupId, Pair<Integer, Integer>> entry :
        regionGroupToOldAndNewLeaderPairMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), stream);
      ReadWriteIOUtils.write(entry.getValue().getLeft(), stream);
      ReadWriteIOUtils.write(entry.getValue().getRight(), stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    runtimeVersion = ReadWriteIOUtils.readLong(byteBuffer);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final int dataRegionGroupId = ReadWriteIOUtils.readInt(byteBuffer);
      final int oldLeaderId = ReadWriteIOUtils.readInt(byteBuffer);
      final int newLeaderId = ReadWriteIOUtils.readInt(byteBuffer);
      regionGroupToOldAndNewLeaderPairMap.put(
          new TConsensusGroupId(TConsensusGroupType.DataRegion, dataRegionGroupId),
          new Pair<>(oldLeaderId, newLeaderId));
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SubscriptionHandleLeaderChangeProcedure that =
        (SubscriptionHandleLeaderChangeProcedure) o;
    return getProcId() == that.getProcId()
        && getCurrentState().equals(that.getCurrentState())
        && getCycles() == that.getCycles()
        && runtimeVersion == that.runtimeVersion
        && regionGroupToOldAndNewLeaderPairMap.equals(that.regionGroupToOldAndNewLeaderPairMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        runtimeVersion,
        regionGroupToOldAndNewLeaderPairMap);
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
          "SubscriptionHandleLeaderChangeProcedure: failed to deserialize region progress, key={}, summary={}",
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

  private Set<Integer> getReadableDataNodeIds(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    final Set<Integer> readableDataNodeIds =
        env
            .getConfigManager()
            .getLoadManager()
            .filterDataNodeThroughStatus(NodeStatus::isReadable)
            .stream()
            .collect(Collectors.toSet());
    if (readableDataNodeIds.isEmpty()) {
      throw new SubscriptionException(
          "No readable DataNode is available to accept subscription metadata/runtime updates during leader change");
    }
    return readableDataNodeIds;
  }

  private String collectRequiredRuntimePushFailures(
      final Set<Integer> readableDataNodeIds, final Map<Integer, TSStatus> respMap) {
    final StringBuilder errorMessageBuilder = new StringBuilder();
    for (final Integer dataNodeId : readableDataNodeIds) {
      final TSStatus status = respMap.get(dataNodeId);
      if (Objects.isNull(status)) {
        errorMessageBuilder
            .append("DataNode ")
            .append(dataNodeId)
            .append(": missing subscription runtime push response; ");
        continue;
      }
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        errorMessageBuilder
            .append("DataNode ")
            .append(dataNodeId)
            .append(": ")
            .append(status)
            .append("; ");
      }
    }
    return errorMessageBuilder.toString();
  }
}
