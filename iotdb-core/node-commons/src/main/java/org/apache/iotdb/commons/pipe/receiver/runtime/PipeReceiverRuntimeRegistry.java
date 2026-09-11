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

package org.apache.iotdb.commons.pipe.receiver.runtime;

import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeReceiverRuntimeRegistry {

  public static final String UNKNOWN = "Unknown";
  public static final String NODE_TYPE_DATA_NODE = "DataNode";
  public static final String NODE_TYPE_CONFIG_NODE = "ConfigNode";
  public static final String PROTOCOL_THRIFT = "thrift";
  public static final String PROTOCOL_AIR_GAP = "air_gap";
  public static final String PROTOCOL_WRITEBACK = "writeback";

  private static final PipeReceiverRuntimeRegistry INSTANCE = new PipeReceiverRuntimeRegistry();

  private final ConcurrentMap<String, SessionRuntimeInfo> sessionInfoMap =
      new ConcurrentHashMap<>();

  private PipeReceiverRuntimeRegistry() {}

  public static PipeReceiverRuntimeRegistry getInstance() {
    return INSTANCE;
  }

  public void registerOrUpdateSession(
      String connectionKey,
      String receiverNodeType,
      int receiverNodeId,
      String protocol,
      String senderAddress,
      int senderPort,
      String userName,
      String senderClusterId,
      String pipeName,
      long pipeCreationTime,
      long handshakeTime) {
    if (isBlank(connectionKey)) {
      return;
    }

    sessionInfoMap.compute(
        connectionKey,
        (key, oldSession) -> {
          final SessionRuntimeInfo session =
              oldSession == null ? new SessionRuntimeInfo(connectionKey) : oldSession;
          synchronized (session) {
            session.receiverNodeType = normalize(receiverNodeType);
            session.receiverNodeId = receiverNodeId;
            session.protocol = normalize(protocol);
            session.senderAddress = normalize(senderAddress);
            session.senderPort = senderPort;
            session.userName = normalize(userName);
            session.senderClusterId = normalize(senderClusterId);
            session.lastHandshakeTime = handshakeTime;
            session.lastTransferTime = Math.max(session.lastTransferTime, handshakeTime);
            session.addPipe(pipeName, pipeCreationTime);
          }
          return session;
        });
  }

  public void markTransfer(String connectionKey, long transferTime) {
    markTransfer(connectionKey, null, Long.MIN_VALUE, transferTime);
  }

  public void markTransfer(
      String connectionKey, String pipeName, long pipeCreationTime, long transferTime) {
    if (isBlank(connectionKey)) {
      return;
    }
    final SessionRuntimeInfo session = sessionInfoMap.get(connectionKey);
    if (session == null) {
      return;
    }
    synchronized (session) {
      session.lastTransferTime = Math.max(session.lastTransferTime, transferTime);
      session.addPipe(pipeName, pipeCreationTime);
    }
  }

  public void removePipe(String connectionKey, String pipeName, long pipeCreationTime) {
    if (isBlank(connectionKey)) {
      return;
    }
    final SessionRuntimeInfo session = sessionInfoMap.get(connectionKey);
    if (session == null) {
      return;
    }
    synchronized (session) {
      session.removePipe(pipeName, pipeCreationTime);
    }
  }

  public void removePipeFromAllSessions(String pipeName, long pipeCreationTime) {
    if (isBlank(pipeName)) {
      return;
    }
    for (SessionRuntimeInfo session : sessionInfoMap.values()) {
      synchronized (session) {
        session.removePipe(pipeName, pipeCreationTime);
      }
    }
  }

  public void deregister(String connectionKey) {
    if (!isBlank(connectionKey)) {
      sessionInfoMap.remove(connectionKey);
    }
  }

  public List<PipeReceiverRuntimeSnapshot> snapshot() {
    final Map<GroupKey, AggregatedRuntimeInfo> aggregatedInfoMap = new HashMap<>();
    for (SessionRuntimeInfo session : sessionInfoMap.values()) {
      synchronized (session) {
        final GroupKey groupKey =
            new GroupKey(
                session.receiverNodeType,
                session.receiverNodeId,
                session.protocol,
                session.senderClusterId,
                session.senderAddress,
                session.userName);
        AggregatedRuntimeInfo aggregatedInfo = aggregatedInfoMap.get(groupKey);
        if (aggregatedInfo == null) {
          aggregatedInfo = new AggregatedRuntimeInfo(groupKey);
          aggregatedInfoMap.put(groupKey, aggregatedInfo);
        }
        aggregatedInfo.senderPorts.add(session.senderPort);
        aggregatedInfo.connectionCount++;
        aggregatedInfo.pipeIds.addAll(session.pipeIds);
        aggregatedInfo.lastHandshakeTime =
            Math.max(aggregatedInfo.lastHandshakeTime, session.lastHandshakeTime);
        aggregatedInfo.lastTransferTime =
            Math.max(aggregatedInfo.lastTransferTime, session.lastTransferTime);
      }
    }

    final List<AggregatedRuntimeInfo> aggregatedInfos = new ArrayList<>(aggregatedInfoMap.values());
    aggregatedInfos.sort(Comparator.comparing(AggregatedRuntimeInfo::getGroupKey));

    final List<PipeReceiverRuntimeSnapshot> snapshots = new ArrayList<>(aggregatedInfos.size());
    for (AggregatedRuntimeInfo aggregatedInfo : aggregatedInfos) {
      snapshots.add(aggregatedInfo.toSnapshot());
    }
    return snapshots;
  }

  public void clear() {
    sessionInfoMap.clear();
  }

  private static String normalize(String value) {
    return isBlank(value) ? UNKNOWN : value;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static String formatPipeId(String pipeName, long pipeCreationTime) {
    if (pipeCreationTime < 0) {
      return pipeName + "@" + UNKNOWN;
    }
    return pipeName + "@" + DateTimeUtils.convertLongToDate(pipeCreationTime, "ms");
  }

  private static class SessionRuntimeInfo {
    private final String connectionKey;
    private String receiverNodeType = UNKNOWN;
    private int receiverNodeId = -1;
    private String protocol = UNKNOWN;
    private String senderAddress = UNKNOWN;
    private int senderPort = -1;
    private String userName = UNKNOWN;
    private String senderClusterId = UNKNOWN;
    private long lastHandshakeTime;
    private long lastTransferTime;
    private final TreeSet<String> pipeIds = new TreeSet<>();

    private SessionRuntimeInfo(String connectionKey) {
      this.connectionKey = connectionKey;
    }

    private void addPipe(String pipeName, long pipeCreationTime) {
      if (!isBlank(pipeName)) {
        pipeIds.add(formatPipeId(pipeName, pipeCreationTime));
      }
    }

    private void removePipe(String pipeName, long pipeCreationTime) {
      if (!isBlank(pipeName)) {
        pipeIds.remove(formatPipeId(pipeName, pipeCreationTime));
      }
    }
  }

  private static class GroupKey implements Comparable<GroupKey> {
    private final String receiverNodeType;
    private final int receiverNodeId;
    private final String protocol;
    private final String senderClusterId;
    private final String senderAddress;
    private final String userName;

    private GroupKey(
        String receiverNodeType,
        int receiverNodeId,
        String protocol,
        String senderClusterId,
        String senderAddress,
        String userName) {
      this.receiverNodeType = receiverNodeType;
      this.receiverNodeId = receiverNodeId;
      this.protocol = protocol;
      this.senderClusterId = senderClusterId;
      this.senderAddress = senderAddress;
      this.userName = userName;
    }

    @Override
    public int compareTo(GroupKey other) {
      int result = receiverNodeType.compareTo(other.receiverNodeType);
      if (result != 0) {
        return result;
      }
      result = Integer.compare(receiverNodeId, other.receiverNodeId);
      if (result != 0) {
        return result;
      }
      result = protocol.compareTo(other.protocol);
      if (result != 0) {
        return result;
      }
      result = senderClusterId.compareTo(other.senderClusterId);
      if (result != 0) {
        return result;
      }
      result = senderAddress.compareTo(other.senderAddress);
      if (result != 0) {
        return result;
      }
      return userName.compareTo(other.userName);
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof GroupKey)) {
        return false;
      }
      final GroupKey groupKey = (GroupKey) object;
      return receiverNodeId == groupKey.receiverNodeId
          && Objects.equals(receiverNodeType, groupKey.receiverNodeType)
          && Objects.equals(protocol, groupKey.protocol)
          && Objects.equals(senderClusterId, groupKey.senderClusterId)
          && Objects.equals(senderAddress, groupKey.senderAddress)
          && Objects.equals(userName, groupKey.userName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          receiverNodeType, receiverNodeId, protocol, senderClusterId, senderAddress, userName);
    }
  }

  private static class AggregatedRuntimeInfo {
    private final GroupKey groupKey;
    private final TreeSet<Integer> senderPorts = new TreeSet<>();
    private final TreeSet<String> pipeIds = new TreeSet<>();
    private int connectionCount;
    private long lastHandshakeTime;
    private long lastTransferTime;

    private AggregatedRuntimeInfo(GroupKey groupKey) {
      this.groupKey = groupKey;
    }

    private GroupKey getGroupKey() {
      return groupKey;
    }

    private PipeReceiverRuntimeSnapshot toSnapshot() {
      return new PipeReceiverRuntimeSnapshot(
          groupKey.receiverNodeType,
          groupKey.receiverNodeId,
          groupKey.protocol,
          groupKey.senderAddress,
          joinIntegerSet(senderPorts),
          connectionCount,
          pipeIds.size(),
          pipeIds.isEmpty() ? UNKNOWN : joinStringSet(pipeIds),
          groupKey.userName,
          groupKey.senderClusterId,
          lastHandshakeTime,
          lastTransferTime);
    }
  }

  private static String joinIntegerSet(TreeSet<Integer> values) {
    final StringJoiner joiner = new StringJoiner(",");
    boolean hasUnknown = false;
    for (Integer value : values) {
      if (value < 0) {
        if (!hasUnknown) {
          joiner.add(UNKNOWN);
          hasUnknown = true;
        }
        continue;
      }
      joiner.add(String.valueOf(value));
    }
    return joiner.toString();
  }

  private static String joinStringSet(TreeSet<String> values) {
    final StringJoiner joiner = new StringJoiner(";");
    for (String value : values) {
      joiner.add(value);
    }
    return joiner.toString();
  }
}
