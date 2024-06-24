/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.plan;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class FragmentInstance implements IConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstance.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final FragmentInstanceId id;
  private final QueryType type;
  // The reference of PlanFragment which this instance is generated from
  private final PlanFragment fragment;

  // Where the FragmentInstance should run
  private ExecutorType executorType;

  private TDataNodeLocation hostDataNode;

  private final TimePredicate globalTimePredicate;

  private final long timeOut;

  private boolean isRoot;

  private final SessionInfo sessionInfo;

  // The num of all FI on the dispatched DataNode in this query
  private int dataNodeFINum;

  private boolean isHighestPriority;

  // indicate which index we are retrying
  private transient int nextRetryIndex = 0;

  // If this query is an EXPLAIN ANALYZE query
  // We need to cache and calculate the statistics of this FragmentInstance if it is.
  private boolean isExplainAnalyze = false;

  // We can add some more params for a specific FragmentInstance
  // So that we can make different FragmentInstance owns different data range.

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      TimePredicate globalTimePredicate,
      QueryType type,
      long timeOut,
      SessionInfo sessionInfo) {
    this.fragment = fragment;
    this.globalTimePredicate = globalTimePredicate;
    this.id = id;
    this.type = type;
    this.timeOut = timeOut > 0 ? timeOut : CONFIG.getQueryTimeoutThreshold();
    this.isRoot = false;
    this.sessionInfo = sessionInfo;
  }

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      TimePredicate globalTimePredicate,
      QueryType type,
      long timeOut,
      SessionInfo sessionInfo,
      boolean isExplainAnalyze,
      boolean isRoot) {
    this(fragment, id, globalTimePredicate, type, timeOut, sessionInfo);
    this.isRoot = isRoot;
    this.isExplainAnalyze = isExplainAnalyze;
  }

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      QueryType type,
      long timeOut,
      SessionInfo sessionInfo,
      boolean isExplainAnalyze,
      boolean isRoot) {
    this(fragment, id, null, type, timeOut, sessionInfo);
    this.isRoot = isRoot;
    this.isExplainAnalyze = isExplainAnalyze;
  }

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      TimePredicate globalTimePredicate,
      QueryType type,
      long timeOut,
      SessionInfo sessionInfo,
      int dataNodeFINum) {
    this(fragment, id, globalTimePredicate, type, timeOut, sessionInfo);
    this.dataNodeFINum = dataNodeFINum;
  }

  public void setExecutorAndHost(ExecutorType executorType) {
    if (executorType == null) {
      return;
    }
    this.executorType = executorType;
    this.hostDataNode = executorType.getDataNodeLocation();
  }

  // Although the HostDataNode is set in method setDataRegionAndHost(),
  // we still keep another method for customized needs
  public void setHostDataNode(TDataNodeLocation hostDataNode) {
    this.hostDataNode = hostDataNode;
  }

  public ExecutorType getExecutorType() {
    return executorType;
  }

  @TestOnly
  public void setExecutorType(ExecutorType executorType) {
    this.executorType = executorType;
  }

  public TRegionReplicaSet getRegionReplicaSet() {
    return executorType.getRegionReplicaSet();
  }

  public PlanFragment getFragment() {
    return fragment;
  }

  public FragmentInstanceId getId() {
    return id;
  }

  public boolean isRoot() {
    return isRoot;
  }

  public boolean isHighestPriority() {
    return isHighestPriority;
  }

  public void setHighestPriority(boolean highestPriority) {
    isHighestPriority = highestPriority;
  }

  public TimePredicate getGlobalTimePredicate() {
    return globalTimePredicate;
  }

  public QueryType getType() {
    return type;
  }

  public int getDataNodeFINum() {
    return dataNodeFINum;
  }

  public void setDataNodeFINum(int dataNodeFINum) {
    this.dataNodeFINum = dataNodeFINum;
  }

  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append(String.format("FragmentInstance-%s:", getId()));
    ret.append(
        String.format(
            "Host: %s ",
            getHostDataNode() == null
                ? "Not set"
                : getHostDataNode().dataNodeId + " - " + getHostDataNode().internalEndPoint));
    ret.append(
        String.format(
            "Region: %s ",
            getRegionReplicaSet() == null ? "Not set" : getRegionReplicaSet().getRegionId()));
    ret.append("\n---- Plan Node Tree ----\n");
    ret.append(PlanNodeUtil.nodeToString(getFragment().getPlanNodeTree()));
    ret.append(String.format("timeOut-%s:", getTimeOut()));
    return ret.toString();
  }

  public static FragmentInstance deserializeFrom(ByteBuffer buffer) {
    FragmentInstanceId id = FragmentInstanceId.deserialize(buffer);
    PlanFragment planFragment = PlanFragment.deserialize(buffer);
    long timeOut = ReadWriteIOUtils.readLong(buffer);
    boolean hasSessionInfo = ReadWriteIOUtils.readBool(buffer);
    SessionInfo sessionInfo = hasSessionInfo ? SessionInfo.deserializeFrom(buffer) : null;
    boolean hasTimePredicate = ReadWriteIOUtils.readBool(buffer);
    TimePredicate globalTimePredicate = hasTimePredicate ? TimePredicate.deserialize(buffer) : null;
    QueryType queryType = QueryType.values()[ReadWriteIOUtils.readInt(buffer)];
    int dataNodeFINum = ReadWriteIOUtils.readInt(buffer);
    FragmentInstance fragmentInstance =
        new FragmentInstance(
            planFragment, id, globalTimePredicate, queryType, timeOut, sessionInfo, dataNodeFINum);
    boolean hasHostDataNode = ReadWriteIOUtils.readBool(buffer);
    fragmentInstance.hostDataNode =
        hasHostDataNode ? ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer) : null;
    fragmentInstance.isExplainAnalyze = ReadWriteIOUtils.readBool(buffer);
    return fragmentInstance;
  }

  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      id.serialize(outputStream);
      fragment.serialize(outputStream);
      ReadWriteIOUtils.write(timeOut, outputStream);
      ReadWriteIOUtils.write(sessionInfo != null, outputStream);
      if (sessionInfo != null) {
        sessionInfo.serialize(outputStream);
      }
      ReadWriteIOUtils.write(globalTimePredicate != null, outputStream);
      if (globalTimePredicate != null) {
        globalTimePredicate.serialize(outputStream);
      }
      ReadWriteIOUtils.write(type.ordinal(), outputStream);
      ReadWriteIOUtils.write(dataNodeFINum, outputStream);
      ReadWriteIOUtils.write(hostDataNode != null, outputStream);
      if (hostDataNode != null) {
        ThriftCommonsSerDeUtils.serializeTDataNodeLocation(hostDataNode, outputStream);
      }
      ReadWriteIOUtils.write(isExplainAnalyze, outputStream);
      return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
    } catch (IOException e) {
      LOGGER.error("Unexpected error occurs when serializing this FragmentInstance.", e);
      throw new SerializationRunTimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FragmentInstance instance = (FragmentInstance) o;
    return Objects.equals(id, instance.id)
        && type == instance.type
        && Objects.equals(fragment, instance.fragment)
        && Objects.equals(executorType, instance.executorType)
        && Objects.equals(hostDataNode, instance.hostDataNode)
        && Objects.equals(globalTimePredicate, instance.globalTimePredicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, fragment, executorType, hostDataNode, globalTimePredicate);
  }

  public TDataNodeLocation getHostDataNode() {
    return hostDataNode;
  }

  public TDataNodeLocation getNextRetriedHostDataNode() {
    nextRetryIndex =
        (nextRetryIndex + 1) % executorType.getRegionReplicaSet().getDataNodeLocations().size();
    this.hostDataNode =
        executorType.getRegionReplicaSet().getDataNodeLocations().get(nextRetryIndex);
    return hostDataNode;
  }

  public long getTimeOut() {
    return timeOut;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  public boolean isExplainAnalyze() {
    return isExplainAnalyze;
  }
}
