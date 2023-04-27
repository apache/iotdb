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
package org.apache.iotdb.db.mpp.plan.planner.plan;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class FragmentInstance implements IConsensusRequest {

  private final Logger logger = LoggerFactory.getLogger(FragmentInstance.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final FragmentInstanceId id;
  private final QueryType type;
  // The reference of PlanFragment which this instance is generated from
  private final PlanFragment fragment;

  // Where the FragmentInstance should run
  private ExecutorType executorType;

  private TDataNodeLocation hostDataNode;

  private Filter timeFilter;

  private final long timeOut;

  private boolean isRoot;

  private final SessionInfo sessionInfo;

  // We can add some more params for a specific FragmentInstance
  // So that we can make different FragmentInstance owns different data range.

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      Filter timeFilter,
      QueryType type,
      long timeOut,
      SessionInfo sessionInfo) {
    this.fragment = fragment;
    this.timeFilter = timeFilter;
    this.id = id;
    this.type = type;
    this.timeOut = timeOut > 0 ? timeOut : config.getQueryTimeoutThreshold();
    this.isRoot = false;
    this.sessionInfo = sessionInfo;
  }

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      Filter timeFilter,
      QueryType type,
      long timeOut,
      SessionInfo sessionInfo,
      boolean isRoot) {
    this(fragment, id, timeFilter, type, timeOut, sessionInfo);
    this.isRoot = isRoot;
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

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public QueryType getType() {
    return type;
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
    boolean hasTimeFilter = ReadWriteIOUtils.readBool(buffer);
    Filter timeFilter = hasTimeFilter ? FilterFactory.deserialize(buffer) : null;
    QueryType queryType = QueryType.values()[ReadWriteIOUtils.readInt(buffer)];
    FragmentInstance fragmentInstance =
        new FragmentInstance(planFragment, id, timeFilter, queryType, timeOut, sessionInfo);
    boolean hasHostDataNode = ReadWriteIOUtils.readBool(buffer);
    fragmentInstance.hostDataNode =
        hasHostDataNode ? ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer) : null;
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
      ReadWriteIOUtils.write(timeFilter != null, outputStream);
      if (timeFilter != null) {
        timeFilter.serialize(outputStream);
      }
      ReadWriteIOUtils.write(type.ordinal(), outputStream);
      ReadWriteIOUtils.write(hostDataNode != null, outputStream);
      if (hostDataNode != null) {
        ThriftCommonsSerDeUtils.serializeTDataNodeLocation(hostDataNode, outputStream);
      }
      return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
    } catch (IOException e) {
      logger.error("Unexpected error occurs when serializing this FragmentInstance.", e);
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
        && Objects.equals(timeFilter, instance.timeFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, fragment, executorType, hostDataNode, timeFilter);
  }

  public TDataNodeLocation getHostDataNode() {
    return hostDataNode;
  }

  public long getTimeOut() {
    return timeOut;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }
}
