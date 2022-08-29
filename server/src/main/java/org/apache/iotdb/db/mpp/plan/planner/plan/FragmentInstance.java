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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
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

  // The Region where the FragmentInstance should run
  private TRegionReplicaSet regionReplicaSet;

  private TDataNodeLocation hostDataNode;

  private Filter timeFilter;

  private final long timeOut;

  private boolean isRoot;

  // We can add some more params for a specific FragmentInstance
  // So that we can make different FragmentInstance owns different data range.

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      Filter timeFilter,
      QueryType type,
      long timeOut) {
    this.fragment = fragment;
    this.timeFilter = timeFilter;
    this.id = id;
    this.type = type;
    this.timeOut = timeOut > 0 ? timeOut : config.getQueryTimeoutThreshold();
    this.isRoot = false;
  }

  public FragmentInstance(
      PlanFragment fragment,
      FragmentInstanceId id,
      Filter timeFilter,
      QueryType type,
      long timeOut,
      boolean isRoot) {
    this(fragment, id, timeFilter, type, timeOut);
    this.isRoot = isRoot;
  }

  public TRegionReplicaSet getDataRegionId() {
    return regionReplicaSet;
  }

  public void setDataRegionAndHost(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
    // TODO: (xingtanzjr) We select the first Endpoint as the default target host for current
    // instance
    if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      this.hostDataNode = regionReplicaSet.getDataNodeLocations().get(0);
    } else {
      // Although the logic to set hostDataNode for standalone is the same as
      // cluster mode currently, it may be made different in later change.
      // So we keep the conditions here.
      this.hostDataNode = regionReplicaSet.getDataNodeLocations().get(0);
    }
  }

  // Although the HostDataNode is set in method setDataRegionAndHost(),
  // we still keep another method for customized needs
  public void setHostDataNode(TDataNodeLocation hostDataNode) {
    this.hostDataNode = hostDataNode;
  }

  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
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

  public String getDownstreamInfo() {
    PlanNode root = getFragment().getPlanNodeTree();
    if (root instanceof FragmentSinkNode) {
      FragmentSinkNode sink = (FragmentSinkNode) root;
      return String.format(
          "(%s, %s, %s)",
          sink.getDownStreamEndpoint(),
          sink.getDownStreamInstanceId(),
          sink.getDownStreamPlanNodeId());
    }
    return "<No downstream>";
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
    boolean hasTimeFilter = ReadWriteIOUtils.readBool(buffer);
    Filter timeFilter = hasTimeFilter ? FilterFactory.deserialize(buffer) : null;
    QueryType queryType = QueryType.values()[ReadWriteIOUtils.readInt(buffer)];
    FragmentInstance fragmentInstance =
        new FragmentInstance(planFragment, id, timeFilter, queryType, timeOut);
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
        && Objects.equals(regionReplicaSet, instance.regionReplicaSet)
        && Objects.equals(hostDataNode, instance.hostDataNode)
        && Objects.equals(timeFilter, instance.timeFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, fragment, regionReplicaSet, hostDataNode, timeFilter);
  }

  public TDataNodeLocation getHostDataNode() {
    return hostDataNode;
  }

  public long getTimeOut() {
    return timeOut;
  }
}
