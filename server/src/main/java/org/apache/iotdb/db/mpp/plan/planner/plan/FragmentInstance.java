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
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

public class FragmentInstance implements IConsensusRequest {

  private final FragmentInstanceId id;
  private final QueryType type;
  // The reference of PlanFragment which this instance is generated from
  private final PlanFragment fragment;

  // The Region where the FragmentInstance should run
  private TRegionReplicaSet regionReplicaSet;

  private TDataNodeLocation hostDataNode;

  private Filter timeFilter;

  // We can add some more params for a specific FragmentInstance
  // So that we can make different FragmentInstance owns different data range.

  public FragmentInstance(
      PlanFragment fragment, FragmentInstanceId id, Filter timeFilter, QueryType type) {
    this.fragment = fragment;
    this.timeFilter = timeFilter;
    this.id = id;
    this.type = type;
  }

  public TRegionReplicaSet getDataRegionId() {
    return regionReplicaSet;
  }

  public void setDataRegionAndHost(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
    // TODO: (xingtanzjr) We select the first Endpoint as the default target host for current
    // instance
    this.hostDataNode = regionReplicaSet.getDataNodeLocations().get(0);
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

  public String getDownstreamInfo() {
    PlanNode root = getFragment().getRoot();
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
            "Host: %s", getHostDataNode() == null ? "Not set" : getHostDataNode().dataNodeId));
    ret.append(
        String.format(
            "Region: %s",
            getRegionReplicaSet() == null ? "Not set" : getRegionReplicaSet().getRegionId()));
    ret.append("---- Plan Node Tree ----\n");
    ret.append(PlanNodeUtil.nodeToString(getFragment().getRoot()));
    return ret.toString();
  }

  public static FragmentInstance deserializeFrom(ByteBuffer buffer) {
    FragmentInstanceId id = FragmentInstanceId.deserialize(buffer);
    PlanFragment planFragment = PlanFragment.deserialize(buffer);
    boolean hasTimeFilter = ReadWriteIOUtils.readBool(buffer);
    Filter timeFilter = hasTimeFilter ? FilterFactory.deserialize(buffer) : null;
    QueryType queryType = QueryType.values()[ReadWriteIOUtils.readInt(buffer)];
    FragmentInstance fragmentInstance =
        new FragmentInstance(planFragment, id, timeFilter, queryType);
    fragmentInstance.regionReplicaSet = ThriftCommonsSerDeUtils.readTRegionReplicaSet(buffer);
    fragmentInstance.hostDataNode = ThriftCommonsSerDeUtils.readTDataNodeLocation(buffer);

    return fragmentInstance;
  }

  @Override
  public void serializeRequest(ByteBuffer buffer) {
    id.serialize(buffer);
    fragment.serialize(buffer);
    ReadWriteIOUtils.write(timeFilter != null, buffer);
    if (timeFilter != null) {
      timeFilter.serialize(buffer);
    }
    ReadWriteIOUtils.write(type.ordinal(), buffer);
    ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, buffer);
    ThriftCommonsSerDeUtils.writeTDataNodeLocation(hostDataNode, buffer);
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
}
