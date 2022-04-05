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
package org.apache.iotdb.db.mpp.sql.planner.plan;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;

import java.nio.ByteBuffer;

public class FragmentInstance implements IConsensusRequest {
  private FragmentInstanceId id;

  // The reference of PlanFragment which this instance is generated from
  private PlanFragment fragment;

  // The Region where the FragmentInstance should run
  private RegionReplicaSet regionReplicaSet;

  private Endpoint hostEndpoint;

  // The index to generate this FragmentInstanceId
  private int index;

  // We can add some more params for a specific FragmentInstance
  // So that we can make different FragmentInstance owns different data range.

  public FragmentInstance(PlanFragment fragment, int index) {
    this.fragment = fragment;
    this.index = index;
    this.id = generateId(fragment.getId(), index);
  }

  public static FragmentInstanceId generateId(PlanFragmentId id, int index) {
    return new FragmentInstanceId(id, String.valueOf(index));
  }

  public RegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(RegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  public Endpoint getHostEndpoint() {
    return hostEndpoint;
  }

  public void setHostEndpoint(Endpoint hostEndpoint) {
    this.hostEndpoint = hostEndpoint;
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
          sink.getDownStreamEndpoint(), sink.getDownStreamInstanceId(), sink.getDownStreamNode());
    }
    return "<No downstream>";
  }

  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append(
        String.format(
            "FragmentInstance-%s:[Host: %s/%s]\n",
            getId(), getHostEndpoint().getIp(), getRegionReplicaSet().getConsensusGroupId()));
    ret.append("---- Plan Node Tree ----\n");
    ret.append(PlanNodeUtil.nodeToString(getFragment().getRoot()));
    return ret.toString();
  }

  /** TODO need to be implemented */
  public static FragmentInstance deserializeFrom(ByteBuffer byteBuffer)
      throws IllegalPathException {
    FragmentInstance fragmentInstance =
        new FragmentInstance(PlanFragment.deserialize(byteBuffer), byteBuffer.getInt());
    fragmentInstance.setRegionReplicaSet(RegionReplicaSet.deserializeImpl(byteBuffer));
    return fragmentInstance;
  }

  @Override
  public void serializeRequest(ByteBuffer byteBuffer) {
    // PlanFragment
    fragment.serialize(byteBuffer);

    // index
    byteBuffer.putInt(index);

    // RegionReplicaSet
    regionReplicaSet.serializeImpl(byteBuffer);
  }
}
