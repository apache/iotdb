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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.sink;

import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class MultiChildrenSinkNode extends SinkNode {

  protected List<PlanNode> children;

  protected final List<DownStreamChannelLocation> downStreamChannelLocationList;

  public MultiChildrenSinkNode(PlanNodeId id) {
    super(id);
    this.children = new ArrayList<>();
    this.downStreamChannelLocationList = new ArrayList<>();
  }

  protected MultiChildrenSinkNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id);
    this.children = children;
    this.downStreamChannelLocationList = downStreamChannelLocationList;
  }

  protected MultiChildrenSinkNode(
      PlanNodeId id, List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id);
    this.children = new ArrayList<>();
    this.downStreamChannelLocationList = downStreamChannelLocationList;
  }

  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  @Override
  public abstract PlanNode clone();

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  public void addChildren(List<PlanNode> children) {
    this.children.addAll(children);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MultiChildrenSinkNode that = (MultiChildrenSinkNode) o;
    return children.equals(that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), children);
  }

  @Override
  public void send() {}

  @Override
  public void close() throws Exception {}

  public List<DownStreamChannelLocation> getDownStreamChannelLocationList() {
    return downStreamChannelLocationList;
  }

  public void addDownStreamChannelLocation(DownStreamChannelLocation downStreamChannelLocation) {
    downStreamChannelLocationList.add(downStreamChannelLocation);
  }

  public int getCurrentLastIndex() {
    return children.size() - 1;
  }

  /**
   * It's possible that the DownStreamChannelLocationList consists of several identical
   * DownStreamChannelLocations. Serializing every DownStreamChannelLocation is not necessary since
   * we could only serialize the unique DownStreamChannelLocation and use an index to get that
   * DownStreamChannelLocation.
   */
  protected void serializeDownStreamChannelLocationList(ByteBuffer byteBuffer) {
    List<Integer> indexOfDownStreamChannelLocation = new ArrayList<>();
    Map<DownStreamChannelLocation, Integer> uniqueDownStreamChannelLocationIndexMap =
        new HashMap<>();
    downStreamChannelLocationList.forEach(
        downStreamChannelLocation -> {
          // Do not use putIfAbsent here to avoid unnecessary hash calculation
          Integer index = uniqueDownStreamChannelLocationIndexMap.get(downStreamChannelLocation);
          if (index != null) {
            indexOfDownStreamChannelLocation.add(index);
          } else {
            indexOfDownStreamChannelLocation.add(uniqueDownStreamChannelLocationIndexMap.size());
            uniqueDownStreamChannelLocationIndexMap.put(
                downStreamChannelLocation, uniqueDownStreamChannelLocationIndexMap.size());
          }
        });
    ReadWriteIOUtils.write(uniqueDownStreamChannelLocationIndexMap.size(), byteBuffer);
    for (DownStreamChannelLocation downStreamChannelLocation :
        uniqueDownStreamChannelLocationIndexMap.keySet()) {
      downStreamChannelLocation.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(indexOfDownStreamChannelLocation.size(), byteBuffer);
    for (int i : indexOfDownStreamChannelLocation) {
      ReadWriteIOUtils.write(i, byteBuffer);
    }
  }

  protected void serializeDownStreamChannelLocationList(DataOutputStream stream)
      throws IOException {
    List<Integer> indexOfDownStreamChannelLocation = new ArrayList<>();
    Map<DownStreamChannelLocation, Integer> uniqueDownStreamChannelLocationIndexMap =
        new HashMap<>();
    downStreamChannelLocationList.forEach(
        downStreamChannelLocation -> {
          // Do not use putIfAbsent here to avoid unnecessary hash calculation
          Integer index = uniqueDownStreamChannelLocationIndexMap.get(downStreamChannelLocation);
          if (index != null) {
            indexOfDownStreamChannelLocation.add(index);
          } else {
            indexOfDownStreamChannelLocation.add(uniqueDownStreamChannelLocationIndexMap.size());
            uniqueDownStreamChannelLocationIndexMap.put(
                downStreamChannelLocation, uniqueDownStreamChannelLocationIndexMap.size());
          }
        });
    ReadWriteIOUtils.write(uniqueDownStreamChannelLocationIndexMap.size(), stream);
    for (DownStreamChannelLocation downStreamChannelLocation :
        uniqueDownStreamChannelLocationIndexMap.keySet()) {
      downStreamChannelLocation.serialize(stream);
    }
    ReadWriteIOUtils.write(indexOfDownStreamChannelLocation.size(), stream);
    for (int i : indexOfDownStreamChannelLocation) {
      ReadWriteIOUtils.write(i, stream);
    }
  }

  protected static List<DownStreamChannelLocation> deserializeDownStreamChannelLocationList(
      ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DownStreamChannelLocation> uniqueDownStreamChannelLocationList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      uniqueDownStreamChannelLocationList.add(DownStreamChannelLocation.deserialize(byteBuffer));
    }
    int downStreamChannelLocationIndexListSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> downStreamChannelLocationIndexList = new ArrayList<>();
    for (int i = 0; i < downStreamChannelLocationIndexListSize; i++) {
      downStreamChannelLocationIndexList.add(ReadWriteIOUtils.readInt(byteBuffer));
    }
    return downStreamChannelLocationIndexList.stream()
        .map(uniqueDownStreamChannelLocationList::get)
        .collect(Collectors.toList());
  }
}
