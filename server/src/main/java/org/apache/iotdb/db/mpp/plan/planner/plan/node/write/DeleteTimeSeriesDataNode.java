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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class DeleteTimeSeriesDataNode extends WritePlanNode {

  private PartialPath deletedPath;
  private TRegionReplicaSet dataRegionReplicaSet;

  public DeleteTimeSeriesDataNode(PlanNodeId id, PartialPath deletedPath) {
    super(id);
    this.deletedPath = deletedPath;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_TIMESERIES_DATA.serialize(byteBuffer);
    ReadWriteIOUtils.write(deletedPath.getFullPath(), byteBuffer);
  }

  public static DeleteTimeSeriesDataNode deserialize(ByteBuffer buffer) {
    PartialPath deletedPath;
    try {
      deletedPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize DeleteTimeSeriesDataNode", e);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new DeleteTimeSeriesDataNode(planNodeId, deletedPath);
  }

  public PartialPath getDeletedPath() {
    return deletedPath;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet dataRegionReplicaSet) {
    this.dataRegionReplicaSet = dataRegionReplicaSet;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    return null;
  }
}
