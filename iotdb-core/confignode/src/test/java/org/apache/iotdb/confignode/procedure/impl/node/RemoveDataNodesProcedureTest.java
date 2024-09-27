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
package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoveDataNodesProcedureTest {

  private TConsensusGroupId consensusGroupId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private TDataNodeLocation fromDataNodeLocation =
      new TDataNodeLocation(
          10,
          new TEndPoint("127.0.0.1", 6667),
          new TEndPoint("127.0.0.1", 6668),
          new TEndPoint("127.0.0.1", 6669),
          new TEndPoint("127.0.0.1", 6670),
          new TEndPoint("127.0.0.1", 6671));

  private TDataNodeLocation toDataNodeLocation =
      new TDataNodeLocation(
          11,
          new TEndPoint("127.0.0.1", 6677),
          new TEndPoint("127.0.0.1", 6678),
          new TEndPoint("127.0.0.1", 6679),
          new TEndPoint("127.0.0.1", 6680),
          new TEndPoint("127.0.0.1", 6681));

  @Test
  public void serDeTest() throws IOException {
    List<TDataNodeLocation> removedDataNodes = Collections.singletonList(fromDataNodeLocation);
    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
    nodeStatusMap.put(10, NodeStatus.Running);
    RemoveDataNodesProcedure procedure0 =
        new RemoveDataNodesProcedure(removedDataNodes, nodeStatusMap);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure0.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure0, ProcedureFactory.getInstance().create(buffer));
    }

    RegionMigrationPlan regionMigrationPlan =
        new RegionMigrationPlan(consensusGroupId, fromDataNodeLocation);
    regionMigrationPlan.setToDataNode(toDataNodeLocation);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      regionMigrationPlan.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(regionMigrationPlan, RegionMigrationPlan.deserialize(buffer));
    }
  }
}
