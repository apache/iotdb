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
package org.apache.iotdb.confignode.manager.load.balancer.router;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.SchemaRegion;

public class RegionRouteMapTest {

  @Test
  public void RegionRouteMapSerDeTest() throws IOException, TTransportException {
    RegionRouteMap regionRouteMap0 = new RegionRouteMap();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
    Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(SchemaRegion, i);
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(regionGroupId);
      for (int j = 0; j < 3; j++) {
        regionReplicaSet.addToDataNodeLocations(
            new TDataNodeLocation(
                j,
                new TEndPoint("0.0.0.0", 6667 + j),
                new TEndPoint("0.0.0.0", 10730 + j),
                new TEndPoint("0.0.0.0", 10740 + j),
                new TEndPoint("0.0.0.0", 10760 + j),
                new TEndPoint("0.0.0.0", 10750 + j)));
      }
      regionLeaderMap.put(regionGroupId, i % 3);
      regionPriorityMap.put(regionGroupId, regionReplicaSet);
    }
    regionRouteMap0.setRegionLeaderMap(regionLeaderMap);
    regionRouteMap0.setRegionPriorityMap(regionPriorityMap);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      TTransport transport = new TIOStreamTransport(outputStream);
      TBinaryProtocol protocol = new TBinaryProtocol(transport);
      regionRouteMap0.serialize(outputStream, protocol);

      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      RegionRouteMap regionRouteMap1 = new RegionRouteMap();
      regionRouteMap1.deserialize(buffer);
      Assert.assertEquals(regionRouteMap0, regionRouteMap1);
    }
  }
}
