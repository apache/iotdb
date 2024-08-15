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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RegionOperationProcedureTest {
  static List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();

  static {
    for (int i = 0; i < 4; i++) {
      int i10 = i * 10;
      dataNodeLocations.add(
          new TDataNodeLocation(
              i,
              new TEndPoint("127.0.0.1", i10),
              new TEndPoint("127.0.0.1", i10 + 1),
              new TEndPoint("127.0.0.1", i10 + 2),
              new TEndPoint("127.0.0.1", i10 + 3),
              new TEndPoint("127.0.0.1", i10 + 4)));
    }
  }

  @Test
  public void regionOperationsMapTest() {
    final TConsensusGroupId chosenId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
    RegionMigrateProcedure procedure0 =
        new RegionMigrateProcedure(
            chosenId,
            dataNodeLocations.get(0),
            dataNodeLocations.get(1),
            dataNodeLocations.get(2),
            dataNodeLocations.get(3));
    procedure0.setProcId(0);
    AddRegionPeerProcedure procedure1 =
        new AddRegionPeerProcedure(chosenId, dataNodeLocations.get(0), dataNodeLocations.get(1));
    procedure1.setProcId(1);
    RemoveRegionPeerProcedure procedure2 =
        new RemoveRegionPeerProcedure(chosenId, dataNodeLocations.get(0), dataNodeLocations.get(1));
    procedure2.setProcId(2);

    int expected = 3;
    Assert.assertEquals(expected, RegionOperationProcedure.getRegionOperations(chosenId));

    procedure0.procedureEndHook();
    expected--;
    Assert.assertEquals(expected, RegionOperationProcedure.getRegionOperations(chosenId));

    procedure1.procedureEndHook();
    expected--;
    Assert.assertEquals(expected, RegionOperationProcedure.getRegionOperations(chosenId));

    procedure2.procedureEndHook();
    expected--;
    Assert.assertEquals(expected, RegionOperationProcedure.getRegionOperations(chosenId));
  }
}
