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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RemoveRegionGroupProcedureTest {
  @Test
  public void serDeTest() throws Exception {
    final TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 10),
            Arrays.asList(
                new TDataNodeLocation(
                    1,
                    new TEndPoint("127.0.0.1", 0),
                    new TEndPoint("127.0.0.1", 1),
                    new TEndPoint("127.0.0.1", 2),
                    new TEndPoint("127.0.0.1", 3),
                    new TEndPoint("127.0.0.1", 4)),
                new TDataNodeLocation(
                    2,
                    new TEndPoint("127.0.0.1", 10),
                    new TEndPoint("127.0.0.1", 11),
                    new TEndPoint("127.0.0.1", 12),
                    new TEndPoint("127.0.0.1", 13),
                    new TEndPoint("127.0.0.1", 14))));
    final RemoveRegionGroupProcedure procedure = new RemoveRegionGroupProcedure(regionReplicaSet);
    // A non-zero cursor so the round-trip actually exercises currentReplicaIndex (de)serialization;
    // equals/hashCode include it, so a dropped/garbled cursor would fail the assertion.
    procedure.setCurrentReplicaIndex(1);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      // Exercises ProcedureType.REMOVE_REGION_GROUP_PROCEDURE + ProcedureFactory registration as
      // well as the procedure's own serialize/deserialize.
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    }
  }
}
