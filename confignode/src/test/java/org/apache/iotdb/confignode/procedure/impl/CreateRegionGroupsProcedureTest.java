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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.procedure.impl.statemachine.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.DataRegion;
import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.SchemaRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateRegionGroupsProcedureTest {

  @Test
  public void serializeDeserializeTest() {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(5);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TDataNodeLocation dataNodeLocation1 = new TDataNodeLocation();
    dataNodeLocation1.setDataNodeId(6);
    dataNodeLocation1.setClientRpcEndPoint(new TEndPoint("0.0.0.1", 6667));
    dataNodeLocation1.setInternalEndPoint(new TEndPoint("0.0.0.1", 10730));
    dataNodeLocation1.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.1", 10740));
    dataNodeLocation1.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.1", 10760));
    dataNodeLocation1.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.1", 10750));

    TConsensusGroupId schemaRegionGroupId = new TConsensusGroupId(SchemaRegion, 1);
    TConsensusGroupId dataRegionGroupId = new TConsensusGroupId(DataRegion, 0);

    TRegionReplicaSet schemaRegionSet =
        new TRegionReplicaSet(schemaRegionGroupId, Collections.singletonList(dataNodeLocation0));
    TRegionReplicaSet dataRegionSet =
        new TRegionReplicaSet(dataRegionGroupId, Collections.singletonList(dataNodeLocation1));

    // to test the equals method of Map<TConsensusGroupId, TRegionReplicaSet>
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions0 =
        new HashMap<TConsensusGroupId, TRegionReplicaSet>() {
          {
            put(dataRegionGroupId, dataRegionSet);
            put(schemaRegionGroupId, schemaRegionSet);
          }
        };
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions1 =
        new HashMap<TConsensusGroupId, TRegionReplicaSet>() {
          {
            put(schemaRegionGroupId, schemaRegionSet);
            put(dataRegionGroupId, dataRegionSet);
          }
        };
    assertEquals(failedRegions0, failedRegions1);

    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    createRegionGroupsPlan.addRegionGroup("root.sg0", dataRegionSet);
    createRegionGroupsPlan.addRegionGroup("root.sg1", schemaRegionSet);

    CreateRegionGroupsProcedure procedure0 =
        new CreateRegionGroupsProcedure(
            TConsensusGroupType.DataRegion, createRegionGroupsPlan, failedRegions0);
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      procedure0.serialize(outputStream);
      CreateRegionGroupsProcedure procedure1 = new CreateRegionGroupsProcedure();
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(ProcedureType.CREATE_REGION_GROUPS.getTypeCode(), buffer.getShort());
      procedure1.deserialize(buffer);
      assertEquals(procedure0, procedure1);
      assertEquals(procedure0.hashCode(), procedure1.hashCode());

      CreateRegionGroupsProcedure procedure2 =
          (CreateRegionGroupsProcedure)
              ProcedureFactory.getInstance()
                  .create(ByteBuffer.wrap(byteArrayOutputStream.getBuf()));
      assertEquals(procedure0, procedure2);
      assertEquals(procedure0.hashCode(), procedure2.hashCode());
    } catch (IOException e) {
      fail();
    }
  }
}
