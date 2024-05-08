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
package org.apache.iotdb.confignode.procedure.impl.statemachine;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RegionMigrateProcedureTest {

  @Test
  public void serDeTest() throws IOException {
    RegionMigrateProcedure procedure0 =
        new RegionMigrateProcedure(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 10),
            new TDataNodeLocation(
                1,
                new TEndPoint("127.0.0.1", 0),
                new TEndPoint("127.0.0.1", 1),
                new TEndPoint("127.0.0.1", 2),
                new TEndPoint("127.0.0.1", 3),
                new TEndPoint("127.0.0.1", 4)),
            new TDataNodeLocation(
                5,
                new TEndPoint("127.0.0.1", 5),
                new TEndPoint("127.0.0.1", 6),
                new TEndPoint("127.0.0.1", 7),
                new TEndPoint("127.0.0.1", 8),
                new TEndPoint("127.0.0.1", 9)));

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure0.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure0, ProcedureFactory.getInstance().create(buffer));
    }
  }
}
