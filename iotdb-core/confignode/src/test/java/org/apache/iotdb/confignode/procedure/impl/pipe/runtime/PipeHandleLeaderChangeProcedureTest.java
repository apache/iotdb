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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PipeHandleLeaderChangeProcedureTest {
  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<TConsensusGroupId, Pair<Integer, Integer>> leaderMap = new HashMap<>();
    leaderMap.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1), new Pair<>(1, 2));
    leaderMap.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2), new Pair<>(2, 3));
    leaderMap.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3), new Pair<>(4, 5));
    leaderMap.put(
        new TConsensusGroupId(TConsensusGroupType.ConfigRegion, Integer.MIN_VALUE),
        new Pair<>(6, 7));

    PipeHandleLeaderChangeProcedure proc = new PipeHandleLeaderChangeProcedure(leaderMap);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      PipeHandleLeaderChangeProcedure proc2 =
          (PipeHandleLeaderChangeProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void deserializeOldFormatConfigRegionTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<TConsensusGroupId, Pair<Integer, Integer>> leaderMap = new HashMap<>();
    leaderMap.put(
        new TConsensusGroupId(TConsensusGroupType.ConfigRegion, Integer.MIN_VALUE),
        new Pair<>(6, 7));

    try {
      outputStream.writeShort(ProcedureType.PIPE_HANDLE_LEADER_CHANGE_PROCEDURE.getTypeCode());
      outputStream.writeLong(Procedure.NO_PROC_ID);
      outputStream.writeInt(ProcedureState.INITIALIZING.ordinal());
      outputStream.writeLong(0L);
      outputStream.writeLong(0L);
      outputStream.writeLong(Procedure.NO_PROC_ID);
      outputStream.writeLong(Procedure.NO_TIMEOUT);
      outputStream.writeInt(-1);
      outputStream.write((byte) 0);
      outputStream.writeInt(-1);
      outputStream.write((byte) 0);
      outputStream.writeInt(0);
      outputStream.write((byte) 0);
      outputStream.writeInt(leaderMap.size());
      outputStream.writeInt(Integer.MIN_VALUE);
      outputStream.writeInt(6);
      outputStream.writeInt(7);

      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      PipeHandleLeaderChangeProcedure proc =
          (PipeHandleLeaderChangeProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(new PipeHandleLeaderChangeProcedure(leaderMap), proc);
    } catch (Exception e) {
      fail();
    }
  }
}
