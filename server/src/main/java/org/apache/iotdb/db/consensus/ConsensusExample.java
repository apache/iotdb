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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.DataRegionStateMachine;
import org.apache.iotdb.db.consensus.statemachine.SchemaRegionStateMachine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class ConsensusExample {

  public static void main(String[] args) throws IllegalPathException, IOException {
    IConsensus consensusImpl =
        IConsensusFactory.getConsensusImpl(
                "org.apache.iotdb.consensus.standalone.StandAloneConsensus",
                new Endpoint("localhost", 6667),
                new File("./"),
                gid -> {
                  switch (gid.getType()) {
                    case SchemaRegion:
                      return new SchemaRegionStateMachine();
                    case DataRegion:
                      return new DataRegionStateMachine();
                  }
                  throw new IllegalArgumentException(
                      String.format("Unexpected consensusGroup %s", gid));
                })
            .orElseThrow(
                () -> new IllegalArgumentException(IConsensusFactory.CONSTRUCT_FAILED_MSG));
    consensusImpl.start();
    InsertRowPlan plan = getInsertRowPlan();

    ConsensusGroupId dataRegionId = new ConsensusGroupId(GroupType.DataRegion, 0);
    ConsensusGroupId schemaRegionId = new ConsensusGroupId(GroupType.SchemaRegion, 1);
    consensusImpl.addConsensusGroup(
        dataRegionId,
        Collections.singletonList(new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667))));
    consensusImpl.addConsensusGroup(
        schemaRegionId,
        Collections.singletonList(new Peer(schemaRegionId, new Endpoint("0.0.0.0", 6667))));

    // The leader node can pass memory structures directly to the consensus layer
    consensusImpl.write(dataRegionId, plan);
    consensusImpl.write(schemaRegionId, plan);

    // TODO pooling to reduce GC overhead
    ByteBuffer buffer =
        ByteBuffer.allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize());
    plan.serialize(buffer);
    buffer.flip();

    // the follower node can pass ByteBuffer into the consensus layer without deserializing it
    consensusImpl.write(dataRegionId, new ByteBufferConsensusRequest(buffer));
    buffer.flip();
    consensusImpl.write(schemaRegionId, new ByteBufferConsensusRequest(buffer));

    consensusImpl.stop();
  }

  private static InsertRowPlan getInsertRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath("root.isp.d1"),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
  }
}
