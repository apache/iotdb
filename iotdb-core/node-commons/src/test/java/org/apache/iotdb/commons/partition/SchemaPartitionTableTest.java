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
package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SchemaPartitionTableTest {

  @Test
  public void reqSerDeTest() throws TException, IOException {
    // Open output stream
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    // Open output protocol
    TTransport transport = new TIOStreamTransport(outputStream);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    // Create SchemaPartitionTable and serialize
    Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      schemaPartitionMap.put(
          new TSeriesPartitionSlot(i), new TConsensusGroupId(TConsensusGroupType.SchemaRegion, i));
    }
    SchemaPartitionTable table0 = new SchemaPartitionTable(schemaPartitionMap);
    table0.serialize(outputStream, protocol);

    // Deserialize by ByteBuffer
    SchemaPartitionTable table1 = new SchemaPartitionTable();
    table1.deserialize(
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    Assert.assertEquals(table0, table1);
  }

  @Test
  public void snapshotSerDeTest() throws TException, IOException {
    // Open output stream
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    // Open output protocol
    TTransport transport = new TIOStreamTransport(outputStream);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    // Create SchemaPartitionTable and serialize
    Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      schemaPartitionMap.put(
          new TSeriesPartitionSlot(i), new TConsensusGroupId(TConsensusGroupType.SchemaRegion, i));
    }
    SchemaPartitionTable table0 = new SchemaPartitionTable(schemaPartitionMap);
    table0.serialize(outputStream, protocol);

    // Open input stream
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.getBuf()));

    // Open input protocol
    transport = new TIOStreamTransport(inputStream);
    protocol = new TBinaryProtocol(transport);

    // Deserialize by input stream and protocol
    SchemaPartitionTable table1 = new SchemaPartitionTable();
    table1.deserialize(inputStream, protocol);
    Assert.assertEquals(table0, table1);
  }
}
