/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitionTest extends SerializeTest {

  @Test
  public void testSerialize() throws TException, IOException {

    int dataPartitionFlag = 10;

    DataPartition dataPartition = new DataPartition(seriesPartitionExecutorClass, 1);

    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        assignedDataPartition =
            generateCreateDataPartitionMap(
                dataPartitionFlag, generateTConsensusGroupId(dataPartitionFlag));
    dataPartition.setDataPartitionMap(assignedDataPartition);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    TIOStreamTransport tioStreamTransport = new TIOStreamTransport(dataOutputStream);
    TProtocol protocol = new TBinaryProtocol(tioStreamTransport);
    dataPartition.serialize(dataOutputStream, protocol);

    DataPartition newDataPartition =
        new DataPartition(new HashMap<>(), seriesPartitionExecutorClass, 1);
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    tioStreamTransport = new TIOStreamTransport(dataInputStream);
    protocol = new TBinaryProtocol(tioStreamTransport);
    newDataPartition.deserialize(dataInputStream, protocol);
    Assert.assertEquals(assignedDataPartition, newDataPartition.getDataPartitionMap());
  }
}
