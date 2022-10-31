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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

/** Utils for serialize and deserialize all the data struct defined by thrift-commons */
public class ThriftCommonsSerDeUtils {

  private ThriftCommonsSerDeUtils() {
    // Empty constructor
  }

  private static TBinaryProtocol generateWriteProtocol(DataOutputStream stream)
      throws TTransportException {
    TTransport transport = new TIOStreamTransport(stream);
    return new TBinaryProtocol(transport);
  }

  private static TBinaryProtocol generateWriteProtocol(ByteBuffer buffer)
      throws TTransportException {
    TTransport transport = new TByteBuffer(buffer);
    return new TBinaryProtocol(transport);
  }

  private static TBinaryProtocol generateReadProtocol(ByteBuffer buffer)
      throws TTransportException {
    TTransport transport = new TByteBuffer(buffer);
    return new TBinaryProtocol(transport);
  }

  public static void serializeTEndPoint(TEndPoint endPoint, DataOutputStream stream) {
    try {
      endPoint.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TEndPoint failed: ", e);
    }
  }

  public static TEndPoint deserializeTEndPoint(ByteBuffer buffer) {
    TEndPoint endPoint = new TEndPoint();
    try {
      endPoint.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TEndPoint failed: ", e);
    }
    return endPoint;
  }

  public static void serializeTDataNodeLocation(
      TDataNodeLocation dataNodeLocation, DataOutputStream stream) {
    try {
      dataNodeLocation.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TDataNodeLocation failed: ", e);
    }
  }

  public static TDataNodeLocation deserializeTDataNodeLocation(ByteBuffer buffer) {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    try {
      dataNodeLocation.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TDataNodeLocation failed: ", e);
    }
    return dataNodeLocation;
  }

  public static void serializeTDataNodeInfo(
      TDataNodeConfiguration dataNodeInfo, DataOutputStream stream) {
    try {
      dataNodeInfo.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TDataNodeInfo failed: ", e);
    }
  }

  public static TDataNodeConfiguration deserializeTDataNodeInfo(ByteBuffer buffer) {
    TDataNodeConfiguration dataNodeInfo = new TDataNodeConfiguration();
    try {
      dataNodeInfo.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TDataNodeInfo failed: ", e);
    }
    return dataNodeInfo;
  }

  public static void serializeTSeriesPartitionSlot(
      TSeriesPartitionSlot seriesPartitionSlot, DataOutputStream stream) {
    try {
      seriesPartitionSlot.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TSeriesPartitionSlot failed: ", e);
    }
  }

  public static TSeriesPartitionSlot deserializeTSeriesPartitionSlot(ByteBuffer buffer) {
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot();
    try {
      seriesPartitionSlot.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TSeriesPartitionSlot failed: ", e);
    }
    return seriesPartitionSlot;
  }

  public static void serializeTTimePartitionSlot(
      TTimePartitionSlot timePartitionSlot, DataOutputStream stream) {
    try {
      timePartitionSlot.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TTimePartitionSlot failed: ", e);
    }
  }

  public static TTimePartitionSlot deserializeTTimePartitionSlot(ByteBuffer buffer) {
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
    try {
      timePartitionSlot.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TTimePartitionSlot failed: ", e);
    }
    return timePartitionSlot;
  }

  public static void serializeTConsensusGroupId(
      TConsensusGroupId consensusGroupId, DataOutputStream stream) {
    try {
      consensusGroupId.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TConsensusGroupId failed: ", e);
    }
  }

  public static TConsensusGroupId deserializeTConsensusGroupId(ByteBuffer buffer) {
    TConsensusGroupId consensusGroupId = new TConsensusGroupId();
    try {
      consensusGroupId.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TConsensusGroupId failed: ", e);
    }
    return consensusGroupId;
  }

  public static void serializeTRegionReplicaSet(
      TRegionReplicaSet regionReplicaSet, DataOutputStream stream) {
    try {
      regionReplicaSet.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TRegionReplicaSet failed: ", e);
    }
  }

  public static TRegionReplicaSet deserializeTRegionReplicaSet(ByteBuffer buffer) {
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    try {
      regionReplicaSet.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TRegionReplicaSet failed: ", e);
    }
    return regionReplicaSet;
  }

  public static void serializeTSchemaNode(TSchemaNode schemaNode, DataOutputStream stream) {
    try {
      schemaNode.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TSchemaNode failed: ", e);
    }
  }

  public static void serializeTSchemaNode(TSchemaNode schemaNode, ByteBuffer buffer) {
    try {
      schemaNode.write(generateWriteProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TSchemaNode failed: ", e);
    }
  }

  public static TSchemaNode deserializeTSchemaNode(ByteBuffer buffer) {
    TSchemaNode schemaNode = new TSchemaNode();
    try {
      schemaNode.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TSchemaNode failed: ", e);
    }
    return schemaNode;
  }
}
