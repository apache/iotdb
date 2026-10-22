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

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.rpc.ConfigurableTByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.iotdb.rpc.TConfigurationConst.defaultTConfiguration;

/** Utils for serialize and deserialize all the data struct defined by thrift-commons */
public class ThriftCommonsSerDeUtils {

  private ThriftCommonsSerDeUtils() {
    // Empty constructor
  }

  private static TBinaryProtocol generateWriteProtocol(final OutputStream stream)
      throws TTransportException {
    return new TBinaryProtocol(new TIOStreamTransport(stream));
  }

  private static TBinaryProtocol generateWriteProtocol(final ByteBuffer buffer)
      throws TTransportException {
    TTransport transport = generateTByteBuffer(buffer);
    return new TBinaryProtocol(transport);
  }

  private static TBinaryProtocol generateReadProtocol(final InputStream stream)
      throws TTransportException {
    return new TBinaryProtocol(new TIOStreamTransport(stream));
  }

  private static TBinaryProtocol generateReadProtocol(final ByteBuffer buffer)
      throws TTransportException {
    TTransport transport = generateTByteBuffer(buffer);
    return new TBinaryProtocol(transport);
  }

  public static void serializeTEndPoint(final TEndPoint endPoint, final OutputStream stream) {
    try {
      endPoint.write(generateWriteProtocol(stream));
    } catch (final TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_ENDPOINT_FAILED, e);
    }
  }

  public static void serializeTEndPoint(final TEndPoint endPoint, final ByteBuffer buffer) {
    try {
      endPoint.write(generateWriteProtocol(buffer));
    } catch (final TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_ENDPOINT_FAILED, e);
    }
  }

  public static TEndPoint deserializeTEndPoint(final InputStream stream) {
    final TEndPoint endPoint = new TEndPoint();
    try {
      endPoint.read(generateReadProtocol(stream));
    } catch (final TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_ENDPOINT_FAILED, e);
    }
    return endPoint;
  }

  public static TEndPoint deserializeTEndPoint(final ByteBuffer buffer) {
    TEndPoint endPoint = new TEndPoint();
    try {
      endPoint.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_ENDPOINT_FAILED, e);
    }
    return endPoint;
  }

  public static void serializeTDataNodeConfiguration(
      TDataNodeConfiguration dataNodeConfiguration, DataOutputStream stream) {
    try {
      dataNodeConfiguration.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_DATA_NODE_CONFIGURATION_FAILED, e);
    }
  }

  public static TDataNodeConfiguration deserializeTDataNodeConfiguration(ByteBuffer buffer) {
    TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    try {
      dataNodeConfiguration.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_DATA_NODE_CONFIGURATION_FAILED, e);
    }
    return dataNodeConfiguration;
  }

  public static void serializeTDataNodeLocation(
      TDataNodeLocation dataNodeLocation, DataOutputStream stream) {
    try {
      dataNodeLocation.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_DATA_NODE_LOCATION_FAILED, e);
    }
  }

  public static TDataNodeLocation deserializeTDataNodeLocation(ByteBuffer buffer) {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    try {
      dataNodeLocation.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_DATA_NODE_LOCATION_FAILED, e);
    }
    return dataNodeLocation;
  }

  public static void serializeTCreateCQReq(TCreateCQReq createCQReq, DataOutputStream stream) {
    try {
      createCQReq.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_CREATE_CQ_REQ_FAILED, e);
    }
  }

  public static TCreateCQReq deserializeTCreateCQReq(ByteBuffer buffer) {
    TCreateCQReq createCQReq = new TCreateCQReq();
    try {
      createCQReq.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_CREATE_CQ_REQ_FAILED, e);
    }
    return createCQReq;
  }

  public static void serializeTDataNodeInfo(
      TDataNodeConfiguration dataNodeInfo, DataOutputStream stream) {
    try {
      dataNodeInfo.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_DATA_NODE_INFO_FAILED, e);
    }
  }

  public static TDataNodeConfiguration deserializeTDataNodeInfo(ByteBuffer buffer) {
    TDataNodeConfiguration dataNodeInfo = new TDataNodeConfiguration();
    try {
      dataNodeInfo.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_DATA_NODE_INFO_FAILED, e);
    }
    return dataNodeInfo;
  }

  public static void serializeTSeriesPartitionSlot(
      TSeriesPartitionSlot seriesPartitionSlot, DataOutputStream stream) {
    try {
      seriesPartitionSlot.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_SERIES_PARTITION_SLOT_FAILED, e);
    }
  }

  public static TSeriesPartitionSlot deserializeTSeriesPartitionSlot(ByteBuffer buffer) {
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot();
    try {
      seriesPartitionSlot.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_SERIES_PARTITION_SLOT_FAILED, e);
    }
    return seriesPartitionSlot;
  }

  public static void serializeTTimePartitionSlotList(
      TTimeSlotList timePartitionSlotList, DataOutputStream stream) {
    try {
      timePartitionSlotList.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_TIME_SLOT_LIST_FAILED, e);
    }
  }

  public static TTimeSlotList deserializeTTimePartitionSlotList(ByteBuffer buffer) {
    TTimeSlotList timePartitionSlotList = new TTimeSlotList();
    try {
      timePartitionSlotList.read(generateWriteProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_TIME_SLOT_LIST_FAILED, e);
    }
    return timePartitionSlotList;
  }

  public static void serializeTTimePartitionSlot(
      TTimePartitionSlot timePartitionSlot, DataOutputStream stream) throws IOException {
    try {
      timePartitionSlot.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_TIME_PARTITION_SLOT_FAILED, e);
    }
  }

  public static TTimePartitionSlot deserializeTTimePartitionSlot(ByteBuffer buffer) {
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
    try {
      timePartitionSlot.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_TIME_PARTITION_SLOT_FAILED, e);
    }
    return timePartitionSlot;
  }

  public static void serializeTConsensusGroupId(
      TConsensusGroupId consensusGroupId, DataOutputStream stream) {
    try {
      consensusGroupId.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_CONSENSUS_GROUP_ID_FAILED, e);
    }
  }

  public static TConsensusGroupId deserializeTConsensusGroupId(ByteBuffer buffer) {
    TConsensusGroupId consensusGroupId = new TConsensusGroupId();
    try {
      consensusGroupId.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_CONSENSUS_GROUP_ID_FAILED, e);
    }
    return consensusGroupId;
  }

  public static void serializeTRegionReplicaSet(
      TRegionReplicaSet regionReplicaSet, DataOutputStream stream) {
    try {
      regionReplicaSet.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_REGION_REPLICA_SET_FAILED, e);
    }
  }

  public static TRegionReplicaSet deserializeTRegionReplicaSet(ByteBuffer buffer) {
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    try {
      regionReplicaSet.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_REGION_REPLICA_SET_FAILED, e);
    }
    return regionReplicaSet;
  }

  public static void serializeTSchemaNode(TSchemaNode schemaNode, DataOutputStream stream) {
    try {
      schemaNode.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_SCHEMA_NODE_FAILED, e);
    }
  }

  public static void serializeTSchemaNode(TSchemaNode schemaNode, ByteBuffer buffer) {
    try {
      schemaNode.write(generateWriteProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_SCHEMA_NODE_FAILED, e);
    }
  }

  public static TSchemaNode deserializeTSchemaNode(ByteBuffer buffer) {
    TSchemaNode schemaNode = new TSchemaNode();
    try {
      schemaNode.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_SCHEMA_NODE_FAILED, e);
    }
    return schemaNode;
  }

  private static ConfigurableTByteBuffer generateTByteBuffer(ByteBuffer buffer)
      throws TTransportException {
    return new ConfigurableTByteBuffer(buffer, defaultTConfiguration);
  }

  public static void serializeTAINodeInfo(
      TAINodeConfiguration aiNodeInfo, DataOutputStream stream) {
    try {
      aiNodeInfo.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_AI_NODE_INFO_FAILED, e);
    }
  }

  public static TAINodeConfiguration deserializeTAINodeInfo(ByteBuffer buffer) {
    TAINodeConfiguration aiNodeInfo = new TAINodeConfiguration();
    try {
      aiNodeInfo.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_AI_NODE_INFO_FAILED, e);
    }
    return aiNodeInfo;
  }

  public static void serializeTAINodeConfiguration(
      TAINodeConfiguration aiNodeConfiguration, DataOutputStream stream) {
    try {
      aiNodeConfiguration.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_DATA_NODE_CONFIGURATION_FAILED, e);
    }
  }

  public static TAINodeConfiguration deserializeTAINodeConfiguration(ByteBuffer buffer) {
    TAINodeConfiguration aiNodeConfiguration = new TAINodeConfiguration();
    try {
      aiNodeConfiguration.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_AI_NODE_CONFIGURATION_FAILED, e);
    }
    return aiNodeConfiguration;
  }

  public static void serializeTAINodeLocation(
      TAINodeLocation aiNodeLocation, DataOutputStream stream) {
    try {
      aiNodeLocation.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.WRITE_T_AI_NODE_LOCATION_FAILED, e);
    }
  }

  public static TAINodeLocation deserializeTAINodeLocation(ByteBuffer buffer) {
    TAINodeLocation aiNodeLocation = new TAINodeLocation();
    try {
      aiNodeLocation.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException(UtilMessages.READ_T_DATA_NODE_LOCATION_FAILED, e);
    }
    return aiNodeLocation;
  }
}
