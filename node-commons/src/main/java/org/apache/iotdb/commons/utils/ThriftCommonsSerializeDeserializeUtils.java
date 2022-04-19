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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Utils for serialize and deserialize all the data struct defined by thrift-commons */
public class ThriftCommonsSerializeDeserializeUtils {

  private ThriftCommonsSerializeDeserializeUtils() {
    // Empty constructor
  }

  public static void writeTEndPoint(TEndPoint endPoint, ByteBuffer buffer) {
    BasicStructureSerializeDeserializeUtil.write(endPoint.getIp(), buffer);
    buffer.putInt(endPoint.getPort());
  }

  public static TEndPoint readTEndPoint(ByteBuffer buffer) {
    TEndPoint endPoint = new TEndPoint();
    endPoint.setIp(BasicStructureSerializeDeserializeUtil.readString(buffer));
    endPoint.setPort(buffer.getInt());
    return endPoint;
  }

  public static void writeTDataNodeLocation(TDataNodeLocation dataNodeLocation, ByteBuffer buffer) {
    buffer.putInt(dataNodeLocation.getDataNodeId());
    writeTEndPoint(dataNodeLocation.getExternalEndPoint(), buffer);
    writeTEndPoint(dataNodeLocation.getInternalEndPoint(), buffer);
    writeTEndPoint(dataNodeLocation.getDataBlockManagerEndPoint(), buffer);
    writeTEndPoint(dataNodeLocation.getConsensusEndPoint(), buffer);
  }

  public static TDataNodeLocation readTDataNodeLocation(ByteBuffer buffer) {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(buffer.getInt());
    dataNodeLocation.setExternalEndPoint(readTEndPoint(buffer));
    dataNodeLocation.setInternalEndPoint(readTEndPoint(buffer));
    dataNodeLocation.setDataBlockManagerEndPoint(readTEndPoint(buffer));
    dataNodeLocation.setConsensusEndPoint(readTEndPoint(buffer));
    return dataNodeLocation;
  }

  public static void writeTSeriesPartitionSlot(TSeriesPartitionSlot seriesPartitionSlot, ByteBuffer buffer) {
    buffer.putInt(seriesPartitionSlot.getSlotId());
  }

  public static TSeriesPartitionSlot readTSeriesPartitionSlot(ByteBuffer buffer) {
    return new TSeriesPartitionSlot(buffer.getInt());
  }

  public static void writeTTimePartitionSlot(TTimePartitionSlot timePartitionSlot, ByteBuffer buffer) {
    buffer.putLong(timePartitionSlot.getStartTime());
  }

  public static TTimePartitionSlot readTTimePartitionSlot(ByteBuffer buffer) {
    return new TTimePartitionSlot(buffer.getLong());
  }

  public static void writeTConsensusGroupId(TConsensusGroupId consensusGroupId, ByteBuffer buffer) {
    buffer.putInt(consensusGroupId.getType().ordinal());
    buffer.putInt(consensusGroupId.getId());
  }

  public static TConsensusGroupId readTConsensusGroupId(ByteBuffer buffer) {
    TConsensusGroupType type = TConsensusGroupType.values()[buffer.getInt()];
    return new TConsensusGroupId(type, buffer.getInt());
  }

  public static void writeTRegionReplicaSet(TRegionReplicaSet regionReplicaSet, ByteBuffer buffer) {
    writeTConsensusGroupId(regionReplicaSet.getRegionId(), buffer);
    buffer.putInt(regionReplicaSet.getDataNodeLocationsSize());
    regionReplicaSet.getDataNodeLocations().forEach(dataNodeLocation -> writeTDataNodeLocation(dataNodeLocation, buffer));
  }

  public static TRegionReplicaSet readTRegionReplicaSet(ByteBuffer buffer) {
    TConsensusGroupId consensusGroupId = readTConsensusGroupId(buffer);
    int dataNodeLocationNum = buffer.getInt();
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    for (int i = 0; i < dataNodeLocationNum; i++) {
      dataNodeLocations.add(readTDataNodeLocation(buffer));
    }
    return new TRegionReplicaSet(consensusGroupId, dataNodeLocations);
  }


}
