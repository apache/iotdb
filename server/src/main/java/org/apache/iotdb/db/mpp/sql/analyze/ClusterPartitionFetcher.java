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
package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.partition.*;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class ClusterPartitionFetcher implements IPartitionFetcher {

  private final ConfigNodeClient client;

  public ClusterPartitionFetcher() throws IoTDBConnectionException, BadNodeUrlException {
    client = new ConfigNodeClient();
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
    try {
      TSchemaPartitionResp schemaPartitionResp =
          client.getSchemaPartition(constructSchemaPartitionReq(patternTree));
      if (schemaPartitionResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseSchemaPartitionResp(schemaPartitionResp);
      }
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
    try {
      TSchemaPartitionResp schemaPartitionResp =
          client.getOrCreateSchemaPartition(constructSchemaPartitionReq(patternTree));
      if (schemaPartitionResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseSchemaPartitionResp(schemaPartitionResp);
      }
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public DataPartition getDataPartition(List<DataPartitionQueryParam> parameterList) {
    return null;
  }

  @Override
  public DataPartition getOrCreateDataPartition(List<DataPartitionQueryParam> parameterList) {
    return null;
  }

  private TSchemaPartitionReq constructSchemaPartitionReq(PathPatternTree patternTree) {
    PublicBAOS baos = new PublicBAOS();
    try {
      patternTree.serialize(baos);
      ByteBuffer serializedPatternTree = ByteBuffer.allocate(baos.size());
      serializedPatternTree.put(baos.getBuf());
      serializedPatternTree.flip();
      return new TSchemaPartitionReq(serializedPatternTree);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private SchemaPartition parseSchemaPartitionResp(TSchemaPartitionResp schemaPartitionResp) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> respSchemaPartitionMap =
        schemaPartitionResp.getSchemaRegionMap();
    Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap = new HashMap<>();
    respSchemaPartitionMap.forEach(
        (storageGroupName, respSchemaRegionMap) -> {
          Map<SeriesPartitionSlot, RegionReplicaSet> schemaRegionMap = new HashMap<>();
          respSchemaRegionMap.forEach(
              (respSeriesPartitionSlot, respRegionReplicaSet) -> {
                SeriesPartitionSlot seriesPartitionSlot =
                    new SeriesPartitionSlot(respSeriesPartitionSlot.getSlotId());
                EndPoint respEndPoint = respRegionReplicaSet.getEndpoint().get(0);
                RegionReplicaSet regionReplicaSet = null;
                try {
                  regionReplicaSet =
                      new RegionReplicaSet(
                          ConsensusGroupId.Factory.create(respRegionReplicaSet.regionId),
                          Collections.singletonList(
                              new DataNodeLocation(
                                  new Endpoint(respEndPoint.getIp(), respEndPoint.getPort()))));
                } catch (IOException e) {
                  e.printStackTrace();
                }
                schemaRegionMap.put(seriesPartitionSlot, regionReplicaSet);
              });
          schemaPartitionMap.put(storageGroupName, schemaRegionMap);
        });
    return new SchemaPartition(schemaPartitionMap);
  }
}
