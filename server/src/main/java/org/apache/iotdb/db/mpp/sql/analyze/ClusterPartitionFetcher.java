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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.partition.*;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ClusterPartitionFetcher implements IPartitionFetcher {

  private final ConfigNodeClient client;

  private SeriesPartitionExecutor partitionExecutor;

  public ClusterPartitionFetcher() throws StatementAnalyzeException {
    try {
      client = new ConfigNodeClient();
    } catch (IoTDBConnectionException | BadNodeUrlException e) {
      throw new StatementAnalyzeException("Couldn't connect config node");
    }
    initPartitionExecutor();
  }

  private void initPartitionExecutor() throws StatementAnalyzeException {
    IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    try {
      Class<?> executor = Class.forName(conf.getSeriesPartitionExecutorClass());
      Constructor<?> executorConstructor = executor.getConstructor(int.class);
      this.partitionExecutor =
          (SeriesPartitionExecutor)
              executorConstructor.newInstance(conf.getSeriesPartitionSlotNum());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new StatementAnalyzeException(
          String.format(
              "Couldn't Constructor SeriesPartitionExecutor class: %s",
              conf.getSeriesPartitionExecutorClass()));
    }
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree)
      throws StatementAnalyzeException {
    try {
      TSchemaPartitionResp schemaPartitionResp =
          client.getSchemaPartition(constructSchemaPartitionReq(patternTree));
      if (schemaPartitionResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseSchemaPartitionResp(schemaPartitionResp);
      }
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException("An error occurred when executing getSchemaPartition()");
    }
    return null;
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree)
      throws StatementAnalyzeException {
    try {
      TSchemaPartitionResp schemaPartitionResp =
          client.getOrCreateSchemaPartition(constructSchemaPartitionReq(patternTree));
      if (schemaPartitionResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseSchemaPartitionResp(schemaPartitionResp);
      }
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateSchemaPartition()");
    }
    return null;
  }

  @Override
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap)
      throws StatementAnalyzeException {
    try {
      TDataPartitionResp dataPartitionResp =
          client.getDataPartition(constructDataPartitionReq(sgNameToQueryParamsMap));
      if (dataPartitionResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseDataPartitionResp(dataPartitionResp);
      }
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException("An error occurred when executing getDataPartition()");
    }
    return null;
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap)
      throws StatementAnalyzeException {
    try {
      TDataPartitionResp dataPartitionResp =
          client.getOrCreateDataPartition(constructDataPartitionReq(sgNameToQueryParamsMap));
      if (dataPartitionResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return parseDataPartitionResp(dataPartitionResp);
      }
    } catch (IoTDBConnectionException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition()");
    }
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

  private TDataPartitionReq constructDataPartitionReq(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
        sgNameToQueryParamsMap.entrySet()) {
      String sgName = entry.getKey();
      List<DataPartitionQueryParam> queryParams = entry.getValue();
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> tmpMap = new HashMap<>();
      queryParams.forEach(
          queryParam -> {
            List<TimePartitionSlot> timePartitionSlotList = queryParam.getTimePartitionSlotList();
            List<TTimePartitionSlot> tTimePartitionSlots =
                timePartitionSlotList.stream()
                    .map(
                        timePartitionSlot ->
                            new TTimePartitionSlot(timePartitionSlot.getStartTime()))
                    .collect(Collectors.toList());
            tmpMap.put(
                new TSeriesPartitionSlot(
                    partitionExecutor
                        .getSeriesPartitionSlot(queryParam.getDevicePath())
                        .getSlotId()),
                tTimePartitionSlots);
          });
      partitionSlotsMap.computeIfAbsent(sgName, key -> new HashMap<>()).putAll(tmpMap);
    }
    return new TDataPartitionReq(partitionSlotsMap);
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

  private DataPartition parseDataPartitionResp(TDataPartitionResp dataPartitionResp) {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        respDataPartitionMap = dataPartitionResp.getDataPartitionMap();
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    respDataPartitionMap.forEach(
        (storageGroupName, respDataRegionMap) -> {
          Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>> dataRegionMap =
              new HashMap<>();
          respDataRegionMap.forEach(
              ((respSeriesPartitionSlot, respTimePartitionSlotListMap) -> {
                SeriesPartitionSlot seriesPartitionSlot =
                    new SeriesPartitionSlot(respSeriesPartitionSlot.getSlotId());
                Map<TimePartitionSlot, List<RegionReplicaSet>> timePartitionSlotListMap =
                    new HashMap<>();
                respTimePartitionSlotListMap.forEach(
                    (respTimePartitionSlot, respRegionReplicaSetList) -> {
                      TimePartitionSlot timePartitionSlot =
                          new TimePartitionSlot(respTimePartitionSlot.getStartTime());
                      List<RegionReplicaSet> regionReplicaSetList = new ArrayList<>();
                      respRegionReplicaSetList.forEach(
                          respRegionReplicaSet -> {
                            RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
                            try {
                              regionReplicaSet.setConsensusGroupId(
                                  ConsensusGroupId.Factory.create(respRegionReplicaSet.regionId));
                            } catch (IOException e) {
                              e.printStackTrace();
                            }
                            List<DataNodeLocation> dataNodeList = new ArrayList<>();
                            respRegionReplicaSet
                                .getEndpoint()
                                .forEach(
                                    respEndpoint -> {
                                      dataNodeList.add(
                                          new DataNodeLocation(
                                              new Endpoint(
                                                  respEndpoint.getIp(), respEndpoint.getPort())));
                                    });
                            regionReplicaSet.setDataNodeList(dataNodeList);
                            regionReplicaSetList.add(regionReplicaSet);
                          });
                      timePartitionSlotListMap.put(timePartitionSlot, regionReplicaSetList);
                    });
                dataRegionMap.put(seriesPartitionSlot, timePartitionSlotListMap);
              }));
          dataPartitionMap.put(storageGroupName, dataRegionMap);
        });
    return new DataPartition(dataPartitionMap);
  }
}
