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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterPartitionFetcher implements IPartitionFetcher {

  private final ConfigNodeClient client;

  private SeriesPartitionExecutor partitionExecutor;

  private static final class ClusterPartitionFetcherHolder {
    private static final ClusterPartitionFetcher INSTANCE = new ClusterPartitionFetcher();

    private ClusterPartitionFetcherHolder() {}
  }

  public static ClusterPartitionFetcher getInstance() {
    return ClusterPartitionFetcherHolder.INSTANCE;
  }

  private ClusterPartitionFetcher() {
    try {
      client = new ConfigNodeClient();
    } catch (IoTDBConnectionException | BadNodeUrlException e) {
      throw new StatementAnalyzeException("Couldn't connect config node");
    }
    initPartitionExecutor();
  }

  private void initPartitionExecutor() {
    // TODO: @crz move to node-commons
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
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
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
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
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
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
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
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
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
      serializedPatternTree.put(baos.getBuf(), 0, baos.size());
      serializedPatternTree.flip();
      return new TSchemaPartitionReq(serializedPatternTree);
    } catch (IOException e) {
      throw new StatementAnalyzeException("An error occurred when serializing pattern tree");
    }
  }

  private TDataPartitionReq constructDataPartitionReq(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    for (Map.Entry<String, List<DataPartitionQueryParam>> entry :
        sgNameToQueryParamsMap.entrySet()) {
      // for each sg
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> deviceToTimePartitionMap =
          new HashMap<>();
      for (DataPartitionQueryParam queryParam : entry.getValue()) {
        deviceToTimePartitionMap.put(
            new TSeriesPartitionSlot(
                partitionExecutor.getSeriesPartitionSlot(queryParam.getDevicePath()).getSlotId()),
            queryParam.getTimePartitionSlotList().stream()
                .map(timePartitionSlot -> new TTimePartitionSlot(timePartitionSlot.getStartTime()))
                .collect(Collectors.toList()));
      }
      partitionSlotsMap.put(entry.getKey(), deviceToTimePartitionMap);
    }
    return new TDataPartitionReq(partitionSlotsMap);
  }

  private SchemaPartition parseSchemaPartitionResp(TSchemaPartitionResp schemaPartitionResp) {
    Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap = new HashMap<>();
    for (Map.Entry<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> sgEntry :
        schemaPartitionResp.getSchemaRegionMap().entrySet()) {
      // for each sg
      String storageGroupName = sgEntry.getKey();
      Map<SeriesPartitionSlot, RegionReplicaSet> deviceToSchemaRegionMap = new HashMap<>();
      for (Map.Entry<TSeriesPartitionSlot, TRegionReplicaSet> deviceEntry :
          sgEntry.getValue().entrySet()) {
        deviceToSchemaRegionMap.put(
            new SeriesPartitionSlot(deviceEntry.getKey().getSlotId()),
            new RegionReplicaSet(deviceEntry.getValue()));
      }
      schemaPartitionMap.put(storageGroupName, deviceToSchemaRegionMap);
    }
    return new SchemaPartition(
        schemaPartitionMap,
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  private DataPartition parseDataPartitionResp(TDataPartitionResp dataPartitionResp) {
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        dataPartitionMap = new HashMap<>();
    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        sgEntry : dataPartitionResp.getDataPartitionMap().entrySet()) {
      // for each sg
      String storageGroupName = sgEntry.getKey();
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          respDeviceToRegionsMap = sgEntry.getValue();
      Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>> deviceToRegionsMap =
          new HashMap<>();
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          deviceEntry : respDeviceToRegionsMap.entrySet()) {
        // for each device
        Map<TimePartitionSlot, List<RegionReplicaSet>> timePartitionToRegionsMap = new HashMap<>();
        for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionEntry :
            deviceEntry.getValue().entrySet()) {
          // for each time partition
          timePartitionToRegionsMap.put(
              new TimePartitionSlot(timePartitionEntry.getKey().getStartTime()),
              timePartitionEntry.getValue().stream()
                  .map(RegionReplicaSet::new)
                  .collect(Collectors.toList()));
        }
        deviceToRegionsMap.put(
            new SeriesPartitionSlot(deviceEntry.getKey().getSlotId()), timePartitionToRegionsMap);
      }
      dataPartitionMap.put(storageGroupName, deviceToRegionsMap);
    }
    return new DataPartition(
        dataPartitionMap,
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }
}
