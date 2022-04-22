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

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterPartitionFetcher implements IPartitionFetcher {

  private final ConfigNodeClient client;

  private final SeriesPartitionExecutor partitionExecutor;

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
    this.partitionExecutor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
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
    return new SchemaPartition(
        schemaPartitionResp.getSchemaRegionMap(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  private DataPartition parseDataPartitionResp(TDataPartitionResp dataPartitionResp) {
    return new DataPartition(
        dataPartitionResp.getDataPartitionMap(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }
}
