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
package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandalonePartitionFetcher implements IPartitionFetcher {

  private final LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  private static final class StandalonePartitionFetcherHolder {
    private static final StandalonePartitionFetcher INSTANCE = new StandalonePartitionFetcher();

    private StandalonePartitionFetcherHolder() {}
  }

  public static StandalonePartitionFetcher getInstance() {
    return StandalonePartitionFetcher.StandalonePartitionFetcherHolder.INSTANCE;
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
    return null;
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
    return null;
  }

  @Override
  public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      PathPatternTree patternTree, Integer level) {
    return null;
  }

  @Override
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    try {
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap = new HashMap<>();
      for (Map.Entry<String, List<DataPartitionQueryParam>> sgEntry :
          sgNameToQueryParamsMap.entrySet()) {
        // for each sg
        String storageGroupName = sgEntry.getKey();
        List<DataPartitionQueryParam> dataPartitionQueryParams = sgEntry.getValue();
        Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            deviceToRegionsMap = new HashMap<>();
        for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
          // for each device
          String deviceId = dataPartitionQueryParam.getDevicePath();
          DataRegionId dataRegionId =
              localConfigNode.getBelongedDataRegionRegionId(new PartialPath(deviceId));
          Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionToRegionsMap =
              new HashMap<>();
          for (TTimePartitionSlot timePartitionSlot :
              dataPartitionQueryParam.getTimePartitionSlotList()) {
            // for each time partition
            timePartitionToRegionsMap.put(
                timePartitionSlot,
                Collections.singletonList(
                    new TRegionReplicaSet(
                        new TConsensusGroupId(dataRegionId.getType(), dataRegionId.getId()),
                        Collections.EMPTY_LIST)));
          }
          deviceToRegionsMap.put(new TSeriesPartitionSlot(), timePartitionToRegionsMap);
        }
        dataPartitionMap.put(storageGroupName, deviceToRegionsMap);
      }
      return new DataPartition(
          dataPartitionMap,
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
    } catch (MetadataException | DataRegionException e) {
      throw new StatementAnalyzeException("An error occurred when executing getDataPartition()");
    }
  }

  @Override
  public DataPartition getDataPartition(List<DataPartitionQueryParam> dataPartitionQueryParams) {
    return null;
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    return getDataPartition(sgNameToQueryParamsMap);
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      List<DataPartitionQueryParam> dataPartitionQueryParams) {
    return null;
  }

  @Override
  public void invalidAllCache() {}
}
