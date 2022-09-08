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

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.mnode.MNodeType;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class StandalonePartitionFetcher implements IPartitionFetcher {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(StandalonePartitionFetcher.class);
  private final LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
  private final StorageEngineV2 storageEngine = StorageEngineV2.getInstance();

  private final SeriesPartitionExecutor executor =
      SeriesPartitionExecutor.getSeriesPartitionExecutor(
          config.getSeriesPartitionExecutorClass(), config.getSeriesPartitionSlotNum());

  private static final class StandalonePartitionFetcherHolder {
    private static final StandalonePartitionFetcher INSTANCE = new StandalonePartitionFetcher();

    private StandalonePartitionFetcherHolder() {}
  }

  public static StandalonePartitionFetcher getInstance() {
    return StandalonePartitionFetcher.StandalonePartitionFetcherHolder.INSTANCE;
  }

  @Override
  public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
    patternTree.constructTree();
    return new SchemaPartition(
        localConfigNode.getSchemaPartition(patternTree),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(PathPatternTree patternTree) {
    patternTree.constructTree();
    return new SchemaPartition(
        localConfigNode.getOrCreateSchemaPartition(patternTree),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
        IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  }

  @Override
  public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
      PathPatternTree patternTree, Integer level) {
    try {
      patternTree.constructTree();
      Set<TSchemaNode> matchedNodes = new HashSet<>();
      Set<PartialPath> involvedStorageGroup = new HashSet<>();
      if (level == null) {
        // Get Child
        for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
          Pair<Set<TSchemaNode>, Set<PartialPath>> result =
              localConfigNode.getChildNodePathInNextLevel(pathPattern);
          matchedNodes.addAll(result.left);
          involvedStorageGroup.addAll(result.right);
        }
      } else {
        for (PartialPath pathPattern : patternTree.getAllPathPatterns()) {
          Pair<List<PartialPath>, Set<PartialPath>> result =
              localConfigNode.getNodesListInGivenLevel(pathPattern, level, false, null);
          matchedNodes.addAll(
              result.left.stream()
                  .map(
                      path ->
                          new TSchemaNode(path.getFullPath(), MNodeType.UNIMPLEMENT.getNodeType()))
                  .collect(Collectors.toList()));
          involvedStorageGroup.addAll(result.right);
        }
      }

      PathPatternTree partitionReq = new PathPatternTree();
      involvedStorageGroup.forEach(
          storageGroup ->
              partitionReq.appendPathPattern(storageGroup.concatNode(MULTI_LEVEL_PATH_WILDCARD)));

      return new SchemaNodeManagementPartition(
          localConfigNode.getSchemaPartition(patternTree),
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
          IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum(),
          matchedNodes);
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataPartition getDataPartition(
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
    try {
      return localConfigNode.getDataPartition(sgNameToQueryParamsMap);
    } catch (MetadataException | DataRegionException e) {
      logger.error("Meet error when get DataPartition", e);
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
    try {
      return localConfigNode.getOrCreateDataPartition(sgNameToQueryParamsMap);
    } catch (MetadataException | DataRegionException e) {
      throw new StatementAnalyzeException("An error occurred when executing getDataPartition()");
    }
  }

  @Override
  public DataPartition getOrCreateDataPartition(
      List<DataPartitionQueryParam> dataPartitionQueryParams) {
    try {
      Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParams =
          splitDataPartitionQueryParam(dataPartitionQueryParams, true);
      return getOrCreateDataPartition(splitDataPartitionQueryParams);
    } catch (MetadataException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
    }
  }

  @Override
  public boolean updateRegionCache(TRegionRouteReq req) {
    return true;
  }

  @Override
  public void invalidAllCache() {}

  /** Split data partition query param by storage group. */
  private Map<String, List<DataPartitionQueryParam>> splitDataPartitionQueryParam(
      List<DataPartitionQueryParam> dataPartitionQueryParams, boolean isAutoCreate)
      throws MetadataException {
    List<String> devicePaths = new ArrayList<>();
    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      devicePaths.add(dataPartitionQueryParam.getDevicePath());
    }
    Map<String, String> deviceToStorageGroup = getDeviceToStorageGroup(devicePaths, isAutoCreate);

    Map<String, List<DataPartitionQueryParam>> result = new HashMap<>();
    for (DataPartitionQueryParam dataPartitionQueryParam : dataPartitionQueryParams) {
      String devicePath = dataPartitionQueryParam.getDevicePath();
      if (deviceToStorageGroup.containsKey(devicePath)) {
        String storageGroup = deviceToStorageGroup.get(devicePath);
        result.computeIfAbsent(storageGroup, v -> new ArrayList<>());
        result.get(storageGroup).add(dataPartitionQueryParam);
      }
    }
    return result;
  }

  /** get deviceToStorageGroup map. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private Map<String, String> getDeviceToStorageGroup(
      List<String> devicePaths, boolean isAutoCreate) throws MetadataException {
    Map<String, String> deviceToStorageGroup = new HashMap<>();
    // miss when devicePath contains *
    for (String devicePath : devicePaths) {
      if (devicePath.contains("*")) {
        return deviceToStorageGroup;
      }
    }
    try {
      deviceToStorageGroup = new HashMap<>();
      List<PartialPath> allStorageGroups = localConfigNode.getAllStorageGroupPaths();
      for (String devicePath : devicePaths) {
        for (PartialPath storageGroup : allStorageGroups) {
          if (PathUtils.isStartWith(devicePath, storageGroup.getFullPath())) {
            deviceToStorageGroup.put(devicePath, storageGroup.getFullPath());
          }
        }
      }
      if (isAutoCreate) {
        // try to auto create storage group
        Set<PartialPath> storageGroupNamesNeedCreated = new HashSet<>();
        for (String devicePath : devicePaths) {
          if (!deviceToStorageGroup.containsKey(devicePath)) {
            PartialPath storageGroupNameNeedCreated =
                MetaUtils.getStorageGroupPathByLevel(
                    new PartialPath(devicePath), config.getDefaultStorageGroupLevel());
            storageGroupNamesNeedCreated.add(storageGroupNameNeedCreated);
            deviceToStorageGroup.put(devicePath, storageGroupNameNeedCreated.getFullPath());
          }
        }
        for (PartialPath storageGroupName : storageGroupNamesNeedCreated) {
          localConfigNode.setStorageGroup(storageGroupName);
        }
      }
    } catch (MetadataException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getOrCreateDataPartition():" + e.getMessage());
    }
    return deviceToStorageGroup;
  }
}
