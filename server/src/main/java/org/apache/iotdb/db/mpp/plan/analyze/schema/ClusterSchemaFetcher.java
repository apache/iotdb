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
package org.apache.iotdb.db.mpp.plan.analyze.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();
  private final DataNodeSchemaCache schemaCache = DataNodeSchemaCache.getInstance();
  private final ITemplateManager templateManager = ClusterTemplateManager.getInstance();

  private final AutoCreateSchemaExecutor autoCreateSchemaExecutor =
      new AutoCreateSchemaExecutor(
          templateManager,
          statement -> {
            long queryId = SessionManager.getInstance().requestQueryId();
            return coordinator.execute(
                statement,
                queryId,
                null,
                "",
                ClusterPartitionFetcher.getInstance(),
                this,
                config.getQueryTimeoutThreshold());
          });
  private final ClusterSchemaFetchExecutor clusterSchemaFetchExecutor =
      new ClusterSchemaFetchExecutor(
          coordinator,
          templateManager,
          () -> SessionManager.getInstance().requestQueryId(),
          (queryId, statement) ->
              coordinator.execute(
                  statement,
                  queryId,
                  null,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  this,
                  config.getQueryTimeoutThreshold()),
          schemaCache::put);

  private static final class ClusterSchemaFetcherHolder {
    private static final ClusterSchemaFetcher INSTANCE = new ClusterSchemaFetcher();

    private ClusterSchemaFetcherHolder() {}
  }

  public static ClusterSchemaFetcher getInstance() {
    return ClusterSchemaFetcherHolder.INSTANCE;
  }

  private ClusterSchemaFetcher() {}

  @Override
  public ClusterSchemaTree fetchSchema(PathPatternTree patternTree) {
    patternTree.constructTree();
    List<PartialPath> pathPatternList = patternTree.getAllPathPatterns();
    List<PartialPath> fullPathList = new ArrayList<>();
    for (PartialPath pattern : pathPatternList) {
      if (!pattern.hasWildcard()) {
        fullPathList.add(pattern);
      }
    }

    if (fullPathList.size() < pathPatternList.size()) {
      return clusterSchemaFetchExecutor.fetchSchemaOfFuzzyMatch(patternTree, false);
    }

    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {
      ClusterSchemaTree schemaTree;
      if (fullPathList.size() == pathPatternList.size()) {
        boolean isAllCached = true;
        schemaTree = new ClusterSchemaTree();
        ClusterSchemaTree cachedSchema;
        Set<String> storageGroupSet = new HashSet<>();
        for (PartialPath fullPath : fullPathList) {
          cachedSchema = schemaCache.get(fullPath);
          if (cachedSchema.isEmpty()) {
            isAllCached = false;
            break;
          } else {
            schemaTree.mergeSchemaTree(cachedSchema);
            storageGroupSet.addAll(cachedSchema.getDatabases());
          }
        }
        if (isAllCached) {
          schemaTree.setDatabases(storageGroupSet);
          return schemaTree;
        }
      }

      return clusterSchemaFetchExecutor.fetchSchemaOfPreciseMatch(fullPathList, patternTree);
    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public ClusterSchemaTree fetchSchemaWithTags(PathPatternTree patternTree) {
    patternTree.constructTree();
    return clusterSchemaFetchExecutor.fetchSchemaOfFuzzyMatch(patternTree, true);
  }

  @Override
  public ISchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      boolean isAligned) {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {
      ClusterSchemaTree schemaTree = schemaCache.get(devicePath, measurements);
      List<Integer> indexOfMissingMeasurements =
          checkMissingMeasurements(schemaTree, devicePath, measurements);

      // all schema can be taken from cache
      if (indexOfMissingMeasurements.isEmpty()) {
        return schemaTree;
      }

      // try fetch the missing schema from remote and cache fetched schema
      ClusterSchemaTree remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfOneDevice(
              devicePath, measurements, indexOfMissingMeasurements);
      if (!remoteSchemaTree.isEmpty()) {
        schemaTree.mergeSchemaTree(remoteSchemaTree);
      }

      if (!config.isAutoCreateSchemaEnabled()) {
        return schemaTree;
      }

      // auto create the still missing schema and merge them into schemaTree
      checkAndAutoCreateMissingMeasurements(
          schemaTree,
          devicePath,
          indexOfMissingMeasurements,
          measurements,
          getDataType,
          null,
          null,
          isAligned);

      return schemaTree;
    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {
    return fetchSchemaListWithAutoCreate(
        devicePathList, measurementsList, tsDataTypesList, null, null, isAlignedList);
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<TSEncoding[]> encodingsList,
      List<CompressionType[]> compressionTypesList,
      List<Boolean> isAlignedList) {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      List<List<Integer>> indexOfMissingMeasurementsList = new ArrayList<>(devicePathList.size());
      boolean hasMissingMeasurement = false;
      for (int i = 0; i < devicePathList.size(); i++) {
        schemaTree.mergeSchemaTree(schemaCache.get(devicePathList.get(i), measurementsList.get(i)));
        List<Integer> indexOfMissingMeasurements =
            checkMissingMeasurements(schemaTree, devicePathList.get(i), measurementsList.get(i));
        indexOfMissingMeasurementsList.add(indexOfMissingMeasurements);
        if (!indexOfMissingMeasurements.isEmpty()) {
          hasMissingMeasurement = true;
        }
      }

      // all schema can be taken from cache
      if (!hasMissingMeasurement) {
        return schemaTree;
      }

      // try fetch the missing schema from remote and cache fetched schema
      ClusterSchemaTree remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfMultiDevices(
              devicePathList, measurementsList, indexOfMissingMeasurementsList);
      if (!remoteSchemaTree.isEmpty()) {
        schemaTree.mergeSchemaTree(remoteSchemaTree);
      }

      if (!config.isAutoCreateSchemaEnabled()) {
        return schemaTree;
      }

      // auto create the still missing schema and merge them into schemaTree
      for (int i = 0; i < devicePathList.size(); i++) {
        int finalI = i;
        checkAndAutoCreateMissingMeasurements(
            schemaTree,
            devicePathList.get(i),
            indexOfMissingMeasurementsList.get(i),
            measurementsList.get(i),
            index -> tsDataTypesList.get(finalI)[index],
            encodingsList == null ? null : encodingsList.get(i),
            compressionTypesList == null ? null : compressionTypesList.get(i),
            isAlignedList.get(i));
      }
      return schemaTree;
    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path) {
    return templateManager.checkTemplateSetInfo(path);
  }

  @Override
  public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
    return templateManager.checkAllRelatedTemplate(pathPattern);
  }

  @Override
  public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
    return templateManager.getAllPathsSetTemplate(templateName);
  }

  // check which measurements are missing and auto create the missing measurements and merge them
  // into given schemaTree
  private void checkAndAutoCreateMissingMeasurements(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<Integer> indexOfMissingMeasurements,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
      boolean isAligned) {
    // check missing measurements
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(
            devicePath,
            indexOfMissingMeasurements.stream()
                .map(index -> measurements[index])
                .collect(Collectors.toList()));
    if (deviceSchemaInfo != null) {
      List<MeasurementSchema> schemaList = deviceSchemaInfo.getMeasurementSchemaList();
      int removedCount = 0;
      for (int i = 0, size = schemaList.size(); i < size; i++) {
        if (schemaList.get(i) != null) {
          indexOfMissingMeasurements.remove(i - removedCount);
          removedCount++;
        }
      }
    }
    if (indexOfMissingMeasurements.isEmpty()) {
      return;
    }

    autoCreateSchemaExecutor.autoCreateMissingMeasurements(
        schemaTree,
        devicePath,
        indexOfMissingMeasurements,
        measurements,
        getDataType,
        encodings,
        compressionTypes,
        isAligned);
  }

  private List<Integer> checkMissingMeasurements(
      ISchemaTree schemaTree, PartialPath devicePath, String[] measurements) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));
    if (deviceSchemaInfo == null) {
      return IntStream.range(0, measurements.length).boxed().collect(Collectors.toList());
    }

    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    List<MeasurementSchema> schemaList = deviceSchemaInfo.getMeasurementSchemaList();
    for (int i = 0; i < measurements.length; i++) {
      if (schemaList.get(i) == null) {
        indexOfMissingMeasurements.add(i);
      }
    }

    return indexOfMissingMeasurements;
  }

  @Override
  public void invalidAllCache() {
    DataNodeSchemaCache.getInstance().cleanUp();
  }
}
