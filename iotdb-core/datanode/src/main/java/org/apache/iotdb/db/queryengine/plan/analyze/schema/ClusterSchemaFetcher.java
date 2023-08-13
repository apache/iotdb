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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();
  private final DataNodeSchemaCache schemaCache = DataNodeSchemaCache.getInstance();
  private final ITemplateManager templateManager = ClusterTemplateManager.getInstance();

  private final AutoCreateSchemaExecutor autoCreateSchemaExecutor =
      new AutoCreateSchemaExecutor(coordinator, templateManager, this);
  private final ClusterSchemaFetchExecutor clusterSchemaFetchExecutor =
      new ClusterSchemaFetchExecutor(coordinator, templateManager, this, schemaCache::put);

  private final NormalSchemaFetcher normalSchemaFetcher =
      new NormalSchemaFetcher(schemaCache, autoCreateSchemaExecutor, clusterSchemaFetchExecutor);
  private final TemplateSchemaFetcher templateSchemaFetcher =
      new TemplateSchemaFetcher(schemaCache, autoCreateSchemaExecutor, clusterSchemaFetchExecutor);

  private static final class ClusterSchemaFetcherHolder {
    private static final ClusterSchemaFetcher INSTANCE = new ClusterSchemaFetcher();

    private ClusterSchemaFetcherHolder() {}
  }

  public static ClusterSchemaFetcher getInstance() {
    return ClusterSchemaFetcherHolder.INSTANCE;
  }

  private ClusterSchemaFetcher() {}

  @Override
  public ClusterSchemaTree fetchSchema(PathPatternTree patternTree, MPPQueryContext context) {
    patternTree.constructTree();
    List<PartialPath> pathPatternList = patternTree.getAllPathPatterns();
    List<PartialPath> explicitPathList = new ArrayList<>();
    List<PartialPath> explicitDevicePatternList = new ArrayList<>();
    for (PartialPath pattern : pathPatternList) {
      if (!pattern.hasWildcard()) {
        explicitPathList.add(pattern);
      } else if (pattern.hasExplicitDevice()
          && templateManager.checkTemplateSetInfo(pattern) != null) {
        explicitDevicePatternList.add(pattern);
      }
    }

    if (explicitPathList.size() + explicitDevicePatternList.size() < pathPatternList.size()) {
      return clusterSchemaFetchExecutor.fetchSchemaOfFuzzyMatch(patternTree, false, context);
    }

    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      boolean isAllCached = true;

      ClusterSchemaTree cachedSchema;
      Set<String> storageGroupSet = new HashSet<>();
      if (!explicitDevicePatternList.isEmpty()) {
        for (PartialPath explicitDevicePattern : explicitDevicePatternList) {
          cachedSchema = schemaCache.getMatchedSchemaWithTemplate(explicitDevicePattern);
          if (cachedSchema.isEmpty()) {
            isAllCached = false;
            break;
          } else {
            schemaTree.mergeSchemaTree(cachedSchema);
            storageGroupSet.addAll(cachedSchema.getDatabases());
          }
        }
      }

      if (isAllCached && !explicitPathList.isEmpty()) {
        for (PartialPath fullPath : explicitPathList) {
          cachedSchema = schemaCache.get(fullPath);
          if (cachedSchema.isEmpty()) {
            isAllCached = false;
            break;
          } else {
            schemaTree.mergeSchemaTree(cachedSchema);
            storageGroupSet.addAll(cachedSchema.getDatabases());
          }
        }
      }

      if (isAllCached) {
        schemaTree.setDatabases(storageGroupSet);
        return schemaTree;
      }

      return clusterSchemaFetchExecutor.fetchSchemaOfPreciseMatchOrPreciseDeviceUsingTemplate(
          pathPatternList, patternTree, context);

    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public ClusterSchemaTree fetchSchemaWithTags(
      PathPatternTree patternTree, MPPQueryContext context) {
    patternTree.constructTree();
    return clusterSchemaFetchExecutor.fetchSchemaOfFuzzyMatch(patternTree, true, context);
  }

  @Override
  public void fetchAndComputeSchemaWithAutoCreate(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation,
      MPPQueryContext context) {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {
      Pair<Template, PartialPath> templateSetInfo =
          templateManager.checkTemplateSetInfo(schemaComputationWithAutoCreation.getDevicePath());
      List<Integer> indexOfMissingMeasurements;
      if (templateSetInfo == null) {
        // normal timeseries
        indexOfMissingMeasurements =
            normalSchemaFetcher.processNormalTimeSeries(schemaComputationWithAutoCreation, context);
      } else {
        // template timeseries
        indexOfMissingMeasurements =
            templateSchemaFetcher.processTemplateTimeSeries(
                templateSetInfo, schemaComputationWithAutoCreation, context);
      }

      // all schema has been taken and processed
      if (indexOfMissingMeasurements.isEmpty()) {
        return;
      }

      // offer null for the rest missing schema processing
      for (int index : indexOfMissingMeasurements) {
        schemaComputationWithAutoCreation.computeMeasurement(index, null);
      }
    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public void fetchAndComputeSchemaWithAutoCreate(
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList,
      MPPQueryContext context) {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {

      List<ISchemaComputationWithAutoCreation> normalTimeSeriesRequestList = new ArrayList<>();
      List<ISchemaComputationWithAutoCreation> templateTimeSeriesRequestList = new ArrayList<>();
      List<Pair<Template, PartialPath>> templateSetInfoList = new ArrayList<>();
      Pair<Template, PartialPath> templateSetInfo;
      for (ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation :
          schemaComputationWithAutoCreationList) {
        templateSetInfo =
            templateManager.checkTemplateSetInfo(schemaComputationWithAutoCreation.getDevicePath());
        if (templateSetInfo == null) {
          normalTimeSeriesRequestList.add(schemaComputationWithAutoCreation);
        } else {
          templateTimeSeriesRequestList.add(schemaComputationWithAutoCreation);
          templateSetInfoList.add(templateSetInfo);
        }
      }

      if (!normalTimeSeriesRequestList.isEmpty()) {
        normalSchemaFetcher.processNormalTimeSeries(normalTimeSeriesRequestList, context);
      }
      if (!templateTimeSeriesRequestList.isEmpty()) {
        templateSchemaFetcher.processTemplateTimeSeries(
            templateSetInfoList, templateTimeSeriesRequestList, context);
      }
    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<TSEncoding[]> encodingsList,
      List<CompressionType[]> compressionTypesList,
      List<Boolean> isAlignedList,
      MPPQueryContext context) {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeseries will be effective.
    schemaCache.takeReadLock();
    try {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      List<List<Integer>> indexOfMissingMeasurementsList = new ArrayList<>(devicePathList.size());
      List<Integer> indexOfDevicesWithMissingMeasurements = new ArrayList<>();
      for (int i = 0; i < devicePathList.size(); i++) {
        schemaTree.mergeSchemaTree(schemaCache.get(devicePathList.get(i), measurementsList.get(i)));
        List<Integer> indexOfMissingMeasurements =
            checkMissingMeasurements(schemaTree, devicePathList.get(i), measurementsList.get(i));
        if (!indexOfMissingMeasurements.isEmpty()) {
          indexOfDevicesWithMissingMeasurements.add(i);
          indexOfMissingMeasurementsList.add(indexOfMissingMeasurements);
        }
      }

      // all schema can be taken from cache
      if (indexOfDevicesWithMissingMeasurements.isEmpty()) {
        return schemaTree;
      }

      // try fetch the missing schema from remote and cache fetched schema
      ClusterSchemaTree remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfMultiDevices(
              devicePathList,
              measurementsList,
              indexOfDevicesWithMissingMeasurements,
              indexOfMissingMeasurementsList,
              context);
      if (!remoteSchemaTree.isEmpty()) {
        schemaTree.mergeSchemaTree(remoteSchemaTree);
      }

      if (!config.isAutoCreateSchemaEnabled()) {
        return schemaTree;
      }

      // auto create the still missing schema and merge them into schemaTree
      List<Integer> indexOfDevicesNeedAutoCreateSchema = new ArrayList<>();
      List<List<Integer>> indexOfMeasurementsNeedAutoCreate = new ArrayList<>();
      List<Integer> indexOfMissingMeasurements;
      int deviceIndex;
      for (int i = 0, size = indexOfDevicesWithMissingMeasurements.size(); i < size; i++) {
        deviceIndex = indexOfDevicesWithMissingMeasurements.get(i);
        indexOfMissingMeasurements = indexOfMissingMeasurementsList.get(i);
        indexOfMissingMeasurements =
            checkMissingMeasurementsAfterSchemaFetch(
                schemaTree,
                devicePathList.get(deviceIndex),
                indexOfMissingMeasurements,
                measurementsList.get(deviceIndex));
        if (!indexOfMissingMeasurements.isEmpty()) {
          indexOfDevicesNeedAutoCreateSchema.add(deviceIndex);
          indexOfMeasurementsNeedAutoCreate.add(indexOfMissingMeasurements);
        }
      }

      if (!indexOfDevicesNeedAutoCreateSchema.isEmpty()) {
        autoCreateSchemaExecutor.autoCreateMissingMeasurements(
            schemaTree,
            devicePathList,
            indexOfDevicesNeedAutoCreateSchema,
            indexOfMeasurementsNeedAutoCreate,
            measurementsList,
            tsDataTypesList,
            encodingsList,
            compressionTypesList,
            isAlignedList,
            context);
      }

      return schemaTree;
    } finally {
      schemaCache.releaseReadLock();
    }
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath devicePath) {
    return templateManager.checkTemplateSetInfo(devicePath);
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetAndPreSetInfo(
      PartialPath timeSeriesPath, String alias) {
    return templateManager.checkTemplateSetAndPreSetInfo(timeSeriesPath, alias);
  }

  @Override
  public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
    return templateManager.checkAllRelatedTemplate(pathPattern);
  }

  @Override
  public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
    return templateManager.getAllPathsSetTemplate(templateName);
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

  private List<Integer> checkMissingMeasurementsAfterSchemaFetch(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<Integer> indexOfTargetMeasurements,
      String[] measurements) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(
            devicePath,
            indexOfTargetMeasurements.stream()
                .map(index -> measurements[index])
                .collect(Collectors.toList()));
    if (deviceSchemaInfo == null) {
      return indexOfTargetMeasurements;
    }

    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    List<MeasurementSchema> schemaList = deviceSchemaInfo.getMeasurementSchemaList();
    for (int i = 0, size = schemaList.size(); i < size; i++) {
      if (schemaList.get(i) == null) {
        indexOfMissingMeasurements.add(indexOfTargetMeasurements.get(i));
      }
    }

    return indexOfMissingMeasurements;
  }
}
