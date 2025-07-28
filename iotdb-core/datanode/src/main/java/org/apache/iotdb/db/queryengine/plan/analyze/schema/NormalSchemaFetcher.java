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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class NormalSchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final TreeDeviceSchemaCacheManager schemaCache;

  private final AutoCreateSchemaExecutor autoCreateSchemaExecutor;
  private final ClusterSchemaFetchExecutor clusterSchemaFetchExecutor;

  NormalSchemaFetcher(
      TreeDeviceSchemaCacheManager schemaCache,
      AutoCreateSchemaExecutor autoCreateSchemaExecutor,
      ClusterSchemaFetchExecutor clusterSchemaFetchExecutor) {
    this.schemaCache = schemaCache;
    this.autoCreateSchemaExecutor = autoCreateSchemaExecutor;
    this.clusterSchemaFetchExecutor = clusterSchemaFetchExecutor;
  }

  /** Given full paths will be */
  private void computePatternTreeNeededReFetch(
      PathPatternTree patternTree, List<String> fullPaths) {
    for (String fullPath : fullPaths) {
      try {
        patternTree.appendFullPath(new MeasurementPath(fullPath));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private PathPatternTree computePatternTreeNeededReFetch(
      PartialPath devicePath,
      String[] measurementList,
      List<Integer> indexOfMissingMeasurements,
      List<String> fullPaths) {
    PathPatternTree patternTree = new PathPatternTree();
    for (int index : indexOfMissingMeasurements) {
      patternTree.appendFullPath(devicePath, measurementList[index]);
    }
    for (String fullPath : fullPaths) {
      try {
        patternTree.appendFullPath(new MeasurementPath(fullPath));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
    return patternTree;
  }

  private PathPatternTree computePatternTreeNeededReFetch(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<Integer> indexOfTargetDevices,
      List<List<Integer>> indexOfTargetMeasurementsList) {
    PathPatternTree patternTree = new PathPatternTree();
    int deviceIndex;
    for (int i = 0, size = indexOfTargetDevices.size(); i < size; i++) {
      deviceIndex = indexOfTargetDevices.get(i);
      for (int measurementIndex : indexOfTargetMeasurementsList.get(i)) {
        patternTree.appendFullPath(
            devicePathList.get(deviceIndex), measurementsList.get(deviceIndex)[measurementIndex]);
      }
    }
    return patternTree;
  }

  List<Integer> processNormalTimeSeries(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation,
      MPPQueryContext context) {
    // [Step 0] Record the input value.
    boolean isAlignedPutIn = schemaComputationWithAutoCreation.isAligned();

    // [Step 1] Cache 1. compute measurements and record logical views.
    List<Integer> indexOfMissingMeasurements =
        schemaCache.computeWithoutTemplate(schemaComputationWithAutoCreation);
    // [Step 2] Cache 2. process recorded logical views. If there is no views now, it returns empty
    // lists.
    Pair<List<Integer>, List<String>> missedIndexAndPathString =
        schemaCache.computeSourceOfLogicalView(schemaComputationWithAutoCreation);
    List<Integer> indexOfMissingLogicalView = missedIndexAndPathString.left;
    List<String> missedPathStringOfLogicalView = missedIndexAndPathString.right;
    // all schema can be taken from cache
    if (indexOfMissingMeasurements.isEmpty() && indexOfMissingLogicalView.isEmpty()) {
      return indexOfMissingMeasurements;
    }
    // [Step 3] Fetch 1. fetch schema from remote. Process logical view first; then process
    // measurements.
    // try fetch the missing raw schema from remote and cache fetched schema
    ClusterSchemaTree remoteSchemaTree;
    if (missedPathStringOfLogicalView.isEmpty()) {
      remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfOneDevice(
              schemaComputationWithAutoCreation.getDevicePath(),
              schemaComputationWithAutoCreation.getMeasurements(),
              indexOfMissingMeasurements,
              context);
    } else {
      PathPatternTree patternTree =
          computePatternTreeNeededReFetch(
              schemaComputationWithAutoCreation.getDevicePath(),
              schemaComputationWithAutoCreation.getMeasurements(),
              indexOfMissingMeasurements,
              missedPathStringOfLogicalView);
      remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaWithPatternTreeAndCache(patternTree, context);
    }
    // make sure all missed views are computed.
    remoteSchemaTree.computeSourceOfLogicalView(
        schemaComputationWithAutoCreation, indexOfMissingLogicalView);
    // check and compute the fetched schema
    indexOfMissingMeasurements =
        remoteSchemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);
    schemaComputationWithAutoCreation.recordRangeOfLogicalViewSchemaListNow();

    // [Step 4] Fetch 2. Some fetched measurements in [Step 3] are views. Process them.
    missedIndexAndPathString =
        schemaCache.computeSourceOfLogicalView(schemaComputationWithAutoCreation);
    indexOfMissingLogicalView = missedIndexAndPathString.left;
    missedPathStringOfLogicalView = missedIndexAndPathString.right;
    if (!missedPathStringOfLogicalView.isEmpty()) {
      ClusterSchemaTree viewSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaWithFullPaths(
              missedPathStringOfLogicalView, context);
      viewSchemaTree.computeSourceOfLogicalView(
          schemaComputationWithAutoCreation, indexOfMissingLogicalView);
    }

    // all schema has been taken and processed
    if (indexOfMissingMeasurements.isEmpty()) {
      return indexOfMissingMeasurements;
    }

    // [Step 5] Auto Create and process the missing schema
    if (config.isAutoCreateSchemaEnabled()) {
      // Check the isAligned value. If the input value is different from the actual value of the
      // existing device, throw exception.
      PartialPath devicePath = schemaComputationWithAutoCreation.getDevicePath();
      validateIsAlignedValueIfAutoCreate(
          schemaComputationWithAutoCreation.isAligned(), isAlignedPutIn, devicePath);
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      autoCreateSchemaExecutor.autoCreateTimeSeries(
          schemaTree,
          devicePath,
          indexOfMissingMeasurements,
          schemaComputationWithAutoCreation.getMeasurements(),
          schemaComputationWithAutoCreation::getDataType,
          isAlignedPutIn,
          context);
      indexOfMissingMeasurements =
          schemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);
    }

    return indexOfMissingMeasurements;
  }

  void processNormalTimeSeries(
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList,
      MPPQueryContext context) {
    // [Step 0] Record the input value.
    List<Boolean> isAlignedPutInList = null;
    if (config.isAutoCreateSchemaEnabled()) {
      isAlignedPutInList =
          schemaComputationWithAutoCreationList.stream()
              .map(ISchemaComputationWithAutoCreation::isAligned)
              .collect(Collectors.toList());
    }

    // [Step 1] Cache 1. compute measurements and record logical views.
    List<Integer> indexOfDevicesWithMissingMeasurements = new ArrayList<>();
    List<List<Integer>> indexOfMissingMeasurementsList =
        new ArrayList<>(schemaComputationWithAutoCreationList.size());
    List<Integer> indexOfMissingMeasurements;
    for (int i = 0, size = schemaComputationWithAutoCreationList.size(); i < size; i++) {
      indexOfMissingMeasurements =
          schemaCache.computeWithoutTemplate(schemaComputationWithAutoCreationList.get(i));
      if (!indexOfMissingMeasurements.isEmpty()) {
        indexOfDevicesWithMissingMeasurements.add(i);
        indexOfMissingMeasurementsList.add(indexOfMissingMeasurements);
      }
    }
    // [Step 2] Cache 2. process recorded logical views.
    boolean hasUnFetchedLogicalView = false;
    List<Pair<List<Integer>, List<String>>> missedIndexAndPathStringOfViewList =
        new ArrayList<>(schemaComputationWithAutoCreationList.size());
    for (ISchemaComputationWithAutoCreation iSchemaComputationWithAutoCreation :
        schemaComputationWithAutoCreationList) {
      Pair<List<Integer>, List<String>> missedIndexAndPathString =
          schemaCache.computeSourceOfLogicalView(iSchemaComputationWithAutoCreation);
      if (!missedIndexAndPathString.left.isEmpty()) {
        hasUnFetchedLogicalView = true;
      }
      missedIndexAndPathStringOfViewList.add(missedIndexAndPathString);
    }
    // all schema can be taken from cache
    if (indexOfDevicesWithMissingMeasurements.isEmpty() && (!hasUnFetchedLogicalView)) {
      return;
    }
    // [Step 3] Fetch 1.fetch schema from remote. Process logical view first; then process
    // measurements.
    // try fetch the missing schema from remote
    ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation;
    ClusterSchemaTree remoteSchemaTree;
    if (!hasUnFetchedLogicalView) {
      remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfMultiDevices(
              schemaComputationWithAutoCreationList.stream()
                  .map(ISchemaComputationWithAutoCreation::getDevicePath)
                  .collect(Collectors.toList()),
              schemaComputationWithAutoCreationList.stream()
                  .map(ISchemaComputationWithAutoCreation::getMeasurements)
                  .collect(Collectors.toList()),
              indexOfDevicesWithMissingMeasurements,
              indexOfMissingMeasurementsList,
              context);
    } else {
      PathPatternTree patternTree =
          computePatternTreeNeededReFetch(
              schemaComputationWithAutoCreationList.stream()
                  .map(ISchemaComputationWithAutoCreation::getDevicePath)
                  .collect(Collectors.toList()),
              schemaComputationWithAutoCreationList.stream()
                  .map(ISchemaComputationWithAutoCreation::getMeasurements)
                  .collect(Collectors.toList()),
              indexOfDevicesWithMissingMeasurements,
              indexOfMissingMeasurementsList);
      List<String> fullPathsNeedReFetch = new ArrayList<>();
      for (Pair<List<Integer>, List<String>> pair : missedIndexAndPathStringOfViewList) {
        fullPathsNeedReFetch.addAll(pair.right);
      }
      computePatternTreeNeededReFetch(patternTree, fullPathsNeedReFetch);
      remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaWithPatternTreeAndCache(patternTree, context);
    }
    // make sure all missed views are computed.
    for (int i = 0; i < schemaComputationWithAutoCreationList.size(); i++) {
      schemaComputationWithAutoCreation = schemaComputationWithAutoCreationList.get(i);
      remoteSchemaTree.computeSourceOfLogicalView(
          schemaComputationWithAutoCreation, missedIndexAndPathStringOfViewList.get(i).left);
    }
    // check and compute the fetched schema
    List<Integer> indexOfDevicesNeedAutoCreateSchema = new ArrayList<>();
    List<List<Integer>> indexOfMeasurementsNeedAutoCreate = new ArrayList<>();
    for (int i = 0; i < indexOfDevicesWithMissingMeasurements.size(); i++) {
      schemaComputationWithAutoCreation =
          schemaComputationWithAutoCreationList.get(indexOfDevicesWithMissingMeasurements.get(i));
      indexOfMissingMeasurements =
          remoteSchemaTree.compute(
              schemaComputationWithAutoCreation, indexOfMissingMeasurementsList.get(i));
      schemaComputationWithAutoCreation.recordRangeOfLogicalViewSchemaListNow();
      if (!indexOfMissingMeasurements.isEmpty()) {
        indexOfDevicesNeedAutoCreateSchema.add(indexOfDevicesWithMissingMeasurements.get(i));
        indexOfMeasurementsNeedAutoCreate.add(indexOfMissingMeasurements);
      }
    }

    // [Step 4] Fetch 2. Some fetched measurements in [Step 3] are views. Process them.
    hasUnFetchedLogicalView = false;
    for (int i = 0, size = schemaComputationWithAutoCreationList.size(); i < size; i++) {
      Pair<List<Integer>, List<String>> missedIndexAndPathString =
          schemaCache.computeSourceOfLogicalView(schemaComputationWithAutoCreationList.get(i));
      if (!missedIndexAndPathString.left.isEmpty()) {
        hasUnFetchedLogicalView = true;
      }
      missedIndexAndPathStringOfViewList.get(i).left = missedIndexAndPathString.left;
      missedIndexAndPathStringOfViewList.get(i).right = missedIndexAndPathString.right;
    }
    if (hasUnFetchedLogicalView) {
      List<String> fullPathsNeedRefetch = new ArrayList<>();
      for (Pair<List<Integer>, List<String>> pair : missedIndexAndPathStringOfViewList) {
        fullPathsNeedRefetch.addAll(pair.right);
      }
      ClusterSchemaTree viewSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaWithFullPaths(fullPathsNeedRefetch, context);
      for (int i = 0, size = schemaComputationWithAutoCreationList.size(); i < size; i++) {
        schemaComputationWithAutoCreation = schemaComputationWithAutoCreationList.get(i);
        viewSchemaTree.computeSourceOfLogicalView(
            schemaComputationWithAutoCreation, missedIndexAndPathStringOfViewList.get(i).left);
      }
    }

    // all schema has been taken and processed
    if (indexOfDevicesNeedAutoCreateSchema.isEmpty()) {
      return;
    }

    // [Step 5] Auto Create and process the missing schema
    if (config.isAutoCreateSchemaEnabled()) {
      List<PartialPath> devicePathList =
          schemaComputationWithAutoCreationList.stream()
              .map(ISchemaComputationWithAutoCreation::getDevicePath)
              .collect(Collectors.toList());
      List<Boolean> isAlignedRealList =
          schemaComputationWithAutoCreationList.stream()
              .map(ISchemaComputationWithAutoCreation::isAligned)
              .collect(Collectors.toList());
      // Check the isAligned value. If the input value is different from the actual value of the
      // existing device, throw exception.
      validateIsAlignedValueIfAutoCreate(isAlignedRealList, isAlignedPutInList, devicePathList);

      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      autoCreateSchemaExecutor.autoCreateTimeSeries(
          schemaTree,
          devicePathList,
          indexOfDevicesNeedAutoCreateSchema,
          indexOfMeasurementsNeedAutoCreate,
          schemaComputationWithAutoCreationList.stream()
              .map(ISchemaComputationWithAutoCreation::getMeasurements)
              .collect(Collectors.toList()),
          schemaComputationWithAutoCreationList.stream()
              .map(
                  o -> {
                    TSDataType[] dataTypes = new TSDataType[o.getMeasurements().length];
                    for (int i = 0, length = dataTypes.length; i < length; i++) {
                      dataTypes[i] = o.getDataType(i);
                    }
                    return dataTypes;
                  })
              .collect(Collectors.toList()),
          isAlignedPutInList,
          context);
      indexOfDevicesWithMissingMeasurements = new ArrayList<>();
      indexOfMissingMeasurementsList = new ArrayList<>();
      for (int i = 0; i < indexOfDevicesNeedAutoCreateSchema.size(); i++) {
        schemaComputationWithAutoCreation =
            schemaComputationWithAutoCreationList.get(indexOfDevicesNeedAutoCreateSchema.get(i));
        indexOfMissingMeasurements =
            schemaTree.compute(
                schemaComputationWithAutoCreation, indexOfMeasurementsNeedAutoCreate.get(i));
        if (!indexOfMissingMeasurements.isEmpty()) {
          indexOfDevicesWithMissingMeasurements.add(indexOfDevicesNeedAutoCreateSchema.get(i));
          indexOfMissingMeasurementsList.add(indexOfMissingMeasurements);
        }
      }

      // all schema has been taken and processed
      if (indexOfDevicesWithMissingMeasurements.isEmpty()) {
        return;
      }
    } else {
      indexOfDevicesWithMissingMeasurements = indexOfDevicesNeedAutoCreateSchema;
      indexOfMissingMeasurementsList = indexOfMeasurementsNeedAutoCreate;
    }

    // offer null for the rest missing schema processing
    for (int i = 0; i < indexOfDevicesWithMissingMeasurements.size(); i++) {
      schemaComputationWithAutoCreation =
          schemaComputationWithAutoCreationList.get(indexOfDevicesWithMissingMeasurements.get(i));
      for (int index : indexOfMissingMeasurementsList.get(i)) {
        schemaComputationWithAutoCreation.computeMeasurement(index, null);
      }
    }
  }

  private void validateIsAlignedValueIfAutoCreate(
      List<Boolean> realValueList, List<Boolean> putInValueList, List<PartialPath> devicePathList) {
    int checkLen =
        Math.min(Math.min(realValueList.size(), putInValueList.size()), devicePathList.size());
    for (int i = 0; i < checkLen; i++) {
      validateIsAlignedValueIfAutoCreate(
          realValueList.get(i), putInValueList.get(i), devicePathList.get(i));
    }
  }

  private void validateIsAlignedValueIfAutoCreate(
      boolean realValue, boolean putInValue, PartialPath devicePath) {
    if (realValue != putInValue) {
      String msg;
      if (realValue) {
        msg =
            "Timeseries under this device is aligned, please use createTimeseries or change device.";
      } else {
        msg =
            "Timeseries under this device is not aligned, please use createTimeseries or change device.";
      }
      throw new SemanticException(new AlignedTimeseriesException(msg, devicePath.getFullPath()));
    }
  }
}
