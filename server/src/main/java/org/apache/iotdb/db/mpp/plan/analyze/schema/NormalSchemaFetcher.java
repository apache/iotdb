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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class NormalSchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final DataNodeSchemaCache schemaCache;

  private final AutoCreateSchemaExecutor autoCreateSchemaExecutor;
  private final ClusterSchemaFetchExecutor clusterSchemaFetchExecutor;

  NormalSchemaFetcher(
      DataNodeSchemaCache schemaCache,
      AutoCreateSchemaExecutor autoCreateSchemaExecutor,
      ClusterSchemaFetchExecutor clusterSchemaFetchExecutor) {
    this.schemaCache = schemaCache;
    this.autoCreateSchemaExecutor = autoCreateSchemaExecutor;
    this.clusterSchemaFetchExecutor = clusterSchemaFetchExecutor;
  }

  List<Integer> processNormalTimeSeries(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation) {
    // [Step 1] Cache 1.
    List<Integer> indexOfMissingMeasurements =
        schemaCache.computeWithoutTemplate(schemaComputationWithAutoCreation);
    // [Step 2] Cache 2.
    Pair<List<Integer>, List<String>> missedIndexAndPathString =
        schemaCache.computeLogicalViewWithoutTemplate(schemaComputationWithAutoCreation);
    List<Integer> indexOfMissingLogicalView = missedIndexAndPathString.left;
    List<String> missedPathStringOfLogicalView = missedIndexAndPathString.right;
    // all schema can be taken from cache
    if (indexOfMissingMeasurements.isEmpty() && indexOfMissingLogicalView.isEmpty()) {
      return indexOfMissingMeasurements;
    }
    // [Step 3] Fetch 1.
    ClusterSchemaTree remoteSchemaTree = null;
    if (missedPathStringOfLogicalView.isEmpty()) {
      // try fetch the missing raw schema from remote and cache fetched schema
      remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfOneDevice(
              schemaComputationWithAutoCreation.getDevicePath(),
              schemaComputationWithAutoCreation.getMeasurements(),
              indexOfMissingMeasurements);
    } else {
      remoteSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaOfOneDevice(
              schemaComputationWithAutoCreation.getDevicePath(),
              schemaComputationWithAutoCreation.getMeasurements(),
              indexOfMissingMeasurements);
      ClusterSchemaTree viewSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaWithFullPaths(missedPathStringOfLogicalView);
      remoteSchemaTree.mergeSchemaTree(viewSchemaTree);
      Set<String> databaseSet = new HashSet<>();
      databaseSet.addAll(remoteSchemaTree.getDatabases());
      databaseSet.addAll(viewSchemaTree.getDatabases());
      remoteSchemaTree.setDatabases(databaseSet);
    }
    // make sure all missed views are computed.
    remoteSchemaTree.computeLogicalView(
        schemaComputationWithAutoCreation, indexOfMissingLogicalView);
    // check and compute the fetched schema
    indexOfMissingMeasurements =
        remoteSchemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);
    schemaComputationWithAutoCreation.recordSizeOfLogicalViewSchemaListNow();

    // [Step 4] Fetch 2. Find all missed logical view
    missedIndexAndPathString =
        schemaCache.computeLogicalViewWithoutTemplate(schemaComputationWithAutoCreation);
    indexOfMissingLogicalView = missedIndexAndPathString.left;
    missedPathStringOfLogicalView = missedIndexAndPathString.right;
    if (!missedPathStringOfLogicalView.isEmpty()) {
      ClusterSchemaTree viewSchemaTree =
          clusterSchemaFetchExecutor.fetchSchemaWithFullPaths(missedPathStringOfLogicalView);
      viewSchemaTree.computeLogicalView(
          schemaComputationWithAutoCreation, indexOfMissingLogicalView);
    }

    // all schema has been taken and processed
    if (indexOfMissingMeasurements.isEmpty()) {
      return indexOfMissingMeasurements;
    }

    // [Step 5] Auto Create
    // auto create and process the missing schema
    if (config.isAutoCreateSchemaEnabled()) {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      autoCreateSchemaExecutor.autoCreateTimeSeries(
          schemaTree,
          schemaComputationWithAutoCreation.getDevicePath(),
          indexOfMissingMeasurements,
          schemaComputationWithAutoCreation.getMeasurements(),
          schemaComputationWithAutoCreation::getDataType,
          schemaComputationWithAutoCreation.isAligned());
      indexOfMissingMeasurements =
          schemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);
    }

    return indexOfMissingMeasurements;
  }

  void processNormalTimeSeries(
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList) {
    List<Integer> indexOfDevicesWithMissingMeasurements = new ArrayList<>();
    List<List<Integer>> indexOfMissingMeasurementsList =
        new ArrayList<>(schemaComputationWithAutoCreationList.size());

    ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation;
    List<Integer> indexOfMissingMeasurements;
    for (int i = 0, size = schemaComputationWithAutoCreationList.size(); i < size; i++) {
      schemaComputationWithAutoCreation = schemaComputationWithAutoCreationList.get(i);
      indexOfMissingMeasurements =
          schemaCache.computeWithoutTemplate(schemaComputationWithAutoCreation);
      if (!indexOfMissingMeasurements.isEmpty()) {
        indexOfDevicesWithMissingMeasurements.add(i);
        indexOfMissingMeasurementsList.add(indexOfMissingMeasurements);
      }
    }

    // all schema can be taken from cache
    if (indexOfDevicesWithMissingMeasurements.isEmpty()) {
      return;
    }

    // try fetch the missing schema from remote
    ClusterSchemaTree remoteSchemaTree =
        clusterSchemaFetchExecutor.fetchSchemaOfMultiDevices(
            schemaComputationWithAutoCreationList.stream()
                .map(ISchemaComputationWithAutoCreation::getDevicePath)
                .collect(Collectors.toList()),
            schemaComputationWithAutoCreationList.stream()
                .map(ISchemaComputationWithAutoCreation::getMeasurements)
                .collect(Collectors.toList()),
            indexOfDevicesWithMissingMeasurements,
            indexOfMissingMeasurementsList);
    // check and compute the fetched schema
    List<Integer> indexOfDevicesNeedAutoCreateSchema = new ArrayList<>();
    List<List<Integer>> indexOfMeasurementsNeedAutoCreate = new ArrayList<>();
    for (int i = 0; i < indexOfDevicesWithMissingMeasurements.size(); i++) {
      schemaComputationWithAutoCreation =
          schemaComputationWithAutoCreationList.get(indexOfDevicesWithMissingMeasurements.get(i));
      indexOfMissingMeasurements =
          remoteSchemaTree.compute(
              schemaComputationWithAutoCreation, indexOfMissingMeasurementsList.get(i));
      if (!indexOfMissingMeasurements.isEmpty()) {
        indexOfDevicesNeedAutoCreateSchema.add(indexOfDevicesWithMissingMeasurements.get(i));
        indexOfMeasurementsNeedAutoCreate.add(indexOfMissingMeasurements);
      }
    }

    // all schema has been taken and processed
    if (indexOfDevicesNeedAutoCreateSchema.isEmpty()) {
      return;
    }

    // auto create and process the missing schema
    if (config.isAutoCreateSchemaEnabled()) {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      autoCreateSchemaExecutor.autoCreateTimeSeries(
          schemaTree,
          schemaComputationWithAutoCreationList.stream()
              .map(ISchemaComputationWithAutoCreation::getDevicePath)
              .collect(Collectors.toList()),
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
          schemaComputationWithAutoCreationList.stream()
              .map(ISchemaComputationWithAutoCreation::isAligned)
              .collect(Collectors.toList()));
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
}
