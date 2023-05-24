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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    List<Integer> indexOfMissingMeasurements =
        schemaCache.computeWithoutTemplate(schemaComputationWithAutoCreation);
    // all schema can be taken from cache
    if (indexOfMissingMeasurements.isEmpty()) {
      return indexOfMissingMeasurements;
    }

    // try fetch the missing raw schema from remote and cache fetched schema
    ClusterSchemaTree remoteSchemaTree =
        clusterSchemaFetchExecutor.fetchSchemaOfOneDevice(
            schemaComputationWithAutoCreation.getDevicePath(),
            schemaComputationWithAutoCreation.getMeasurements(),
            indexOfMissingMeasurements);
    // check and compute the fetched schema
    // <RawFullPath，Index>
    Map<String, Integer> logicalViewSchemaMap = new HashMap<>();
    indexOfMissingMeasurements =
        remoteSchemaTree.compute(
            schemaComputationWithAutoCreation, indexOfMissingMeasurements, logicalViewSchemaMap);

    // try fetch the missing view schema from remote and cache fetched schema
    if (!logicalViewSchemaMap.isEmpty()) {
      ClusterSchemaTree remoteViewSchemaTree =
          clusterSchemaFetchExecutor.fetchSchema(new ArrayList<>(logicalViewSchemaMap.keySet()));
      for (MeasurementPath measurementPath : remoteViewSchemaTree.getAllMeasurement()) {
        schemaComputationWithAutoCreation.computeViewMeasurement(
            logicalViewSchemaMap.get(measurementPath.getFullPath()),
            new MeasurementSchemaInfo(
                measurementPath.getMeasurement(), measurementPath.getMeasurementSchema(), null),
            measurementPath.getDevice(),
            measurementPath.isUnderAlignedEntity());
        // remain index is missing view measurements
        logicalViewSchemaMap.remove(measurementPath.getFullPath());
      }
      // all schema has been taken and processed
      if (indexOfMissingMeasurements.isEmpty() && logicalViewSchemaMap.isEmpty()) {
        return indexOfMissingMeasurements;
      }
      // auto create and process the missing schema
      if (config.isAutoCreateSchemaEnabled()) {
        ClusterSchemaTree schemaTree = new ClusterSchemaTree();
        List<PartialPath> devicePathList = new ArrayList<>();
        List<Integer> indexOfTargetDevices;
        List<List<Integer>> indexOfTargetMeasurementsList = new ArrayList<>();
        List<String[]> measurementsList = new ArrayList<>();
        List<TSDataType[]> tsDataTypesList = new ArrayList<>();
        List<Boolean> isAlignedList = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : logicalViewSchemaMap.entrySet()) {
          PartialPath path = null;
          try {
            path = new PartialPath(entry.getKey());
          } catch (IllegalPathException e) {
            throw new RuntimeException(e);
          }
          devicePathList.add(path.getDevicePath());
          indexOfTargetMeasurementsList.add(Collections.singletonList(0));
          measurementsList.add(new String[] {path.getMeasurement()});
          tsDataTypesList.add(
              new TSDataType[] {schemaComputationWithAutoCreation.getDataType(entry.getValue())});
          isAlignedList.add(schemaComputationWithAutoCreation.isAligned());
        }
        devicePathList.add(schemaComputationWithAutoCreation.getDevicePath());
        indexOfTargetMeasurementsList.add(indexOfMissingMeasurements);
        measurementsList.add(schemaComputationWithAutoCreation.getMeasurements());
        tsDataTypesList.add(
            IntStream.range(0, schemaComputationWithAutoCreation.getMeasurements().length)
                .mapToObj(schemaComputationWithAutoCreation::getDataType)
                .toArray(TSDataType[]::new));
        isAlignedList.add(schemaComputationWithAutoCreation.isAligned());
        indexOfTargetDevices =
            IntStream.range(0, devicePathList.size()).boxed().collect(Collectors.toList());
        autoCreateSchemaExecutor.autoCreateTimeSeries(
            schemaTree,
            devicePathList,
            indexOfTargetDevices,
            indexOfTargetMeasurementsList,
            measurementsList,
            tsDataTypesList,
            isAlignedList);
        indexOfMissingMeasurements =
            schemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);
        for (MeasurementPath measurementPath : schemaTree.getAllMeasurement()) {
          if (logicalViewSchemaMap.containsKey(measurementPath.getFullPath())) {
            schemaComputationWithAutoCreation.computeViewMeasurement(
                logicalViewSchemaMap.get(measurementPath.getFullPath()),
                new MeasurementSchemaInfo(
                    measurementPath.getMeasurement(), measurementPath.getMeasurementSchema(), null),
                measurementPath.getDevice(),
                measurementPath.isUnderAlignedEntity());
          }
        }
      }

      return indexOfMissingMeasurements;

    } else {
      // all schema has been taken and processed
      if (indexOfMissingMeasurements.isEmpty()) {
        return indexOfMissingMeasurements;
      }

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
