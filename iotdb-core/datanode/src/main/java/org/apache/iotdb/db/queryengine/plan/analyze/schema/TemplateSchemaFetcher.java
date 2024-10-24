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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

class TemplateSchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final TreeDeviceSchemaCacheManager templateSchemaCache;

  private final AutoCreateSchemaExecutor autoCreateSchemaExecutor;
  private final ClusterSchemaFetchExecutor clusterSchemaFetchExecutor;

  TemplateSchemaFetcher(
      TreeDeviceSchemaCacheManager templateSchemaCache,
      AutoCreateSchemaExecutor autoCreateSchemaExecutor,
      ClusterSchemaFetchExecutor clusterSchemaFetchExecutor) {
    this.templateSchemaCache = templateSchemaCache;
    this.autoCreateSchemaExecutor = autoCreateSchemaExecutor;
    this.clusterSchemaFetchExecutor = clusterSchemaFetchExecutor;
  }

  List<Integer> processTemplateTimeSeries(
      Pair<Template, PartialPath> templateSetInfo,
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation,
      MPPQueryContext context) {
    PartialPath devicePath = schemaComputationWithAutoCreation.getDevicePath();
    String[] measurements = schemaComputationWithAutoCreation.getMeasurements();
    Template template = templateSetInfo.getLeft();
    List<String> extensionMeasurementList = new ArrayList<>();
    List<TSDataType> extensionDataTypeList = new ArrayList<>();
    for (int i = 0; i < measurements.length; i++) {
      if (!template.hasSchema(measurements[i])) {
        extensionMeasurementList.add(measurements[i]);
        extensionDataTypeList.add(schemaComputationWithAutoCreation.getDataType(i));
      }
    }

    if (!extensionMeasurementList.isEmpty() && config.isAutoCreateSchemaEnabled()) {
      autoCreateSchemaExecutor.autoExtendTemplate(
          template.getName(), extensionMeasurementList, extensionDataTypeList, context);
    }

    List<Integer> indexOfMissingMeasurements =
        templateSchemaCache.computeWithTemplate(schemaComputationWithAutoCreation);
    // all schema can be taken from cache
    if (indexOfMissingMeasurements.isEmpty()) {
      return indexOfMissingMeasurements;
    }

    if (indexOfMissingMeasurements.size() < measurements.length) {
      // activated but missing measurement in template
      return indexOfMissingMeasurements;
    }

    // not activated or not cached
    // try fetch the missing schema from remote and cache fetched schema
    ClusterSchemaTree remoteSchemaTree =
        clusterSchemaFetchExecutor.fetchSchemaOfOneDevice(
            schemaComputationWithAutoCreation.getDevicePath(),
            schemaComputationWithAutoCreation.getMeasurements(),
            indexOfMissingMeasurements,
            context);
    // check and compute the fetched schema
    indexOfMissingMeasurements =
        remoteSchemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);

    // all schema has been taken and processed
    if (indexOfMissingMeasurements.isEmpty()) {
      // already activated
      return indexOfMissingMeasurements;
    }

    // not activated
    // auto create and process the missing schema
    if (config.isAutoCreateSchemaEnabled()) {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      autoCreateSchemaExecutor.autoActivateTemplate(
          schemaTree, devicePath, template.getId(), context);
      indexOfMissingMeasurements =
          schemaTree.compute(schemaComputationWithAutoCreation, indexOfMissingMeasurements);
    }

    return indexOfMissingMeasurements;
  }

  void processTemplateTimeSeries(
      List<Pair<Template, PartialPath>> templateSetInfoList,
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList,
      MPPQueryContext context) {

    List<Integer> indexOfDevicesWithMissingMeasurements = new ArrayList<>();
    List<List<Integer>> indexOfMissingMeasurementsList =
        new ArrayList<>(schemaComputationWithAutoCreationList.size());

    Map<String, TemplateExtendInfo> extensionMeasurementMap = new HashMap<>();

    ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation;
    List<Integer> indexOfMissingMeasurements;
    String[] measurements;
    Template template;
    for (int i = 0, size = schemaComputationWithAutoCreationList.size(); i < size; i++) {
      schemaComputationWithAutoCreation = schemaComputationWithAutoCreationList.get(i);
      template = templateSetInfoList.get(i).left;
      measurements = schemaComputationWithAutoCreation.getMeasurements();
      for (int j = 0; j < measurements.length; j++) {
        if (!template.hasSchema(measurements[j])) {
          extensionMeasurementMap
              .computeIfAbsent(template.getName(), TemplateExtendInfo::new)
              .addMeasurement(
                  measurements[j],
                  schemaComputationWithAutoCreation.getDataType(j),
                  getDefaultEncoding(schemaComputationWithAutoCreation.getDataType(j)),
                  TSFileDescriptor.getInstance().getConfig().getCompressor());
        }
      }
    }
    if (!extensionMeasurementMap.isEmpty() && config.isAutoCreateSchemaEnabled()) {
      autoCreateSchemaExecutor.autoExtendTemplate(extensionMeasurementMap, context);
    }

    for (int i = 0, size = schemaComputationWithAutoCreationList.size(); i < size; i++) {
      schemaComputationWithAutoCreation = schemaComputationWithAutoCreationList.get(i);
      indexOfMissingMeasurements =
          templateSchemaCache.computeWithTemplate(schemaComputationWithAutoCreation);
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
            indexOfMissingMeasurementsList,
            context);
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
      autoCreateSchemaExecutor.autoActivateTemplate(
          schemaTree,
          indexOfDevicesNeedAutoCreateSchema.stream()
              .map(index -> schemaComputationWithAutoCreationList.get(index).getDevicePath())
              .collect(Collectors.toList()),
          indexOfDevicesNeedAutoCreateSchema.stream()
              .map(templateSetInfoList::get)
              .collect(Collectors.toList()),
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
}
