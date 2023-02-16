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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

class AutoCreateSchemaExecutor {

  private final ITemplateManager templateManager;
  private final Function<Statement, ExecutionResult> statementExecutor;

  AutoCreateSchemaExecutor(
      ITemplateManager templateManager, Function<Statement, ExecutionResult> statementExecutor) {
    this.templateManager = templateManager;
    this.statementExecutor = statementExecutor;
  }

  // auto create the missing measurements and merge them into given schemaTree
  void autoCreateMissingMeasurements(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<Integer> indexOfTargetMeasurements,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      boolean isAligned) {
    // check whether there is template should be activated
    Pair<Template, PartialPath> templateInfo = templateManager.checkTemplateSetInfo(devicePath);
    if (templateInfo != null) {
      Template template = templateInfo.left;
      List<Integer> indexOfMeasurementsNotInTemplate =
          checkMeasurementsInSchemaTemplate(
              devicePath, indexOfTargetMeasurements, measurements, isAligned, template);
      if (indexOfMeasurementsNotInTemplate.size() < indexOfTargetMeasurements.size()) {
        // there are measurements in schema template
        internalActivateTemplate(devicePath);
        for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
          schemaTree.appendSingleMeasurement(
              devicePath.concatNode(entry.getKey()),
              (MeasurementSchema) entry.getValue(),
              null,
              null,
              template.isDirectAligned());
        }
      }
      if (indexOfMeasurementsNotInTemplate.isEmpty()) {
        return;
      }
      // there are measurements need to be created as normal timeseries
      indexOfTargetMeasurements = indexOfMeasurementsNotInTemplate;
    }

    // auto create the rest missing timeseries
    List<String> missingMeasurements = new ArrayList<>(indexOfTargetMeasurements.size());
    List<TSDataType> dataTypesOfMissingMeasurement =
        new ArrayList<>(indexOfTargetMeasurements.size());
    List<TSEncoding> encodingsOfMissingMeasurement =
        new ArrayList<>(indexOfTargetMeasurements.size());
    List<CompressionType> compressionTypesOfMissingMeasurement =
        new ArrayList<>(indexOfTargetMeasurements.size());
    indexOfTargetMeasurements.forEach(
        index -> {
          TSDataType tsDataType = getDataType.apply(index);
          // tsDataType == null means insert null value to a non-exist series
          // should skip creating them
          if (tsDataType != null) {
            missingMeasurements.add(measurements[index]);
            dataTypesOfMissingMeasurement.add(tsDataType);
            encodingsOfMissingMeasurement.add(getDefaultEncoding(tsDataType));
            compressionTypesOfMissingMeasurement.add(
                TSFileDescriptor.getInstance().getConfig().getCompressor());
          }
        });

    if (!missingMeasurements.isEmpty()) {
      internalCreateTimeSeries(
          schemaTree,
          devicePath,
          missingMeasurements,
          dataTypesOfMissingMeasurement,
          encodingsOfMissingMeasurement,
          compressionTypesOfMissingMeasurement,
          isAligned);
    }
  }

  void autoCreateMissingMeasurements(
      ClusterSchemaTree schemaTree,
      List<PartialPath> devicePathList,
      List<Integer> indexOfTargetDevices,
      List<List<Integer>> indexOfTargetMeasurementsList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<TSEncoding[]> encodingsList,
      List<CompressionType[]> compressionTypesList,
      List<Boolean> isAlignedList) {
    // check whether there is template should be activated

    Map<PartialPath, Pair<Template, PartialPath>> devicesNeedActivateTemplate = new HashMap<>();
    Map<PartialPath, Pair<Boolean, MeasurementGroup>> devicesNeedAutoCreateTimeSeries =
        new HashMap<>();
    int deviceIndex;
    PartialPath devicePath;
    List<Integer> indexOfTargetMeasurements;
    Pair<Template, PartialPath> templateInfo;
    Template template;
    List<Integer> indexOfMeasurementsNotInTemplate;
    for (int i = 0, size = indexOfTargetDevices.size(); i < size; i++) {
      deviceIndex = indexOfTargetDevices.get(i);
      devicePath = devicePathList.get(deviceIndex);
      indexOfTargetMeasurements = indexOfTargetMeasurementsList.get(i);

      templateInfo = devicesNeedActivateTemplate.get(devicePath);
      if (templateInfo == null) {
        templateInfo = templateManager.checkTemplateSetInfo(devicePath);
      }

      if (templateInfo == null) {
        indexOfMeasurementsNotInTemplate = indexOfTargetMeasurements;
      } else {
        template = templateInfo.left;
        indexOfMeasurementsNotInTemplate =
            checkMeasurementsInSchemaTemplate(
                devicePath,
                indexOfTargetMeasurements,
                measurementsList.get(deviceIndex),
                isAlignedList.get(deviceIndex),
                template);
        if (indexOfMeasurementsNotInTemplate.size() < indexOfTargetMeasurements.size()) {
          // there are measurements in schema template
          devicesNeedActivateTemplate.putIfAbsent(devicePath, templateInfo);
        }
      }

      if (!indexOfMeasurementsNotInTemplate.isEmpty()) {
        // there are measurements need to be created as normal timeseries
        int finalDeviceIndex = deviceIndex;
        List<Integer> finalIndexOfMeasurementsNotInTemplate = indexOfMeasurementsNotInTemplate;
        devicesNeedAutoCreateTimeSeries.compute(
            devicePath,
            (k, v) -> {
              if (v == null) {
                v = new Pair<>(isAlignedList.get(finalDeviceIndex), new MeasurementGroup());
              }
              MeasurementGroup measurementGroup = v.right;
              String[] measurements = measurementsList.get(finalDeviceIndex);
              TSDataType[] tsDataTypes = tsDataTypesList.get(finalDeviceIndex);
              TSEncoding[] encodings =
                  encodingsList == null ? null : encodingsList.get(finalDeviceIndex);
              CompressionType[] compressionTypes =
                  compressionTypesList == null ? null : compressionTypesList.get(finalDeviceIndex);
              for (int measurementIndex : finalIndexOfMeasurementsNotInTemplate) {
                if (tsDataTypes[measurementIndex] == null) {
                  continue;
                }
                measurementGroup.addMeasurement(
                    measurements[measurementIndex],
                    tsDataTypes[measurementIndex],
                    encodings == null
                        ? getDefaultEncoding(tsDataTypes[measurementIndex])
                        : encodings[measurementIndex],
                    compressionTypes == null
                        ? TSFileDescriptor.getInstance().getConfig().getCompressor()
                        : compressionTypes[measurementIndex]);
              }
              return v;
            });
      }
    }

    if (!devicesNeedActivateTemplate.isEmpty()) {
      internalActivateTemplate(devicesNeedActivateTemplate);
      for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry :
          devicesNeedActivateTemplate.entrySet()) {
        devicePath = entry.getKey();
        template = entry.getValue().left;
        for (Map.Entry<String, IMeasurementSchema> measurementEntry :
            template.getSchemaMap().entrySet()) {
          schemaTree.appendSingleMeasurement(
              devicePath.concatNode(measurementEntry.getKey()),
              (MeasurementSchema) measurementEntry.getValue(),
              null,
              null,
              template.isDirectAligned());
        }
      }
    }

    if (!devicesNeedAutoCreateTimeSeries.isEmpty()) {
      internalCreateTimeSeries(schemaTree, devicesNeedAutoCreateTimeSeries);
    }
  }

  private List<Integer> checkMeasurementsInSchemaTemplate(
      PartialPath devicePath,
      List<Integer> indexOfTargetMeasurements,
      String[] measurements,
      boolean isAligned,
      Template template) {
    // check whether there is template should be activated
    boolean shouldActivateTemplate = false;
    for (int index : indexOfTargetMeasurements) {
      if (template.hasSchema(measurements[index])) {
        shouldActivateTemplate = true;
        break;
      }
    }
    if (shouldActivateTemplate) {
      List<Integer> recheckedIndexOfMissingMeasurements = new ArrayList<>();
      for (int index : indexOfTargetMeasurements) {
        if (!template.hasSchema(measurements[index])) {
          recheckedIndexOfMissingMeasurements.add(index);
        }
      }
      return recheckedIndexOfMissingMeasurements;
    } else {
      return indexOfTargetMeasurements;
    }
  }

  // try to create the target timeseries and merge schema of successfully created
  // timeseries and existing timeseries into given schemaTree
  private void internalCreateTimeSeries(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> tsDataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned) {
    List<MeasurementPath> measurementPathList =
        executeInternalCreateTimeseriesStatement(
            new InternalCreateTimeSeriesStatement(
                devicePath, measurements, tsDataTypes, encodings, compressors, isAligned));

    Set<Integer> alreadyExistingMeasurementIndexSet =
        measurementPathList.stream()
            .map(o -> measurements.indexOf(o.getMeasurement()))
            .collect(Collectors.toSet());

    schemaTree.appendMeasurementPaths(measurementPathList);

    for (int i = 0, size = measurements.size(); i < size; i++) {
      if (alreadyExistingMeasurementIndexSet.contains(i)) {
        continue;
      }

      schemaTree.appendSingleMeasurement(
          devicePath.concatNode(measurements.get(i)),
          new MeasurementSchema(
              measurements.get(i), tsDataTypes.get(i), encodings.get(i), compressors.get(i)),
          null,
          null,
          isAligned);
    }
  }

  // auto create timeseries and return the existing timeseries info
  private List<MeasurementPath> executeInternalCreateTimeseriesStatement(Statement statement) {

    ExecutionResult executionResult = statementExecutor.apply(statement);

    int statusCode = executionResult.status.getCode();
    if (statusCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return Collections.emptyList();
    }

    if (statusCode != TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(executionResult.status.getMessage(), statusCode));
    }

    Set<String> failedCreationSet = new HashSet<>();
    List<MeasurementPath> alreadyExistingMeasurements = new ArrayList<>();
    for (TSStatus subStatus : executionResult.status.subStatus) {
      if (subStatus.code == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
        alreadyExistingMeasurements.add(
            MeasurementPath.parseDataFromString(subStatus.getMessage()));
      } else {
        failedCreationSet.add(subStatus.message);
      }
    }

    if (!failedCreationSet.isEmpty()) {
      throw new SemanticException(new MetadataException(String.join("; ", failedCreationSet)));
    }

    return alreadyExistingMeasurements;
  }

  private void internalActivateTemplate(PartialPath devicePath) {
    ExecutionResult executionResult =
        statementExecutor.apply(new ActivateTemplateStatement(devicePath));
    TSStatus status = executionResult.status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      throw new SemanticException(new IoTDBException(status.getMessage(), status.getCode()));
    }
  }

  private void internalActivateTemplate(
      Map<PartialPath, Pair<Template, PartialPath>> devicesNeedActivateTemplate) {
    ExecutionResult executionResult =
        statementExecutor.apply(
            new InternalBatchActivateTemplateStatement(devicesNeedActivateTemplate));
    TSStatus status = executionResult.status;
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || status.getCode() == TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      return;
    }
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      Set<String> failedActivationSet = new HashSet<>();
      for (TSStatus subStatus : status.subStatus) {
        if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && subStatus.getCode() != TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
          failedActivationSet.add(subStatus.message);
        }
      }
      if (!failedActivationSet.isEmpty()) {
        throw new SemanticException(new MetadataException(String.join("; ", failedActivationSet)));
      }
    } else {
      throw new SemanticException(new IoTDBException(status.getMessage(), status.getCode()));
    }
  }

  private void internalCreateTimeSeries(
      ClusterSchemaTree schemaTree,
      Map<PartialPath, Pair<Boolean, MeasurementGroup>> devicesNeedAutoCreateTimeSeries) {

    List<MeasurementPath> measurementPathList =
        executeInternalCreateTimeseriesStatement(
            new InternalCreateMultiTimeSeriesStatement(devicesNeedAutoCreateTimeSeries));

    schemaTree.appendMeasurementPaths(measurementPathList);

    Map<PartialPath, Set<String>> alreadyExistingMeasurementMap = new HashMap<>();
    for (MeasurementPath measurementPath : measurementPathList) {
      alreadyExistingMeasurementMap
          .computeIfAbsent(measurementPath.getDevicePath(), k -> new HashSet<>())
          .add(measurementPath.getMeasurement());
    }
    Set<String> measurementSet;
    MeasurementGroup measurementGroup;
    for (Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry :
        devicesNeedAutoCreateTimeSeries.entrySet()) {
      measurementSet = alreadyExistingMeasurementMap.get(entry.getKey());
      measurementGroup = entry.getValue().right;
      for (int i = 0, size = measurementGroup.size(); i < size; i++) {
        if (measurementSet != null
            && measurementSet.contains(measurementGroup.getMeasurements().get(i))) {
          continue;
        }
        schemaTree.appendSingleMeasurement(
            entry.getKey().concatNode(measurementGroup.getMeasurements().get(i)),
            new MeasurementSchema(
                measurementGroup.getMeasurements().get(i),
                measurementGroup.getDataTypes().get(i),
                measurementGroup.getEncodings().get(i),
                measurementGroup.getCompressors().get(i)),
            null,
            null,
            entry.getValue().left);
      }
    }
  }
}
