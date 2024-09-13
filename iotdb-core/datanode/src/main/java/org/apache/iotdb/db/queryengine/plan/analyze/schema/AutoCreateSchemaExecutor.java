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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.AlterSchemaTemplateStatement;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateAlterOperationType;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

class AutoCreateSchemaExecutor {
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Coordinator coordinator;
  private final ITemplateManager templateManager;
  private final ISchemaFetcher schemaFetcher;

  AutoCreateSchemaExecutor(
      Coordinator coordinator, ITemplateManager templateManager, ISchemaFetcher schemaFetcher) {
    this.coordinator = coordinator;
    this.templateManager = templateManager;
    this.schemaFetcher = schemaFetcher;
  }

  private ExecutionResult executeStatement(Statement statement, MPPQueryContext context) {

    return coordinator.executeForTreeModel(
        statement,
        SessionManager.getInstance().requestQueryId(),
        context == null ? null : context.getSession(),
        "",
        ClusterPartitionFetcher.getInstance(),
        schemaFetcher,
        context == null || context.getQueryType().equals(QueryType.WRITE)
            ? config.getQueryTimeoutThreshold()
            : context.getTimeOut());
  }

  // Auto create the missing measurements and merge them into given schemaTree
  void autoCreateTimeSeries(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<Integer> indexOfTargetMeasurements,
      String[] measurements,
      IntFunction<TSDataType> getDataType,
      boolean isAligned,
      MPPQueryContext context) {
    // Auto create the rest missing timeseries
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
          // Should skip creating them
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
          isAligned,
          context);
    }
  }

  void autoCreateTimeSeries(
      ClusterSchemaTree schemaTree,
      List<PartialPath> devicePathList,
      List<Integer> indexOfTargetDevices,
      List<List<Integer>> indexOfTargetMeasurementsList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList,
      MPPQueryContext context) {
    // Check whether there is template should be activated
    Map<PartialPath, Pair<Boolean, MeasurementGroup>> devicesNeedAutoCreateTimeSeries =
        new HashMap<>();
    int deviceIndex;
    PartialPath devicePath;
    List<Integer> indexOfTargetMeasurements;
    for (int i = 0, size = indexOfTargetDevices.size(); i < size; i++) {
      deviceIndex = indexOfTargetDevices.get(i);
      devicePath = devicePathList.get(deviceIndex);
      indexOfTargetMeasurements = indexOfTargetMeasurementsList.get(i);

      // There are measurements need to be created as normal timeseries
      int finalDeviceIndex = deviceIndex;
      List<Integer> finalIndexOfMeasurementsNotInTemplate = indexOfTargetMeasurements;
      devicesNeedAutoCreateTimeSeries.compute(
          devicePath,
          (k, v) -> {
            if (v == null) {
              v = new Pair<>(isAlignedList.get(finalDeviceIndex), new MeasurementGroup());
            }
            MeasurementGroup measurementGroup = v.right;
            String[] measurements = measurementsList.get(finalDeviceIndex);
            TSDataType[] tsDataTypes = tsDataTypesList.get(finalDeviceIndex);
            for (int measurementIndex : finalIndexOfMeasurementsNotInTemplate) {
              if (tsDataTypes[measurementIndex] == null) {
                continue;
              }
              measurementGroup.addMeasurement(
                  measurements[measurementIndex],
                  tsDataTypes[measurementIndex],
                  getDefaultEncoding(tsDataTypes[measurementIndex]),
                  TSFileDescriptor.getInstance().getConfig().getCompressor());
            }
            return v;
          });
    }

    if (!devicesNeedAutoCreateTimeSeries.isEmpty()) {
      internalCreateTimeSeries(schemaTree, devicesNeedAutoCreateTimeSeries, context);
    }
  }

  // Used for insert record or tablet
  void autoExtendTemplate(
      String templateName,
      List<String> measurementList,
      List<TSDataType> dataTypeList,
      MPPQueryContext context) {
    long startTime = System.nanoTime();
    try {
      String userName = context.getSession().getUserName();
      if (!AuthorityChecker.SUPER_USER.equals(userName)) {
        TSStatus status =
            AuthorityChecker.getTSStatus(
                AuthorityChecker.checkSystemPermission(
                    userName, PrivilegeType.EXTEND_TEMPLATE.ordinal()),
                PrivilegeType.EXTEND_TEMPLATE);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
        }
      }
    } finally {
      PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
    }
    internalExtendTemplate(templateName, measurementList, dataTypeList, null, null, context);
  }

  // Used for insert records or tablets
  void autoExtendTemplate(
      Map<String, TemplateExtendInfo> templateExtendInfoMap, MPPQueryContext context) {
    long startTime = System.nanoTime();
    try {
      String userName = context.getSession().getUserName();
      if (!AuthorityChecker.SUPER_USER.equals(userName)) {
        TSStatus status =
            AuthorityChecker.getTSStatus(
                AuthorityChecker.checkSystemPermission(
                    userName, PrivilegeType.EXTEND_TEMPLATE.ordinal()),
                PrivilegeType.EXTEND_TEMPLATE);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
        }
      }
    } finally {
      PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
    }
    TemplateExtendInfo templateExtendInfo;
    for (Map.Entry<String, TemplateExtendInfo> entry : templateExtendInfoMap.entrySet()) {
      templateExtendInfo = entry.getValue().deduplicate();
      internalExtendTemplate(
          entry.getKey(),
          templateExtendInfo.getMeasurements(),
          templateExtendInfo.getDataTypes(),
          templateExtendInfo.getEncodings(),
          templateExtendInfo.getCompressors(),
          context);
    }
  }

  void autoActivateTemplate(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      int templateId,
      MPPQueryContext context) {
    internalActivateTemplate(devicePath, context);
    Template template = templateManager.getTemplate(templateId);
    schemaTree.appendTemplateDevice(devicePath, template.isDirectAligned(), templateId, template);
  }

  void autoActivateTemplate(
      ClusterSchemaTree schemaTree,
      List<PartialPath> deviceList,
      List<Pair<Template, PartialPath>> templateSetInfoList,
      MPPQueryContext context) {
    Map<PartialPath, Pair<Template, PartialPath>> devicesNeedActivateTemplate = new HashMap<>();
    for (int i = 0; i < deviceList.size(); i++) {
      devicesNeedActivateTemplate.put(
          deviceList.get(i),
          new Pair<>(
              templateManager.getTemplate(templateSetInfoList.get(i).left.getId()),
              templateSetInfoList.get(i).right));
    }
    internalActivateTemplate(devicesNeedActivateTemplate, context);
    PartialPath devicePath;
    Template template;
    for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry :
        devicesNeedActivateTemplate.entrySet()) {
      devicePath = entry.getKey();
      // Take the latest template
      template = templateManager.getTemplate(entry.getValue().left.getId());
      schemaTree.appendTemplateDevice(
          devicePath, template.isDirectAligned(), template.getId(), template);
    }
  }

  // Used for load TsFile
  @SuppressWarnings("squid:S107")
  void autoCreateMissingMeasurements(
      ClusterSchemaTree schemaTree,
      List<PartialPath> devicePathList,
      List<Integer> indexOfTargetDevices,
      List<List<Integer>> indexOfTargetMeasurementsList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<TSEncoding[]> encodingsList,
      List<CompressionType[]> compressionTypesList,
      List<Boolean> isAlignedList,
      MPPQueryContext context) {
    // Check whether there is template should be activated

    Map<PartialPath, Pair<Template, PartialPath>> devicesNeedActivateTemplate = new HashMap<>();
    Map<PartialPath, Pair<Boolean, MeasurementGroup>> devicesNeedAutoCreateTimeSeries =
        new HashMap<>();
    int deviceIndex;
    PartialPath devicePath;
    List<Integer> indexOfTargetMeasurements;
    Pair<Template, PartialPath> templateInfo;
    Template template;
    List<Integer> indexOfMeasurementsNotInTemplate;
    Map<String, TemplateExtendInfo> templateExtendInfoMap = new HashMap<>();
    for (int i = 0, size = indexOfTargetDevices.size(); i < size; i++) {
      deviceIndex = indexOfTargetDevices.get(i);
      devicePath = devicePathList.get(deviceIndex);
      indexOfTargetMeasurements = indexOfTargetMeasurementsList.get(i);

      templateInfo = devicesNeedActivateTemplate.get(devicePath);
      if (templateInfo == null) {
        templateInfo = templateManager.checkTemplateSetInfo(devicePath);
      }

      if (templateInfo == null) {
        // There are measurements need to be created as normal timeseries
        int finalDeviceIndex = deviceIndex;
        List<Integer> finalIndexOfMeasurementsNotInTemplate = indexOfTargetMeasurements;
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
      } else {
        template = templateInfo.left;
        indexOfMeasurementsNotInTemplate =
            checkMeasurementsInSchemaTemplate(
                indexOfTargetMeasurements, measurementsList.get(deviceIndex), template);
        if (schemaTree.getMatchedDevices(devicePath).isEmpty()) {
          // Not activated yet
          devicesNeedActivateTemplate.putIfAbsent(devicePath, templateInfo);
        }

        if (!indexOfMeasurementsNotInTemplate.isEmpty()) {
          List<Integer> finalIndexOfMeasurementsNotInTemplate1 = indexOfMeasurementsNotInTemplate;
          int finalDeviceIndex1 = deviceIndex;
          templateExtendInfoMap.compute(
              template.getName(),
              (k, v) -> {
                TemplateExtendInfo templateExtendInfo;
                if (v == null) {
                  templateExtendInfo = new TemplateExtendInfo(k);
                } else {
                  templateExtendInfo = v;
                }

                String measurement;
                TSDataType dataType;
                TSEncoding encoding;
                CompressionType compressionType;
                for (int index : finalIndexOfMeasurementsNotInTemplate1) {
                  measurement = measurementsList.get(finalDeviceIndex1)[index];
                  dataType = tsDataTypesList.get(finalDeviceIndex1)[index];
                  if (encodingsList != null && encodingsList.get(finalDeviceIndex1) != null) {
                    encoding = encodingsList.get(finalDeviceIndex1)[index];
                  } else {
                    encoding = getDefaultEncoding(dataType);
                  }
                  if (compressionTypesList != null
                      && compressionTypesList.get(finalDeviceIndex1) != null) {
                    compressionType = compressionTypesList.get(finalDeviceIndex1)[index];
                  } else {
                    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
                  }
                  templateExtendInfo.addMeasurement(
                      measurement, dataType, encoding, compressionType);
                }
                return templateExtendInfo;
              });
        }
      }
    }

    if (!templateExtendInfoMap.isEmpty()) {
      for (Map.Entry<String, TemplateExtendInfo> entry : templateExtendInfoMap.entrySet()) {
        TemplateExtendInfo templateExtendInfo = entry.getValue().deduplicate();
        internalExtendTemplate(
            entry.getKey(),
            templateExtendInfo.getMeasurements(),
            templateExtendInfo.getDataTypes(),
            templateExtendInfo.getEncodings(),
            templateExtendInfo.getCompressors(),
            context);
      }
      for (Pair<Template, PartialPath> value : devicesNeedActivateTemplate.values()) {
        value.left = templateManager.getTemplate(value.left.getId());
      }
    }

    if (!devicesNeedActivateTemplate.isEmpty()) {
      internalActivateTemplate(devicesNeedActivateTemplate, context);
      for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry :
          devicesNeedActivateTemplate.entrySet()) {
        devicePath = entry.getKey();
        // Take the latest template
        template = templateManager.getTemplate(entry.getValue().left.getId());
        schemaTree.appendTemplateDevice(
            devicePath, template.isDirectAligned(), template.getId(), template);
      }
    }

    if (!devicesNeedAutoCreateTimeSeries.isEmpty()) {
      internalCreateTimeSeries(schemaTree, devicesNeedAutoCreateTimeSeries, context);
    }
  }

  private List<Integer> checkMeasurementsInSchemaTemplate(
      List<Integer> indexOfTargetMeasurements, String[] measurements, Template template) {
    // Check whether there is template should be activated
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

  // Try to create the target timeseries and merge schema of successfully created
  // timeseries and existing timeseries into given schemaTree
  private void internalCreateTimeSeries(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> tsDataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned,
      MPPQueryContext context) {
    List<MeasurementPath> measurementPathList =
        executeInternalCreateTimeseriesStatement(
            new InternalCreateTimeSeriesStatement(
                devicePath, measurements, tsDataTypes, encodings, compressors, isAligned),
            context);

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
          devicePath.concatAsMeasurementPath(measurements.get(i)),
          new MeasurementSchema(
              measurements.get(i), tsDataTypes.get(i), encodings.get(i), compressors.get(i)),
          null,
          null,
          null,
          isAligned);
    }
  }

  // Auto create timeseries and return the existing timeseries info
  private List<MeasurementPath> executeInternalCreateTimeseriesStatement(
      final Statement statement, final MPPQueryContext context) {
    final TSStatus status =
        AuthorityChecker.checkAuthority(statement, context.getSession().getUserName());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
    }

    ExecutionResult executionResult = executeStatement(statement, context);

    final int statusCode = executionResult.status.getCode();
    if (statusCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return Collections.emptyList();
    }

    if (statusCode != TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(executionResult.status.getMessage(), statusCode));
    }

    final Set<TSStatus> failedCreationSet = new HashSet<>();
    final List<MeasurementPath> alreadyExistingMeasurements = new ArrayList<>();
    for (final TSStatus subStatus : executionResult.status.subStatus) {
      if (subStatus.code == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
        alreadyExistingMeasurements.add(
            MeasurementPath.parseDataFromString(subStatus.getMessage()));
      } else {
        failedCreationSet.add(subStatus);
      }
    }

    if (!failedCreationSet.isEmpty()) {
      throw new SemanticException(
          new MetadataException(
              failedCreationSet.stream()
                  .map(TSStatus::toString)
                  .collect(Collectors.joining("; "))));
    }

    return alreadyExistingMeasurements;
  }

  private void internalActivateTemplate(PartialPath devicePath, MPPQueryContext context) {
    ActivateTemplateStatement statement = new ActivateTemplateStatement(devicePath);
    TSStatus status =
        AuthorityChecker.checkAuthority(statement, context.getSession().getUserName());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
    }
    ExecutionResult executionResult = executeStatement(statement, context);
    status = executionResult.status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      throw new SemanticException(new IoTDBException(status.getMessage(), status.getCode()));
    }
  }

  private void internalActivateTemplate(
      Map<PartialPath, Pair<Template, PartialPath>> devicesNeedActivateTemplate,
      MPPQueryContext context) {
    InternalBatchActivateTemplateStatement statement =
        new InternalBatchActivateTemplateStatement(devicesNeedActivateTemplate);
    TSStatus status =
        AuthorityChecker.checkAuthority(statement, context.getSession().getUserName());
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
    }
    ExecutionResult executionResult = executeStatement(statement, context);
    status = executionResult.status;
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
      Map<PartialPath, Pair<Boolean, MeasurementGroup>> devicesNeedAutoCreateTimeSeries,
      MPPQueryContext context) {

    List<MeasurementPath> measurementPathList =
        executeInternalCreateTimeseriesStatement(
            new InternalCreateMultiTimeSeriesStatement(devicesNeedAutoCreateTimeSeries), context);

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
            entry.getKey().concatAsMeasurementPath(measurementGroup.getMeasurements().get(i)),
            new MeasurementSchema(
                measurementGroup.getMeasurements().get(i),
                measurementGroup.getDataTypes().get(i),
                measurementGroup.getEncodings().get(i),
                measurementGroup.getCompressors().get(i)),
            null,
            null,
            null,
            entry.getValue().left);
      }
    }
  }

  private void internalExtendTemplate(
      String templateName,
      List<String> measurementList,
      List<TSDataType> dataTypeList,
      List<TSEncoding> encodingList,
      List<CompressionType> compressionTypeList,
      MPPQueryContext context) {

    ExecutionResult executionResult =
        executeStatement(
            new AlterSchemaTemplateStatement(
                templateName,
                measurementList,
                dataTypeList,
                encodingList,
                compressionTypeList,
                TemplateAlterOperationType.EXTEND_TEMPLATE),
            context);
    TSStatus status = executionResult.status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode()
            != TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE.getStatusCode()) {
      throw new SemanticException(new IoTDBException(status.getMessage(), status.getCode()));
    }
  }
}
