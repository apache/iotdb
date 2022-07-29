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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import io.airlift.concurrent.SetThreadName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();
  private final DataNodeSchemaCache schemaCache = DataNodeSchemaCache.getInstance();
  private final ITemplateManager templateManager = ClusterTemplateManager.getInstance();

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
    Map<Integer, Template> templateMap = new HashMap<>();
    patternTree.constructTree();
    for (PartialPath pattern : patternTree.getAllPathPatterns()) {
      templateMap.putAll(templateManager.checkAllRelatedTemplate(pattern));
    }
    return executeSchemaFetchQuery(new SchemaFetchStatement(patternTree, templateMap));
  }

  private ClusterSchemaTree executeSchemaFetchQuery(SchemaFetchStatement schemaFetchStatement) {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    try {
      ExecutionResult executionResult =
          coordinator.execute(
              schemaFetchStatement,
              queryId,
              null,
              "",
              ClusterPartitionFetcher.getInstance(),
              this,
              config.getQueryTimeoutThreshold());
      // TODO: (xingtanzjr) throw exception
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            String.format(
                "cannot fetch schema, status is: %s, msg is: %s",
                executionResult.status.getCode(), executionResult.status.getMessage()));
      }
      try (SetThreadName threadName = new SetThreadName(executionResult.queryId.getId())) {
        ClusterSchemaTree result = new ClusterSchemaTree();
        List<String> storageGroupList = new ArrayList<>();
        while (coordinator.getQueryExecution(queryId).hasNextResult()) {
          // The query will be transited to FINISHED when invoking getBatchResult() at the last time
          // So we don't need to clean up it manually
          Optional<TsBlock> tsBlock;
          try {
            tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
          } catch (IoTDBException e) {
            throw new RuntimeException("Fetch Schema failed. ", e);
          }
          if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
            break;
          }
          Column column = tsBlock.get().getColumn(0);
          for (int i = 0; i < column.getPositionCount(); i++) {
            parseFetchedData(column.getBinary(i), result, storageGroupList);
          }
        }
        result.setStorageGroups(storageGroupList);
        return result;
      }
    } finally {
      coordinator.removeQueryExecution(queryId);
    }
  }

  private void parseFetchedData(
      Binary data, ClusterSchemaTree resultSchemaTree, List<String> storageGroupList) {
    InputStream inputStream = new ByteArrayInputStream(data.getValues());
    try {
      byte type = ReadWriteIOUtils.readByte(inputStream);
      if (type == 0) {
        int size = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < size; i++) {
          storageGroupList.add(ReadWriteIOUtils.readString(inputStream));
        }
      } else if (type == 1) {
        resultSchemaTree.mergeSchemaTree(ClusterSchemaTree.deserialize(inputStream));
      } else {
        throw new RuntimeException(
            new MetadataException("Failed to fetch schema because of unrecognized data"));
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
  }

  @Override
  public ISchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean isAligned) {

    ClusterSchemaTree schemaTree = schemaCache.get(devicePath, measurements);
    Pair<List<String>, List<TSDataType>> missingMeasurements =
        checkMissingMeasurements(schemaTree, devicePath, measurements, tsDataTypes);

    PathPatternTree patternTree = new PathPatternTree();
    for (String measurement : missingMeasurements.left) {
      patternTree.appendFullPath(devicePath, measurement);
    }

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    ClusterSchemaTree remoteSchemaTree = fetchSchema(patternTree);
    schemaTree.mergeSchemaTree(remoteSchemaTree);
    schemaCache.put(remoteSchemaTree);

    if (!config.isAutoCreateSchemaEnabled()) {
      return schemaTree;
    }

    ClusterSchemaTree missingSchemaTree =
        checkAndAutoCreateMissingMeasurements(
            remoteSchemaTree,
            devicePath,
            missingMeasurements.left.toArray(new String[0]),
            missingMeasurements.right.toArray(new TSDataType[0]),
            isAligned);

    schemaTree.mergeSchemaTree(missingSchemaTree);
    schemaCache.put(missingSchemaTree);

    return schemaTree;
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {

    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    PathPatternTree patternTree = new PathPatternTree();
    for (int i = 0; i < devicePathList.size(); i++) {
      schemaTree.mergeSchemaTree(schemaCache.get(devicePathList.get(i), measurementsList.get(i)));
      List<String> missingMeasurements =
          checkMissingMeasurements(
                  schemaTree,
                  devicePathList.get(i),
                  measurementsList.get(i),
                  tsDataTypesList.get(i))
              .left;
      for (String measurement : missingMeasurements) {
        patternTree.appendFullPath(devicePathList.get(i), measurement);
      }
    }

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    ClusterSchemaTree remoteSchemaTree = fetchSchema(patternTree);
    schemaTree.mergeSchemaTree(remoteSchemaTree);
    schemaCache.put(remoteSchemaTree);

    if (!config.isAutoCreateSchemaEnabled()) {
      return schemaTree;
    }

    ClusterSchemaTree missingSchemaTree;
    for (int i = 0; i < devicePathList.size(); i++) {
      missingSchemaTree =
          checkAndAutoCreateMissingMeasurements(
              schemaTree,
              devicePathList.get(i),
              measurementsList.get(i),
              tsDataTypesList.get(i),
              isAlignedList.get(i));
      schemaTree.mergeSchemaTree(missingSchemaTree);
      schemaCache.put(missingSchemaTree);
    }
    return schemaTree;
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

  private ClusterSchemaTree checkAndAutoCreateMissingMeasurements(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      String[] measurements,
      TSDataType[] tsDataTypes,
      boolean isAligned) {

    Pair<List<String>, List<TSDataType>> checkResult =
        checkMissingMeasurements(schemaTree, devicePath, measurements, tsDataTypes);

    List<String> missingMeasurements = checkResult.left;
    List<TSDataType> dataTypesOfMissingMeasurement = checkResult.right;

    ClusterSchemaTree reFetchedSchemaTree = new ClusterSchemaTree();

    if (missingMeasurements.isEmpty()) {
      return reFetchedSchemaTree;
    }

    Pair<Template, PartialPath> templateInfo = templateManager.checkTemplateSetInfo(devicePath);
    if (templateInfo != null) {
      Template template = templateInfo.left;
      boolean shouldActivateTemplate = false;
      for (String missingMeasurement : missingMeasurements) {
        if (template.hasSchema(missingMeasurement)) {
          shouldActivateTemplate = true;
          break;
        }
      }

      if (shouldActivateTemplate) {
        internalActivateTemplate(devicePath);
        List<String> recheckedMissingMeasurements = new ArrayList<>();
        List<TSDataType> recheckedMissingDataTypes = new ArrayList<>();
        for (int i = 0; i < missingMeasurements.size(); i++) {
          if (!template.hasSchema(missingMeasurements.get(i))) {
            recheckedMissingMeasurements.add(missingMeasurements.get(i));
            recheckedMissingDataTypes.add(dataTypesOfMissingMeasurement.get(i));
          }
        }
        missingMeasurements = recheckedMissingMeasurements;
        dataTypesOfMissingMeasurement = recheckedMissingDataTypes;
        for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
          schemaTree.appendSingleMeasurement(
              devicePath.concatNode(entry.getKey()),
              (MeasurementSchema) entry.getValue(),
              null,
              template.isDirectAligned());
        }

        if (missingMeasurements.isEmpty()) {
          return schemaTree;
        }
      }
    }

    schemaTree.mergeSchemaTree(
        internalCreateTimeseries(
            devicePath, missingMeasurements, dataTypesOfMissingMeasurement, isAligned));

    return schemaTree;
  }

  private Pair<List<String>, List<TSDataType>> checkMissingMeasurements(
      ISchemaTree schemaTree,
      PartialPath devicePath,
      String[] measurements,
      TSDataType[] tsDataTypes) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));
    if (deviceSchemaInfo == null) {
      return new Pair<>(Arrays.asList(measurements), Arrays.asList(tsDataTypes));
    }

    List<String> missingMeasurements = new ArrayList<>();
    List<TSDataType> dataTypesOfMissingMeasurement = new ArrayList<>();
    List<MeasurementSchema> schemaList = deviceSchemaInfo.getMeasurementSchemaList();
    for (int i = 0; i < measurements.length; i++) {
      if (schemaList.get(i) == null) {
        missingMeasurements.add(measurements[i]);
        dataTypesOfMissingMeasurement.add(tsDataTypes[i]);
      }
    }

    return new Pair<>(missingMeasurements, dataTypesOfMissingMeasurement);
  }

  private ClusterSchemaTree internalCreateTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> tsDataTypes,
      boolean isAligned) {

    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    for (TSDataType dataType : tsDataTypes) {
      encodings.add(getDefaultEncoding(dataType));
      compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
    }

    List<MeasurementPath> measurementPathList =
        executeInternalCreateTimeseriesStatement(
            new InternalCreateTimeSeriesStatement(
                devicePath, measurements, tsDataTypes, encodings, compressors, isAligned));

    Set<Integer> alreadyExistingMeasurementIndexSet =
        measurementPathList.stream()
            .map(o -> measurements.indexOf(o.getMeasurement()))
            .collect(Collectors.toSet());

    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
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
          isAligned);
    }

    return schemaTree;
  }

  private List<MeasurementPath> executeInternalCreateTimeseriesStatement(
      InternalCreateTimeSeriesStatement statement) {

    ExecutionResult executionResult = executeStatement(statement);

    // TODO: throw exception
    int statusCode = executionResult.status.getCode();
    if (statusCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return Collections.emptyList();
    }

    List<String> failedCreationList = new ArrayList<>();
    List<MeasurementPath> alreadyExistingMeasurements = new ArrayList<>();
    for (TSStatus subStatus : executionResult.status.subStatus) {
      if (subStatus.code == TSStatusCode.MEASUREMENT_ALREADY_EXIST.getStatusCode()) {
        alreadyExistingMeasurements.add(
            (MeasurementPath)
                PathDeserializeUtil.deserialize(
                    ByteBuffer.wrap(subStatus.getMessage().getBytes())));
      } else {
        failedCreationList.add(subStatus.message);
      }
    }

    if (!failedCreationList.isEmpty()) {
      StringBuilder stringBuilder = new StringBuilder();
      for (String message : failedCreationList) {
        stringBuilder.append(message).append("\n");
      }
      throw new RuntimeException(String.format("Failed to auto create schema\n %s", stringBuilder));
    }

    return alreadyExistingMeasurements;
  }

  public void internalActivateTemplate(PartialPath devicePath) {
    ExecutionResult executionResult = executeStatement(new ActivateTemplateStatement(devicePath));
    TSStatus status = executionResult.status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
    }
  }

  private ExecutionResult executeStatement(Statement statement) {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    return coordinator.execute(
        statement,
        queryId,
        null,
        "",
        ClusterPartitionFetcher.getInstance(),
        this,
        config.getQueryTimeoutThreshold());
  }

  @Override
  public void invalidAllCache() {
    DataNodeSchemaCache.getInstance().cleanUp();
  }
}
