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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.utils.SetThreadName;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    return fetchSchema(patternTree, false);
  }

  @Override
  public ClusterSchemaTree fetchSchemaWithTags(PathPatternTree patternTree) {
    return fetchSchema(patternTree, true);
  }

  private ClusterSchemaTree fetchSchema(PathPatternTree patternTree, boolean withTags) {
    Map<Integer, Template> templateMap = new HashMap<>();
    patternTree.constructTree();
    for (PartialPath pattern : patternTree.getAllPathPatterns()) {
      templateMap.putAll(templateManager.checkAllRelatedTemplate(pattern));
    }
    return executeSchemaFetchQuery(new SchemaFetchStatement(patternTree, templateMap, withTags));
  }

  private ClusterSchemaTree executeSchemaFetchQuery(SchemaFetchStatement schemaFetchStatement) {
    long queryId = SessionManager.getInstance().requestQueryId();
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
      coordinator.cleanupQueryExecution(queryId);
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
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      boolean isAligned) {
    schemaCache.takeReadLock();
    try {
      ClusterSchemaTree schemaTree = schemaCache.get(devicePath, measurements);
      List<Integer> indexOfMissingMeasurements =
          checkMissingMeasurements(schemaTree, devicePath, measurements);

      if (indexOfMissingMeasurements.isEmpty()) {
        return schemaTree;
      }

      PathPatternTree patternTree = new PathPatternTree();
      for (int index : indexOfMissingMeasurements) {
        patternTree.appendFullPath(devicePath, measurements[index]);
      }

      ClusterSchemaTree remoteSchemaTree = fetchSchema(patternTree);
      if (!remoteSchemaTree.isEmpty()) {
        schemaTree.mergeSchemaTree(remoteSchemaTree);
        schemaCache.put(remoteSchemaTree);
      }

      if (!config.isAutoCreateSchemaEnabled()) {
        return schemaTree;
      }

      ClusterSchemaTree missingSchemaTree =
          checkAndAutoCreateMissingMeasurements(
              remoteSchemaTree,
              devicePath,
              indexOfMissingMeasurements,
              measurements,
              getDataType,
              null,
              null,
              isAligned);

      schemaTree.mergeSchemaTree(missingSchemaTree);
      schemaCache.put(missingSchemaTree);

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
    schemaCache.takeReadLock();

    try {
      ClusterSchemaTree schemaTree = new ClusterSchemaTree();
      PathPatternTree patternTree = new PathPatternTree();
      List<List<Integer>> indexOfMissingMeasurementsList = new ArrayList<>(devicePathList.size());
      for (int i = 0; i < devicePathList.size(); i++) {
        schemaTree.mergeSchemaTree(schemaCache.get(devicePathList.get(i), measurementsList.get(i)));
        List<Integer> indexOfMissingMeasurements =
            checkMissingMeasurements(schemaTree, devicePathList.get(i), measurementsList.get(i));
        indexOfMissingMeasurementsList.add(indexOfMissingMeasurements);
        for (int index : indexOfMissingMeasurements) {
          patternTree.appendFullPath(devicePathList.get(i), measurementsList.get(i)[index]);
        }
      }

      if (patternTree.isEmpty()) {
        return schemaTree;
      }

      ClusterSchemaTree remoteSchemaTree = fetchSchema(patternTree);
      if (!remoteSchemaTree.isEmpty()) {
        schemaTree.mergeSchemaTree(remoteSchemaTree);
        schemaCache.put(remoteSchemaTree);
      }

      if (!config.isAutoCreateSchemaEnabled()) {
        return schemaTree;
      }

      ClusterSchemaTree missingSchemaTree;
      for (int i = 0; i < devicePathList.size(); i++) {
        int finalI = i;
        missingSchemaTree =
            checkAndAutoCreateMissingMeasurements(
                schemaTree,
                devicePathList.get(i),
                indexOfMissingMeasurementsList.get(i),
                measurementsList.get(i),
                index -> tsDataTypesList.get(finalI)[index],
                encodingsList == null ? null : encodingsList.get(i),
                compressionTypesList == null ? null : compressionTypesList.get(i),
                isAlignedList.get(i));
        schemaTree.mergeSchemaTree(missingSchemaTree);
        schemaCache.put(missingSchemaTree);
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

  private ClusterSchemaTree checkAndAutoCreateMissingMeasurements(
      ClusterSchemaTree schemaTree,
      PartialPath devicePath,
      List<Integer> indexOfMissingMeasurements,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
      boolean isAligned) {
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

    ClusterSchemaTree reFetchedSchemaTree = new ClusterSchemaTree();

    if (indexOfMissingMeasurements.isEmpty()) {
      return reFetchedSchemaTree;
    }

    Pair<Template, PartialPath> templateInfo = templateManager.checkTemplateSetInfo(devicePath);
    if (templateInfo != null) {
      Template template = templateInfo.left;
      boolean shouldActivateTemplate = false;
      for (int index : indexOfMissingMeasurements) {
        if (template.hasSchema(measurements[index])) {
          shouldActivateTemplate = true;
          break;
        }
      }

      if (shouldActivateTemplate) {
        internalActivateTemplate(devicePath);
        List<Integer> recheckedIndexOfMissingMeasurements = new ArrayList<>();
        for (int i = 0; i < indexOfMissingMeasurements.size(); i++) {
          if (!template.hasSchema(measurements[i])) {
            recheckedIndexOfMissingMeasurements.add(indexOfMissingMeasurements.get(i));
          }
        }
        indexOfMissingMeasurements = recheckedIndexOfMissingMeasurements;
        for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
          schemaTree.appendSingleMeasurement(
              devicePath.concatNode(entry.getKey()),
              (MeasurementSchema) entry.getValue(),
              null,
              null,
              template.isDirectAligned());
        }

        if (indexOfMissingMeasurements.isEmpty()) {
          return schemaTree;
        }
      }
    }

    List<String> missingMeasurements = new ArrayList<>(indexOfMissingMeasurements.size());
    List<TSDataType> dataTypesOfMissingMeasurement =
        new ArrayList<>(indexOfMissingMeasurements.size());
    List<TSEncoding> encodingsOfMissingMeasurement =
        new ArrayList<>(indexOfMissingMeasurements.size());
    List<CompressionType> compressionTypesOfMissingMeasurement =
        new ArrayList<>(indexOfMissingMeasurements.size());
    indexOfMissingMeasurements.forEach(
        index -> {
          TSDataType tsDataType = getDataType.apply(index);
          // tsDataType == null means insert null value to a non-exist series
          // should skip creating them
          if (tsDataType != null) {
            missingMeasurements.add(measurements[index]);
            dataTypesOfMissingMeasurement.add(tsDataType);
            encodingsOfMissingMeasurement.add(
                encodings == null ? getDefaultEncoding(tsDataType) : encodings[index]);
            compressionTypesOfMissingMeasurement.add(
                compressionTypes == null
                    ? TSFileDescriptor.getInstance().getConfig().getCompressor()
                    : compressionTypes[index]);
          }
        });

    if (!missingMeasurements.isEmpty()) {
      schemaTree.mergeSchemaTree(
          internalCreateTimeseries(
              devicePath,
              missingMeasurements,
              dataTypesOfMissingMeasurement,
              encodingsOfMissingMeasurement,
              compressionTypesOfMissingMeasurement,
              isAligned));
    }
    return schemaTree;
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

  private ClusterSchemaTree internalCreateTimeseries(
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
          null,
          isAligned);
    }

    schemaCache.put(schemaTree);

    return schemaTree;
  }

  private List<MeasurementPath> executeInternalCreateTimeseriesStatement(
      InternalCreateTimeSeriesStatement statement) {

    ExecutionResult executionResult = executeStatement(statement);

    int statusCode = executionResult.status.getCode();
    if (statusCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return Collections.emptyList();
    }

    if (statusCode != TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(executionResult.status.getMessage(), statusCode));
    }

    Set<String> failedCreationList = new HashSet<>();
    List<MeasurementPath> alreadyExistingMeasurements = new ArrayList<>();
    for (TSStatus subStatus : executionResult.status.subStatus) {
      if (subStatus.code == TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
        alreadyExistingMeasurements.add(
            MeasurementPath.parseDataFromString(subStatus.getMessage()));
      } else {
        failedCreationList.add(subStatus.message);
      }
    }

    if (!failedCreationList.isEmpty()) {
      throw new SemanticException(new MetadataException(String.join(";, ", failedCreationList)));
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
    long queryId = SessionManager.getInstance().requestQueryId();
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
