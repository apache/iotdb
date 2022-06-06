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
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesByDeviceStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class StandaloneSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Coordinator coordinator = Coordinator.getInstance();
  private final LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();
  private final IPartitionFetcher partitionFetcher = StandalonePartitionFetcher.getInstance();

  private StandaloneSchemaFetcher() {}

  public static StandaloneSchemaFetcher getInstance() {
    return new StandaloneSchemaFetcher();
  }

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree) {
    patternTree.constructTree();
    Set<String> storageGroupSet = new HashSet<>();
    SchemaTree schemaTree = new SchemaTree();
    List<PartialPath> partialPathList = patternTree.splitToPathList();
    try {
      for (PartialPath path : partialPathList) {
        String storageGroup = localConfigNode.getBelongedStorageGroup(path).getFullPath();
        storageGroupSet.add(storageGroup);
        SchemaRegionId schemaRegionId = localConfigNode.getBelongedSchemaRegionId(path);
        ISchemaRegion schemaRegion = schemaEngine.getSchemaRegion(schemaRegionId);
        schemaTree.appendMeasurementPaths(schemaRegion.getMeasurementPaths(path, false));
      }
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
    schemaTree.setStorageGroups(new ArrayList<>(storageGroupSet));
    return schemaTree;
  }

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree, SchemaPartition schemaPartition) {
    return fetchSchema(patternTree);
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean aligned) {
    SchemaTree schemaTree = new SchemaTree();

    PathPatternTree patternTree = new PathPatternTree(devicePath, measurements);

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    SchemaTree fetchedSchemaTree;

    if (!config.isAutoCreateSchemaEnabled()) {
      fetchedSchemaTree =
          fetchSchema(patternTree, partitionFetcher.getSchemaPartition(patternTree));
      schemaTree.mergeSchemaTree(fetchedSchemaTree);
      return schemaTree;
    }

    fetchedSchemaTree =
        fetchSchema(patternTree, partitionFetcher.getOrCreateSchemaPartition(patternTree));
    schemaTree.mergeSchemaTree(fetchedSchemaTree);

    SchemaTree missingSchemaTree =
        checkAndAutoCreateMissingMeasurements(
            fetchedSchemaTree, devicePath, measurements, tsDataTypes, aligned);

    schemaTree.mergeSchemaTree(missingSchemaTree);

    return schemaTree;
  }

  @Override
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {
    //    Map<PartialPath, List<String>> deviceToMeasurementMap = new HashMap<>();
    //    for (int i = 0; i < devicePath.size(); i++) {
    //      deviceToMeasurementMap.put(devicePath.get(i), Arrays.asList(measurements.get(i)));
    //    }
    //    // todo implement auto create schema
    //    return fetchSchema(new PathPatternTree(deviceToMeasurementMap));

    SchemaTree schemaTree = new SchemaTree();
    PathPatternTree patternTree = new PathPatternTree();
    for (int i = 0; i < devicePathList.size(); i++) {
      patternTree.appendPaths(devicePathList.get(i), Arrays.asList(measurementsList.get(i)));
    }

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    SchemaTree fetchedSchemaTree;

    if (!config.isAutoCreateSchemaEnabled()) {
      fetchedSchemaTree =
          fetchSchema(patternTree, partitionFetcher.getSchemaPartition(patternTree));
      schemaTree.mergeSchemaTree(fetchedSchemaTree);
      return schemaTree;
    }

    fetchedSchemaTree =
        fetchSchema(patternTree, partitionFetcher.getOrCreateSchemaPartition(patternTree));
    schemaTree.mergeSchemaTree(fetchedSchemaTree);

    SchemaTree missingSchemaTree;
    for (int i = 0; i < devicePathList.size(); i++) {
      missingSchemaTree =
          checkAndAutoCreateMissingMeasurements(
              schemaTree,
              devicePathList.get(i),
              measurementsList.get(i),
              tsDataTypesList.get(i),
              isAlignedList.get(i));
      schemaTree.mergeSchemaTree(missingSchemaTree);
    }
    return schemaTree;
  }

  @Override
  public void invalidAllCache() {}

  private Pair<List<String>, List<TSDataType>> checkMissingMeasurements(
      SchemaTree schemaTree,
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

  private SchemaTree checkAndAutoCreateMissingMeasurements(
      SchemaTree schemaTree,
      PartialPath devicePath,
      String[] measurements,
      TSDataType[] tsDataTypes,
      boolean isAligned) {

    Pair<List<String>, List<TSDataType>> checkResult =
        checkMissingMeasurements(schemaTree, devicePath, measurements, tsDataTypes);

    List<String> missingMeasurements = checkResult.left;
    List<TSDataType> dataTypesOfMissingMeasurement = checkResult.right;

    if (missingMeasurements.isEmpty()) {
      return new SchemaTree();
    }

    internalCreateTimeseries(
        devicePath, missingMeasurements, dataTypesOfMissingMeasurement, isAligned);

    SchemaTree reFetchSchemaTree =
        fetchSchema(new PathPatternTree(devicePath, missingMeasurements));

    Pair<List<String>, List<TSDataType>> recheckResult =
        checkMissingMeasurements(
            reFetchSchemaTree,
            devicePath,
            missingMeasurements.toArray(new String[0]),
            dataTypesOfMissingMeasurement.toArray(new TSDataType[0]));

    missingMeasurements = recheckResult.left;
    if (!missingMeasurements.isEmpty()) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("(");
      for (String missingMeasurement : missingMeasurements) {
        stringBuilder.append(missingMeasurement).append(" ");
      }
      stringBuilder.append(")");
      throw new RuntimeException(
          String.format(
              "Failed to auto create schema, devicePath: %s, measurements: %s",
              devicePath.getFullPath(), stringBuilder));
    }

    return reFetchSchemaTree;
  }

  private void internalCreateTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> tsDataTypes,
      boolean isAligned) {

    if (isAligned) {
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement =
          new CreateAlignedTimeSeriesStatement();
      createAlignedTimeSeriesStatement.setDevicePath(devicePath);
      createAlignedTimeSeriesStatement.setMeasurements(measurements);
      createAlignedTimeSeriesStatement.setDataTypes(tsDataTypes);
      List<TSEncoding> encodings = new ArrayList<>();
      List<CompressionType> compressors = new ArrayList<>();
      for (TSDataType dataType : tsDataTypes) {
        encodings.add(getDefaultEncoding(dataType));
        compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
      }
      createAlignedTimeSeriesStatement.setEncodings(encodings);
      createAlignedTimeSeriesStatement.setCompressors(compressors);
      createAlignedTimeSeriesStatement.setAliasList(null);

      executeCreateStatement(createAlignedTimeSeriesStatement);
    } else {

      executeCreateTimeseriesByDeviceStatement(
          new CreateTimeSeriesByDeviceStatement(devicePath, measurements, tsDataTypes));
    }
  }

  private void executeCreateStatement(Statement statement) {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    ExecutionResult executionResult =
        coordinator.execute(statement, queryId, null, "", partitionFetcher, this);
    // TODO: throw exception
    try {
      int statusCode = executionResult.status.getCode();
      if (statusCode != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && statusCode != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
        throw new RuntimeException(
            "cannot auto create schema, status is: " + executionResult.status);
      }
    } finally {
      coordinator.getQueryExecution(queryId).stopAndCleanup();
    }
  }

  private void executeCreateTimeseriesByDeviceStatement(
      CreateTimeSeriesByDeviceStatement statement) {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    ExecutionResult executionResult =
        coordinator.execute(statement, queryId, null, "", partitionFetcher, this);
    // TODO: throw exception
    try {
      int statusCode = executionResult.status.getCode();
      if (statusCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }

      for (TSStatus subStatus : executionResult.status.subStatus) {
        if (subStatus.code != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
          throw new RuntimeException(
              "cannot auto create schema, status is: " + executionResult.status);
        }
      }
    } finally {
      coordinator.getQueryExecution(queryId).stopAndCleanup();
    }
  }
}
