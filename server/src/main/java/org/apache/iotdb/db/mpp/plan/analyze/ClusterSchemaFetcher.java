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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesByDeviceStatement;
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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  private final DataNodeSchemaCache schemaCache = DataNodeSchemaCache.getInstance();

  private static final class ClusterSchemaFetcherHolder {
    private static final ClusterSchemaFetcher INSTANCE = new ClusterSchemaFetcher();

    private ClusterSchemaFetcherHolder() {}
  }

  public static ClusterSchemaFetcher getInstance() {
    return ClusterSchemaFetcherHolder.INSTANCE;
  }

  private ClusterSchemaFetcher() {}

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree) {
    return fetchSchema(patternTree, partitionFetcher.getSchemaPartition(patternTree));
  }

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree, SchemaPartition schemaPartition) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
        schemaPartition.getSchemaPartitionMap();
    List<String> storageGroups = new ArrayList<>(schemaPartitionMap.keySet());

    SchemaFetchStatement schemaFetchStatement = new SchemaFetchStatement(patternTree);
    schemaFetchStatement.setSchemaPartition(schemaPartition);

    SchemaTree result = executeSchemaFetchQuery(schemaFetchStatement);
    result.setStorageGroups(storageGroups);
    return result;
  }

  private SchemaTree executeSchemaFetchQuery(SchemaFetchStatement schemaFetchStatement) {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    ExecutionResult executionResult =
        coordinator.execute(schemaFetchStatement, queryId, null, "", partitionFetcher, this);
    // TODO: (xingtanzjr) throw exception
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          String.format(
              "cannot fetch schema, status is: %s, msg is: %s",
              executionResult.status.getCode(), executionResult.status.getMessage()));
    }
    SchemaTree result = new SchemaTree();
    while (coordinator.getQueryExecution(queryId).hasNextResult()) {
      // The query will be transited to FINISHED when invoking getBatchResult() at the last time
      // So we don't need to clean up it manually
      Optional<TsBlock> tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
      if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
        break;
      }
      Binary binary;
      SchemaTree fetchedSchemaTree;
      Column column = tsBlock.get().getColumn(0);
      for (int i = 0; i < column.getPositionCount(); i++) {
        binary = column.getBinary(i);
        fetchedSchemaTree = SchemaTree.deserialize(ByteBuffer.wrap(binary.getValues()));
        result.mergeSchemaTree(fetchedSchemaTree);
      }
    }
    return result;
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean isAligned) {

    SchemaTree schemaTree = schemaCache.get(devicePath, measurements);
    Pair<List<String>, List<TSDataType>> missingMeasurements =
        checkMissingMeasurements(schemaTree, devicePath, measurements, tsDataTypes);

    PathPatternTree patternTree =
        new PathPatternTree(devicePath, missingMeasurements.left.toArray(new String[0]));

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    SchemaTree remoteSchemaTree;

    if (!config.isAutoCreateSchemaEnabled()) {
      remoteSchemaTree = fetchSchema(patternTree, partitionFetcher.getSchemaPartition(patternTree));
      schemaTree.mergeSchemaTree(remoteSchemaTree);
      schemaCache.put(remoteSchemaTree);
      return schemaTree;
    }

    remoteSchemaTree =
        fetchSchema(patternTree, partitionFetcher.getOrCreateSchemaPartition(patternTree));
    schemaTree.mergeSchemaTree(remoteSchemaTree);
    schemaCache.put(remoteSchemaTree);

    SchemaTree missingSchemaTree =
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
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {

    SchemaTree schemaTree = new SchemaTree();
    PathPatternTree patternTree = new PathPatternTree();
    for (int i = 0; i < devicePathList.size(); i++) {
      schemaTree.mergeSchemaTree(schemaCache.get(devicePathList.get(i), measurementsList.get(i)));
      patternTree.appendPaths(
          devicePathList.get(i),
          checkMissingMeasurements(
                  schemaTree,
                  devicePathList.get(i),
                  measurementsList.get(i),
                  tsDataTypesList.get(i))
              .left);
    }

    if (patternTree.isEmpty()) {
      return schemaTree;
    }

    SchemaTree remoteSchemaTree;

    if (!config.isAutoCreateSchemaEnabled()) {
      remoteSchemaTree = fetchSchema(patternTree, partitionFetcher.getSchemaPartition(patternTree));
      schemaTree.mergeSchemaTree(remoteSchemaTree);
      schemaCache.put(remoteSchemaTree);
      return schemaTree;
    }

    remoteSchemaTree =
        fetchSchema(patternTree, partitionFetcher.getOrCreateSchemaPartition(patternTree));
    schemaTree.mergeSchemaTree(remoteSchemaTree);
    schemaCache.put(remoteSchemaTree);

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
      schemaCache.put(missingSchemaTree);
    }
    return schemaTree;
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

  @Override
  public void invalidAllCache() {
    DataNodeSchemaCache.getInstance().cleanUp();
  }
}
