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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.execution.Coordinator;
import org.apache.iotdb.db.mpp.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SchemaFetchStatement;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();

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
    SchemaPartition schemaPartition = partitionFetcher.getSchemaPartition(patternTree);
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
        schemaPartition.getSchemaPartitionMap();
    List<String> storageGroups = new ArrayList<>(schemaPartitionMap.keySet());

    SchemaFetchStatement schemaFetchStatement = new SchemaFetchStatement(patternTree);
    schemaFetchStatement.setSchemaPartition(schemaPartition);

    QueryId queryId =
        new QueryId(String.valueOf(SessionManager.getInstance().requestQueryId(false)));
    ExecutionResult executionResult =
        coordinator.execute(schemaFetchStatement, queryId, null, "", partitionFetcher, this);
    // TODO: (xingtanzjr) throw exception
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException("cannot fetch schema, status is: " + executionResult.status);
    }
    SchemaTree result = new SchemaTree();
    while (coordinator.getQueryExecution(queryId).hasNextResult()) {
      TsBlock tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
      if (tsBlock == null) {
        break;
      }
      result.setStorageGroups(storageGroups);
      Binary binary;
      SchemaTree fetchedSchemaTree;
      Column column = tsBlock.getColumn(0);
      for (int i = 0; i < column.getPositionCount(); i++) {
        binary = column.getBinary(i);
        fetchedSchemaTree = SchemaTree.deserialize(ByteBuffer.wrap(binary.getValues()));
        result.mergeSchemaTree(fetchedSchemaTree);
      }
    }
    // TODO: (xingtanzjr) need to release this query's resource here. This is a temporary way
    coordinator.getQueryExecution(queryId).stopAndCleanup();
    return result;
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean isAligned) {

    SchemaTree schemaTree = fetchSchema(new PathPatternTree(devicePath, measurements));

    if (!config.isAutoCreateSchemaEnabled()) {
      return schemaTree;
    }

    schemaTree.mergeSchemaTree(
        checkAndAutoCreateMissingMeasurements(
            schemaTree, devicePath, measurements, tsDataTypes, isAligned));

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
    DeviceSchemaInfo deviceSchemaInfo = null;
    try {
      deviceSchemaInfo = schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));
    } catch (PathNotExistException e) {
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

      executeCreateStatement(createAlignedTimeSeriesStatement);
    } else {
      // todo @zyk implement batch create
      for (int i = 0; i < measurements.size(); i++) {
        CreateTimeSeriesStatement createTimeSeriesStatement = new CreateTimeSeriesStatement();
        createTimeSeriesStatement.setPath(devicePath.concatNode(measurements.get(i)));
        createTimeSeriesStatement.setDataType(tsDataTypes.get(i));
        createTimeSeriesStatement.setEncoding(getDefaultEncoding(tsDataTypes.get(i)));
        createTimeSeriesStatement.setCompressor(
            TSFileDescriptor.getInstance().getConfig().getCompressor());
        createTimeSeriesStatement.setProps(Collections.emptyMap());

        executeCreateStatement(createTimeSeriesStatement);
      }
    }
  }

  private void executeCreateStatement(Statement statement) {
    QueryId queryId =
        new QueryId(String.valueOf(SessionManager.getInstance().requestQueryId(false)));
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

  @Override
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<TSDataType[]> tsDataTypesList,
      List<Boolean> isAlignedList) {

    PathPatternTree patternTree = new PathPatternTree();
    for (int i = 0; i < devicePathList.size(); i++) {
      patternTree.appendPaths(devicePathList.get(i), Arrays.asList(measurementsList.get(i)));
    }

    SchemaTree schemaTree = fetchSchema(patternTree);

    if (!config.isAutoCreateSchemaEnabled()) {
      return schemaTree;
    }

    for (int i = 0; i < devicePathList.size(); i++) {
      schemaTree.mergeSchemaTree(
          checkAndAutoCreateMissingMeasurements(
              schemaTree,
              devicePathList.get(i),
              measurementsList.get(i),
              tsDataTypesList.get(i),
              isAlignedList.get(i)));
    }
    return schemaTree;
  }
}
