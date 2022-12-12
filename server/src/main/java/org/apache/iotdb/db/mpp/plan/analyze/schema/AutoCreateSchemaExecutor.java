/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class AutoCreateSchemaExecutor {

  private final Function<Statement, ExecutionResult> statementExecutor;

  AutoCreateSchemaExecutor(Function<Statement, ExecutionResult> statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  // try to create the target timeseries and return schemaTree involving successfully created
  // timeseries and existing timeseries
  ClusterSchemaTree internalCreateTimeseries(
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

    return schemaTree;
  }

  // auto create timeseries and return the existing timeseries info
  private List<MeasurementPath> executeInternalCreateTimeseriesStatement(
      InternalCreateTimeSeriesStatement statement) {

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

  void internalActivateTemplate(PartialPath devicePath) {
    ExecutionResult executionResult =
        statementExecutor.apply(new ActivateTemplateStatement(devicePath));
    TSStatus status = executionResult.status;
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
    }
  }
}
