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

package org.apache.iotdb.db.pipe.receiver.visitor;

import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * The {@link PipeStatementPatternParseVisitor} will transform the schema {@link Statement}s using
 * {@link IoTDBPipePattern}. Rule:
 *
 * <p>1. All patterns in the output {@link Statement} will be the intersection of the original
 * {@link Statement}'s patterns and the given {@link IoTDBPipePattern}.
 *
 * <p>2. If a pattern does not intersect with the {@link IoTDBPipePattern}, it's dropped.
 *
 * <p>3. If all the patterns in the {@link Statement} is dropped, the {@link Statement} is dropped.
 *
 * <p>4. The output {@link Statement} can be the altered form of the original one because it's read
 * from the {@link SRStatementGenerator} and will no longer be used.
 */
public class PipeStatementPatternParseVisitor
    extends StatementVisitor<Optional<Statement>, IoTDBPipePattern> {
  @Override
  public Optional<Statement> visitNode(
      final StatementNode statement, final IoTDBPipePattern pattern) {
    return Optional.of((Statement) statement);
  }

  @Override
  public Optional<Statement> visitCreateTimeseries(
      final CreateTimeSeriesStatement statement, final IoTDBPipePattern pattern) {
    return pattern.matchesMeasurement(
            statement.getPath().getIDeviceID(), statement.getPath().getMeasurement())
        ? Optional.of(statement)
        : Optional.empty();
  }

  @Override
  public Optional<Statement> visitCreateAlignedTimeseries(
      final CreateAlignedTimeSeriesStatement statement, final IoTDBPipePattern pattern) {
    final int[] filteredIndexes =
        IntStream.range(0, statement.getMeasurements().size())
            .filter(
                index ->
                    pattern.matchesMeasurement(
                        statement.getDevicePath().getIDeviceIDAsFullDevice(),
                        statement.getMeasurements().get(index)))
            .toArray();
    if (filteredIndexes.length == 0) {
      return Optional.empty();
    }
    final CreateAlignedTimeSeriesStatement targetCreateAlignedTimeSeriesStatement =
        new CreateAlignedTimeSeriesStatement();
    targetCreateAlignedTimeSeriesStatement.setDevicePath(statement.getDevicePath());
    Arrays.stream(filteredIndexes)
        .forEach(
            index -> {
              targetCreateAlignedTimeSeriesStatement.addMeasurement(
                  statement.getMeasurements().get(index));
              targetCreateAlignedTimeSeriesStatement.addDataType(
                  statement.getDataTypes().get(index));
              targetCreateAlignedTimeSeriesStatement.addEncoding(
                  statement.getEncodings().get(index));
              targetCreateAlignedTimeSeriesStatement.addCompressor(
                  statement.getCompressors().get(index));
              // Non-null lists
              targetCreateAlignedTimeSeriesStatement.addTagsList(
                  statement.getTagsList().get(index));
              targetCreateAlignedTimeSeriesStatement.addAttributesList(
                  statement.getAttributesList().get(index));
              targetCreateAlignedTimeSeriesStatement.addAliasList(
                  statement.getAliasList().get(index));
            });
    return Optional.of(targetCreateAlignedTimeSeriesStatement);
  }

  // For logical view with tags/attributes
  @Override
  public Optional<Statement> visitAlterTimeSeries(
      final AlterTimeSeriesStatement alterTimeSeriesStatement, final IoTDBPipePattern pattern) {
    return pattern.matchesMeasurement(
            alterTimeSeriesStatement.getPath().getIDeviceID(),
            alterTimeSeriesStatement.getPath().getMeasurement())
        ? Optional.of(alterTimeSeriesStatement)
        : Optional.empty();
  }

  @Override
  public Optional<Statement> visitActivateTemplate(
      final ActivateTemplateStatement activateTemplateStatement, final IoTDBPipePattern pattern) {
    return pattern.matchDevice(activateTemplateStatement.getPath().getFullPath())
        ? Optional.of(activateTemplateStatement)
        : Optional.empty();
  }

  @Override
  public Optional<Statement> visitCreateLogicalView(
      final CreateLogicalViewStatement createLogicalViewStatement, final IoTDBPipePattern pattern) {
    final int[] filteredIndexes =
        IntStream.range(0, createLogicalViewStatement.getTargetPathList().size())
            .filter(
                index ->
                    pattern.matchesMeasurement(
                        createLogicalViewStatement
                            .getTargetPathList()
                            .get(index)
                            .getIDeviceIDAsFullDevice(),
                        createLogicalViewStatement.getTargetPathList().get(index).getMeasurement()))
            .toArray();
    if (filteredIndexes.length == 0) {
      return Optional.empty();
    }
    createLogicalViewStatement.setTargetFullPaths(
        IoTDBPipePattern.applyIndexesOnList(
            filteredIndexes, createLogicalViewStatement.getTargetPathList()));
    createLogicalViewStatement.setViewExpressions(
        IoTDBPipePattern.applyIndexesOnList(
            filteredIndexes, createLogicalViewStatement.getViewExpressions()));

    return Optional.of(createLogicalViewStatement);
  }
}
