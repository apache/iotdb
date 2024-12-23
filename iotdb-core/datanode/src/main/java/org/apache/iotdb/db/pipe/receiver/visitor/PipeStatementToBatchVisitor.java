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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This visitor will convert {@link Statement}s to batch and is stateful, and can only be used in a
 * single thread. It only guarantees the correctness when is used by the schema snapshot and shall
 * not be used for other purposes.
 *
 * <p>When the visitor is called, it shall encounter no:
 *
 * <p>1. {@link StatementNode}s other than {@link Statement}s
 *
 * <p>2. TimeSeries with identical path.
 *
 * <p>3. Alter to the timeSeries and templates in batch.
 */
public class PipeStatementToBatchVisitor extends StatementVisitor<Optional<Statement>, Void> {

  private static final int MAX_SCHEMA_BATCH_SIZE =
      PipeConfig.getInstance().getPipeSnapshotExecutionMaxBatchSize();

  private final List<CreateTimeSeriesStatement> createTimeSeriesStatements = new ArrayList<>();

  private final List<CreateAlignedTimeSeriesStatement> createAlignedTimeSeriesStatements =
      new ArrayList<>();

  private final List<ActivateTemplateStatement> activateTemplateStatements = new ArrayList<>();

  @Override
  public Optional<Statement> visitNode(final StatementNode statement, final Void context) {
    return Optional.of((Statement) statement);
  }

  @Override
  public Optional<Statement> visitCreateTimeseries(
      final CreateTimeSeriesStatement statement, final Void context) {
    createTimeSeriesStatements.add(statement);
    return createTimeSeriesStatements.size() + createAlignedTimeSeriesStatements.size()
            >= MAX_SCHEMA_BATCH_SIZE
        ? Optional.of(getTimeSeriesBatchStatement())
        : Optional.empty();
  }

  @Override
  public Optional<Statement> visitCreateAlignedTimeseries(
      final CreateAlignedTimeSeriesStatement statement, final Void context) {
    createAlignedTimeSeriesStatements.add(statement);
    return createTimeSeriesStatements.size() + createAlignedTimeSeriesStatements.size()
            >= MAX_SCHEMA_BATCH_SIZE
        ? Optional.of(getTimeSeriesBatchStatement())
        : Optional.empty();
  }

  private InternalCreateMultiTimeSeriesStatement getTimeSeriesBatchStatement() {
    final InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement =
        new InternalCreateMultiTimeSeriesStatement(new HashMap<>());
    createTimeSeriesStatements.forEach(
        statement ->
            addNonAlignedTimeSeriesToBatchStatement(
                statement, internalCreateMultiTimeSeriesStatement));
    createAlignedTimeSeriesStatements.forEach(
        statement ->
            addAlignedTimeSeriesToBatchStatement(
                statement, internalCreateMultiTimeSeriesStatement));
    createTimeSeriesStatements.clear();
    createAlignedTimeSeriesStatements.clear();
    return internalCreateMultiTimeSeriesStatement;
  }

  private void addNonAlignedTimeSeriesToBatchStatement(
      final CreateTimeSeriesStatement statement,
      final InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement) {
    final MeasurementGroup group =
        internalCreateMultiTimeSeriesStatement
            .getDeviceMap()
            .computeIfAbsent(
                statement.getPath().getDevicePath(),
                devicePath -> new Pair<>(false, new MeasurementGroup()))
            .getRight();
    if (group.addMeasurement(
        statement.getPath().getMeasurement(),
        statement.getDataType(),
        statement.getEncoding(),
        statement.getCompressor())) {
      group.addAttributes(statement.getAttributes());
      group.addTags(statement.getTags());
      group.addProps(statement.getProps());
      group.addAlias(statement.getAlias());
    }
  }

  private void addAlignedTimeSeriesToBatchStatement(
      final CreateAlignedTimeSeriesStatement statement,
      final InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement) {
    final MeasurementGroup group =
        internalCreateMultiTimeSeriesStatement
            .getDeviceMap()
            .computeIfAbsent(
                statement.getDevicePath(), devicePath -> new Pair<>(true, new MeasurementGroup()))
            .getRight();
    for (int i = 0; i < statement.getMeasurements().size(); ++i) {
      if (group.addMeasurement(
          statement.getMeasurements().get(i),
          statement.getDataTypes().get(i),
          statement.getEncodings().get(i),
          statement.getCompressors().get(i))) {
        group.addProps(new HashMap<>());
        // Non-null lists
        group.addTags(statement.getTagsList().get(i));
        group.addAttributes(statement.getAttributesList().get(i));
        group.addAlias(statement.getAliasList().get(i));
      }
    }
  }

  @Override
  public Optional<Statement> visitActivateTemplate(
      final ActivateTemplateStatement activateTemplateStatement, final Void context) {
    activateTemplateStatements.add(activateTemplateStatement);
    return activateTemplateStatements.size() >= MAX_SCHEMA_BATCH_SIZE
        ? Optional.of(getTemplateBatchStatement())
        : Optional.empty();
  }

  private BatchActivateTemplateStatement getTemplateBatchStatement() {
    final BatchActivateTemplateStatement batchActivateTemplateStatement =
        new BatchActivateTemplateStatement(
            activateTemplateStatements.stream()
                .map(ActivateTemplateStatement::getPath)
                .collect(Collectors.toList()));
    activateTemplateStatements.clear();
    return batchActivateTemplateStatement;
  }

  public List<Optional<Statement>> getRemainBatches() {
    return Arrays.asList(
        !(createTimeSeriesStatements.isEmpty() && createAlignedTimeSeriesStatements.isEmpty())
            ? Optional.of(getTimeSeriesBatchStatement())
            : Optional.empty(),
        !activateTemplateStatements.isEmpty()
            ? Optional.of(getTemplateBatchStatement())
            : Optional.empty());
  }

  public void clear() {
    createTimeSeriesStatements.clear();
    createAlignedTimeSeriesStatements.clear();
    activateTemplateStatements.clear();
  }
}
