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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PipePlanToStatementVisitor extends PlanVisitor<Statement, Void> {

  @Override
  public Statement visitPlan(final PlanNode node, final Void context) {
    throw new UnsupportedOperationException(
        String.format(
            "PipePlanToStatementVisitor does not support visiting general plan, PlanNode: %s",
            node));
  }

  @Override
  public InsertRowStatement visitInsertRow(final InsertRowNode node, final Void context) {
    final InsertRowStatement statement = new InsertRowStatement();
    statement.setDevicePath(node.getTargetPath());
    statement.setTime(node.getTime());
    statement.setMeasurements(node.getMeasurements());
    statement.setDataTypes(node.getDataTypes());
    statement.setValues(node.getValues());
    statement.setNeedInferType(node.isNeedInferType());
    statement.setAligned(node.isAligned());
    statement.setMeasurementSchemas(node.getMeasurementSchemas());
    return statement;
  }

  @Override
  public Statement visitRelationalInsertTablet(RelationalInsertTabletNode node, Void context) {
    return new InsertTabletStatement(node);
  }

  @Override
  public InsertTabletStatement visitInsertTablet(final InsertTabletNode node, final Void context) {
    return new InsertTabletStatement(node);
  }

  @Override
  public InsertRowsStatement visitInsertRows(final InsertRowsNode node, final Void context) {
    final InsertRowsStatement statement = new InsertRowsStatement();
    statement.setInsertRowStatementList(
        node.getInsertRowNodeList().stream()
            .map(insertRowNode -> visitInsertRow(insertRowNode, context))
            .collect(Collectors.toList()));
    return statement;
  }

  @Override
  public CreateTimeSeriesStatement visitCreateTimeSeries(
      final CreateTimeSeriesNode node, final Void context) {
    final CreateTimeSeriesStatement statement = new CreateTimeSeriesStatement();
    statement.setPath(node.getPath());
    statement.setDataType(node.getDataType());
    statement.setEncoding(node.getEncoding());
    statement.setCompressor(node.getCompressor());
    statement.setProps(node.getProps());
    statement.setAttributes(node.getAttributes());
    statement.setAlias(node.getAlias());
    statement.setTags(node.getTags());
    return statement;
  }

  @Override
  public CreateAlignedTimeSeriesStatement visitCreateAlignedTimeSeries(
      final CreateAlignedTimeSeriesNode node, final Void context) {
    final CreateAlignedTimeSeriesStatement statement = new CreateAlignedTimeSeriesStatement();
    statement.setDataTypes(node.getDataTypes());
    statement.setCompressors(node.getCompressors());
    statement.setEncodings(node.getEncodings());
    statement.setAttributesList(node.getAttributesList());
    statement.setAliasList(node.getAliasList());
    statement.setDevicePath(node.getDevicePath());
    statement.setMeasurements(node.getMeasurements());
    statement.setTagsList(node.getTagsList());
    return statement;
  }

  @Override
  public CreateMultiTimeSeriesStatement visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesNode node, final Void context) {
    final CreateMultiTimeSeriesStatement statement = new CreateMultiTimeSeriesStatement();

    final List<MeasurementPath> paths = new ArrayList<>();
    final List<TSDataType> dataTypes = new ArrayList<>();
    final List<TSEncoding> encodings = new ArrayList<>();
    final List<CompressionType> compressors = new ArrayList<>();
    final List<Map<String, String>> propsList = new ArrayList<>();
    final List<String> aliasList = new ArrayList<>();
    final List<Map<String, String>> tagsList = new ArrayList<>();
    final List<Map<String, String>> attributesList = new ArrayList<>();

    for (Map.Entry<PartialPath, MeasurementGroup> path2Group :
        node.getMeasurementGroupMap().entrySet()) {
      MeasurementGroup group = path2Group.getValue();
      dataTypes.addAll(
          Objects.nonNull(group.getDataTypes()) ? group.getDataTypes() : new ArrayList<>());
      encodings.addAll(
          Objects.nonNull(group.getEncodings()) ? group.getEncodings() : new ArrayList<>());
      compressors.addAll(
          Objects.nonNull(group.getCompressors()) ? group.getCompressors() : new ArrayList<>());
      propsList.addAll(
          Objects.nonNull(group.getPropsList()) ? group.getPropsList() : new ArrayList<>());
      aliasList.addAll(
          Objects.nonNull(group.getAliasList()) ? group.getAliasList() : new ArrayList<>());
      tagsList.addAll(
          Objects.nonNull(group.getTagsList()) ? group.getTagsList() : new ArrayList<>());
      attributesList.addAll(
          Objects.nonNull(group.getAttributesList())
              ? group.getAttributesList()
              : new ArrayList<>());
      if (Objects.nonNull(group.getMeasurements())) {
        for (int i = 0; i < group.getMeasurements().size(); ++i) {
          paths.add(path2Group.getKey().concatAsMeasurementPath(group.getMeasurements().get(i)));
        }
      }
    }

    statement.setPaths(paths);
    statement.setDataTypes(dataTypes);
    statement.setEncodings(encodings);
    statement.setCompressors(compressors);
    statement.setPropsList(propsList.isEmpty() ? null : propsList);
    statement.setAliasList(aliasList.isEmpty() ? null : aliasList);
    statement.setTagsList(tagsList.isEmpty() ? null : tagsList);
    statement.setAttributesList(attributesList.isEmpty() ? null : attributesList);
    return statement;
  }

  @Override
  public AlterTimeSeriesStatement visitAlterTimeSeries(
      final AlterTimeSeriesNode node, final Void context) {
    final AlterTimeSeriesStatement statement = new AlterTimeSeriesStatement();
    statement.setAlterMap(node.getAlterMap());
    statement.setAlterType(node.getAlterType());
    statement.setAttributesMap(node.getAttributesMap());
    statement.setAlias(node.getAlias());
    statement.setTagsMap(node.getTagsMap());
    statement.setPath(node.getPath());
    return statement;
  }

  @Override
  public InternalCreateTimeSeriesStatement visitInternalCreateTimeSeries(
      final InternalCreateTimeSeriesNode node, final Void context) {
    return new InternalCreateTimeSeriesStatement(
        node.getDevicePath(),
        node.getMeasurementGroup().getMeasurements(),
        node.getMeasurementGroup().getDataTypes(),
        node.getMeasurementGroup().getEncodings(),
        node.getMeasurementGroup().getCompressors(),
        node.isAligned());
  }

  @Override
  public ActivateTemplateStatement visitActivateTemplate(
      final ActivateTemplateNode node, final Void context) {
    final ActivateTemplateStatement statement = new ActivateTemplateStatement();
    statement.setPath(node.getActivatePath());
    return statement;
  }

  @Override
  public BatchActivateTemplateStatement visitInternalBatchActivateTemplate(
      final InternalBatchActivateTemplateNode node, final Void context) {
    return new BatchActivateTemplateStatement(
        new ArrayList<>(node.getTemplateActivationMap().keySet()));
  }

  @Override
  public InternalCreateMultiTimeSeriesStatement visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesNode node, final Void context) {
    return new InternalCreateMultiTimeSeriesStatement(node.getDeviceMap());
  }

  @Override
  public BatchActivateTemplateStatement visitBatchActivateTemplate(
      final BatchActivateTemplateNode node, final Void context) {
    return new BatchActivateTemplateStatement(
        new ArrayList<>(node.getTemplateActivationMap().keySet()));
  }

  @Override
  public CreateLogicalViewStatement visitCreateLogicalView(
      final CreateLogicalViewNode node, final Void context) {
    final CreateLogicalViewStatement statement = new CreateLogicalViewStatement();
    statement.setTargetFullPaths(node.getViewPathList());
    statement.setViewExpressions(new ArrayList<>(node.getViewPathToSourceExpressionMap().values()));
    return statement;
  }

  // We do not support AlterLogicalViewNode parsing and use direct rpc instead

  @Override
  public DeleteDataStatement visitDeleteData(final DeleteDataNode node, final Void context) {
    final DeleteDataStatement statement = new DeleteDataStatement();
    statement.setDeleteEndTime(node.getDeleteEndTime());
    statement.setDeleteStartTime(node.getDeleteStartTime());
    statement.setPathList(node.getPathList());
    return statement;
  }
}
