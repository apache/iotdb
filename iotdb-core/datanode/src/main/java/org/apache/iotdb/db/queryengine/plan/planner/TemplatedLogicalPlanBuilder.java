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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.DEVICE;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices set in only one template, using template can avoid many
 * unnecessary judgements.
 */
public class TemplatedLogicalPlanBuilder extends LogicalPlanBuilder {

  private final MPPQueryContext context;

  private final Analysis analysis;

  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;

  public TemplatedLogicalPlanBuilder(
      Analysis analysis,
      MPPQueryContext context,
      List<String> measurementList,
      List<IMeasurementSchema> schemaList) {
    super(analysis, context);
    this.analysis = analysis;
    this.context = context;
    this.measurementList = measurementList;
    this.schemaList = schemaList;
  }

  public TemplatedLogicalPlanBuilder planRawDataSource(
      PartialPath devicePath,
      Ordering scanOrder,
      long offset,
      long limit,
      boolean lastLevelUseWildcard) {
    List<PlanNode> sourceNodeList = new ArrayList<>();

    if (analysis.getDeviceTemplate().isDirectAligned()) {
      AlignedPath path = new AlignedPath(devicePath);
      path.setMeasurementList(measurementList);
      path.addSchemas(schemaList);

      // if value filter push down is implemented,
      // the serializeUseTemplate and deserializeUseTemplate method of AlignedSeriesScanNode also
      // need adapt
      AlignedSeriesScanNode alignedSeriesScanNode =
          new AlignedSeriesScanNode(
              context.getQueryId().genPlanNodeId(),
              path,
              scanOrder,
              limit,
              offset,
              null,
              lastLevelUseWildcard);
      sourceNodeList.add(alignedSeriesScanNode);
    } else {
      for (int i = 0; i < measurementList.size(); i++) {
        MeasurementPath measurementPath =
            new MeasurementPath(devicePath.concatNode(measurementList.get(i)), schemaList.get(i));
        SeriesScanNode seriesScanNode =
            new SeriesScanNode(
                context.getQueryId().genPlanNodeId(),
                measurementPath,
                scanOrder,
                limit,
                offset,
                null);
        sourceNodeList.add(seriesScanNode);
      }
    }

    this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);
    return this;
  }

  public TemplatedLogicalPlanBuilder planFilter(
      Expression filterExpression, boolean isGroupByTime, Ordering scanOrder) {

    if (filterExpression == null) {
      return this;
    }

    FilterNode filterNode =
        new FilterNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            null,
            filterExpression,
            isGroupByTime,
            scanOrder,
            true);
    analysis.setFromWhere(filterNode);

    this.root = filterNode;

    return this;
  }

  // ===================== Methods below are used for aggregation =============================

  public TemplatedLogicalPlanBuilder planRawDataAggregation(
      Set<Expression> aggregationExpressions,
      Expression groupByExpression,
      GroupByTimeParameter groupByTimeParameter,
      GroupByParameter groupByParameter,
      boolean outputEndTime,
      Ordering scanOrder,
      List<AggregationDescriptor> deduplicatedAggregationDescriptorList) {
    if (aggregationExpressions == null) {
      return this;
    }

    this.root =
        new RawDataAggregationNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            deduplicatedAggregationDescriptorList,
            groupByTimeParameter,
            groupByParameter,
            groupByExpression,
            outputEndTime,
            scanOrder,
            true);
    return this;
  }

  public TemplatedLogicalPlanBuilder planSlidingWindowAggregation(
      QueryStatement queryStatement,
      Set<Expression> aggregationExpressions,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    if (!queryStatement.isGroupByTime() || !analysis.getGroupByTimeParameter().hasOverlap()) {
      return this;
    }

    LinkedHashSet<Expression> slidingWindowsExpressions = new LinkedHashSet<>();
    aggregationExpressions.forEach(
        expression -> {
          if (!DEVICE.equalsIgnoreCase(expression.getOutputSymbol())) {
            slidingWindowsExpressions.add(expression);
          }
        });
    this.root =
        createSlidingWindowAggregationNode(
            this.getRoot(),
            slidingWindowsExpressions,
            groupByTimeParameter,
            AggregationStep.FINAL,
            scanOrder);

    return this;
  }

  @Override
  public TemplatedLogicalPlanBuilder withNewRoot(PlanNode newRoot) {
    this.root = newRoot;
    return this;
  }
}
