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
package org.apache.iotdb.db.mpp.sql.planner;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.*;
import org.apache.iotdb.db.mpp.sql.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.FillQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.*;
import java.util.stream.Collectors;

/** Generate a logical plan for the statement. */
public class LogicalPlanner {

  private final MPPQueryContext context;
  private final List<PlanOptimizer> optimizers;

  public LogicalPlanner(MPPQueryContext context, List<PlanOptimizer> optimizers) {
    this.context = context;
    this.optimizers = optimizers;
  }

  public LogicalQueryPlan plan(Analysis analysis) {
    PlanNode rootNode = new LogicalPlanVisitor(analysis).process(analysis.getStatement());

    // optimize the query logical plan
    if (analysis.getStatement() instanceof QueryStatement) {
      for (PlanOptimizer optimizer : optimizers) {
        rootNode = optimizer.optimize(rootNode, context);
      }
    }

    return new LogicalQueryPlan(context, rootNode);
  }

  /**
   * This visitor is used to generate a logical plan for the statement and returns the {@link
   * PlanNode}.
   */
  private class LogicalPlanVisitor extends StatementVisitor<PlanNode, MPPQueryContext> {

    private final Analysis analysis;

    public LogicalPlanVisitor(Analysis analysis) {
      this.analysis = analysis;
    }

    @Override
    public PlanNode visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      PlanBuilder planBuilder = planSelectComponent(queryStatement);

      if (queryStatement.getWhereCondition() != null) {
        planBuilder =
            planQueryFilter(planBuilder, queryStatement.getWhereCondition().getQueryFilter());
      }

      if (queryStatement.isGroupByLevel()) {
        planBuilder =
            planGroupByLevel(
                planBuilder,
                ((AggregationQueryStatement) queryStatement).getGroupByLevelComponent());
      }

      if (queryStatement instanceof FillQueryStatement) {
        planBuilder =
            planFill(planBuilder, ((FillQueryStatement) queryStatement).getFillComponent());
      }

      planBuilder = planFilterNull(planBuilder, queryStatement.getFilterNullComponent());
      planBuilder = planSort(planBuilder, queryStatement.getResultOrder());
      planBuilder = planLimit(planBuilder, queryStatement.getRowLimit());
      planBuilder = planOffset(planBuilder, queryStatement.getRowOffset());
      return planBuilder.getRoot();
    }

    private PlanBuilder planSelectComponent(QueryStatement queryStatement) {
      // TODO: generate SourceNode for QueryFilter
      Map<String, Set<SourceNode>> deviceNameToSourceNodesMap = new HashMap<>();

      for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
        Set<SourceNode> sourceNodes = planResultColumn(resultColumn);
        for (SourceNode sourceNode : sourceNodes) {
          String deviceName = sourceNode.getDeviceName();
          deviceNameToSourceNodesMap
              .computeIfAbsent(deviceName, k -> new HashSet<>())
              .add(sourceNode);
        }
      }

      if (queryStatement.isAlignByDevice()) {
        DeviceMergeNode deviceMergeNode = new DeviceMergeNode(PlanNodeIdAllocator.generateId());
        for (Map.Entry<String, Set<SourceNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
          String deviceName = entry.getKey();
          List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
          if (planNodes.size() == 1) {
            deviceMergeNode.addChildDeviceNode(deviceName, planNodes.get(0));
          } else {
            TimeJoinNode timeJoinNode =
                new TimeJoinNode(
                    PlanNodeIdAllocator.generateId(),
                    queryStatement.getResultOrder(),
                    null,
                    planNodes);
            deviceMergeNode.addChildDeviceNode(deviceName, timeJoinNode);
          }
        }
        return new PlanBuilder(deviceMergeNode);
      }

      List<PlanNode> planNodes =
          deviceNameToSourceNodesMap.entrySet().stream()
              .flatMap(entry -> entry.getValue().stream())
              .collect(Collectors.toList());
      TimeJoinNode timeJoinNode =
          new TimeJoinNode(
              PlanNodeIdAllocator.generateId(), queryStatement.getResultOrder(), null, planNodes);
      return new PlanBuilder(timeJoinNode);
    }

    private Set<SourceNode> planResultColumn(ResultColumn resultColumn) {
      Set<SourceNode> resultSourceNodeSet = new HashSet<>();
      resultColumn.getExpression().collectPlanNode(resultSourceNodeSet);
      return resultSourceNodeSet;
    }

    private PlanBuilder planQueryFilter(PlanBuilder planBuilder, QueryFilter queryFilter) {
      if (queryFilter == null) {
        return planBuilder;
      }

      return planBuilder.withNewRoot(
          new FilterNode(PlanNodeIdAllocator.generateId(), planBuilder.getRoot(), queryFilter));
    }

    private PlanBuilder planGroupByLevel(
        PlanBuilder planBuilder, GroupByLevelComponent groupByLevelComponent) {
      if (groupByLevelComponent == null) {
        return planBuilder;
      }

      return planBuilder.withNewRoot(
          new GroupByLevelNode(
              PlanNodeIdAllocator.generateId(),
              planBuilder.getRoot(),
              groupByLevelComponent.getLevels(),
              groupByLevelComponent.getGroupedPathMap()));
    }

    private PlanBuilder planFill(PlanBuilder planBuilder, FillComponent fillComponent) {
      // TODO: support Fill
      return planBuilder;
    }

    private PlanBuilder planFilterNull(
        PlanBuilder planBuilder, FilterNullComponent filterNullComponent) {
      if (filterNullComponent == null) {
        return planBuilder;
      }

      return planBuilder.withNewRoot(
          new FilterNullNode(
              PlanNodeIdAllocator.generateId(),
              planBuilder.getRoot(),
              filterNullComponent.getWithoutPolicyType(),
              filterNullComponent.getWithoutNullColumns().stream()
                  .map(Expression::getExpressionString)
                  .collect(Collectors.toList())));
    }

    private PlanBuilder planSort(PlanBuilder planBuilder, OrderBy resultOrder) {
      if (resultOrder == null || resultOrder == OrderBy.TIMESTAMP_ASC) {
        return planBuilder;
      }

      return planBuilder.withNewRoot(
          new SortNode(PlanNodeIdAllocator.generateId(), planBuilder.getRoot(), null, resultOrder));
    }

    private PlanBuilder planLimit(PlanBuilder planBuilder, int rowLimit) {
      if (rowLimit == 0) {
        return planBuilder;
      }

      return planBuilder.withNewRoot(
          new LimitNode(PlanNodeIdAllocator.generateId(), rowLimit, planBuilder.getRoot()));
    }

    private PlanBuilder planOffset(PlanBuilder planBuilder, int rowOffset) {
      if (rowOffset == 0) {
        return planBuilder;
      }

      return planBuilder.withNewRoot(
          new OffsetNode(PlanNodeIdAllocator.generateId(), planBuilder.getRoot(), rowOffset));
    }

    @Override
    public PlanNode visitCreateTimeseries(
        CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
      return new CreateTimeSeriesNode(
          PlanNodeIdAllocator.generateId(),
          createTimeSeriesStatement.getPath(),
          createTimeSeriesStatement.getDataType(),
          createTimeSeriesStatement.getEncoding(),
          createTimeSeriesStatement.getCompressor(),
          createTimeSeriesStatement.getProps(),
          createTimeSeriesStatement.getTags(),
          createTimeSeriesStatement.getAttributes(),
          createTimeSeriesStatement.getAlias());
    }

    @Override
    public PlanNode visitCreateAlignedTimeseries(
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement,
        MPPQueryContext context) {
      return new CreateAlignedTimeSeriesNode(
          PlanNodeIdAllocator.generateId(),
          createAlignedTimeSeriesStatement.getDevicePath(),
          createAlignedTimeSeriesStatement.getMeasurements(),
          createAlignedTimeSeriesStatement.getDataTypes(),
          createAlignedTimeSeriesStatement.getEncodings(),
          createAlignedTimeSeriesStatement.getCompressors(),
          createAlignedTimeSeriesStatement.getAliasList(),
          createAlignedTimeSeriesStatement.getTagsList(),
          createAlignedTimeSeriesStatement.getTagOffsets(),
          createAlignedTimeSeriesStatement.getAttributesList());
    }

    @Override
    public PlanNode visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      return new AlterTimeSeriesNode(
          PlanNodeIdAllocator.generateId(),
          alterTimeSeriesStatement.getPath(),
          alterTimeSeriesStatement.getAlterType(),
          alterTimeSeriesStatement.getAlterMap(),
          alterTimeSeriesStatement.getAlias(),
          alterTimeSeriesStatement.getTagsMap(),
          alterTimeSeriesStatement.getAttributesMap());
    }

    @Override
    public PlanNode visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      // set schema in insert node
      // convert insert statement to insert node
      List<MeasurementSchema> measurementSchemas =
          analysis
              .getSchemaTree()
              .searchMeasurementSchema(
                  insertTabletStatement.getDevicePath(),
                  Arrays.asList(insertTabletStatement.getMeasurements()));
      return new InsertTabletNode(
          PlanNodeIdAllocator.generateId(),
          insertTabletStatement.getDevicePath(),
          insertTabletStatement.isAligned(),
          measurementSchemas.toArray(new MeasurementSchema[0]),
          insertTabletStatement.getDataTypes(),
          insertTabletStatement.getTimes(),
          insertTabletStatement.getBitMaps(),
          insertTabletStatement.getColumns(),
          insertTabletStatement.getRowCount());
    }

    @Override
    public PlanNode visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
      // set schema in insert node
      // convert insert statement to insert node
      List<MeasurementSchema> measurementSchemas =
          analysis
              .getSchemaTree()
              .searchMeasurementSchema(
                  insertRowStatement.getDevicePath(),
                  Arrays.asList(insertRowStatement.getMeasurements()));
      return new InsertRowNode(
          PlanNodeIdAllocator.generateId(),
          insertRowStatement.getDevicePath(),
          insertRowStatement.isAligned(),
          measurementSchemas.toArray(new MeasurementSchema[0]),
          insertRowStatement.getDataTypes(),
          insertRowStatement.getTime(),
          insertRowStatement.getValues());
    }
  }

  private class PlanBuilder {

    private PlanNode root;

    public PlanBuilder(PlanNode root) {
      this.root = root;
    }

    public PlanNode getRoot() {
      return root;
    }

    public PlanBuilder withNewRoot(PlanNode newRoot) {
      return new PlanBuilder(newRoot);
    }
  }
}
