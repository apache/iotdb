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
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.*;
import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.*;

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
  private static class LogicalPlanVisitor extends StatementVisitor<PlanNode, MPPQueryContext> {

    private final Analysis analysis;

    public LogicalPlanVisitor(Analysis analysis) {
      this.analysis = analysis;
    }

    @Override
    public PlanNode visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      QueryPlanBuilder planBuilder = new QueryPlanBuilder();
      planBuilder.planRawDataQuerySource(
          analysis.getDeviceIdToPathsMap(),
          queryStatement.getResultOrder(),
          queryStatement.isAlignByDevice());

      if (queryStatement.getWhereCondition() != null) {
        planBuilder.planQueryFilter(
            queryStatement.getWhereCondition().getQueryFilter(), analysis.getOutputColumnNames());
      }

      planBuilder.planFilterNull(queryStatement.getFilterNullComponent());
      planBuilder.planSort(queryStatement.getResultOrder());
      planBuilder.planLimit(queryStatement.getRowLimit());
      planBuilder.planOffset(queryStatement.getRowOffset());
      return planBuilder.getRoot();
    }

    @Override
    public PlanNode visitAggregationQuery(
        AggregationQueryStatement queryStatement, MPPQueryContext context) {
      return null;
    }

    @Override
    public PlanNode visitFillQuery(FillQueryStatement queryStatement, MPPQueryContext context) {
      return null;
    }

    @Override
    public PlanNode visitGroupByQuery(
        GroupByQueryStatement queryStatement, MPPQueryContext context) {
      return null;
    }

    @Override
    public PlanNode visitGroupByFillQuery(
        GroupByFillQueryStatement queryStatement, MPPQueryContext context) {
      return null;
    }

    @Override
    public PlanNode visitLastQuery(LastQueryStatement queryStatement, MPPQueryContext context) {
      return null;
    }

    @Override
    public PlanNode visitUDTFQuery(UDTFQueryStatement queryStatement, MPPQueryContext context) {
      return null;
    }

    @Override
    public PlanNode visitUDAFQuery(UDAFQueryStatement queryStatement, MPPQueryContext context) {
      return null;
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
}
