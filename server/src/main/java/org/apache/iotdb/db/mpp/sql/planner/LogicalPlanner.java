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

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AuthorNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.*;
import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.FillQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    PlanNode rootNode = new LogicalPlanVisitor(analysis).process(analysis.getStatement(), context);

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
      QueryPlanBuilder planBuilder = new QueryPlanBuilder(context);

      planBuilder.planRawDataQuerySource(
          queryStatement.getDeviceNameToPathsMap(),
          queryStatement.getResultOrder(),
          queryStatement.isAlignByDevice(),
          analysis.getQueryFilter(),
          queryStatement.getSelectedPathNames());

      planBuilder.planFilterNull(queryStatement.getFilterNullComponent());
      planBuilder.planLimit(queryStatement.getRowLimit());
      planBuilder.planOffset(queryStatement.getRowOffset());
      return planBuilder.getRoot();
    }

    @Override
    public PlanNode visitAggregationQuery(
        AggregationQueryStatement queryStatement, MPPQueryContext context) {
      QueryPlanBuilder planBuilder = new QueryPlanBuilder(context);
      Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap;

      if (analysis.getQueryFilter() != null
          && analysis.getQueryFilter().getType() != ExpressionType.GLOBAL_TIME) {
        // with value filter
        planBuilder.planAggregationSourceWithValueFilter(
            queryStatement.getDeviceNameToAggregationsMap(),
            queryStatement.getDeviceNameToPathsMap(),
            queryStatement.getResultOrder(),
            queryStatement.isAlignByDevice(),
            analysis.getQueryFilter(),
            queryStatement.getSelectedPathNames());
      } else {
        // without value filter
        planBuilder.planAggregationSourceWithoutValueFilter(
            queryStatement.getDeviceNameToAggregationsMap(),
            queryStatement.getResultOrder(),
            queryStatement.isAlignByDevice(),
            analysis.getQueryFilter());
      }

      planBuilder.planGroupByLevel(queryStatement.getGroupByLevelComponent());
      planBuilder.planFilterNull(queryStatement.getFilterNullComponent());
      planBuilder.planLimit(queryStatement.getRowLimit());
      planBuilder.planOffset(queryStatement.getRowOffset());
      return planBuilder.getRoot();
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
    public PlanNode visitFillQuery(FillQueryStatement queryStatement, MPPQueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PlanNode visitLastQuery(LastQueryStatement queryStatement, MPPQueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PlanNode visitUDTFQuery(UDTFQueryStatement queryStatement, MPPQueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PlanNode visitUDAFQuery(UDAFQueryStatement queryStatement, MPPQueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PlanNode visitCreateTimeseries(
        CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
      return new CreateTimeSeriesNode(
          context.getQueryId().genPlanNodeId(),
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
          context.getQueryId().genPlanNodeId(),
          createAlignedTimeSeriesStatement.getDevicePath(),
          createAlignedTimeSeriesStatement.getMeasurements(),
          createAlignedTimeSeriesStatement.getDataTypes(),
          createAlignedTimeSeriesStatement.getEncodings(),
          createAlignedTimeSeriesStatement.getCompressors(),
          createAlignedTimeSeriesStatement.getAliasList(),
          createAlignedTimeSeriesStatement.getTagsList(),
          createAlignedTimeSeriesStatement.getAttributesList());
    }

    @Override
    public PlanNode visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      return new AlterTimeSeriesNode(
          context.getQueryId().genPlanNodeId(),
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
          context.getQueryId().genPlanNodeId(),
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
          context.getQueryId().genPlanNodeId(),
          insertRowStatement.getDevicePath(),
          insertRowStatement.isAligned(),
          measurementSchemas.toArray(new MeasurementSchema[0]),
          insertRowStatement.getDataTypes(),
          insertRowStatement.getTime(),
          insertRowStatement.getValues());
    }

    @Override
    public PlanNode visitCreateUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitCreateRole(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitAlterUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitGrantUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitGrantRole(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitGrantRoleToUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitRevokeUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitRevokeRole(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitRevokeRoleFromUser(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitDropUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitDropRole(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListUser(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListRole(AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListPrivilegesUser(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListPrivilegesRole(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListUserPrivileges(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListRolePrivileges(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListAllRoleOfUser(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    @Override
    public PlanNode visitListAllUserOfRole(
        AuthorStatement authorStatement, MPPQueryContext context) {
      return getNewAuthorNode(authorStatement, context);
    }

    public AuthorNode getNewAuthorNode(AuthorStatement authorStatement, MPPQueryContext context) {
      try {
        return new AuthorNode(
            context.getQueryId().genPlanNodeId(),
            authorStatement.getAuthorType(),
            authorStatement.getUserName(),
            authorStatement.getRoleName(),
            authorStatement.getPassWord(),
            authorStatement.getNewPassword(),
            authorStatement.getPrivilegeList(),
            authorStatement.getNodeName());
      } catch (AuthException e) {
        return null;
      }
    }

    @Override
    public PlanNode visitInsertRows(
        InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
      // set schema in insert node
      // convert insert statement to insert node
      InsertRowsNode insertRowsNode = new InsertRowsNode(context.getQueryId().genPlanNodeId());
      for (int i = 0; i < insertRowsStatement.getInsertRowStatementList().size(); i++) {
        InsertRowStatement insertRowStatement =
            insertRowsStatement.getInsertRowStatementList().get(i);
        List<MeasurementSchema> measurementSchemas =
            analysis
                .getSchemaTree()
                .searchMeasurementSchema(
                    insertRowStatement.getDevicePath(),
                    Arrays.asList(insertRowStatement.getMeasurements()));
        insertRowsNode.addOneInsertRowNode(
            new InsertRowNode(
                insertRowsNode.getPlanNodeId(),
                insertRowStatement.getDevicePath(),
                insertRowStatement.isAligned(),
                measurementSchemas.toArray(new MeasurementSchema[0]),
                insertRowStatement.getDataTypes(),
                insertRowStatement.getTime(),
                insertRowStatement.getValues()),
            i);
      }
      return insertRowsNode;
    }

    @Override
    public PlanNode visitInsertMultiTablets(
        InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
      // set schema in insert node
      // convert insert statement to insert node
      InsertMultiTabletsNode insertMultiTabletsNode =
          new InsertMultiTabletsNode(context.getQueryId().genPlanNodeId());
      List<InsertTabletNode> insertTabletNodeList = new ArrayList<>();
      for (int i = 0; i < insertMultiTabletsStatement.getInsertTabletStatementList().size(); i++) {
        InsertTabletStatement insertTabletStatement =
            insertMultiTabletsStatement.getInsertTabletStatementList().get(i);
        List<MeasurementSchema> measurementSchemas =
            analysis
                .getSchemaTree()
                .searchMeasurementSchema(
                    insertTabletStatement.getDevicePath(),
                    Arrays.asList(insertTabletStatement.getMeasurements()));
        insertTabletNodeList.add(
            new InsertTabletNode(
                insertMultiTabletsNode.getPlanNodeId(),
                insertTabletStatement.getDevicePath(),
                insertTabletStatement.isAligned(),
                measurementSchemas.toArray(new MeasurementSchema[0]),
                insertTabletStatement.getDataTypes(),
                insertTabletStatement.getTimes(),
                insertTabletStatement.getBitMaps(),
                insertTabletStatement.getColumns(),
                insertTabletStatement.getRowCount()));
      }
      insertMultiTabletsNode.setInsertTabletNodeList(insertTabletNodeList);
      return insertMultiTabletsNode;
    }

    @Override
    public PlanNode visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
      // set schema in insert node
      // convert insert statement to insert node
      InsertRowsNode insertRowsNode = new InsertRowsNode(context.getQueryId().genPlanNodeId());
      for (int i = 0; i < insertRowsOfOneDeviceStatement.getInsertRowStatementList().size(); i++) {
        InsertRowStatement insertRowStatement =
            insertRowsOfOneDeviceStatement.getInsertRowStatementList().get(i);
        List<MeasurementSchema> measurementSchemas =
            analysis
                .getSchemaTree()
                .searchMeasurementSchema(
                    insertRowStatement.getDevicePath(),
                    Arrays.asList(insertRowStatement.getMeasurements()));
        insertRowsNode.addOneInsertRowNode(
            new InsertRowNode(
                insertRowsNode.getPlanNodeId(),
                insertRowStatement.getDevicePath(),
                insertRowStatement.isAligned(),
                measurementSchemas.toArray(new MeasurementSchema[0]),
                insertRowStatement.getDataTypes(),
                insertRowStatement.getTime(),
                insertRowStatement.getValues()),
            i);
      }
      return insertRowsNode;
    }
  }
}
