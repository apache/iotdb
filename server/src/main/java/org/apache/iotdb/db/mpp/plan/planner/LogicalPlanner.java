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
package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.metadata.utils.TimeseriesVersionUtil;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InvalidateSchemaCacheNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InvalidateSchemaCacheStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.LastPointFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesByDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

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
    public PlanNode visitNode(StatementNode node, MPPQueryContext context) {
      throw new UnsupportedOperationException(
          "Unsupported statement type: " + node.getClass().getName());
    }

    @Override
    public PlanNode visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);

      if (queryStatement.isLastQuery()) {
        return planBuilder
            .planLast(analysis.getSourceExpressions(), analysis.getGlobalTimeFilter())
            .getRoot();
      }

      if (queryStatement.isAlignByDevice()) {
        Map<String, PlanNode> deviceToSubPlanMap = new HashMap<>();
        for (String deviceName : analysis.getDeviceToSourceExpressions().keySet()) {
          LogicalPlanBuilder subPlanBuilder = new LogicalPlanBuilder(context);
          subPlanBuilder =
              subPlanBuilder.withNewRoot(
                  visitQueryBody(
                      queryStatement,
                      analysis.getDeviceToIsRawDataSource().get(deviceName),
                      analysis.getDeviceToSourceExpressions().get(deviceName),
                      analysis.getDeviceToAggregationExpressions().get(deviceName),
                      analysis.getDeviceToAggregationTransformExpressions().get(deviceName),
                      analysis.getDeviceToTransformExpressions().get(deviceName),
                      analysis.getDeviceToQueryFilter() != null
                          ? analysis.getDeviceToQueryFilter().get(deviceName)
                          : null,
                      context));
          deviceToSubPlanMap.put(deviceName, subPlanBuilder.getRoot());
        }
        // convert to ALIGN BY DEVICE view
        planBuilder =
            planBuilder.planDeviceView(
                deviceToSubPlanMap,
                analysis.getRespDatasetHeader().getRespColumns().stream()
                    .distinct()
                    .collect(Collectors.toList()),
                analysis.getDeviceToMeasurementIndexesMap(),
                queryStatement.getResultOrder());
      } else {
        planBuilder =
            planBuilder.withNewRoot(
                visitQueryBody(
                    queryStatement,
                    analysis.isRawDataSource(),
                    analysis.getSourceExpressions(),
                    analysis.getAggregationExpressions(),
                    analysis.getAggregationTransformExpressions(),
                    analysis.getTransformExpressions(),
                    analysis.getQueryFilter(),
                    context));
      }

      // other common upstream node
      planBuilder =
          planBuilder
              .planFilterNull(analysis.getFilterNullParameter())
              .planFill(analysis.getFillDescriptor())
              .planOffset(queryStatement.getRowOffset())
              .planLimit(queryStatement.getRowLimit());

      return planBuilder.getRoot();
    }

    public PlanNode visitQueryBody(
        QueryStatement queryStatement,
        boolean isRawDataSource,
        Set<Expression> sourceExpressions,
        Set<Expression> aggregationExpressions,
        Set<Expression> aggregationTransformExpressions,
        Set<Expression> transformExpressions,
        Expression queryFilter,
        MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);

      // plan data source node
      if (isRawDataSource) {
        planBuilder =
            planBuilder.planRawDataSource(
                sourceExpressions, queryStatement.getResultOrder(), analysis.getGlobalTimeFilter());

        if (queryStatement.isAggregationQuery()) {
          if (analysis.hasValueFilter()) {
            planBuilder =
                planBuilder.planFilterAndTransform(
                    queryFilter,
                    aggregationTransformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId());
          } else {
            planBuilder =
                planBuilder.planTransform(
                    aggregationTransformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId());
          }

          boolean outputPartial =
              queryStatement.isGroupByLevel()
                  || (queryStatement.isGroupByTime()
                      && analysis.getGroupByTimeParameter().hasOverlap());
          AggregationStep curStep =
              outputPartial ? AggregationStep.PARTIAL : AggregationStep.SINGLE;
          planBuilder =
              planBuilder.planAggregation(
                  aggregationExpressions,
                  analysis.getGroupByTimeParameter(),
                  curStep,
                  analysis.getTypeProvider(),
                  queryStatement.getResultOrder());

          if (curStep.isOutputPartial()) {
            if (queryStatement.isGroupByTime() && analysis.getGroupByTimeParameter().hasOverlap()) {
              curStep =
                  queryStatement.isGroupByLevel()
                      ? AggregationStep.INTERMEDIATE
                      : AggregationStep.FINAL;
              planBuilder =
                  planBuilder.planSlidingWindowAggregation(
                      aggregationExpressions,
                      analysis.getGroupByTimeParameter(),
                      curStep,
                      queryStatement.getResultOrder());
            }

            if (queryStatement.isGroupByLevel()) {
              curStep = AggregationStep.FINAL;
              planBuilder =
                  planBuilder.planGroupByLevel(
                      analysis.getGroupByLevelExpressions(),
                      curStep,
                      analysis.getGroupByTimeParameter(),
                      queryStatement.getResultOrder());
            }
          }

          planBuilder =
              planBuilder.planTransform(
                  transformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId());
        } else {
          if (analysis.hasValueFilter()) {
            planBuilder =
                planBuilder.planFilterAndTransform(
                    queryFilter,
                    transformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId());
          } else {
            planBuilder =
                planBuilder.planTransform(
                    transformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId());
          }
        }
      } else {
        planBuilder =
            planBuilder.planAggregationSource(
                sourceExpressions,
                queryStatement.getResultOrder(),
                analysis.getGlobalTimeFilter(),
                analysis.getGroupByTimeParameter(),
                aggregationExpressions,
                analysis.getGroupByLevelExpressions(),
                analysis.getTypeProvider());
      }

      return planBuilder.getRoot();
    }

    public PlanNode visitLastPointFetch(
        LastPointFetchStatement lastPointFetchStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder.planLast(analysis.getSourceExpressions(), null).getRoot();
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
          createTimeSeriesStatement.getAlias(),
          TimeseriesVersionUtil.generateVersion());
    }

    @Override
    public PlanNode visitCreateAlignedTimeseries(
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement,
        MPPQueryContext context) {
      int size = createAlignedTimeSeriesStatement.getMeasurements().size();
      List<String> versionList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        versionList.add(TimeseriesVersionUtil.generateVersion());
      }
      return new CreateAlignedTimeSeriesNode(
          context.getQueryId().genPlanNodeId(),
          createAlignedTimeSeriesStatement.getDevicePath(),
          createAlignedTimeSeriesStatement.getMeasurements(),
          createAlignedTimeSeriesStatement.getDataTypes(),
          createAlignedTimeSeriesStatement.getEncodings(),
          createAlignedTimeSeriesStatement.getCompressors(),
          createAlignedTimeSeriesStatement.getAliasList(),
          createAlignedTimeSeriesStatement.getTagsList(),
          createAlignedTimeSeriesStatement.getAttributesList(),
          versionList);
    }

    @Override
    public PlanNode visitCreateTimeseriesByDevice(
        CreateTimeSeriesByDeviceStatement createTimeSeriesByDeviceStatement,
        MPPQueryContext context) {
      int size = createTimeSeriesByDeviceStatement.getMeasurements().size();

      MeasurementGroup measurementGroup = new MeasurementGroup();
      for (int i = 0; i < size; i++) {
        measurementGroup.addMeasurement(
            createTimeSeriesByDeviceStatement.getMeasurements().get(i),
            createTimeSeriesByDeviceStatement.getTsDataTypes().get(i),
            getDefaultEncoding(createTimeSeriesByDeviceStatement.getTsDataTypes().get(i)),
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            TimeseriesVersionUtil.generateVersion());
      }

      return new CreateMultiTimeSeriesNode(
          context.getQueryId().genPlanNodeId(),
          Collections.singletonMap(
              createTimeSeriesByDeviceStatement.getDevicePath(), measurementGroup));
    }

    @Override
    public PlanNode visitCreateMultiTimeseries(
        CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, MPPQueryContext context) {
      int size = createMultiTimeSeriesStatement.getPaths().size();
      List<String> versionList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        versionList.add(TimeseriesVersionUtil.generateVersion());
      }
      return new CreateMultiTimeSeriesNode(
          context.getQueryId().genPlanNodeId(),
          createMultiTimeSeriesStatement.getPaths(),
          createMultiTimeSeriesStatement.getDataTypes(),
          createMultiTimeSeriesStatement.getEncodings(),
          createMultiTimeSeriesStatement.getCompressors(),
          createMultiTimeSeriesStatement.getPropsList(),
          createMultiTimeSeriesStatement.getAliasList(),
          createMultiTimeSeriesStatement.getTagsList(),
          createMultiTimeSeriesStatement.getAttributesList(),
          versionList);
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
      // convert insert statement to insert node
      return new InsertTabletNode(
          context.getQueryId().genPlanNodeId(),
          insertTabletStatement.getDevicePath(),
          insertTabletStatement.isAligned(),
          insertTabletStatement.getMeasurements(),
          insertTabletStatement.getDataTypes(),
          insertTabletStatement.getTimes(),
          insertTabletStatement.getBitMaps(),
          insertTabletStatement.getColumns(),
          insertTabletStatement.getRowCount());
    }

    @Override
    public PlanNode visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
      // convert insert statement to insert node
      return new InsertRowNode(
          context.getQueryId().genPlanNodeId(),
          insertRowStatement.getDevicePath(),
          insertRowStatement.isAligned(),
          insertRowStatement.getMeasurements(),
          insertRowStatement.getDataTypes(),
          insertRowStatement.getTime(),
          insertRowStatement.getValues(),
          insertRowStatement.isNeedInferType());
    }

    @Override
    public PlanNode visitShowTimeSeries(
        ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planTimeSeriesSchemaSource(
              showTimeSeriesStatement.getPathPattern(),
              showTimeSeriesStatement.getKey(),
              showTimeSeriesStatement.getValue(),
              showTimeSeriesStatement.getLimit(),
              showTimeSeriesStatement.getOffset(),
              showTimeSeriesStatement.isOrderByHeat(),
              showTimeSeriesStatement.isContains(),
              showTimeSeriesStatement.isPrefixPath())
          .planSchemaQueryMerge(showTimeSeriesStatement.isOrderByHeat())
          .planOffset(showTimeSeriesStatement.getOffset())
          .planLimit(showTimeSeriesStatement.getLimit())
          .getRoot();
    }

    @Override
    public PlanNode visitShowDevices(
        ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planDeviceSchemaSource(
              showDevicesStatement.getPathPattern(),
              showDevicesStatement.getLimit(),
              showDevicesStatement.getOffset(),
              showDevicesStatement.isPrefixPath(),
              showDevicesStatement.hasSgCol())
          .planSchemaQueryMerge(false)
          .planOffset(showDevicesStatement.getOffset())
          .planLimit(showDevicesStatement.getLimit())
          .getRoot();
    }

    @Override
    public PlanNode visitCountDevices(
        CountDevicesStatement countDevicesStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planDevicesCountSource(
              countDevicesStatement.getPartialPath(), countDevicesStatement.isPrefixPath())
          .planCountMerge()
          .getRoot();
    }

    @Override
    public PlanNode visitCountTimeSeries(
        CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planTimeSeriesCountSource(
              countTimeSeriesStatement.getPartialPath(), countTimeSeriesStatement.isPrefixPath())
          .planCountMerge()
          .getRoot();
    }

    @Override
    public PlanNode visitCountLevelTimeSeries(
        CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planLevelTimeSeriesCountSource(
              countLevelTimeSeriesStatement.getPartialPath(),
              countLevelTimeSeriesStatement.isPrefixPath(),
              countLevelTimeSeriesStatement.getLevel())
          .planCountMerge()
          .getRoot();
    }

    @Override
    public PlanNode visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planNodePathsSchemaSource(countStatement.getPartialPath(), countStatement.getLevel())
          .planSchemaQueryMerge(false)
          .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
          .planNodePathsCount()
          .getRoot();
    }

    @Override
    public PlanNode visitInsertRows(
        InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
      // convert insert statement to insert node
      InsertRowsNode insertRowsNode = new InsertRowsNode(context.getQueryId().genPlanNodeId());
      for (int i = 0; i < insertRowsStatement.getInsertRowStatementList().size(); i++) {
        InsertRowStatement insertRowStatement =
            insertRowsStatement.getInsertRowStatementList().get(i);
        insertRowsNode.addOneInsertRowNode(
            new InsertRowNode(
                insertRowsNode.getPlanNodeId(),
                insertRowStatement.getDevicePath(),
                insertRowStatement.isAligned(),
                insertRowStatement.getMeasurements(),
                insertRowStatement.getDataTypes(),
                insertRowStatement.getTime(),
                insertRowStatement.getValues(),
                insertRowStatement.isNeedInferType()),
            i);
      }
      return insertRowsNode;
    }

    @Override
    public PlanNode visitInsertMultiTablets(
        InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
      // convert insert statement to insert node
      InsertMultiTabletsNode insertMultiTabletsNode =
          new InsertMultiTabletsNode(context.getQueryId().genPlanNodeId());
      for (int i = 0; i < insertMultiTabletsStatement.getInsertTabletStatementList().size(); i++) {
        InsertTabletStatement insertTabletStatement =
            insertMultiTabletsStatement.getInsertTabletStatementList().get(i);
        insertMultiTabletsNode.addInsertTabletNode(
            new InsertTabletNode(
                insertMultiTabletsNode.getPlanNodeId(),
                insertTabletStatement.getDevicePath(),
                insertTabletStatement.isAligned(),
                insertTabletStatement.getMeasurements(),
                insertTabletStatement.getDataTypes(),
                insertTabletStatement.getTimes(),
                insertTabletStatement.getBitMaps(),
                insertTabletStatement.getColumns(),
                insertTabletStatement.getRowCount()),
            i);
      }
      return insertMultiTabletsNode;
    }

    @Override
    public PlanNode visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
      // convert insert statement to insert node
      InsertRowsOfOneDeviceNode insertRowsOfOneDeviceNode =
          new InsertRowsOfOneDeviceNode(context.getQueryId().genPlanNodeId());

      List<InsertRowNode> insertRowNodeList = new ArrayList<>();
      List<Integer> insertRowNodeIndexList = new ArrayList<>();
      for (int i = 0; i < insertRowsOfOneDeviceStatement.getInsertRowStatementList().size(); i++) {
        InsertRowStatement insertRowStatement =
            insertRowsOfOneDeviceStatement.getInsertRowStatementList().get(i);
        insertRowNodeList.add(
            new InsertRowNode(
                insertRowsOfOneDeviceNode.getPlanNodeId(),
                insertRowStatement.getDevicePath(),
                insertRowStatement.isAligned(),
                insertRowStatement.getMeasurements(),
                insertRowStatement.getDataTypes(),
                insertRowStatement.getTime(),
                insertRowStatement.getValues(),
                insertRowStatement.isNeedInferType()));
        insertRowNodeIndexList.add(i);
      }

      insertRowsOfOneDeviceNode.setInsertRowNodeList(insertRowNodeList);
      insertRowsOfOneDeviceNode.setInsertRowNodeIndexList(insertRowNodeIndexList);
      return insertRowsOfOneDeviceNode;
    }

    @Override
    public PlanNode visitSchemaFetch(
        SchemaFetchStatement schemaFetchStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planSchemaFetchMerge()
          .planSchemaFetchSource(
              new ArrayList<>(
                  schemaFetchStatement.getSchemaPartition().getSchemaPartitionMap().keySet()),
              schemaFetchStatement.getPatternTree())
          .getRoot();
    }

    @Override
    public PlanNode visitShowChildPaths(
        ShowChildPathsStatement showChildPathsStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planNodePathsSchemaSource(showChildPathsStatement.getPartialPath(), -1)
          .planSchemaQueryMerge(false)
          .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
          .getRoot();
    }

    @Override
    public PlanNode visitShowChildNodes(
        ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
      LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(context);
      return planBuilder
          .planNodePathsSchemaSource(showChildNodesStatement.getPartialPath(), -1)
          .planSchemaQueryMerge(false)
          .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
          .planNodePathsConvert()
          .getRoot();
    }

    @Override
    public PlanNode visitDeleteTimeseries(
        DeleteTimeSeriesStatement deleteTimeSeriesStatement, MPPQueryContext context) {
      return new DeleteTimeSeriesNode(
          context.getQueryId().genPlanNodeId(), deleteTimeSeriesStatement.getPaths());
    }

    @Override
    public PlanNode visitInvalidateSchemaCache(
        InvalidateSchemaCacheStatement invalidateSchemaCacheStatement, MPPQueryContext context) {
      return new InvalidateSchemaCacheNode(context.getQueryId().genPlanNodeId(), null);
    }

    @Override
    public PlanNode visitDeleteData(
        DeleteDataStatement deleteDataStatement, MPPQueryContext context) {
      return new DeleteDataNode(
          context.getQueryId().genPlanNodeId(),
          deleteDataStatement.getPathList(),
          deleteDataStatement.getDeleteStartTime(),
          deleteDataStatement.getDeleteEndTime());
    }
  }
}
