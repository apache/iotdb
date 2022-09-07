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
package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This visitor is used to generate a logical plan for the statement and returns the {@link
 * PlanNode}.
 */
public class LogicalPlanVisitor extends StatementVisitor<PlanNode, MPPQueryContext> {

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
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    if (queryStatement.isLastQuery()) {
      return planBuilder
          .planLast(
              analysis.getSourceExpressions(),
              analysis.getGlobalTimeFilter(),
              analysis.getMergeOrderParameter())
          .getRoot();
    }

    if (queryStatement.isAlignByDevice()) {
      Map<String, PlanNode> deviceToSubPlanMap = new TreeMap<>();
      for (String deviceName : analysis.getDeviceToSourceExpressions().keySet()) {
        LogicalPlanBuilder subPlanBuilder = new LogicalPlanBuilder(analysis, context);
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
                    analysis.getDeviceToHavingExpression() != null
                        ? analysis.getDeviceToHavingExpression().get(deviceName)
                        : null,
                    analysis.getDeviceToMeasurementIndexesMap().get(deviceName),
                    context));
        deviceToSubPlanMap.put(deviceName, subPlanBuilder.getRoot());
      }
      // convert to ALIGN BY DEVICE view
      planBuilder =
          planBuilder.planDeviceView(
              deviceToSubPlanMap,
              analysis.getOutputExpressions(),
              analysis.getDeviceToMeasurementIndexesMap(),
              queryStatement.getResultTimeOrder());
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
                  analysis.getHavingExpression(),
                  null,
                  context));
    }

    // other common upstream node
    planBuilder =
        planBuilder
            .planFill(analysis.getFillDescriptor(), queryStatement.getResultTimeOrder())
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
      Expression havingExpression,
      List<Integer> measurementIndexes, // only used in ALIGN BY DEVICE
      MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // plan data source node
    if (isRawDataSource) {
      planBuilder =
          planBuilder.planRawDataSource(
              sourceExpressions,
              queryStatement.getResultTimeOrder(),
              analysis.getGlobalTimeFilter());

      if (queryStatement.isAggregationQuery()) {
        if (analysis.hasValueFilter()) {
          planBuilder =
              planBuilder.planFilterAndTransform(
                  queryFilter,
                  aggregationTransformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        } else {
          planBuilder =
              planBuilder.planTransform(
                  aggregationTransformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        }

        boolean outputPartial =
            queryStatement.isGroupByLevel()
                || (queryStatement.isGroupByTime()
                    && analysis.getGroupByTimeParameter().hasOverlap());
        AggregationStep curStep = outputPartial ? AggregationStep.PARTIAL : AggregationStep.SINGLE;
        planBuilder =
            planBuilder.planAggregation(
                aggregationExpressions,
                analysis.getGroupByTimeParameter(),
                curStep,
                queryStatement.getResultTimeOrder());

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
                    queryStatement.getResultTimeOrder());
          }

          if (queryStatement.isGroupByLevel()) {
            curStep = AggregationStep.FINAL;
            planBuilder =
                planBuilder.planGroupByLevel(
                    analysis.getGroupByLevelExpressions(),
                    curStep,
                    analysis.getGroupByTimeParameter(),
                    queryStatement.getResultTimeOrder());
          }
        }

        if (queryStatement.isGroupByLevel()) {
          planBuilder = // plan Having with GroupByLevel
              planBuilder.planFilterAndTransform(
                  havingExpression,
                  analysis.getGroupByLevelExpressions().keySet(),
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        } else {
          if (havingExpression != null) {
            planBuilder = // plan Having without GroupByLevel
                planBuilder.planFilterAndTransform(
                    havingExpression,
                    transformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId(),
                    queryStatement.getResultTimeOrder());
          } else {
            planBuilder = // no Having
                planBuilder.planTransform(
                    transformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId(),
                    queryStatement.getResultTimeOrder());
          }
        }
      } else {
        if (analysis.hasValueFilter()) {
          planBuilder =
              planBuilder.planFilterAndTransform(
                  queryFilter,
                  transformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        } else {
          planBuilder =
              planBuilder.planTransform(
                  transformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        }
      }
    } else {
      AggregationStep curStep =
          (analysis.getGroupByLevelExpressions() != null
                  || (analysis.getGroupByTimeParameter() != null
                      && analysis.getGroupByTimeParameter().hasOverlap()))
              ? AggregationStep.PARTIAL
              : AggregationStep.SINGLE;

      boolean needTransform = false;
      for (Expression expression : transformExpressions) {
        if (ExpressionAnalyzer.checkIsNeedTransform(expression)) {
          needTransform = true;
          break;
        }
      }

      if (!needTransform && measurementIndexes != null) {
        planBuilder =
            planBuilder.planAggregationSourceWithIndexAdjust(
                sourceExpressions,
                curStep,
                queryStatement.getResultTimeOrder(),
                analysis.getGlobalTimeFilter(),
                analysis.getGroupByTimeParameter(),
                measurementIndexes,
                aggregationExpressions,
                aggregationTransformExpressions,
                analysis.getGroupByLevelExpressions());
        if (queryStatement.isGroupByLevel()) {
          planBuilder = // plan Having with GroupByLevel
              planBuilder.planFilterAndTransform(
                  havingExpression,
                  analysis.getGroupByLevelExpressions().keySet(),
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        } else {
          planBuilder = // plan Having without GroupByLevel
              planBuilder.planFilterAndTransform(
                  havingExpression,
                  transformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        }
      } else {
        planBuilder =
            planBuilder.planAggregationSource(
                sourceExpressions,
                curStep,
                queryStatement.getResultTimeOrder(),
                analysis.getGlobalTimeFilter(),
                analysis.getGroupByTimeParameter(),
                aggregationExpressions,
                aggregationTransformExpressions,
                analysis.getGroupByLevelExpressions());

        if (queryStatement.isGroupByLevel()) {
          planBuilder = // plan Having with GroupByLevel
              planBuilder.planFilterAndTransform(
                  havingExpression,
                  analysis.getGroupByLevelExpressions().keySet(),
                  queryStatement.isGroupByTime(),
                  queryStatement.getSelectComponent().getZoneId(),
                  queryStatement.getResultTimeOrder());
        } else {
          if (havingExpression != null) {
            planBuilder = // plan Having without GroupByLevel
                planBuilder.planFilterAndTransform(
                    havingExpression,
                    transformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId(),
                    queryStatement.getResultTimeOrder());
          } else {
            planBuilder = // no Having
                planBuilder.planTransform(
                    transformExpressions,
                    queryStatement.isGroupByTime(),
                    queryStatement.getSelectComponent().getZoneId(),
                    queryStatement.getResultTimeOrder());
          }
        }
      }
    }

    return planBuilder.getRoot();
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
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, MPPQueryContext context) {
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
  public PlanNode visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement,
      MPPQueryContext context) {
    int size = internalCreateTimeSeriesStatement.getMeasurements().size();

    MeasurementGroup measurementGroup = new MeasurementGroup();
    for (int i = 0; i < size; i++) {
      measurementGroup.addMeasurement(
          internalCreateTimeSeriesStatement.getMeasurements().get(i),
          internalCreateTimeSeriesStatement.getTsDataTypes().get(i),
          internalCreateTimeSeriesStatement.getEncodings().get(i),
          internalCreateTimeSeriesStatement.getCompressors().get(i));
    }

    return new InternalCreateTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        internalCreateTimeSeriesStatement.getDevicePath(),
        measurementGroup,
        internalCreateTimeSeriesStatement.isAligned());
  }

  @Override
  public PlanNode visitCreateMultiTimeseries(
      CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, MPPQueryContext context) {
    return new CreateMultiTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        createMultiTimeSeriesStatement.getPaths(),
        createMultiTimeSeriesStatement.getDataTypes(),
        createMultiTimeSeriesStatement.getEncodings(),
        createMultiTimeSeriesStatement.getCompressors(),
        createMultiTimeSeriesStatement.getPropsList(),
        createMultiTimeSeriesStatement.getAliasList(),
        createMultiTimeSeriesStatement.getTagsList(),
        createMultiTimeSeriesStatement.getAttributesList());
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
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // If there is only one region, we can push down the offset and limit operation to
    // source operator.
    boolean canPushDownOffsetLimit =
        analysis.getSchemaPartitionInfo() != null
            && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1
            && !showTimeSeriesStatement.isOrderByHeat();

    int limit = showTimeSeriesStatement.getLimit();
    int offset = showTimeSeriesStatement.getOffset();
    if (showTimeSeriesStatement.isOrderByHeat()) {
      limit = 0;
      offset = 0;
    } else if (!canPushDownOffsetLimit) {
      limit = showTimeSeriesStatement.getLimit() + showTimeSeriesStatement.getOffset();
      offset = 0;
    }
    planBuilder =
        planBuilder
            .planTimeSeriesSchemaSource(
                showTimeSeriesStatement.getPathPattern(),
                showTimeSeriesStatement.getKey(),
                showTimeSeriesStatement.getValue(),
                limit,
                offset,
                showTimeSeriesStatement.isOrderByHeat(),
                showTimeSeriesStatement.isContains(),
                showTimeSeriesStatement.isPrefixPath(),
                analysis.getRelatedTemplateInfo())
            .planSchemaQueryMerge(showTimeSeriesStatement.isOrderByHeat());
    // show latest timeseries
    if (showTimeSeriesStatement.isOrderByHeat()
        && null != analysis.getDataPartitionInfo()
        && 0 != analysis.getDataPartitionInfo().getDataPartitionMap().size()) {
      PlanNode lastPlanNode =
          new LogicalPlanBuilder(analysis, context)
              .planLast(
                  analysis.getSourceExpressions(),
                  analysis.getGlobalTimeFilter(),
                  new OrderByParameter())
              .getRoot();
      planBuilder = planBuilder.planSchemaQueryOrderByHeat(lastPlanNode);
    }

    if (canPushDownOffsetLimit) {
      return planBuilder.getRoot();
    }

    return planBuilder
        .planOffset(showTimeSeriesStatement.getOffset())
        .planLimit(showTimeSeriesStatement.getLimit())
        .getRoot();
  }

  @Override
  public PlanNode visitShowDevices(
      ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // If there is only one region, we can push down the offset and limit operation to
    // source operator.
    boolean canPushDownOffsetLimit =
        analysis.getSchemaPartitionInfo() != null
            && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1;

    int limit = showDevicesStatement.getLimit();
    int offset = showDevicesStatement.getOffset();
    if (!canPushDownOffsetLimit) {
      limit = showDevicesStatement.getLimit() + showDevicesStatement.getOffset();
      offset = 0;
    }

    planBuilder =
        planBuilder
            .planDeviceSchemaSource(
                showDevicesStatement.getPathPattern(),
                limit,
                offset,
                showDevicesStatement.isPrefixPath(),
                showDevicesStatement.hasSgCol())
            .planSchemaQueryMerge(false);

    if (!canPushDownOffsetLimit) {
      return planBuilder
          .planOffset(showDevicesStatement.getOffset())
          .planLimit(showDevicesStatement.getLimit())
          .getRoot();
    }
    return planBuilder.getRoot();
  }

  @Override
  public PlanNode visitCountDevices(
      CountDevicesStatement countDevicesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planDevicesCountSource(
            countDevicesStatement.getPathPattern(), countDevicesStatement.isPrefixPath())
        .planCountMerge()
        .getRoot();
  }

  @Override
  public PlanNode visitCountTimeSeries(
      CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planTimeSeriesCountSource(
            countTimeSeriesStatement.getPathPattern(),
            countTimeSeriesStatement.isPrefixPath(),
            countTimeSeriesStatement.getKey(),
            countTimeSeriesStatement.getValue(),
            countTimeSeriesStatement.isContains(),
            analysis.getRelatedTemplateInfo())
        .planCountMerge()
        .getRoot();
  }

  @Override
  public PlanNode visitCountLevelTimeSeries(
      CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planLevelTimeSeriesCountSource(
            countLevelTimeSeriesStatement.getPathPattern(),
            countLevelTimeSeriesStatement.isPrefixPath(),
            countLevelTimeSeriesStatement.getLevel(),
            countLevelTimeSeriesStatement.getKey(),
            countLevelTimeSeriesStatement.getValue(),
            countLevelTimeSeriesStatement.isContains())
        .planCountMerge()
        .getRoot();
  }

  @Override
  public PlanNode visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(countStatement.getPathPattern(), countStatement.getLevel())
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
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    List<String> storageGroupList =
        new ArrayList<>(analysis.getSchemaPartitionInfo().getSchemaPartitionMap().keySet());
    return planBuilder
        .planSchemaFetchMerge(storageGroupList)
        .planSchemaFetchSource(
            storageGroupList,
            schemaFetchStatement.getPatternTree(),
            schemaFetchStatement.getTemplateMap())
        .getRoot();
  }

  @Override
  public PlanNode visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(showChildPathsStatement.getPartialPath(), -1)
        .planSchemaQueryMerge(false)
        .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
        .getRoot();
  }

  @Override
  public PlanNode visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(showChildNodesStatement.getPartialPath(), -1)
        .planSchemaQueryMerge(false)
        .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
        .planNodePathsConvert()
        .getRoot();
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

  @Override
  public PlanNode visitActivateTemplate(
      ActivateTemplateStatement activateTemplateStatement, MPPQueryContext context) {
    return new ActivateTemplateNode(
        context.getQueryId().genPlanNodeId(),
        activateTemplateStatement.getPath(),
        analysis.getTemplateSetInfo().right.get(0).getNodeLength() - 1,
        analysis.getTemplateSetInfo().left.getId());
  }

  @Override
  public PlanNode visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    planBuilder =
        planBuilder
            .planPathsUsingTemplateSource(analysis.getTemplateSetInfo().left.getId())
            .planSchemaQueryMerge(false);
    return planBuilder.getRoot();
  }
}
