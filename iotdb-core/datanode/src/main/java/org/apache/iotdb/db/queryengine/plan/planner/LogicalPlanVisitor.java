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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.TransformToViewExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.ShowLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ENDTIME;

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
  public PlanNode visitExplainAnalyze(
      ExplainAnalyzeStatement explainAnalyzeStatement, MPPQueryContext context) {
    PlanNode root = visitQuery(explainAnalyzeStatement.getQueryStatement(), context);
    root =
        new ExplainAnalyzeNode(
            context.getQueryId().genPlanNodeId(),
            root,
            explainAnalyzeStatement.isVerbose(),
            context.getLocalQueryId(),
            context.getTimeOut());
    context
        .getTypeProvider()
        .setTreeModelType(ColumnHeaderConstant.EXPLAIN_ANALYZE, TSDataType.TEXT);
    return root;
  }

  @Override
  public PlanNode visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
    if (analysis.allDevicesInOneTemplate()) {
      return new TemplatedLogicalPlan(analysis, queryStatement, context).visitQuery();
    }

    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    if (queryStatement.isLastQuery()) {
      planBuilder = planBuilder.planLast(analysis, analysis.getTimeseriesOrderingForLastQuery());

      if (queryStatement.hasOrderBy() && !queryStatement.onlyOrderByTimeseries()) {
        planBuilder = planBuilder.planOrderBy(queryStatement.getSortItemList());
      }

      planBuilder =
          planBuilder
              .planOffset(queryStatement.getRowOffset())
              .planLimit(queryStatement.getRowLimit());
      return planBuilder.getRoot();
    }

    if (queryStatement.isAlignByDevice()) {
      Map<IDeviceID, PlanNode> deviceToSubPlanMap = new LinkedHashMap<>();
      for (PartialPath device : analysis.getDeviceList()) {
        IDeviceID deviceID = device.getIDeviceIDAsFullDevice();
        LogicalPlanBuilder subPlanBuilder = new LogicalPlanBuilder(analysis, context);
        subPlanBuilder =
            subPlanBuilder.withNewRoot(
                visitQueryBody(
                    queryStatement,
                    analysis.getDeviceToSourceExpressions().get(deviceID),
                    analysis.getDeviceToSourceTransformExpressions().get(deviceID),
                    analysis.getDeviceToWhereExpression() != null
                        ? analysis.getDeviceToWhereExpression().get(deviceID)
                        : null,
                    analysis.getDeviceToAggregationExpressions().get(deviceID),
                    analysis.getDeviceToGroupByExpression() != null
                        ? analysis.getDeviceToGroupByExpression().get(deviceID)
                        : null,
                    context));
        // order by device, expression, push down sortOperator
        if (queryStatement.needPushDownSort()) {
          subPlanBuilder =
              subPlanBuilder.planOrderBy(
                  analysis.getDeviceToOrderByExpressions().get(deviceID),
                  analysis.getDeviceToSortItems().get(deviceID));
        }
        deviceToSubPlanMap.put(deviceID, subPlanBuilder.getRoot());
      }

      // convert to ALIGN BY DEVICE view
      planBuilder =
          planBuilder.planDeviceView(
              deviceToSubPlanMap,
              analysis.getDeviceViewOutputExpressions(),
              analysis.getDeviceViewInputIndexesMap(),
              analysis.getSelectExpressions(),
              queryStatement,
              analysis);
    } else {
      planBuilder =
          planBuilder.withNewRoot(
              visitQueryBody(
                  queryStatement,
                  analysis.getSourceExpressions(),
                  analysis.getSourceTransformExpressions(),
                  analysis.getWhereExpression(),
                  analysis.getAggregationExpressions(),
                  analysis.getGroupByExpression(),
                  context));
    }

    if (queryStatement.isAggregationQuery()) {
      planBuilder =
          planBuilder.planHavingAndTransform(
              analysis.getHavingExpression(),
              analysis.getSelectExpressions(),
              analysis.getOrderByExpressions(),
              queryStatement.isGroupByTime(),
              queryStatement.getResultTimeOrder());
    }

    if (!queryStatement.needPushDownSort()) {
      planBuilder = planBuilder.planOrderBy(queryStatement, analysis);
    }

    // other upstream node
    planBuilder =
        planBuilder
            .planFill(analysis.getFillDescriptor(), queryStatement.getResultTimeOrder())
            .planOffset(queryStatement.getRowOffset());

    if (!analysis.isUseTopKNode() || queryStatement.hasOffset()) {
      planBuilder = planBuilder.planLimit(queryStatement.getRowLimit());
    }

    if (queryStatement.hasModelInference()) {
      planBuilder.planInference(analysis);
    }

    // plan select into
    if (queryStatement.isAlignByDevice()) {
      planBuilder = planBuilder.planDeviceViewInto(analysis.getDeviceViewIntoPathDescriptor());
    } else {
      planBuilder = planBuilder.planInto(analysis.getIntoPathDescriptor());
    }

    return planBuilder.getRoot();
  }

  public PlanNode visitQueryBody(
      QueryStatement queryStatement,
      Set<Expression> sourceExpressions,
      Set<Expression> sourceTransformExpressions,
      Expression whereExpression,
      Set<Expression> aggregationExpressions,
      Expression groupByExpression,
      MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    if (aggregationExpressions == null) {
      // raw data query
      planBuilder =
          planBuilder
              .planRawDataSource(
                  sourceExpressions,
                  queryStatement.getResultTimeOrder(),
                  0,
                  pushDownLimitToScanNode(queryStatement, analysis),
                  analysis.isLastLevelUseWildcard())
              .planWhereAndSourceTransform(
                  whereExpression,
                  sourceTransformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getResultTimeOrder());
    } else {
      // aggregation query
      planBuilder =
          planBuilder
              .planRawDataSource(
                  sourceExpressions,
                  queryStatement.getResultTimeOrder(),
                  0,
                  0,
                  analysis.isLastLevelUseWildcard())
              .planWhereAndSourceTransform(
                  whereExpression,
                  sourceTransformExpressions,
                  queryStatement.isGroupByTime(),
                  queryStatement.getResultTimeOrder());

      boolean outputPartial =
          queryStatement.isGroupByLevel()
              || queryStatement.isGroupByTag()
              || (queryStatement.isGroupByTime()
                  && analysis.getGroupByTimeParameter().hasOverlap());
      AggregationStep curStep = outputPartial ? AggregationStep.PARTIAL : AggregationStep.SINGLE;
      planBuilder =
          planBuilder.planRawDataAggregation(
              aggregationExpressions,
              groupByExpression,
              analysis.getGroupByTimeParameter(),
              analysis.getGroupByParameter(),
              queryStatement.isOutputEndTime(),
              curStep,
              queryStatement.getResultTimeOrder());

      if (queryStatement.isGroupByTime() && analysis.getGroupByTimeParameter().hasOverlap()) {
        curStep =
            (queryStatement.isGroupByLevel() || queryStatement.isGroupByTag())
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
        planBuilder =
            planBuilder.planGroupByLevel(
                analysis.getCrossGroupByExpressions(),
                analysis.getGroupByTimeParameter(),
                queryStatement.getResultTimeOrder());
      } else if (queryStatement.isGroupByTag()) {
        planBuilder =
            planBuilder.planGroupByTag(
                analysis.getCrossGroupByExpressions(),
                analysis.getTagKeys(),
                analysis.getTagValuesToGroupedTimeseriesOperands(),
                analysis.getGroupByTimeParameter(),
                queryStatement.getResultTimeOrder());
      }

      if (queryStatement.isOutputEndTime()) {
        context.getTypeProvider().setTreeModelType(ENDTIME, TSDataType.INT64);
        if (queryStatement.isGroupByTime()) {
          planBuilder =
              planBuilder.planEndTimeColumnInject(
                  analysis.getGroupByTimeParameter(),
                  queryStatement.getResultTimeOrder().isAscending());
        }
      }
    }

    return planBuilder.getRoot();
  }

  static long pushDownLimitToScanNode(QueryStatement queryStatement, Analysis analysis) {
    // `order by time|device LIMIT N align by device` and no value filter,
    // can push down limitValue to ScanNode
    if (queryStatement.isAlignByDevice()
        && queryStatement.hasLimit()
        && !analysis.hasValueFilter()
        && (queryStatement.isOrderByBasedOnDevice() || queryStatement.isOrderByBasedOnTime())) {

      // both `offset` and `limit` exist, push `limit+offset` down as limitValue
      if (queryStatement.hasOffset()) {
        return queryStatement.getRowOffset() + queryStatement.getRowLimit();
      }

      // only `limit` exist, push `limit` down as limitValue
      return queryStatement.getRowLimit();
    }

    return 0;
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
  public PlanNode visitCreateMultiTimeSeries(
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
  public PlanNode visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement,
      MPPQueryContext context) {
    return new InternalCreateMultiTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        internalCreateMultiTimeSeriesStatement.getDeviceMap());
  }

  @Override
  public PlanNode visitAlterTimeSeries(
      AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
    return new AlterTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        alterTimeSeriesStatement.getPath(),
        alterTimeSeriesStatement.getAlterType(),
        alterTimeSeriesStatement.getAlterMap(),
        alterTimeSeriesStatement.getAlias(),
        alterTimeSeriesStatement.getTagsMap(),
        alterTimeSeriesStatement.getAttributesMap(),
        alterTimeSeriesStatement.isAlterView());
  }

  @Override
  public PlanNode visitInsertTablet(
      InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
    // convert insert statement to insert node
    InsertTabletNode insertNode =
        new InsertTabletNode(
            context.getQueryId().genPlanNodeId(),
            insertTabletStatement.getDevicePath(),
            insertTabletStatement.isAligned(),
            insertTabletStatement.getMeasurements(),
            insertTabletStatement.getDataTypes(),
            insertTabletStatement.getMeasurementSchemas(),
            insertTabletStatement.getTimes(),
            insertTabletStatement.getBitMaps(),
            insertTabletStatement.getColumns(),
            insertTabletStatement.getRowCount());
    insertNode.setFailedMeasurementNumber(insertTabletStatement.getFailedMeasurementNumber());
    return insertNode;
  }

  @Override
  public PlanNode visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
    // convert insert statement to insert node
    InsertRowNode insertNode =
        new InsertRowNode(
            context.getQueryId().genPlanNodeId(),
            insertRowStatement.getDevicePath(),
            insertRowStatement.isAligned(),
            insertRowStatement.getMeasurements(),
            insertRowStatement.getDataTypes(),
            insertRowStatement.getMeasurementSchemas(),
            insertRowStatement.getTime(),
            insertRowStatement.getValues(),
            insertRowStatement.isNeedInferType());
    insertNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
    return insertNode;
  }

  @Override
  public PlanNode visitPipeEnrichedStatement(
      PipeEnrichedStatement pipeEnrichedStatement, MPPQueryContext context) {
    WritePlanNode node =
        (WritePlanNode) pipeEnrichedStatement.getInnerStatement().accept(this, context);

    if (node instanceof LoadTsFileNode) {
      return node;
    } else if (node instanceof InsertNode) {
      return new PipeEnrichedInsertNode((InsertNode) node);
    } else if (node instanceof DeleteDataNode) {
      return new PipeEnrichedDeleteDataNode((DeleteDataNode) node);
    }

    return new PipeEnrichedWritePlanNode(node);
  }

  @Override
  public PlanNode visitLoadFile(LoadTsFileStatement loadTsFileStatement, MPPQueryContext context) {
    final List<Boolean> isTableModel = new ArrayList<>();
    for (int i = 0; i < loadTsFileStatement.getResources().size(); i++) {
      isTableModel.add(
          loadTsFileStatement.getModel().equals(LoadTsFileConfigurator.MODEL_TABLE_VALUE));
    }
    return new LoadTsFileNode(
        context.getQueryId().genPlanNodeId(),
        loadTsFileStatement.getResources(),
        isTableModel,
        loadTsFileStatement.getDatabase());
  }

  @Override
  public PlanNode visitShowTimeSeries(
      ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    long limit = showTimeSeriesStatement.getLimit();
    long offset = showTimeSeriesStatement.getOffset();
    if (showTimeSeriesStatement.hasTimeCondition()) {
      planBuilder =
          planBuilder
              .planTimeseriesRegionScan(analysis.getDeviceToTimeseriesSchemas(), false)
              .planLimit(limit)
              .planOffset(offset);
      return planBuilder.getRoot();
    }

    // If there is only one region, we can push down the offset and limit operation to
    // source operator.
    boolean canPushDownOffsetLimit =
        analysis.getSchemaPartitionInfo() != null
            && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1
            && !showTimeSeriesStatement.isOrderByHeat();

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
                showTimeSeriesStatement.getSchemaFilter(),
                limit,
                offset,
                showTimeSeriesStatement.isOrderByHeat(),
                showTimeSeriesStatement.isPrefixPath(),
                analysis.getRelatedTemplateInfo(),
                showTimeSeriesStatement.getAuthorityScope())
            .planSchemaQueryMerge(showTimeSeriesStatement.isOrderByHeat());

    // show latest timeseries
    if (showTimeSeriesStatement.isOrderByHeat()
        && null != analysis.getDataPartitionInfo()
        && !analysis.getDataPartitionInfo().getDataPartitionMap().isEmpty()) {
      PlanNode lastPlanNode =
          new LogicalPlanBuilder(analysis, context).planLast(analysis, null).getRoot();
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

    if (showDevicesStatement.hasTimeCondition()) {
      planBuilder =
          planBuilder
              .planDeviceRegionScan(analysis.getDevicePathToContextMap(), false)
              .planLimit(showDevicesStatement.getLimit())
              .planOffset(showDevicesStatement.getOffset());
      return planBuilder.getRoot();
    }

    // If there is only one region, we can push down the offset and limit operation to
    // source operator.
    boolean canPushDownOffsetLimit =
        analysis.getSchemaPartitionInfo() != null
            && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1;

    long limit = showDevicesStatement.getLimit();
    long offset = showDevicesStatement.getOffset();
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
                showDevicesStatement.hasSgCol(),
                showDevicesStatement.getSchemaFilter(),
                showDevicesStatement.getAuthorityScope())
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

    if (countDevicesStatement.hasTimeCondition()) {
      planBuilder = planBuilder.planDeviceRegionScan(analysis.getDevicePathToContextMap(), true);
      return planBuilder.getRoot();
    }

    return planBuilder
        .planDevicesCountSource(
            countDevicesStatement.getPathPattern(),
            countDevicesStatement.isPrefixPath(),
            countDevicesStatement.getAuthorityScope())
        .planCountMerge()
        .getRoot();
  }

  @Override
  public PlanNode visitCountTimeSeries(
      CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    if (countTimeSeriesStatement.hasTimeCondition()) {
      planBuilder =
          planBuilder.planTimeseriesRegionScan(analysis.getDeviceToTimeseriesSchemas(), true);
      return planBuilder.getRoot();
    }

    return planBuilder
        .planTimeSeriesCountSource(
            countTimeSeriesStatement.getPathPattern(),
            countTimeSeriesStatement.isPrefixPath(),
            countTimeSeriesStatement.getSchemaFilter(),
            analysis.getRelatedTemplateInfo(),
            countTimeSeriesStatement.getAuthorityScope())
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
            countLevelTimeSeriesStatement.getSchemaFilter(),
            analysis.getRelatedTemplateInfo(),
            countLevelTimeSeriesStatement.getAuthorityScope())
        .planCountMerge()
        .getRoot();
  }

  @Override
  public PlanNode visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(
            countStatement.getPathPattern(),
            countStatement.getLevel(),
            countStatement.getAuthorityScope())
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
      InsertRowNode insertRowNode =
          new InsertRowNode(
              insertRowsNode.getPlanNodeId(),
              insertRowStatement.getDevicePath(),
              insertRowStatement.isAligned(),
              insertRowStatement.getMeasurements(),
              insertRowStatement.getDataTypes(),
              insertRowStatement.getMeasurementSchemas(),
              insertRowStatement.getTime(),
              insertRowStatement.getValues(),
              insertRowStatement.isNeedInferType());
      insertRowNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
      insertRowsNode.addOneInsertRowNode(insertRowNode, i);
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
      InsertTabletNode insertTabletNode =
          new InsertTabletNode(
              insertMultiTabletsNode.getPlanNodeId(),
              insertTabletStatement.getDevicePath(),
              insertTabletStatement.isAligned(),
              insertTabletStatement.getMeasurements(),
              insertTabletStatement.getDataTypes(),
              insertTabletStatement.getMeasurementSchemas(),
              insertTabletStatement.getTimes(),
              insertTabletStatement.getBitMaps(),
              insertTabletStatement.getColumns(),
              insertTabletStatement.getRowCount());
      insertTabletNode.setFailedMeasurementNumber(
          insertTabletStatement.getFailedMeasurementNumber());
      insertMultiTabletsNode.addInsertTabletNode(insertTabletNode, i);
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
      InsertRowNode insertRowNode =
          new InsertRowNode(
              insertRowsOfOneDeviceNode.getPlanNodeId(),
              insertRowStatement.getDevicePath(),
              insertRowStatement.isAligned(),
              insertRowStatement.getMeasurements(),
              insertRowStatement.getDataTypes(),
              insertRowStatement.getMeasurementSchemas(),
              insertRowStatement.getTime(),
              insertRowStatement.getValues(),
              insertRowStatement.isNeedInferType());
      insertRowNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
      insertRowNodeList.add(insertRowNode);
      insertRowNodeIndexList.add(i);
    }

    insertRowsOfOneDeviceNode.setInsertRowNodeList(insertRowNodeList);
    insertRowsOfOneDeviceNode.setInsertRowNodeIndexList(insertRowNodeIndexList);
    return insertRowsOfOneDeviceNode;
  }

  @Override
  public PlanNode visitSeriesSchemaFetch(
      SeriesSchemaFetchStatement seriesSchemaFetchStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    List<String> storageGroupList =
        new ArrayList<>(analysis.getSchemaPartitionInfo().getSchemaPartitionMap().keySet());
    return planBuilder
        .planSchemaFetchMerge(storageGroupList)
        .planSeriesSchemaFetchSource(
            storageGroupList,
            seriesSchemaFetchStatement.getPatternTree(),
            seriesSchemaFetchStatement.getTemplateMap(),
            seriesSchemaFetchStatement.isWithTags(),
            seriesSchemaFetchStatement.isWithAttributes(),
            seriesSchemaFetchStatement.isWithTemplate(),
            seriesSchemaFetchStatement.isWithAliasForce())
        .getRoot();
  }

  @Override
  public PlanNode visitDeviceSchemaFetch(
      DeviceSchemaFetchStatement deviceSchemaFetchStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    List<String> storageGroupList =
        new ArrayList<>(analysis.getSchemaPartitionInfo().getSchemaPartitionMap().keySet());
    return planBuilder
        .planSchemaFetchMerge(storageGroupList)
        .planDeviceSchemaFetchSource(
            storageGroupList,
            deviceSchemaFetchStatement.getPatternTree(),
            deviceSchemaFetchStatement.getAuthorityScope())
        .getRoot();
  }

  @Override
  public PlanNode visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(
            showChildPathsStatement.getPartialPath(),
            -1,
            showChildPathsStatement.getAuthorityScope())
        .planSchemaQueryMerge(false)
        .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
        .getRoot();
  }

  @Override
  public PlanNode visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(
            showChildNodesStatement.getPartialPath(),
            -1,
            showChildNodesStatement.getAuthorityScope())
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
  public PlanNode visitBatchActivateTemplate(
      BatchActivateTemplateStatement batchActivateTemplateStatement, MPPQueryContext context) {
    Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>();
    for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry :
        analysis.getDeviceTemplateSetInfoMap().entrySet()) {
      templateActivationMap.put(
          entry.getKey(),
          new Pair<>(entry.getValue().left.getId(), entry.getValue().right.getNodeLength() - 1));
    }
    return new BatchActivateTemplateNode(
        context.getQueryId().genPlanNodeId(), templateActivationMap);
  }

  @Override
  public PlanNode visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement internalBatchActivateTemplateStatement,
      MPPQueryContext context) {
    Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>();
    for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry :
        internalBatchActivateTemplateStatement.getDeviceMap().entrySet()) {
      templateActivationMap.put(
          entry.getKey(),
          new Pair<>(entry.getValue().left.getId(), entry.getValue().right.getNodeLength() - 1));
    }
    return new InternalBatchActivateTemplateNode(
        context.getQueryId().genPlanNodeId(), templateActivationMap);
  }

  @Override
  public PlanNode visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    planBuilder =
        planBuilder
            .planPathsUsingTemplateSource(
                analysis.getSpecifiedTemplateRelatedPathPatternList(),
                analysis.getTemplateSetInfo().left.getId(),
                showPathsUsingTemplateStatement.getAuthorityScope())
            .planSchemaQueryMerge(false);
    return planBuilder.getRoot();
  }

  @Override
  public PlanNode visitShowQueries(
      ShowQueriesStatement showQueriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    planBuilder =
        planBuilder
            .planShowQueries(analysis) // push Filter down
            .planOffset(showQueriesStatement.getRowOffset())
            .planLimit(showQueriesStatement.getRowLimit());
    return planBuilder.getRoot();
  }

  @Override
  public PlanNode visitCreateLogicalView(
      CreateLogicalViewStatement createLogicalViewStatement, MPPQueryContext context) {
    List<ViewExpression> viewExpressionList = new ArrayList<>();
    if (createLogicalViewStatement.getViewExpressions() == null) {
      // Transform all Expressions into ViewExpressions.
      TransformToViewExpressionVisitor transformToViewExpressionVisitor =
          new TransformToViewExpressionVisitor();
      List<Expression> expressionList = createLogicalViewStatement.getSourceExpressionList();
      for (Expression expression : expressionList) {
        viewExpressionList.add(transformToViewExpressionVisitor.process(expression, null));
      }
    } else {
      viewExpressionList = createLogicalViewStatement.getViewExpressions();
    }

    return new CreateLogicalViewNode(
        context.getQueryId().genPlanNodeId(),
        createLogicalViewStatement.getTargetPathList(),
        viewExpressionList);
  }

  @Override
  public PlanNode visitShowLogicalView(
      ShowLogicalViewStatement showLogicalViewStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // If there is only one region, we can push down the offset and limit operation to
    // source operator.
    boolean canPushDownOffsetLimit =
        analysis.getSchemaPartitionInfo() != null
            && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1;

    long limit = showLogicalViewStatement.getLimit();
    long offset = showLogicalViewStatement.getOffset();
    if (!canPushDownOffsetLimit) {
      limit = showLogicalViewStatement.getLimit() + showLogicalViewStatement.getOffset();
      offset = 0;
    }
    planBuilder =
        planBuilder
            .planLogicalViewSchemaSource(
                showLogicalViewStatement.getPathPattern(),
                showLogicalViewStatement.getSchemaFilter(),
                limit,
                offset,
                showLogicalViewStatement.getAuthorityScope())
            .planSchemaQueryMerge(false);

    if (canPushDownOffsetLimit) {
      return planBuilder.getRoot();
    }

    return planBuilder
        .planOffset(showLogicalViewStatement.getOffset())
        .planLimit(showLogicalViewStatement.getLimit())
        .getRoot();
  }
}
