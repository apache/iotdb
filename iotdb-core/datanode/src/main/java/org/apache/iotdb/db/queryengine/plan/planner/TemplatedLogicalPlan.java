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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.utils.SchemaUtils;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.DEVICE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.ENDTIME;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.END_TIME_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchSourceExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo.makeLayout;
import static org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanBuilder.updateTypeProviderByPartialAggregation;
import static org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanVisitor.pushDownLimitToScanNode;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode.getDeduplicatedDescriptors;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices set in only one template, using template can avoid many
 * unnecessary judgements.
 */
public class TemplatedLogicalPlan {

  private final Analysis analysis;
  private final QueryStatement queryStatement;
  private final MPPQueryContext context;
  private final List<String> measurementList;
  private final List<IMeasurementSchema> schemaList;

  private final long limitValue;

  private static final long OFFSET_VALUE = 0;

  private final Expression whereExpression;

  // to fix this query: `select s1 from root.** where s2>1 align by device`,
  // while project measurements are [s1], but newMeasurements should be [s1,s2]
  private List<String> newMeasurementList;

  private List<IMeasurementSchema> newSchemaList;

  private Map<String, List<InputLocation>> filterLayoutMap;

  List<AggregationDescriptor> aggregationDescriptorList;

  public TemplatedLogicalPlan(
      Analysis analysis, QueryStatement queryStatement, MPPQueryContext context) {
    this.analysis = analysis;
    this.queryStatement = queryStatement;
    this.context = context;

    this.measurementList = analysis.getMeasurementList();
    this.schemaList = analysis.getMeasurementSchemaList();
    this.newMeasurementList = measurementList;
    this.newSchemaList = schemaList;

    this.limitValue = pushDownLimitToScanNode(queryStatement, analysis);

    this.whereExpression = analysis.getWhereExpression();

    // for align by device query with template, store common variables for once
    if (queryStatement.isAggregationQuery()) {
      initAggQueryCommonVariables();
    } else {
      initNonAggQueryCommonVariables();
    }
  }

  private void initAggQueryCommonVariables() {
    if (whereExpression != null) {
      newMeasurementList = new ArrayList<>(measurementList);
      newSchemaList = new ArrayList<>(schemaList);
      Set<String> selectMeasurements = new HashSet<>(measurementList);
      List<Expression> whereSourceExpressions = searchSourceExpressions(whereExpression);
      for (Expression expression : whereSourceExpressions) {
        if (expression instanceof TimeSeriesOperand) {
          String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
          if (!analysis.getDeviceTemplate().getSchemaMap().containsKey(measurement)) {
            continue;
          }
          if (!selectMeasurements.contains(measurement)) {
            newMeasurementList.add(measurement);
            newSchemaList.add(analysis.getDeviceTemplate().getSchema(measurement));
          }
        }
      }

      filterLayoutMap = makeLayout(newMeasurementList);

      analysis
          .getExpressionTypes()
          .forEach(
              (key, value) ->
                  context
                      .getTypeProvider()
                      .setTreeModelType(key.getNode().getOutputSymbol(), value));
    }

    if (queryStatement.isOutputEndTime()) {
      context
          .getTypeProvider()
          .setTreeModelType(END_TIME_EXPRESSION.getOutputSymbol(), TSDataType.INT64);
    }
    if (queryStatement.isCountTimeAggregation()) {
      context.getTypeProvider().setTreeModelType("count_time(*)", TSDataType.INT64);
      context.getTypeProvider().setTreeModelType("count_time(Time)", TSDataType.INT64);
    }

    List<Integer> deviceToMeasurementIndexes =
        new ArrayList<>(analysis.getAggregationExpressions().size());
    for (int i = 1; i <= analysis.getAggregationExpressions().size(); i++) {
      deviceToMeasurementIndexes.add(i);
    }

    context
        .getTypeProvider()
        .setTemplatedInfo(
            new TemplatedInfo(
                newMeasurementList,
                newSchemaList,
                newSchemaList.stream()
                    .map(IMeasurementSchema::getType)
                    .collect(Collectors.toList()),
                queryStatement.getResultTimeOrder(),
                analysis.isLastLevelUseWildcard(),
                analysis.getDeviceViewOutputExpressions().stream()
                    .map(Expression::getExpressionString)
                    .collect(Collectors.toList()),
                deviceToMeasurementIndexes,
                OFFSET_VALUE,
                limitValue,
                whereExpression,
                queryStatement.isGroupByTime(),
                analysis.getDeviceTemplate().getSchemaMap(),
                filterLayoutMap,
                null,
                true,
                analysis.getGroupByTimeParameter(),
                queryStatement.isOutputEndTime()));
  }

  private void initNonAggQueryCommonVariables() {
    if (whereExpression != null) {
      if (!analysis.isTemplateWildCardQuery()) {
        newMeasurementList = new ArrayList<>(measurementList);
        newSchemaList = new ArrayList<>(schemaList);
        Set<String> selectExpressions = new HashSet<>(measurementList);
        List<Expression> whereSourceExpressions = searchSourceExpressions(whereExpression);
        for (Expression expression : whereSourceExpressions) {
          if (expression instanceof TimeSeriesOperand) {
            String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
            if (!analysis.getDeviceTemplate().getSchemaMap().containsKey(measurement)) {
              continue;
            }
            if (!selectExpressions.contains(measurement)) {
              selectExpressions.add(measurement);
              newMeasurementList.add(measurement);
              newSchemaList.add(analysis.getDeviceTemplate().getSchema(measurement));
            }
          }
        }
      }

      filterLayoutMap = makeLayout(newMeasurementList);

      analysis
          .getExpressionTypes()
          .forEach(
              (key, value) ->
                  context
                      .getTypeProvider()
                      .setTreeModelType(key.getNode().getOutputSymbol(), value));
    }

    List<Integer> deviceToMeasurementIndexes =
        new ArrayList<>(analysis.getSelectExpressions().size() - 1);
    for (int i = 1; i < analysis.getSelectExpressions().size(); i++) {
      deviceToMeasurementIndexes.add(i);
    }

    context
        .getTypeProvider()
        .setTemplatedInfo(
            new TemplatedInfo(
                newMeasurementList,
                newSchemaList,
                newSchemaList.stream()
                    .map(IMeasurementSchema::getType)
                    .collect(Collectors.toList()),
                queryStatement.getResultTimeOrder(),
                analysis.isLastLevelUseWildcard(),
                analysis.getDeviceViewOutputExpressions().stream()
                    .map(Expression::getExpressionString)
                    .collect(Collectors.toList()),
                deviceToMeasurementIndexes,
                OFFSET_VALUE,
                limitValue,
                whereExpression,
                queryStatement.isGroupByTime(),
                analysis.getDeviceTemplate().getSchemaMap(),
                filterLayoutMap,
                null,
                false,
                analysis.getGroupByTimeParameter(),
                queryStatement.isOutputEndTime()));
  }

  public PlanNode visitQuery() {
    if (queryStatement.isAggregationQuery()) {
      return visitAggregation();
    }

    LogicalPlanBuilder planBuilder =
        new TemplatedLogicalPlanBuilder(analysis, context, measurementList, schemaList);

    Map<IDeviceID, PlanNode> deviceToSubPlanMap = new LinkedHashMap<>();
    for (PartialPath devicePath : analysis.getDeviceList()) {
      IDeviceID deviceID = devicePath.getIDeviceIDAsFullDevice();
      PlanNode rootNode = visitQueryBody(devicePath);

      LogicalPlanBuilder subPlanBuilder =
          new TemplatedLogicalPlanBuilder(analysis, context, measurementList, schemaList)
              .withNewRoot(rootNode);

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

    if (!queryStatement.needPushDownSort()) {
      planBuilder = planBuilder.planOrderBy(queryStatement, analysis);
    }

    planBuilder =
        planBuilder
            .planFill(analysis.getFillDescriptor(), queryStatement.getResultTimeOrder())
            .planOffset(queryStatement.getRowOffset());

    if (!analysis.isUseTopKNode() || queryStatement.hasOffset()) {
      planBuilder = planBuilder.planLimit(queryStatement.getRowLimit());
    }

    return planBuilder.getRoot();
  }

  public PlanNode visitQueryBody(PartialPath devicePath) {

    TemplatedLogicalPlanBuilder planBuilder =
        new TemplatedLogicalPlanBuilder(analysis, context, newMeasurementList, newSchemaList);

    planBuilder =
        planBuilder
            .planRawDataSource(
                devicePath,
                queryStatement.getResultTimeOrder(),
                OFFSET_VALUE,
                limitValue,
                analysis.isLastLevelUseWildcard())
            .planFilter(
                whereExpression,
                queryStatement.isGroupByTime(),
                queryStatement.getResultTimeOrder());

    return planBuilder.getRoot();
  }

  // ============== Methods below are used for templated aggregation ======================

  private PlanNode visitAggregation() {
    // group by level and group by tag is not allowed in align by device
    boolean outputPartial =
        queryStatement.isGroupByTime() && analysis.getGroupByTimeParameter().hasOverlap();
    AggregationStep curStep = outputPartial ? AggregationStep.PARTIAL : AggregationStep.SINGLE;
    aggregationDescriptorList =
        constructAggregationDescriptorList(analysis.getAggregationExpressions(), curStep);
    updateTypeProvider(analysis.getAggregationExpressions());
    if (curStep.isOutputPartial()) {
      aggregationDescriptorList.forEach(
          aggregationDescriptor ->
              updateTypeProviderByPartialAggregation(
                  aggregationDescriptor, context.getTypeProvider()));
    }

    LogicalPlanBuilder templatedPlanBuilder =
        new TemplatedLogicalPlanBuilder(analysis, context, measurementList, schemaList);
    Map<IDeviceID, PlanNode> deviceToSubPlanMap = new LinkedHashMap<>();
    aggregationDescriptorList = getDeduplicatedDescriptors(aggregationDescriptorList);

    boolean needCheckAscending = queryStatement.isGroupByTime();
    List<AggregationDescriptor> ascendingDescriptors = new ArrayList<>();
    List<AggregationDescriptor> descendingDescriptors = new ArrayList<>();
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      if (needCheckAscending
          || SchemaUtils.isConsistentWithScanOrder(
              aggregationDescriptor.getAggregationType(), queryStatement.getResultTimeOrder())) {
        ascendingDescriptors.add(aggregationDescriptor);
      } else {
        descendingDescriptors.add(aggregationDescriptor);
      }
    }
    context.getTypeProvider().getTemplatedInfo().setAscendingDescriptorList(ascendingDescriptors);
    context.getTypeProvider().getTemplatedInfo().setDescendingDescriptorList(descendingDescriptors);

    for (PartialPath devicePath : analysis.getDeviceList()) {
      IDeviceID deviceName = devicePath.getIDeviceID();
      PlanNode rootNode = visitDeviceAggregationBody(devicePath);

      LogicalPlanBuilder subPlanBuilder =
          new TemplatedLogicalPlanBuilder(analysis, context, measurementList, schemaList)
              .withNewRoot(rootNode);

      deviceToSubPlanMap.put(deviceName, subPlanBuilder.getRoot());
    }

    templatedPlanBuilder =
        templatedPlanBuilder
            .planDeviceView(
                deviceToSubPlanMap,
                analysis.getDeviceViewOutputExpressions(),
                null,
                analysis.getSelectExpressions(),
                queryStatement,
                analysis)
            .planHavingAndTransform(
                analysis.getHavingExpression(),
                analysis.getSelectExpressions(),
                analysis.getOrderByExpressions(),
                queryStatement.isGroupByTime(),
                queryStatement.getResultTimeOrder());

    if (!queryStatement.needPushDownSort()) {
      templatedPlanBuilder = templatedPlanBuilder.planOrderBy(queryStatement, analysis);
    }

    templatedPlanBuilder =
        templatedPlanBuilder
            .planFill(analysis.getFillDescriptor(), queryStatement.getResultTimeOrder())
            .planOffset(queryStatement.getRowOffset());

    if (!analysis.isUseTopKNode() || queryStatement.hasOffset()) {
      templatedPlanBuilder = templatedPlanBuilder.planLimit(queryStatement.getRowLimit());
    }

    return templatedPlanBuilder.getRoot();
  }

  private PlanNode visitDeviceAggregationBody(PartialPath devicePath) {
    TemplatedLogicalPlanBuilder templatedPlanBuilder =
        new TemplatedLogicalPlanBuilder(analysis, context, newMeasurementList, newSchemaList);

    templatedPlanBuilder =
        templatedPlanBuilder
            .planRawDataSource(
                devicePath,
                queryStatement.getResultTimeOrder(),
                OFFSET_VALUE,
                limitValue,
                analysis.isLastLevelUseWildcard())
            .planFilter(
                whereExpression,
                queryStatement.isGroupByTime(),
                queryStatement.getResultTimeOrder())
            .planRawDataAggregation(
                analysis.getAggregationExpressions(),
                null,
                analysis.getGroupByTimeParameter(),
                analysis.getGroupByParameter(),
                queryStatement.isOutputEndTime(),
                queryStatement.getResultTimeOrder(),
                aggregationDescriptorList)
            .planSlidingWindowAggregation(
                queryStatement,
                analysis.getAggregationExpressions(),
                analysis.getGroupByTimeParameter(),
                queryStatement.getResultTimeOrder());

    return templatedPlanBuilder.getRoot();
  }

  private List<AggregationDescriptor> constructAggregationDescriptorList(
      Set<Expression> aggregationExpressions, AggregationStep curStep) {
    return aggregationExpressions.stream()
        .map(
            expression -> {
              Validate.isTrue(expression instanceof FunctionExpression);
              return new AggregationDescriptor(
                  ((FunctionExpression) expression).getFunctionName(),
                  curStep,
                  expression.getExpressions(),
                  ((FunctionExpression) expression).getFunctionAttributes());
            })
        .collect(Collectors.toList());
  }

  void updateTypeProvider(Collection<Expression> expressions) {
    if (expressions == null) {
      return;
    }
    expressions.forEach(
        expression -> {
          if (!expression.getExpressionString().equals(DEVICE)
              && !expression.getExpressionString().equals(ENDTIME)) {
            context
                .getTypeProvider()
                .setTreeModelType(expression.getExpressionString(), getPreAnalyzedType(expression));
          }
        });
  }

  private TSDataType getPreAnalyzedType(Expression expression) {
    return analysis.getType(expression);
  }
}
