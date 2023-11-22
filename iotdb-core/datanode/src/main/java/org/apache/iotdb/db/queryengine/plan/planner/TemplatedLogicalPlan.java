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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchSourceExpressions;
import static org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanVisitor.pushDownLimitToScanNode;

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

  public TemplatedLogicalPlan(
      Analysis analysis, QueryStatement queryStatement, MPPQueryContext context) {
    this.analysis = analysis;
    this.queryStatement = queryStatement;
    this.context = context;

    measurementList = analysis.getMeasurementList();
    schemaList = analysis.getMeasurementSchemaList();
  }

  public PlanNode visitQuery() {
    LogicalPlanBuilder planBuilder =
        new TemplatedLogicalPlanBuilder(analysis, context, measurementList, schemaList);

    Map<String, PlanNode> deviceToSubPlanMap = new LinkedHashMap<>();
    long limitValue = pushDownLimitToScanNode(queryStatement, analysis);
    for (PartialPath devicePath : analysis.getDeviceList()) {
      String deviceName = devicePath.getFullPath();
      PlanNode rootNode = visitQueryBody(devicePath, analysis, queryStatement, context, limitValue);

      LogicalPlanBuilder subPlanBuilder =
          new TemplatedLogicalPlanBuilder(analysis, context, measurementList, schemaList)
              .withNewRoot(rootNode);

      // sortOperator push down
      if (queryStatement.needPushDownSort()) {
        subPlanBuilder =
            subPlanBuilder.planOrderBy(
                analysis.getDeviceToOrderByExpressions().get(deviceName),
                analysis.getDeviceToSortItems().get(deviceName));
      }
      deviceToSubPlanMap.put(deviceName, subPlanBuilder.getRoot());
    }

    // convert to ALIGN BY DEVICE view
    planBuilder =
        planBuilder.planDeviceView(
            deviceToSubPlanMap,
            analysis.getDeviceViewOutputExpressions(),
            analysis.getDeviceViewInputIndexesMap(),
            analysis.getSelectExpressions(),
            queryStatement);

    if (planBuilder.getRoot() instanceof TopKNode) {
      analysis.setUseTopKNode();
    }

    if (!queryStatement.needPushDownSort()) {
      planBuilder =
          planBuilder.planOrderBy(
              queryStatement, analysis.getOrderByExpressions(), analysis.getSelectExpressions());
    }

    // other upstream node
    planBuilder =
        planBuilder
            .planFill(analysis.getFillDescriptor(), queryStatement.getResultTimeOrder())
            .planOffset(queryStatement.getRowOffset())
            .planLimit(queryStatement.getRowLimit());

    return planBuilder.getRoot();
  }

  public PlanNode visitQueryBody(
      PartialPath devicePath,
      Analysis analysis,
      QueryStatement queryStatement,
      MPPQueryContext context,
      long limitValue) {

    List<String> mergedMeasurementList = measurementList;
    List<IMeasurementSchema> mergedSchemaList = schemaList;

    // to fix this query: `select s1 from root.** where s2>1 align by device`
    // or `select s1 from root.** order by s2 align by device`.
    Expression whereExpression =
        analysis.getDeviceToWhereExpression() != null
            ? analysis.getDeviceToWhereExpression().get(devicePath.getFullPath())
            : null;
    if (whereExpression != null && !analysis.isTemplateWildCardQuery()) {
      mergedMeasurementList = new ArrayList<>(measurementList);
      mergedSchemaList = new ArrayList<>(schemaList);
      Set<String> selectExpressions = new HashSet<>(measurementList);
      List<Expression> whereSourceExpressions = searchSourceExpressions(whereExpression);
      for (Expression expression : whereSourceExpressions) {
        if (expression instanceof TimeSeriesOperand) {
          String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
          if (!selectExpressions.contains(measurement)) {
            selectExpressions.add(measurement);
            mergedMeasurementList.add(measurement);
            mergedSchemaList.add(analysis.getDeviceTemplate().getSchema(measurement));
          }
        }
      }
    }

    TemplatedLogicalPlanBuilder planBuilder =
        new TemplatedLogicalPlanBuilder(analysis, context, mergedMeasurementList, mergedSchemaList);

    planBuilder =
        planBuilder.planRawDataSource(
            devicePath,
            queryStatement.getResultTimeOrder(),
            analysis.getGlobalTimeFilter(),
            0,
            limitValue,
            analysis.isLastLevelUseWildcard());

    if (whereExpression != null) {
      Expression[] outputExpressions = new Expression[measurementList.size()];
      for (int i = 0; i < analysis.getMeasurementList().size(); i++) {
        outputExpressions[i] =
            new TimeSeriesOperand(
                new MeasurementPath(
                    devicePath.concatNode(measurementList.get(i)).getNodes(), schemaList.get(i)));
      }

      planBuilder =
          planBuilder.planFilter(
              whereExpression,
              outputExpressions,
              queryStatement.isGroupByTime(),
              queryStatement.getSelectComponent().getZoneId(),
              queryStatement.getResultTimeOrder());
    }

    if (context.getTypeProvider().getTemplatedInfo() == null) {
      context
          .getTypeProvider()
          .setTemplatedInfo(
              new TemplatedInfo(
                  mergedMeasurementList,
                  mergedSchemaList,
                  mergedSchemaList.stream()
                      .map(IMeasurementSchema::getType)
                      .collect(Collectors.toList()),
                  new HashSet<>(mergedMeasurementList),
                  analysis.getGlobalTimeFilter(),
                  queryStatement.getResultTimeOrder(),
                  analysis.isLastLevelUseWildcard(),
                  0,
                  limitValue));
    }

    return planBuilder.getRoot();
  }
}
