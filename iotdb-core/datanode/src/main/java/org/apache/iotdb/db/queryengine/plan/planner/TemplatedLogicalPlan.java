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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.DEVICE;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ENDTIME;

public class TemplatedLogicalPlan {

  private Analysis analysis;
  private QueryStatement queryStatement;
  private MPPQueryContext context;
  // If it's not `select *` query, may change the value of list below
  List<String> measurementList = null;
  List<IMeasurementSchema> schemaList = null;

  public TemplatedLogicalPlan(
      Analysis analysis, QueryStatement queryStatement, MPPQueryContext context) {
    this.analysis = analysis;
    this.queryStatement = queryStatement;
    this.context = context;

    measurementList = new ArrayList<>(analysis.getTemplateTypes().getSchemaMap().keySet());
    schemaList = new ArrayList<>(analysis.getTemplateTypes().getSchemaMap().values());
  }

  public PlanNode visitQuery() {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    Map<String, PlanNode> deviceToSubPlanMap = new LinkedHashMap<>();
    for (PartialPath devicePath : analysis.getDeviceList()) {
      String deviceName = devicePath.getFullPath();
      LogicalPlanBuilder subPlanBuilder = new LogicalPlanBuilder(analysis, context);
      subPlanBuilder =
          subPlanBuilder.withNewRoot(
              visitQueryBody(
                  devicePath,
                  analysis,
                  queryStatement,
                  analysis.getDeviceToSourceExpressions().get(deviceName),
                  analysis.getDeviceToSourceTransformExpressions().get(deviceName),
                  analysis.getDeviceToWhereExpression() != null
                      ? analysis.getDeviceToWhereExpression().get(deviceName)
                      : null,
                  analysis.getDeviceToAggregationExpressions().get(deviceName),
                  analysis.getDeviceToGroupByExpression() != null
                      ? analysis.getDeviceToGroupByExpression().get(deviceName)
                      : null,
                  analysis.getDeviceViewInputIndexesMap().get(deviceName),
                  context));

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

    if (queryStatement.isAggregationQuery()) {
      planBuilder =
          planBuilder.planHavingAndTransform(
              analysis.getHavingExpression(),
              analysis.getSelectExpressions(),
              analysis.getOrderByExpressions(),
              queryStatement.isGroupByTime(),
              queryStatement.getSelectComponent().getZoneId(),
              queryStatement.getResultTimeOrder());
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

    // plan select into
    if (queryStatement.isAlignByDevice()) {
      planBuilder = planBuilder.planDeviceViewInto(analysis.getDeviceViewIntoPathDescriptor());
    } else {
      planBuilder = planBuilder.planInto(analysis.getIntoPathDescriptor());
    }

    return planBuilder.getRoot();
  }

  public PlanNode visitQueryBody(
      PartialPath devicePath,
      Analysis analysis,
      QueryStatement queryStatement,
      Set<Expression> sourceExpressions,
      Set<Expression> sourceTransformExpressions,
      Expression whereExpression,
      Set<Expression> aggregationExpressions,
      Expression groupByExpression,
      List<Integer> deviceViewInputIndexes,
      MPPQueryContext context) {
    return planRawDataSource(
        devicePath,
        queryStatement.getResultTimeOrder(),
        analysis.getGlobalTimeFilter(),
        0,
        pushDownLimitToScanNode(queryStatement),
        analysis.isLastLevelUseWildcard());
  }

  private long pushDownLimitToScanNode(QueryStatement queryStatement) {
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

  public PlanNode planRawDataSource(
      PartialPath devicePath,
      Ordering scanOrder,
      Filter timeFilter,
      long offset,
      long limit,
      boolean lastLevelUseWildcard) {
    List<PlanNode> sourceNodeList = new ArrayList<>();

    AlignedPath path = new AlignedPath(devicePath);
    path.setMeasurementList(measurementList);
    path.addSchemas(schemaList);

    AlignedSeriesScanNode alignedSeriesScanNode =
        new AlignedSeriesScanNode(
            context.getQueryId().genPlanNodeId(),
            path,
            scanOrder,
            timeFilter,
            timeFilter,
            limit,
            offset,
            null,
            lastLevelUseWildcard);
    sourceNodeList.add(alignedSeriesScanNode);

    // just group and put into type provider
    // updateTypeProvider(sourceExpressions);

    return convergeWithTimeJoin(sourceNodeList, scanOrder);
  }

  private void updateTypeProvider(Collection<Expression> expressions) {
    if (expressions == null) {
      return;
    }
    expressions.forEach(
        expression -> {
          if (!expression.getExpressionString().equals(DEVICE)
              && !expression.getExpressionString().equals(ENDTIME)) {
            context
                .getTypeProvider()
                .setType(expression.getExpressionString(), analysis.getType(expression));
          }
        });
  }

  private PlanNode convergeWithTimeJoin(List<PlanNode> sourceNodes, Ordering mergeOrder) {
    PlanNode tmpNode;
    if (sourceNodes.size() == 1) {
      tmpNode = sourceNodes.get(0);
    } else {
      tmpNode = new TimeJoinNode(context.getQueryId().genPlanNodeId(), mergeOrder, sourceNodes);
    }
    return tmpNode;
  }
}
