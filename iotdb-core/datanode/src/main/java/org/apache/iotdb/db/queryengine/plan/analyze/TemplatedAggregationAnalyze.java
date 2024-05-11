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
package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.DEVICE_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.END_TIME_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeExpressionType;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDataPartition;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceToWhere;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewInput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeFrom;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetInGroupByTimeForDevice;

/** Methods in this class are used for aggregation, templated with align by device situation. */
public class TemplatedAggregationAnalyze {

  static boolean analyzeAggregation(
      Analysis analysis,
      QueryStatement queryStatement,
      IPartitionFetcher partitionFetcher,
      ISchemaTree schemaTree,
      MPPQueryContext context,
      Template template) {

    List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

    if (canPushDownLimitOffsetInGroupByTimeForDevice(queryStatement)) {
      // remove the device which won't appear in resultSet after limit/offset
      deviceList = pushDownLimitOffsetInGroupByTimeForDevice(deviceList, queryStatement);
    }

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    analyzeSelect(queryStatement, analysis, outputExpressions, template);

    analyzeDeviceToWhere(analysis, queryStatement);
    if (deviceList.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return true;
    }
    analysis.setDeviceList(deviceList);

    if (analysis.getWhereExpression() != null
        && ConstantOperand.FALSE.equals(analysis.getWhereExpression())) {
      analyzeOutput(analysis, queryStatement, outputExpressions);
      analysis.setFinishQueryAfterAnalyze(true);
      return true;
    }

    analyzeDeviceToAggregation(analysis);
    analyzeDeviceToSourceTransform(analysis);
    analyzeDeviceToSource(analysis);

    analyzeDeviceViewOutput(analysis, queryStatement);
    analyzeDeviceViewInput(analysis);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);

    context.generateGlobalTimeFilter(analysis);
    // fetch partition information
    analyzeDataPartition(analysis, schemaTree, partitionFetcher, context.getGlobalTimeFilter());
    return true;
  }

  private static void analyzeSelect(
      QueryStatement queryStatement,
      Analysis analysis,
      List<Pair<Expression, String>> outputExpressions,
      Template template) {

    LinkedHashSet<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(DEVICE_EXPRESSION);
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }

    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());

    Set<Expression> aggregationExpressions = new LinkedHashSet<>();
    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      if (paginationController.hasCurOffset()) {
        paginationController.consumeOffset();
      } else if (paginationController.hasCurLimit()) {
        Expression selectExpression = resultColumn.getExpression();
        outputExpressions.add(new Pair<>(selectExpression, resultColumn.getAlias()));
        selectExpressions.add(selectExpression);
        aggregationExpressions.add(selectExpression);
      } else {
        break;
      }
    }

    analysis.setDeviceTemplate(template);
    List<String> measurementList = new ArrayList<>();
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    Set<String> measurementSet = new HashSet<>();
    for (Expression selectExpression : selectExpressions) {
      if ("device".equalsIgnoreCase(selectExpression.getOutputSymbol())) {
        continue;
      }

      String measurement = selectExpression.getExpressions().get(0).getOutputSymbol();
      if (!template.getSchemaMap().containsKey(measurement)) {
        throw new IllegalArgumentException(
            "Measurement " + measurement + " is not found in template");
      }

      // for agg1(s1) + agg2(s1), only record s1 for one time
      if (!measurementSet.contains(measurement)) {
        measurementSet.add(measurement);
        measurementList.add(measurement);
        measurementSchemaList.add(template.getSchemaMap().get(measurement));
      }

      analyzeExpressionType(analysis, selectExpression);
    }

    analysis.setMeasurementList(measurementList);
    analysis.setMeasurementSchemaList(measurementSchemaList);
    analysis.setAggregationExpressions(aggregationExpressions);
    analysis.setOutputExpressions(outputExpressions);
    analysis.setSelectExpressions(selectExpressions);
  }

  private static void analyzeDeviceToSourceTransform(Analysis analysis) {
    // TODO add having into SourceTransform
    analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceToSource(Analysis analysis) {
    // TODO add having into Source
    analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
    analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceToAggregation(Analysis analysis) {
    // TODO need add having clause?
    analysis.setDeviceToAggregationExpressions(analysis.getDeviceToSelectExpressions());
  }
}
