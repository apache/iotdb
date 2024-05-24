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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
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
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeGroupByTime;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.normalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDataPartition;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceToWhere;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewInput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeFrom;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetInGroupByTimeForDevice;

/** Methods in this class are used for aggregation, templated with align by device situation. */
public class TemplatedAggregationAnalyze {

  static boolean canBuildAggregationPlanUseTemplate(
      Analysis analysis,
      QueryStatement queryStatement,
      IPartitionFetcher partitionFetcher,
      ISchemaTree schemaTree,
      MPPQueryContext context,
      Template template) {

    // not support order by expression and non-aligned template
    if (queryStatement.hasOrderByExpression() || !template.isDirectAligned()) {
      return false;
    }

    analysis.setNoWhereAndAggregation(false);

    List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

    if (canPushDownLimitOffsetInGroupByTimeForDevice(queryStatement)) {
      // remove the device which won't appear in resultSet after limit/offset
      deviceList = pushDownLimitOffsetInGroupByTimeForDevice(deviceList, queryStatement);
    }

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    boolean valid = analyzeSelect(queryStatement, analysis, outputExpressions, template);
    if (!valid) {
      return false;
    }

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

    analyzeHaving(analysis, queryStatement);

    analyzeDeviceToAggregation(analysis);
    analyzeDeviceToSourceTransform(analysis);
    analyzeDeviceToSource(analysis);

    analyzeDeviceViewOutput(analysis, queryStatement);
    analyzeDeviceViewInput(analysis, queryStatement);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);

    analyzeGroupByTime(analysis, queryStatement);
    context.generateGlobalTimeFilter(analysis);

    // fetch partition information
    analyzeDataPartition(analysis, schemaTree, partitionFetcher, context.getGlobalTimeFilter());
    return true;
  }

  private static boolean analyzeSelect(
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
        analysis.setDeviceTemplate(null);
        // TODO not support agg(*), agg(s1+1), count_time(*) now
        return false;
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
    return true;
  }

  private static void analyzeHaving(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.hasHaving()) {
      return;
    }

    // TODO not support having count(s1) + sum(s2) expression
    Set<Expression> aggregationExpressions = analysis.getAggregationExpressions();

    Expression havingExpression = queryStatement.getHavingCondition().getPredicate();

    // Set<Expression> normalizedAggregationExpressions = new LinkedHashSet<>();
    for (Expression aggregationExpression : searchAggregationExpressions(havingExpression)) {
      Expression normalizedAggregationExpression = normalizeExpression(aggregationExpression);

      analyzeExpressionType(analysis, aggregationExpression);
      analyzeExpressionType(analysis, normalizedAggregationExpression);

      aggregationExpressions.add(aggregationExpression);
      // normalizedAggregationExpressions.add(normalizedAggregationExpression);
    }

    TSDataType outputType = analyzeExpressionType(analysis, havingExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(
          String.format(
              "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: %s.",
              outputType));
    }
    analysis.setHavingExpression(havingExpression);
  }

  private static void analyzeDeviceToSourceTransform(Analysis analysis) {
    // TODO add having into SourceTransform
    analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceToSource(Analysis analysis) {
    analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
    analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceToAggregation(Analysis analysis) {
    // TODO need add having clause?
    analysis.setDeviceToAggregationExpressions(analysis.getDeviceToSelectExpressions());
  }
}
