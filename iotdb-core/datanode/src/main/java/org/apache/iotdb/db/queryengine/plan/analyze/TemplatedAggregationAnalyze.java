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
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ENDTIME;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.DEVICE_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeExpressionType;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeGroupByTime;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.normalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDataPartition;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceToWhere;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeDeviceViewOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAnalyze.analyzeFrom;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

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
      analysis.setDeviceTemplate(null);
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

    valid = analyzeHaving(analysis, queryStatement);
    if (!valid) {
      analysis.setDeviceTemplate(null);
      return false;
    }

    analyzeDeviceToExpressions(analysis);

    analyzeDeviceViewOutput(analysis, queryStatement);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);

    analyzeGroupByTime(analysis, queryStatement);
    context.generateGlobalTimeFilter(analysis);

    // fetch partition information
    analyzeDataPartition(analysis, schemaTree, partitionFetcher, context);
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
      return false;
    }

    analysis.setDeviceTemplate(template);

    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());

    Set<Expression> aggregationExpressions = new LinkedHashSet<>();
    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      Expression selectExpression = resultColumn.getExpression();

      if (selectExpression instanceof FunctionExpression
          && COUNT_TIME.equalsIgnoreCase(
              ((FunctionExpression) selectExpression).getFunctionName())) {
        outputExpressions.add(new Pair<>(selectExpression, resultColumn.getAlias()));
        selectExpressions.add(selectExpression);
        aggregationExpressions.add(selectExpression);

        analysis.getExpressionTypes().put(NodeRef.of(selectExpression), TSDataType.INT64);
        ((FunctionExpression) selectExpression)
            .setExpressions(Collections.singletonList(new TimestampOperand()));
        continue;
      }

      List<Expression> subExpressions;
      if (selectExpression.getOutputSymbol().contains("*")) {
        // when exist wildcard, only support agg(*) and count_time(*)
        if (selectExpression instanceof FunctionExpression
            && selectExpression.getExpressions().size() == 1
            && "*".equalsIgnoreCase(selectExpression.getExpressions().get(0).getOutputSymbol())) {
          subExpressions = new ArrayList<>();
          FunctionExpression functionExpression = (FunctionExpression) selectExpression;
          for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
            FunctionExpression subFunctionExpression =
                new FunctionExpression(
                    functionExpression.getFunctionName(),
                    functionExpression.getFunctionAttributes(),
                    Collections.singletonList(
                        new TimeSeriesOperand(
                            new PartialPath(new String[] {entry.getKey()}),
                            entry.getValue().getType())));
            subFunctionExpression.setFunctionType(functionExpression.getFunctionType());
            subExpressions.add(subFunctionExpression);
          }
        } else {
          return false;
        }
      } else {
        subExpressions = Collections.singletonList(selectExpression);
      }

      for (Expression expression : subExpressions) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
        } else if (paginationController.hasCurLimit()) {
          outputExpressions.add(new Pair<>(expression, resultColumn.getAlias()));
          selectExpressions.add(expression);
          aggregationExpressions.add(expression);
          analyzeExpressionType(analysis, expression);
        } else {
          break;
        }
      }
    }

    List<String> measurementList = new ArrayList<>();
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    Set<String> measurementSet = new HashSet<>();

    if (queryStatement.isCountTimeAggregation()) {
      measurementList = new ArrayList<>(template.getSchemaMap().keySet());
      measurementSchemaList = new ArrayList<>(template.getSchemaMap().values());
    } else {
      int idx = 0;
      for (Expression selectExpression : selectExpressions) {
        idx++;
        if (idx == 1
            || (idx == 2 && ENDTIME.equalsIgnoreCase(selectExpression.getOutputSymbol()))) {
          continue;
        }

        String measurement = selectExpression.getExpressions().get(0).getOutputSymbol();
        //  not support agg(*), agg(s1+1) now
        if (!template.getSchemaMap().containsKey(measurement)) {
          return false;
        }

        // for agg1(s1) + agg2(s1), only record s1 for one time
        if (!measurementSet.contains(measurement)) {
          measurementSet.add(measurement);
          measurementList.add(measurement);
          measurementSchemaList.add(template.getSchemaMap().get(measurement));
        }
      }
    }

    analysis.setMeasurementList(measurementList);
    analysis.setMeasurementSchemaList(measurementSchemaList);
    analysis.setAggregationExpressions(aggregationExpressions);
    analysis.setOutputExpressions(outputExpressions);
    analysis.setSelectExpressions(selectExpressions);
    return true;
  }

  private static boolean analyzeHaving(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.hasHaving()) {
      return true;
    }

    Set<String> measurementSet = new HashSet<>(analysis.getMeasurementList());
    Set<Expression> aggregationExpressions = analysis.getAggregationExpressions();
    Expression havingExpression = queryStatement.getHavingCondition().getPredicate();
    for (Expression aggregationExpression : searchAggregationExpressions(havingExpression)) {
      Expression normalizedAggregationExpression = normalizeExpression(aggregationExpression);

      // not support having agg(s1+s2) temporarily
      if (!((normalizedAggregationExpression).getExpressions().get(0)
          instanceof TimeSeriesOperand)) {
        return false;
      }

      String measurement =
          normalizedAggregationExpression.getExpressions().get(0).getOutputSymbol();
      if (!measurementSet.contains(measurement)) {
        // adapt this case: select agg(s1) from xx having agg(s3)
        measurementSet.add(measurement);
        analysis.getMeasurementList().add(measurement);
        analysis
            .getMeasurementSchemaList()
            .add(analysis.getDeviceTemplate().getSchema(measurement));
      }

      analyzeExpressionType(analysis, aggregationExpression);
      analyzeExpressionType(analysis, normalizedAggregationExpression);

      aggregationExpressions.add(aggregationExpression);
    }

    TSDataType outputType = analyzeExpressionType(analysis, havingExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(
          String.format(
              "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: %s.",
              outputType));
    }
    analysis.setHavingExpression(havingExpression);

    return true;
  }

  private static void analyzeDeviceToExpressions(Analysis analysis) {
    analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());

    analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
    analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());

    analysis.setDeviceToAggregationExpressions(analysis.getDeviceToSelectExpressions());
  }
}
