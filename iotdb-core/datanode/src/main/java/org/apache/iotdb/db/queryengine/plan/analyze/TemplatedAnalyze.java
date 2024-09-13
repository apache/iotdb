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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.TemplatedConcatRemoveUnExistentMeasurementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TREE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.CONFIG;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.DEVICE_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.END_TIME_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.WHERE_WRONG_TYPE_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeDeviceViewSpecialProcess;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeExpressionType;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeFill;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.concatDeviceAndBindSchemaForExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.getMeasurementExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer.analyzeExpressionForTemplatedQuery;
import static org.apache.iotdb.db.queryengine.plan.analyze.TemplatedAggregationAnalyze.canBuildAggregationPlanUseTemplate;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices with same template, using template can avoid many
 * unnecessary judgements.
 *
 * <p>e.g. for query `SELECT * FROM root.xx.** order by device/time/expression align by device`, the
 * device list of `root.xx.**` must use same template.
 */
public class TemplatedAnalyze {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplatedAnalyze.class);

  private TemplatedAnalyze() {}

  /**
   * examine that if all devices are in same template, if true, use the TemplatedAnalyze,
   * TemplatedLogicalPlan, TemplatedLogicalPlanBuilder to optimize it.
   */
  public static boolean canBuildPlanUseTemplate(
      Analysis analysis,
      QueryStatement queryStatement,
      IPartitionFetcher partitionFetcher,
      ISchemaTree schemaTree,
      MPPQueryContext context) {
    if (queryStatement.getGroupByComponent() != null
        || queryStatement.isSelectInto()
        || queryStatement.hasFill()
        || schemaTree.hasNormalTimeSeries()) {
      return false;
    }

    List<Template> templates = schemaTree.getUsingTemplates();
    if (templates.size() != 1 || templates.get(0) == null) {
      return false;
    }

    Template template = templates.get(0);

    if (queryStatement.isAggregationQuery()) {
      return canBuildAggregationPlanUseTemplate(
          analysis, queryStatement, partitionFetcher, schemaTree, context, template);
    }

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if ("*".equals(expression.getOutputSymbol())) {
        for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
          } else if (paginationController.hasCurLimit()) {
            String measurementName = entry.getKey();
            TimeSeriesOperand measurementPath =
                new TimeSeriesOperand(
                    new PartialPath(new String[] {measurementName}), entry.getValue().getType());
            // reserve memory for this expression
            context.reserveMemoryForFrontEnd(measurementPath.ramBytesUsed());
            outputExpressions.add(new Pair<>(measurementPath, null));
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        if (queryStatement.getSelectComponent().getResultColumns().size() == 1
            && queryStatement.getSeriesOffset() == 0
            && queryStatement.getSeriesLimit() == 0) {
          analysis.setTemplateWildCardQuery();
        }
      } else if (expression instanceof TimeSeriesOperand) {
        String measurementName = ((TimeSeriesOperand) expression).getPath().getMeasurement();
        if (template.getSchemaMap().containsKey(measurementName)) {
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
          } else if (paginationController.hasCurLimit()) {
            TimeSeriesOperand measurementPath =
                new TimeSeriesOperand(
                    new PartialPath(new String[] {measurementName}),
                    template.getSchemaMap().get(measurementName).getType());
            // reserve memory for this expression
            context.reserveMemoryForFrontEnd(measurementPath.ramBytesUsed());
            outputExpressions.add(new Pair<>(measurementPath, resultColumn.getAlias()));
          } else {
            break;
          }
        }
      } else {
        return false;
      }
    }

    if (queryStatement.hasOrderByExpression()) {
      return false;
    }

    analyzeSelect(queryStatement, analysis, outputExpressions, template);

    List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

    analyzeDeviceToWhere(analysis, queryStatement);
    if (analysis.getWhereExpression() != null
        && analysis.getWhereExpression().equals(ConstantOperand.FALSE)) {
      analyzeOutput(analysis, queryStatement, outputExpressions);
      analysis.setFinishQueryAfterAnalyze(true);
      return true;
    }

    if (deviceList.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return true;
    }
    analysis.setDeviceList(deviceList);

    analyzeDeviceToOrderBy(analysis, queryStatement, schemaTree, deviceList, context);
    analyzeDeviceToSourceTransform(analysis);
    analyzeDeviceToSource(analysis);

    analyzeDeviceViewOutput(analysis, queryStatement);

    analyzeFill(analysis, queryStatement);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);

    context.generateGlobalTimeFilter(analysis);
    // fetch partition information
    analyzeDataPartition(analysis, schemaTree, partitionFetcher, context);
    return true;
  }

  private static void analyzeSelect(
      QueryStatement queryStatement,
      Analysis analysis,
      List<Pair<Expression, String>> outputExpressions,
      Template template) {
    List<String> measurementList = new ArrayList<>();
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    LinkedHashSet<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(DEVICE_EXPRESSION);
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }
    for (Pair<Expression, String> pair : outputExpressions) {
      if (!selectExpressions.contains(pair.left)) {
        selectExpressions.add(pair.left);
        String measurementName = ((TimeSeriesOperand) pair.getLeft()).getPath().getMeasurement();
        measurementList.add(measurementName);
        measurementSchemaList.add(template.getSchema(measurementName));
      }
    }
    analysis.setOutputExpressions(outputExpressions);
    analysis.setSelectExpressions(selectExpressions);
    analysis.setDeviceTemplate(template);
    analysis.setMeasurementList(measurementList);
    analysis.setMeasurementSchemaList(measurementSchemaList);
  }

  static List<PartialPath> analyzeFrom(QueryStatement queryStatement, ISchemaTree schemaTree) {
    // device path patterns in FROM clause
    List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();

    Set<PartialPath> deviceSet = new HashSet<>();
    for (PartialPath devicePattern : devicePatternList) {
      deviceSet.addAll(
          schemaTree.getMatchedDevices(devicePattern).stream()
              .map(DeviceSchemaInfo::getDevicePath)
              .collect(Collectors.toList()));
    }

    return queryStatement.getResultDeviceOrder() == Ordering.ASC
        ? deviceSet.stream().sorted().collect(Collectors.toList())
        : deviceSet.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
  }

  static void analyzeDeviceToWhere(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.hasWhere()) {
      return;
    }

    analysis.setNoWhereAndAggregation(false);
    Expression wherePredicate =
        new TemplatedConcatRemoveUnExistentMeasurementVisitor()
            .process(
                queryStatement.getWhereCondition().getPredicate(),
                analysis.getDeviceTemplate().getSchemaMap());
    wherePredicate = PredicateUtils.simplifyPredicate(wherePredicate);
    if (!wherePredicate.equals(ConstantOperand.TRUE)) {
      analysis.setWhereExpression(wherePredicate);

      TSDataType outputType = analyzeExpressionForTemplatedQuery(analysis, wherePredicate);
      if (outputType != TSDataType.BOOLEAN) {
        throw new SemanticException(String.format(WHERE_WRONG_TYPE_ERROR_MSG, outputType));
      }
    }
  }

  static void analyzeDeviceToOrderBy(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceSet,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasOrderByExpression()) {
      return;
    }

    Map<IDeviceID, Set<Expression>> deviceToOrderByExpressions = new LinkedHashMap<>();
    Map<IDeviceID, List<SortItem>> deviceToSortItems = new LinkedHashMap<>();
    // build the device-view outputColumn for the sortNode above the deviceViewNode
    Set<Expression> deviceViewOrderByExpression = new LinkedHashSet<>();
    for (PartialPath device : deviceSet) {
      Set<Expression> orderByExpressionsForOneDevice = new LinkedHashSet<>();
      for (Expression expressionForItem : queryStatement.getExpressionSortItemList()) {
        List<Expression> expressions =
            concatDeviceAndBindSchemaForExpression(
                expressionForItem, device, schemaTree, queryContext);
        if (expressions.isEmpty()) {
          throw new SemanticException(
              String.format(
                  "%s in order by clause doesn't exist.", expressionForItem.getExpressionString()));
        }
        if (expressions.size() > 1) {
          throw new SemanticException(
              String.format(
                  "%s in order by clause shouldn't refer to more than one timeseries.",
                  expressionForItem.getExpressionString()));
        }
        expressionForItem = expressions.get(0);
        TSDataType dataType = analyzeExpressionType(analysis, expressionForItem);
        if (!dataType.isComparable()) {
          throw new SemanticException(
              String.format("The data type of %s is not comparable", dataType));
        }

        Expression deviceViewExpression = getMeasurementExpression(expressionForItem, analysis);
        analyzeExpressionType(analysis, deviceViewExpression);

        deviceViewOrderByExpression.add(deviceViewExpression);
        orderByExpressionsForOneDevice.add(expressionForItem);
      }
      deviceToSortItems.put(
          device.getIDeviceIDAsFullDevice(),
          queryStatement.getUpdatedSortItems(orderByExpressionsForOneDevice));
      deviceToOrderByExpressions.put(device.getIDeviceID(), orderByExpressionsForOneDevice);
    }

    analysis.setOrderByExpressions(deviceViewOrderByExpression);
    queryStatement.updateSortItems(deviceViewOrderByExpression);
    analysis.setDeviceToSortItems(deviceToSortItems);
    analysis.setDeviceToOrderByExpressions(deviceToOrderByExpressions);
  }

  private static void analyzeDeviceToSourceTransform(Analysis analysis) {
    analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());
  }

  static void analyzeDeviceViewOutput(Analysis analysis, QueryStatement queryStatement) {
    Set<Expression> selectExpressions = analysis.getSelectExpressions();
    // if no order by, just set deviceViewOutputExpressions as selectExpressions
    Set<Expression> deviceViewOutputExpressions = new LinkedHashSet<>();

    if (queryStatement.isAggregationQuery()) {
      deviceViewOutputExpressions.add(DEVICE_EXPRESSION);
      if (queryStatement.isOutputEndTime()) {
        deviceViewOutputExpressions.add(END_TIME_EXPRESSION);
      }
      for (Expression selectExpression : selectExpressions) {
        deviceViewOutputExpressions.addAll(searchAggregationExpressions(selectExpression));
      }
      if (queryStatement.hasHaving()) {
        deviceViewOutputExpressions.addAll(
            searchAggregationExpressions(analysis.getHavingExpression()));
      }
      if (queryStatement.hasOrderByExpression()) {
        for (Expression orderByExpression : analysis.getOrderByExpressions()) {
          deviceViewOutputExpressions.addAll(searchAggregationExpressions(orderByExpression));
        }
      }
    } else {
      deviceViewOutputExpressions.addAll(selectExpressions);
      if (queryStatement.hasOrderByExpression()) {
        deviceViewOutputExpressions.addAll(analysis.getOrderByExpressions());
      }
    }

    analysis.setDeviceViewOutputExpressions(deviceViewOutputExpressions);
    analysis.setDeviceViewSpecialProcess(
        analyzeDeviceViewSpecialProcess(deviceViewOutputExpressions, queryStatement, analysis));
  }

  private static void analyzeDeviceToSource(Analysis analysis) {
    analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
    analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());
  }

  static void analyzeDataPartition(
      Analysis analysis,
      ISchemaTree schemaTree,
      IPartitionFetcher partitionFetcher,
      MPPQueryContext context) {
    // TemplatedDevice has no views, so there is no need to use outputDeviceToQueriedDevicesMap
    Set<IDeviceID> deviceSet =
        analysis.getDeviceList().stream()
            .map(PartialPath::getIDeviceIDAsFullDevice)
            .collect(Collectors.toSet());
    DataPartition dataPartition =
        fetchDataPartitionByDevices(deviceSet, schemaTree, context, partitionFetcher);
    analysis.setDataPartitionInfo(dataPartition);
  }

  private static DataPartition fetchDataPartitionByDevices(
      Set<IDeviceID> deviceSet,
      ISchemaTree schemaTree,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher) {
    long startTime = System.nanoTime();
    try {
      Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
          getTimePartitionSlotList(context.getGlobalTimeFilter(), context);
      // there is no satisfied time range
      if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
        return new DataPartition(
            Collections.emptyMap(),
            CONFIG.getSeriesPartitionExecutorClass(),
            CONFIG.getSeriesPartitionSlotNum());
      }
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (IDeviceID deviceID : deviceSet) {
        DataPartitionQueryParam queryParam =
            new DataPartitionQueryParam(deviceID, res.left, res.right.left, res.right.right);
        sgNameToQueryParamsMap
            .computeIfAbsent(schemaTree.getBelongedDatabase(deviceID), key -> new ArrayList<>())
            .add(queryParam);
      }

      if (res.right.left || res.right.right) {
        return partitionFetcher.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
      } else {
        return partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      }
    } finally {
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TREE_TYPE, PARTITION_FETCHER, System.nanoTime() - startTime);
    }
  }
}
