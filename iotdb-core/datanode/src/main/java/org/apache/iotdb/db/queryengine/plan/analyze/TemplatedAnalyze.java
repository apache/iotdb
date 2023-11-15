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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.MeasurementNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
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
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.normalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices with same template, using template can avoid many
 * unnecessary judgements.
 *
 * <p>e.g. for query `SELECT * FROM root.xx.** order by device/time/expression align by device`, the
 * device list of `root.xx.**` must use same template.
 */
public class TemplatedAnalyze {

  private static final Logger logger = LoggerFactory.getLogger(TemplatedAnalyze.class);

  /** examine that if all devices are in same template */
  public static boolean canBuildPlanUseTemplate(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaFetcher schemaFetcher,
      IPartitionFetcher partitionFetcher,
      ISchemaTree schemaTree) {
    if (queryStatement.isAggregationQuery()
        || queryStatement.isGroupBy()
        || queryStatement.isGroupByTime()
        || queryStatement.isSelectInto()) {
      return false;
    }

    Template template = null;
    List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();
    for (PartialPath devicePath : devicePatternList) {
      Map<Integer, Template> templateMap = schemaFetcher.checkAllRelatedTemplate(devicePath);
      if (templateMap != null && templateMap.size() == 1) {
        if (template == null) {
          template = templateMap.values().iterator().next();
        } else {
          if (templateMap.values().iterator().next().getId() != template.getId()) {
            template = null;
            break;
          }
        }
      } else {
        return false;
      }
    }

    List<String> measurementList = new ArrayList<>();
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    if (template != null) {
      for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if ("*".equals(expression.getOutputSymbol())) {
          for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
            measurementList.add(entry.getKey());
            measurementSchemaList.add(entry.getValue());
          }
        } else if (expression instanceof TimeSeriesOperand) {
          String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
          if (template.getSchemaMap().containsKey(measurement)) {
            measurementList.add(measurement);
            measurementSchemaList.add(template.getSchema(measurement));
          }
        } else {
          return false;
        }
      }
    }

    analysis.setDeviceTemplate(template);
    analysis.setMeasurementList(measurementList);
    analysis.setMeasurementSchemaList(measurementSchemaList);

    visitQuery(analysis, queryStatement, partitionFetcher, schemaTree);

    return true;
  }

  /** add multi-threading impl */
  public static void visitQuery(
      Analysis analysis,
      QueryStatement queryStatement,
      IPartitionFetcher partitionFetcher,
      ISchemaTree schemaTree) {

    long startTime = System.currentTimeMillis();

    List<Pair<Expression, String>> outputExpressions;

    List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);
    logger.warn("--- [analyzeFrom] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    analyzeDeviceToWhere(analysis, queryStatement, schemaTree, deviceList);
    logger.warn("--- [analyzeDeviceToWhere] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    // deviceToSelectExpression, deviceToSourceTransformExpression and deviceToOutputExpression is
    // all no need.
    outputExpressions = analyzeSelect(analysis, queryStatement);
    logger.warn("--- [analyzeSelect] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    if (deviceList.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return;
    }
    analysis.setDeviceList(deviceList);

    analyzeDeviceToOrderBy(analysis, queryStatement, schemaTree, deviceList);
    analyzeDeviceToSourceTransform(analysis);
    analyzeDeviceToSource(analysis);

    logger.warn(
        "--- [analyzeDeviceToSource + analyzeDeviceToSourceTransform] : {}ms",
        System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    analyzeDeviceViewOutput(analysis, queryStatement);
    analyzeDeviceViewInput(analysis);

    logger.warn("--- [analyzeDeviceView] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    analyzeFill(analysis, queryStatement);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);
    logger.warn("--- [analyzeOutput] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    // fetch partition information
    analyzeDataPartition(analysis, schemaTree, partitionFetcher);
    logger.warn("--- [analyzeDataPartition] : {}ms", System.currentTimeMillis() - startTime);
  }

  private static List<PartialPath> analyzeFrom(
      QueryStatement queryStatement, ISchemaTree schemaTree) {
    // device path patterns in FROM clause
    List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();

    Set<PartialPath> deviceSet = new HashSet<>();
    for (PartialPath devicePattern : devicePatternList) {
      // get all matched devices
      // TODO isPrefixMatch可否设置为false? analyzeFrom能否直接返回schemaTree的全部devices?
      deviceSet.addAll(
          schemaTree.getMatchedDevices(devicePattern, false).stream()
              .map(DeviceSchemaInfo::getDevicePath)
              .collect(Collectors.toList()));
    }

    // TODO 是否一定要排序?  最终的sourceNodeList已经会排序?
    return queryStatement.getResultDeviceOrder() == Ordering.ASC
        ? deviceSet.stream().sorted().collect(Collectors.toList())
        : deviceSet.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
  }

  private static void analyzeDeviceToWhere(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceList) {
    if (!queryStatement.hasWhere()) {
      return;
    }

    analysis.setAllExpressionTimeSeriesOperand(false);
    Map<String, Expression> deviceToWhereExpression = new HashMap<>();
    Iterator<PartialPath> deviceIterator = deviceList.iterator();
    while (deviceIterator.hasNext()) {
      PartialPath devicePath = deviceIterator.next();
      Expression whereExpression;
      try {
        whereExpression =
            normalizeExpression(analyzeWhereSplitByDevice(queryStatement, devicePath, schemaTree));
      } catch (MeasurementNotExistException e) {
        logger.warn(
            "Meets MeasurementNotExistException in analyzeDeviceToWhere when executing align by device, "
                + "error msg: {}",
            e.getMessage());
        deviceIterator.remove();
        continue;
      }

      TSDataType outputType = analyzeExpressionType(analysis, whereExpression);
      if (outputType != TSDataType.BOOLEAN) {
        throw new SemanticException(String.format(WHERE_WRONG_TYPE_ERROR_MSG, outputType));
      }

      deviceToWhereExpression.put(devicePath.getFullPath(), whereExpression);
    }
    analysis.setDeviceToWhereExpression(deviceToWhereExpression);
  }

  private static Expression analyzeWhereSplitByDevice(
      QueryStatement queryStatement, PartialPath devicePath, ISchemaTree schemaTree) {
    List<Expression> conJunctions =
        ExpressionAnalyzer.concatDeviceAndBindSchemaForPredicate(
            queryStatement.getWhereCondition().getPredicate(), devicePath, schemaTree, true);
    return ExpressionUtils.constructQueryFilter(
        conJunctions.stream().distinct().collect(Collectors.toList()));
  }

  static List<Pair<Expression, String>> analyzeSelect(
      Analysis analysis, QueryStatement queryStatement) {

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();

    for (int i = 0; i < analysis.getMeasurementList().size(); i++) {
      String measurementName = analysis.getMeasurementList().get(i);
      IMeasurementSchema measurementSchema = analysis.getMeasurementSchemaList().get(i);
      TimeSeriesOperand measurementPath =
          new TimeSeriesOperand(
              new MeasurementPath(new String[] {measurementName}, measurementSchema));
      outputExpressions.add(new Pair<>(measurementPath, null));
    }

    Set<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(DEVICE_EXPRESSION);
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }
    outputExpressions.forEach(pair -> selectExpressions.add(pair.getLeft()));
    analysis.setSelectExpressions(selectExpressions);

    return outputExpressions;
  }

  private static void analyzeDeviceToOrderBy(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceSet) {
    if (!queryStatement.hasOrderByExpression()) {
      return;
    }

    Map<String, Set<Expression>> deviceToOrderByExpressions = new LinkedHashMap<>();
    Map<String, List<SortItem>> deviceToSortItems = new LinkedHashMap<>();
    // build the device-view outputColumn for the sortNode above the deviceViewNode
    Set<Expression> deviceViewOrderByExpression = new LinkedHashSet<>();
    for (PartialPath device : deviceSet) {
      Set<Expression> orderByExpressionsForOneDevice = new LinkedHashSet<>();
      for (Expression expressionForItem : queryStatement.getExpressionSortItemList()) {
        List<Expression> expressions =
            concatDeviceAndBindSchemaForExpression(expressionForItem, device, schemaTree);
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
          device.getFullPath(), queryStatement.getUpdatedSortItems(orderByExpressionsForOneDevice));
      deviceToOrderByExpressions.put(device.getFullPath(), orderByExpressionsForOneDevice);
    }

    analysis.setOrderByExpressions(deviceViewOrderByExpression);
    queryStatement.updateSortItems(deviceViewOrderByExpression);
    analysis.setDeviceToSortItems(deviceToSortItems);
    analysis.setDeviceToOrderByExpressions(deviceToOrderByExpressions);
  }

  private static void analyzeDeviceToSourceTransform(Analysis analysis) {
    analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDeviceViewOutput(Analysis analysis, QueryStatement queryStatement) {
    Set<Expression> selectExpressions = analysis.getSelectExpressions();
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
      // TODO can also just set, instead of addAll in normal process?
      deviceViewOutputExpressions = selectExpressions;
      if (queryStatement.hasOrderByExpression()) {
        deviceViewOutputExpressions.addAll(analysis.getOrderByExpressions());
      }
    }
    analysis.setDeviceViewOutputExpressions(deviceViewOutputExpressions);
    analysis.setDeviceViewSpecialProcess(
        analyzeDeviceViewSpecialProcess(deviceViewOutputExpressions, queryStatement, analysis));
  }

  private static void analyzeDeviceViewInput(Analysis analysis) {
    List<Integer> indexes = new ArrayList<>();
    // index-0 is `Device`
    for (int i = 1; i < analysis.getSelectExpressions().size(); i++) {
      indexes.add(i);
    }
    Map<String, List<Integer>> deviceViewInputIndexesMap = new HashMap<>();
    for (PartialPath devicePath : analysis.getDeviceList()) {
      deviceViewInputIndexesMap.put(devicePath.getFullPath(), indexes);
    }
    analysis.setDeviceViewInputIndexesMap(deviceViewInputIndexesMap);
  }

  private static void analyzeDeviceToSource(Analysis analysis) {
    analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
    analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());
  }

  private static void analyzeDataPartition(
      Analysis analysis, ISchemaTree schemaTree, IPartitionFetcher partitionFetcher) {
    // TemplatedDevice has no views, so there is no need to use outputDeviceToQueriedDevicesMap
    Set<String> deviceSet =
        analysis.getDeviceList().stream().map(PartialPath::getFullPath).collect(Collectors.toSet());
    DataPartition dataPartition =
        fetchDataPartitionByDevices(
            deviceSet, schemaTree, analysis.getGlobalTimeFilter(), partitionFetcher);
    analysis.setDataPartitionInfo(dataPartition);
  }

  private static DataPartition fetchDataPartitionByDevices(
      Set<String> deviceSet,
      ISchemaTree schemaTree,
      Filter globalTimeFilter,
      IPartitionFetcher partitionFetcher) {
    long startTime = System.nanoTime();
    try {
      Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
          getTimePartitionSlotList(globalTimeFilter);
      // there is no satisfied time range
      if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
        return new DataPartition(
            Collections.emptyMap(),
            CONFIG.getSeriesPartitionExecutorClass(),
            CONFIG.getSeriesPartitionSlotNum());
      }
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (String devicePath : deviceSet) {
        DataPartitionQueryParam queryParam =
            new DataPartitionQueryParam(devicePath, res.left, res.right.left, res.right.right);
        sgNameToQueryParamsMap
            .computeIfAbsent(schemaTree.getBelongedDatabase(devicePath), key -> new ArrayList<>())
            .add(queryParam);
      }

      if (res.right.left || res.right.right) {
        return partitionFetcher.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
      } else {
        return partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      }
    } finally {
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(PARTITION_FETCHER, System.nanoTime() - startTime);
    }
  }
}
