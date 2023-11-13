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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
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

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ENDTIME;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.CONFIG;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.DEVICE_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.END_TIME_EXPRESSION;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.WHERE_WRONG_TYPE_ERROR_MSG;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeDeviceViewSpecialProcess;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeExpressionType;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeFill;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.analyzeOutput;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.checkDeviceViewInputUniqueness;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.concatDeviceAndBindSchemaForExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.getMeasurementExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.normalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchSourceExpressions;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetInGroupByTimeForDevice;

/**
 * This class provides accelerated implementation for multiple devices align by device query. This
 * optimization is only used for devices with same template, using template can avoid many
 * unnecessary judgements.
 *
 * <p>e.g. for query `SELECT * FROM root.xx.** order by device/time/expression align by device`, the
 * device list of `root.xx.**` must use same template.
 */
public class TemplatedDeviceAnalyze {

  private static final Logger logger = LoggerFactory.getLogger(TemplatedDeviceAnalyze.class);

  private boolean isWildCardQuery;

  private boolean isOriginalMeasurementQuery;

  private Analysis analysis;

  private QueryStatement queryStatement;

  private MPPQueryContext context;

  private ISchemaTree schemaTree;

  private final IPartitionFetcher partitionFetcher;

  public TemplatedDeviceAnalyze(
      Analysis analysis,
      QueryStatement queryStatement,
      MPPQueryContext context,
      ISchemaTree schemaTree,
      IPartitionFetcher partitionFetcher,
      Template template) {
    this.analysis = analysis;
    this.queryStatement = queryStatement;
    this.context = context;
    this.schemaTree = schemaTree;
    this.partitionFetcher = partitionFetcher;

    if (queryStatement.getSelectComponent().getResultColumns().size() == 1) {
      if ("*"
          .equals(
              queryStatement
                  .getSelectComponent()
                  .getResultColumns()
                  .get(0)
                  .getExpression()
                  .getOutputSymbol())) {

        isWildCardQuery = true;
      }
    }
  }

  /** add multi-threading impl */
  public Analysis visitQuery() {

    long startTime = System.currentTimeMillis();

    List<Pair<Expression, String>> outputExpressions;

    // TODO-1 change the return value of this method, return `deviceList + template`
    List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);
    logger.warn("--- [analyzeFrom] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    if (canPushDownLimitOffsetInGroupByTimeForDevice(queryStatement)) {
      // remove the device which won't appear in resultSet after limit/offset
      deviceList = pushDownLimitOffsetInGroupByTimeForDevice(deviceList, queryStatement);
    }

    // TODO-2 optimize for where filter using template
    analyzeDeviceToWhere(analysis, queryStatement, schemaTree, deviceList);
    logger.warn("--- [analyzeDeviceToWhere] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    outputExpressions = analyzeSelectUseTemplate(analysis, queryStatement, schemaTree, deviceList);
    logger.warn("--- [analyzeSelect] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    if (deviceList.isEmpty()) {
      return finishQuery(queryStatement, analysis);
    }
    analysis.setDeviceList(deviceList);

    // analyzeDeviceToGroupBy(analysis, queryStatement, schemaTree, deviceList);
    analyzeDeviceToOrderBy(analysis, queryStatement, schemaTree, deviceList);
    // analyzeHaving(analysis, queryStatement, schemaTree, deviceList);
    // analyzeDeviceToAggregation(analysis, queryStatement);
    analyzeDeviceToSourceTransform(analysis, queryStatement);
    analyzeDeviceToSource(analysis, queryStatement);

    logger.warn(
        "--- [analyzeDeviceToSource + analyzeDeviceToSourceTransform] : {}ms",
        System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    analyzeDeviceViewOutput(analysis, queryStatement);
    analyzeDeviceViewInput(analysis, queryStatement);

    logger.warn("--- [analyzeDeviceView] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    // analyzeInto(analysis, queryStatement, deviceList, outputExpressions);
    // analyzeGroupByTime(analysis, queryStatement);
    analyzeFill(analysis, queryStatement);

    // generate result set header according to output expressions
    analyzeOutput(analysis, queryStatement, outputExpressions);
    logger.warn("--- [analyzeOutput] : {}ms", System.currentTimeMillis() - startTime);
    startTime = System.currentTimeMillis();

    // fetch partition information
    analyzeDataPartition(analysis, queryStatement, schemaTree);
    logger.warn("--- [analyzeDataPartition] : {}ms", System.currentTimeMillis() - startTime);

    return analysis;
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

  private static Analysis finishQuery(QueryStatement queryStatement, Analysis analysis) {
    if (queryStatement.isSelectInto()) {
      analysis.setRespDatasetHeader(
          DatasetHeaderFactory.getSelectIntoHeader(queryStatement.isAlignByDevice()));
    }
    if (queryStatement.isLastQuery()) {
      analysis.setRespDatasetHeader(DatasetHeaderFactory.getLastQueryHeader());
    }
    analysis.setFinishQueryAfterAnalyze(true);
    return analysis;
  }

  static List<Pair<Expression, String>> analyzeSelectUseTemplate(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceList) {

    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    Map<String, Set<Expression>> deviceToSelectExpressions = new HashMap<>();

    for (Map.Entry<String, IMeasurementSchema> entry :
        analysis.getTemplateTypes().getSchemaMap().entrySet()) {
      String measurementName = entry.getKey();
      IMeasurementSchema measurementSchema = entry.getValue();
      TimeSeriesOperand measurementPath =
          new TimeSeriesOperand(
              new MeasurementPath(new String[] {measurementName}, measurementSchema));
      // analyzeExpression(analysis, measurementPath);
      outputExpressions.add(new Pair<>(measurementPath, null));
      //      for (PartialPath devicePath : deviceList) {
      //        // TODO how to determine whether a device is aligned device
      //        TimeSeriesOperand fullPath =
      //            new TimeSeriesOperand(
      //                new MeasurementPath(
      //                    devicePath.concatNode(measurementName), measurementSchema, true));
      //        // analyzeExpression(analysis, fullPath);
      //        deviceToSelectExpressions
      //            .computeIfAbsent(devicePath.getFullPath(), k -> new LinkedHashSet<>())
      //            .add(fullPath);
      //      }
    }

    Set<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(DEVICE_EXPRESSION);
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }
    outputExpressions.forEach(pair -> selectExpressions.add(pair.getLeft()));
    analysis.setSelectExpressions(selectExpressions);
    analysis.setDeviceToSelectExpressions(deviceToSelectExpressions);

    return outputExpressions;
  }

  private void analyzeDeviceToOrderBy(
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

  private void analyzeDeviceToSourceTransform(Analysis analysis, QueryStatement queryStatement) {
    if (queryStatement.isAggregationQuery()) {
      Map<String, Set<Expression>> deviceToSourceTransformExpressions =
          analysis.getDeviceToSourceTransformExpressions();
      Map<String, Set<Expression>> deviceToAggregationExpressions =
          analysis.getDeviceToAggregationExpressions();

      for (Map.Entry<String, Set<Expression>> entry : deviceToAggregationExpressions.entrySet()) {
        String deviceName = entry.getKey();
        Set<Expression> aggregationExpressions = entry.getValue();

        Set<Expression> sourceTransformExpressions =
            deviceToSourceTransformExpressions.computeIfAbsent(
                deviceName, k -> new LinkedHashSet<>());

        for (Expression expression : aggregationExpressions) {
          // if count_time aggregation exist, it can exist only one count_time(*)
          if (queryStatement.isCountTimeAggregation()) {
            for (Expression countTimeSourceExpression :
                ((FunctionExpression) expression).getCountTimeExpressions()) {

              analyzeExpressionType(analysis, countTimeSourceExpression);
              sourceTransformExpressions.add(countTimeSourceExpression);
            }
          } else {
            // We just process first input Expression of AggregationFunction,
            // keep other input Expressions as origin
            // If AggregationFunction need more than one input series,
            // we need to reconsider the process of it
            sourceTransformExpressions.add(expression.getExpressions().get(0));
          }
        }

        if (queryStatement.hasGroupByExpression()) {
          sourceTransformExpressions.add(analysis.getDeviceToGroupByExpression().get(deviceName));
        }
      }
    } else {
      if (isWildCardQuery || isOriginalMeasurementQuery) {
        analysis.setDeviceToSourceTransformExpressions(analysis.getDeviceToSelectExpressions());
      } else {
        updateDeviceToSourceTransformAndOutputExpressions(
            analysis, analysis.getDeviceToSelectExpressions());
        if (queryStatement.hasOrderByExpression()) {
          updateDeviceToSourceTransformAndOutputExpressions(
              analysis, analysis.getDeviceToOrderByExpressions());
        }
      }
    }
  }

  private static void updateDeviceToSourceTransformAndOutputExpressions(
      Analysis analysis, Map<String, Set<Expression>> deviceToSelectExpressions) {
    // two maps to be updated
    Map<String, Set<Expression>> deviceToSourceTransformExpressions =
        analysis.getDeviceToSourceTransformExpressions();
    Map<String, Set<Expression>> deviceToOutputExpressions =
        analysis.getDeviceToOutputExpressions();

    for (Map.Entry<String, Set<Expression>> entry : deviceToSelectExpressions.entrySet()) {
      String deviceName = entry.getKey();
      Set<Expression> expressions = entry.getValue();

      Set<Expression> normalizedExpressions = new LinkedHashSet<>();
      for (Expression expression : expressions) {
        Expression normalizedExpression = normalizeExpression(expression);
        analyzeExpressionType(analysis, normalizedExpression);

        normalizedExpressions.add(normalizedExpression);
      }
      deviceToOutputExpressions
          .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
          .addAll(expressions);
      deviceToSourceTransformExpressions
          .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
          .addAll(normalizedExpressions);
    }
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

  private void analyzeDeviceViewInput(Analysis analysis, QueryStatement queryStatement) {
    if (isWildCardQuery || isOriginalMeasurementQuery) {
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
      return;
    }

    List<String> deviceViewOutputColumns =
        analysis.getDeviceViewOutputExpressions().stream()
            .map(Expression::getOutputSymbol)
            .collect(Collectors.toList());

    Map<String, Set<String>> deviceToOutputColumnsMap = new LinkedHashMap<>();
    Map<String, Set<Expression>> deviceToOutputExpressions =
        analysis.getDeviceToOutputExpressions();
    for (Map.Entry<String, Set<Expression>> entry : deviceToOutputExpressions.entrySet()) {
      Set<Expression> outputExpressionsUnderDevice = entry.getValue();
      checkDeviceViewInputUniqueness(outputExpressionsUnderDevice);

      Set<String> outputColumns = new LinkedHashSet<>();
      if (queryStatement.isOutputEndTime()) {
        outputColumns.add(ENDTIME);
      }
      for (Expression expression : outputExpressionsUnderDevice) {
        outputColumns.add(getMeasurementExpression(expression, analysis).getOutputSymbol());
      }
      deviceToOutputColumnsMap.put(entry.getKey(), outputColumns);
    }

    Map<String, List<Integer>> deviceViewInputIndexesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : deviceToOutputColumnsMap.entrySet()) {
      String deviceName = entry.getKey();
      List<String> outputsUnderDevice = new ArrayList<>(entry.getValue());

      List<Integer> indexes = new ArrayList<>();
      for (String output : outputsUnderDevice) {
        int index = deviceViewOutputColumns.indexOf(output);
        if (index < 1) {
          throw new IllegalStateException(
              String.format(
                  "output column '%s' is not stored in %s", output, deviceViewOutputColumns));
        }
        indexes.add(index);
      }
      deviceViewInputIndexesMap.put(deviceName, indexes);
    }
    analysis.setDeviceViewInputIndexesMap(deviceViewInputIndexesMap);
  }

  private void analyzeDeviceToSource(Analysis analysis, QueryStatement queryStatement) {
    if (isWildCardQuery || isOriginalMeasurementQuery) {
      analysis.setDeviceToSourceExpressions(analysis.getDeviceToSelectExpressions());
      analysis.setDeviceToOutputExpressions(analysis.getDeviceToSelectExpressions());
      return;
    }

    Map<String, Set<Expression>> deviceToSourceExpressions = new HashMap<>();
    Map<String, Set<Expression>> deviceToSourceTransformExpressions =
        analysis.getDeviceToSourceTransformExpressions();

    for (Map.Entry<String, Set<Expression>> entry : deviceToSourceTransformExpressions.entrySet()) {
      String deviceName = entry.getKey();
      Set<Expression> sourceTransformExpressions = entry.getValue();

      Set<Expression> sourceExpressions = new LinkedHashSet<>();
      sourceTransformExpressions.forEach(
          expression -> sourceExpressions.addAll(searchSourceExpressions(expression)));

      deviceToSourceExpressions.put(deviceName, sourceExpressions);
    }

    if (queryStatement.hasWhere()) {
      Map<String, Expression> deviceToWhereExpression = analysis.getDeviceToWhereExpression();
      for (Map.Entry<String, Expression> deviceWhereExpressionEntry :
          deviceToWhereExpression.entrySet()) {
        String deviceName = deviceWhereExpressionEntry.getKey();
        Expression whereExpression = deviceWhereExpressionEntry.getValue();
        deviceToSourceExpressions
            .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
            .addAll(searchSourceExpressions(whereExpression));
      }
    }

    for (Map.Entry<String, Set<Expression>> entry : deviceToSourceExpressions.entrySet()) {
      String deviceName = entry.getKey();
      Set<Expression> sourceExpressionsUnderDevice = entry.getValue();
      Set<String> queriedDevices = new HashSet<>();
      for (Expression expression : sourceExpressionsUnderDevice) {
        queriedDevices.add(ExpressionAnalyzer.getDeviceNameInSourceExpression(expression));
      }
      if (queriedDevices.size() > 1) {
        throw new SemanticException(
            "Cross-device queries are not supported in ALIGN BY DEVICE queries.");
      }
    }

    analysis.setDeviceToSourceExpressions(deviceToSourceExpressions);
  }

  private void analyzeDataPartition(
      Analysis analysis, QueryStatement queryStatement, ISchemaTree schemaTree) {
    Set<String> deviceSet = new HashSet<>();
    if (queryStatement.isAlignByDevice()) {
      deviceSet =
          analysis.getDeviceList().stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet());
    } else {
      for (Expression expression : analysis.getSourceExpressions()) {
        deviceSet.add(ExpressionAnalyzer.getDeviceNameInSourceExpression(expression));
      }
    }
    DataPartition dataPartition =
        fetchDataPartitionByDevices(deviceSet, schemaTree, analysis.getGlobalTimeFilter());
    analysis.setDataPartitionInfo(dataPartition);
  }

  private DataPartition fetchDataPartitionByDevices(
      Set<String> deviceSet, ISchemaTree schemaTree, Filter globalTimeFilter) {
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
