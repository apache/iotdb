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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FilterNullParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.literal.Literal;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Analyze the statement and generate Analysis. */
public class Analyzer {
  private static final Logger logger = LoggerFactory.getLogger(Analyzer.class);

  private final MPPQueryContext context;

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private final TypeProvider typeProvider;

  public Analyzer(
      MPPQueryContext context, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    this.context = context;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
    this.typeProvider = new TypeProvider();
  }

  public Analysis analyze(Statement statement) {
    return new AnalyzeVisitor().process(statement, context);
  }

  private String getLogHeader() {
    return String.format("Query[%s]:", context.getQueryId());
  }

  /** This visitor is used to analyze each type of Statement and returns the {@link Analysis}. */
  private final class AnalyzeVisitor extends StatementVisitor<Analysis, MPPQueryContext> {

    @Override
    public Analysis visitNode(StatementNode node, MPPQueryContext context) {
      throw new UnsupportedOperationException(
          "Unsupported statement type: " + node.getClass().getName());
    }

    @Override
    public Analysis visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      try {
        // check for semantic errors
        queryStatement.semanticCheck();

        // concat path and construct path pattern tree
        PathPatternTree patternTree = new PathPatternTree();
        QueryStatement rewrittenStatement =
            (QueryStatement) new ConcatPathRewriter().rewrite(queryStatement, patternTree);
        analysis.setStatement(rewrittenStatement);

        // request schema fetch API
        logger.info("{} fetch query schema...", getLogHeader());
        SchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
        logger.info("{} fetch schema done", getLogHeader());
        // If there is no leaf node in the schema tree, the query should be completed immediately
        if (schemaTree.isEmpty()) {
          analysis.setRespDatasetHeader(new DatasetHeader(new ArrayList<>(), false));
          return analysis;
        }

        List<Pair<Expression, String>> outputExpressions;
        Set<Expression> selectExpressions = new HashSet<>();
        Map<String, Set<Expression>> sourceExpressions = new HashMap<>();
        // Example 1: select s1, s1 + s2 as t, udf(udf(s1)) from root.sg.d1
        //   outputExpressions: [<root.sg.d1.s1,null>, <root.sg.d1.s1 + root.sg.d1.s2,t>,
        //                       <udf(udf(root.sg.d1.s1)),null>]
        //   selectExpressions: [root.sg.d1.s1, root.sg.d1.s1 + root.sg.d1.s2,
        //                       udf(udf(root.sg.d1.s1))]
        //   sourceExpressions: {root.sg.d1 -> [root.sg.d1.s1, root.sg.d1.s2]}
        //
        // Example 2: select s1, s2, s3 as t from root.sg.* align by device
        //   outputExpressions: [<s1,null>, <s2,null>, <s1,t>]
        //   selectExpressions: [root.sg.d1.s1, root.sg.d1.s2, root.sg.d1.s3,
        //                       root.sg.d2.s1, root.sg.d2.s2]
        //   sourceExpressions: {root.sg.d1 -> [root.sg.d1.s1, root.sg.d1.s2, root.sg.d1.s2],
        //                       root.sg.d2 -> [root.sg.d2.s1, root.sg.d2.s2]}
        //
        // Example 3: select sum(s1) + 1 as t, count(s2) from root.sg.d1
        //   outputExpressions: [<sum(root.sg.d1.s1) + 1,t>, <count(root.sg.d1.s2),t>]
        //   selectExpressions: [sum(root.sg.d1.s1) + 1, count(root.sg.d1.s2)]
        //   aggregationExpressions: {root.sg.d1 -> [sum(root.sg.d1.s1), count(root.sg.d1.s2)]}
        //   sourceExpressions: {root.sg.d1 -> [sum(root.sg.d1.s1), count(root.sg.d1.s2)]}
        //
        // Example 4: select sum(s1) + 1 as t, count(s2) from root.sg.d1 where s1 > 1
        //   outputExpressions: [<sum(root.sg.d1.s1) + 1,t>, <count(root.sg.d1.s2),t>]
        //   selectExpressions: [sum(root.sg.d1.s1) + 1, count(root.sg.d1.s2)]
        //   aggregationExpressions: {root.sg.d1 -> [sum(root.sg.d1.s1), count(root.sg.d1.s2)]}
        //   sourceExpressions: {root.sg.d1 -> [root.sg.d1.s1, root.sg.d1.s2]}
        List<DeviceSchemaInfo> deviceSchemaInfos = new ArrayList<>();
        // a set that contains all measurement names,
        Set<String> measurementSet = new HashSet<>();
        if (queryStatement.isAlignByDevice()) {
          Map<String, Set<String>> deviceToMeasurementsMap = new HashMap<>();
          outputExpressions =
              analyzeFrom(
                  queryStatement,
                  schemaTree,
                  deviceSchemaInfos,
                  selectExpressions,
                  deviceToMeasurementsMap,
                  measurementSet);

          Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
          List<String> allMeasurements =
              outputExpressions.stream()
                  .map(Pair::getLeft)
                  .map(Expression::getExpressionString)
                  .distinct()
                  .collect(Collectors.toList());
          for (String deviceName : deviceToMeasurementsMap.keySet()) {
            List<String> measurementsUnderDeivce =
                new ArrayList<>(deviceToMeasurementsMap.get(deviceName));
            List<Integer> indexes = new ArrayList<>();
            for (String measurement : measurementsUnderDeivce) {
              indexes.add(
                  allMeasurements.indexOf(measurement) + 1); // add 1 to skip the device column
            }
            deviceToMeasurementIndexesMap.put(deviceName, indexes);
          }
          analysis.setDeviceToMeasurementIndexesMap(deviceToMeasurementIndexesMap);
        } else {
          outputExpressions = analyzeSelect(queryStatement, schemaTree);
          selectExpressions =
              outputExpressions.stream().map(Pair::getLeft).collect(Collectors.toSet());
        }

        if (queryStatement.isGroupByLevel()) {
          // map from grouped expression to set of input expressions
          Map<Expression, Set<Expression>> groupByLevelExpressions =
              analyzeGroupByLevel(queryStatement, outputExpressions, selectExpressions);
          analysis.setGroupByLevelExpressions(groupByLevelExpressions);
        }

        if (queryStatement.isGroupByTime()) {
          analysis.setGroupByTimeParameter(
              new GroupByTimeParameter(queryStatement.getGroupByTimeComponent()));
        }

        // extract global time filter from query filter and determine if there is a value filter
        Pair<Filter, Boolean> resultPair = analyzeGlobalTimeFilter(queryStatement);
        Filter globalTimeFilter = resultPair.left;
        boolean hasValueFilter = resultPair.right;
        analysis.setGlobalTimeFilter(globalTimeFilter);
        analysis.setHasValueFilter(hasValueFilter);

        // generate sourceExpression according to selectExpressions
        boolean isValueFilterAggregation = queryStatement.isAggregationQuery() && hasValueFilter;
        boolean isRawDataSource = !queryStatement.isAggregationQuery() || isValueFilterAggregation;
        for (Expression selectExpr : selectExpressions) {
          updateSource(selectExpr, sourceExpressions, isRawDataSource);
        }
        if (isValueFilterAggregation) {
          Map<String, Set<Expression>> aggregationExpressions = new HashMap<>();
          for (Expression selectExpr : selectExpressions) {
            analyzeAggregation(selectExpr, aggregationExpressions);
          }
          analysis.setAggregationExpressions(aggregationExpressions);
        }

        if (queryStatement.getWhereCondition() != null) {
          if (queryStatement.isAlignByDevice()) {
            Map<String, Expression> deviceToQueryFilter =
                analyzeWhereSplitByDevice(queryStatement, deviceSchemaInfos, measurementSet);
            deviceToQueryFilter
                .values()
                .forEach(
                    queryFilter -> updateSource(queryFilter, sourceExpressions, isRawDataSource));
            analysis.setDeviceToQueryFilter(deviceToQueryFilter);
          } else {
            Expression queryFilter = analyzeWhere(queryStatement, schemaTree);
            // update sourceExpression according to queryFilter
            updateSource(queryFilter, sourceExpressions, isRawDataSource);
            analysis.setQueryFilter(queryFilter);
          }
        }
        analysis.setSourceExpressions(sourceExpressions);
        analysis.setSelectExpressions(selectExpressions);

        if (queryStatement.getFilterNullComponent() != null) {
          FilterNullParameter filterNullParameter =
              analyzeWithoutNull(queryStatement, schemaTree, selectExpressions);
          analysis.setFilterNullParameter(filterNullParameter);
        }

        if (queryStatement.getFillComponent() != null) {
          FillComponent fillComponent = queryStatement.getFillComponent();
          if (fillComponent.getFillPolicy() == FillPolicy.VALUE) {
            List<Expression> fillColumnList =
                outputExpressions.stream()
                    .map(Pair::getLeft)
                    .distinct()
                    .collect(Collectors.toList());
            for (Expression fillColumn : fillColumnList) {
              checkDataTypeConsistencyInFill(fillColumn, fillComponent.getFillValue());
            }
          }
          analysis.setFillDescriptor(
              new FillDescriptor(fillComponent.getFillPolicy(), fillComponent.getFillValue()));
        }

        // generate result set header according to output expressions
        DatasetHeader datasetHeader = analyzeOutput(queryStatement, outputExpressions);
        analysis.setRespDatasetHeader(datasetHeader);
        analysis.setTypeProvider(typeProvider);

        // fetch partition information
        Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
        for (String devicePath : sourceExpressions.keySet()) {
          DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
          queryParam.setDevicePath(devicePath);
          sgNameToQueryParamsMap
              .computeIfAbsent(
                  schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
              .add(queryParam);
        }
        DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
        analysis.setDataPartitionInfo(dataPartition);
      } catch (StatementAnalyzeException e) {
        logger.error("Meet error when analyzing the query statement: ", e);
        throw new StatementAnalyzeException("Meet error when analyzing the query statement");
      }
      return analysis;
    }

    private List<Pair<Expression, String>> analyzeSelect(
        QueryStatement queryStatement, SchemaTree schemaTree) {
      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      ColumnPaginationController paginationController =
          new ColumnPaginationController(
              queryStatement.getSeriesLimit(),
              queryStatement.getSeriesOffset(),
              queryStatement.isLastQuery() || queryStatement.isGroupByLevel());

      for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
        boolean hasAlias = resultColumn.hasAlias();
        List<Expression> resultExpressions =
            ExpressionAnalyzer.removeWildcardInExpression(resultColumn.getExpression(), schemaTree);
        if (hasAlias && resultExpressions.size() > 1) {
          throw new SemanticException(
              String.format(
                  "alias '%s' can only be matched with one time series", resultColumn.getAlias()));
        }
        for (Expression expression : resultExpressions) {
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            Expression expressionWithoutAlias =
                ExpressionAnalyzer.removeAliasFromExpression(expression);
            String alias =
                !Objects.equals(expressionWithoutAlias, expression)
                    ? expression.getExpressionString()
                    : null;
            alias = hasAlias ? resultColumn.getAlias() : alias;
            outputExpressions.add(new Pair<>(expressionWithoutAlias, alias));
            ExpressionAnalyzer.updateTypeProvider(expressionWithoutAlias, typeProvider);
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
      }
      return outputExpressions;
    }

    private List<Pair<Expression, String>> analyzeFrom(
        QueryStatement queryStatement,
        SchemaTree schemaTree,
        List<DeviceSchemaInfo> allDeviceSchemaInfos,
        Set<Expression> selectExpressions,
        Map<String, Set<String>> deviceToMeasurementsMap,
        Set<String> measurementSet) {
      // device path patterns in FROM clause
      List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();

      // a list of measurement name with alias (null if alias not exist)
      List<Pair<Expression, String>> measurementWithAliasList =
          getAllMeasurements(queryStatement, measurementSet);

      // a list contains all selected paths
      List<MeasurementPath> allSelectedPaths = new ArrayList<>();
      for (PartialPath devicePattern : devicePatternList) {
        // get matched device path and all measurement schema infos under this device
        List<DeviceSchemaInfo> deviceSchemaInfos = schemaTree.getMatchedDevices(devicePattern);
        allDeviceSchemaInfos.addAll(deviceSchemaInfos);
        for (DeviceSchemaInfo deviceSchema : deviceSchemaInfos) {
          // add matched path into allSelectedPaths
          allSelectedPaths.addAll(deviceSchema.getMeasurements(measurementSet));
        }
      }

      // convert allSelectedPaths to a map from measurement name to corresponding paths
      Map<String, List<MeasurementPath>> measurementNameToPathsMap = new HashMap<>();
      Map<String, String> aliasToMeasurementNameMap = new HashMap<>();
      for (MeasurementPath measurementPath : allSelectedPaths) {
        measurementNameToPathsMap
            .computeIfAbsent(measurementPath.getMeasurement(), key -> new ArrayList<>())
            .add(measurementPath);
        if (measurementPath.isMeasurementAliasExists()) {
          if (aliasToMeasurementNameMap.containsKey(measurementPath.getMeasurementAlias())
              && !Objects.equals(
                  aliasToMeasurementNameMap.get(measurementPath.getMeasurementAlias()),
                  measurementPath.getMeasurement())) {
            throw new SemanticException(
                String.format(
                    "ALIGN BY DEVICE: alias '%s' can only be matched with one measurement",
                    measurementPath.getMeasurementAlias()));
          }
          aliasToMeasurementNameMap.put(
              measurementPath.getMeasurementAlias(), measurementPath.getMeasurement());
        }
      }
      // check whether the datatype of paths which has the same measurement name are consistent
      // if not, throw a SemanticException
      measurementNameToPathsMap.values().forEach(this::checkDataTypeConsistencyInAlignByDevice);

      // apply SLIMIT & SOFFSET and set outputExpressions & selectExpressions
      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      ColumnPaginationController paginationController =
          new ColumnPaginationController(
              queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);
      for (Pair<Expression, String> measurementAliasPair : measurementWithAliasList) {
        String measurement =
            ExpressionAnalyzer.getPathInSourceExpression(measurementAliasPair.left).toString();
        if (measurementNameToPathsMap.containsKey(measurement)) {
          List<MeasurementPath> measurementPaths = measurementNameToPathsMap.get(measurement);
          measurementPaths.forEach(MeasurementPath::removeMeasurementAlias);
          TSDataType dataType = measurementPaths.get(0).getSeriesType();
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            outputExpressions.add(measurementAliasPair);
            typeProvider.setType(measurementAliasPair.left.getExpressionString(), dataType);
            for (MeasurementPath measurementPath : measurementPaths) {
              Expression tmpExpression =
                  ExpressionAnalyzer.replacePathInSourceExpression(
                      measurementAliasPair.left, measurementPath);
              typeProvider.setType(tmpExpression.getExpressionString(), dataType);
              selectExpressions.add(tmpExpression);
              deviceToMeasurementsMap
                  .computeIfAbsent(measurementPath.getDevice(), key -> new LinkedHashSet<>())
                  .add(measurementAliasPair.left.getExpressionString());
            }
            paginationController.consumeLimit();
          } else {
            break;
          }
        } else if (aliasToMeasurementNameMap.containsKey(measurement)
            && measurementNameToPathsMap.containsKey(aliasToMeasurementNameMap.get(measurement))) {
          List<MeasurementPath> measurementPaths =
              measurementNameToPathsMap.get(aliasToMeasurementNameMap.get(measurement));
          measurementPaths.forEach(MeasurementPath::removeMeasurementAlias);
          TSDataType dataType = measurementPaths.get(0).getSeriesType();
          Expression expressionWithAlias = measurementAliasPair.left;
          Expression expressionWithoutAlias =
              ExpressionAnalyzer.removeAliasInMeasurementExpression(
                  measurementAliasPair.left, aliasToMeasurementNameMap);
          String alias =
              measurementAliasPair.right != null
                  ? measurementAliasPair.right
                  : expressionWithAlias.getExpressionString();
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            outputExpressions.add(new Pair<>(expressionWithoutAlias, alias));
            typeProvider.setType(expressionWithoutAlias.getExpressionString(), dataType);
            for (MeasurementPath measurementPath : measurementPaths) {
              Expression tmpExpression =
                  ExpressionAnalyzer.replacePathInSourceExpression(
                      expressionWithoutAlias, measurementPath);
              typeProvider.setType(tmpExpression.getExpressionString(), dataType);
              selectExpressions.add(tmpExpression);
              deviceToMeasurementsMap
                  .computeIfAbsent(measurementPath.getDevice(), key -> new LinkedHashSet<>())
                  .add(expressionWithoutAlias.getExpressionString());
            }
            paginationController.consumeLimit();
          } else {
            break;
          }
        } else if (measurement.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
          for (String measurementName : measurementNameToPathsMap.keySet()) {
            List<MeasurementPath> measurementPaths = measurementNameToPathsMap.get(measurementName);
            measurementPaths.forEach(MeasurementPath::removeMeasurementAlias);
            TSDataType dataType = measurementPaths.get(0).getSeriesType();
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              // replace `*` with exact measurement name
              Expression replacedMeasurement =
                  ExpressionAnalyzer.replacePathInSourceExpression(
                      measurementAliasPair.left, measurementName);
              typeProvider.setType(replacedMeasurement.getExpressionString(), dataType);
              outputExpressions.add(new Pair<>(replacedMeasurement, measurementAliasPair.right));
              for (MeasurementPath measurementPath : measurementPaths) {
                // replace `*` with exact measurement path
                Expression tmpExpression =
                    ExpressionAnalyzer.replacePathInSourceExpression(
                        measurementAliasPair.left, measurementPath);
                typeProvider.setType(tmpExpression.getExpressionString(), dataType);
                selectExpressions.add(tmpExpression);
                deviceToMeasurementsMap
                    .computeIfAbsent(measurementPath.getDevice(), key -> new LinkedHashSet<>())
                    .add(replacedMeasurement.getExpressionString());
              }
              paginationController.consumeLimit();
            } else {
              break;
            }
          }
          if (!paginationController.hasCurLimit()) {
            break;
          }
        } else {
          // do nothing or warning
        }
      }
      return outputExpressions;
    }

    private List<Pair<Expression, String>> getAllMeasurements(
        QueryStatement queryStatement, Set<String> measurementSet) {
      List<Pair<Expression, String>> measurementWithAliasList =
          queryStatement.getSelectComponent().getResultColumns().stream()
              .map(
                  resultColumn ->
                      ExpressionAnalyzer.getMeasurementWithAliasInSourceExpression(
                          resultColumn.getExpression(), resultColumn.getAlias()))
              .collect(Collectors.toList());
      measurementSet.addAll(
          measurementWithAliasList.stream()
              .map(Pair::getLeft)
              .map(ExpressionAnalyzer::collectPaths)
              .flatMap(Set::stream)
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
      return measurementWithAliasList;
    }

    private Pair<Filter, Boolean> analyzeGlobalTimeFilter(QueryStatement queryStatement) {
      Filter globalTimeFilter = null;
      boolean hasValueFilter = false;
      if (queryStatement.getWhereCondition() != null) {
        Pair<Filter, Boolean> resultPair =
            ExpressionAnalyzer.transformToGlobalTimeFilter(
                queryStatement.getWhereCondition().getPredicate());
        globalTimeFilter = resultPair.left;
        hasValueFilter = resultPair.right;
      }
      if (queryStatement.isGroupByTime()) {
        GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
        Filter groupByFilter =
            new GroupByFilter(
                groupByTimeComponent.getInterval(),
                groupByTimeComponent.getSlidingStep(),
                groupByTimeComponent.getStartTime(),
                groupByTimeComponent.getEndTime());
        if (globalTimeFilter == null) {
          globalTimeFilter = groupByFilter;
        } else {
          // TODO: optimize the filter
          globalTimeFilter = FilterFactory.and(globalTimeFilter, groupByFilter);
        }
      }
      return new Pair<>(globalTimeFilter, hasValueFilter);
    }

    private void updateSource(
        Expression selectExpr,
        Map<String, Set<Expression>> sourceExpressions,
        boolean isRawDataSource) {
      for (Expression sourceExpression :
          ExpressionAnalyzer.searchSourceExpressions(selectExpr, isRawDataSource)) {
        sourceExpressions
            .computeIfAbsent(
                ExpressionAnalyzer.getDeviceNameInSourceExpression(sourceExpression),
                key -> new HashSet<>())
            .add(sourceExpression);
      }
    }

    private void analyzeAggregation(
        Expression selectExpr, Map<String, Set<Expression>> aggregationExpressions) {
      for (Expression aggregationExpression :
          ExpressionAnalyzer.searchAggregationExpressions(selectExpr)) {
        aggregationExpressions
            .computeIfAbsent(
                ExpressionAnalyzer.getDeviceNameInSourceExpression(aggregationExpression),
                key -> new HashSet<>())
            .add(aggregationExpression);
      }
    }

    private Expression analyzeWhere(QueryStatement queryStatement, SchemaTree schemaTree) {
      List<Expression> rewrittenPredicates =
          ExpressionAnalyzer.removeWildcardInQueryFilter(
              queryStatement.getWhereCondition().getPredicate(),
              queryStatement.getFromComponent().getPrefixPaths(),
              schemaTree,
              typeProvider);
      return ExpressionUtils.constructQueryFilter(
          rewrittenPredicates.stream().distinct().collect(Collectors.toList()));
    }

    private Map<String, Expression> analyzeWhereSplitByDevice(
        QueryStatement queryStatement,
        List<DeviceSchemaInfo> deviceSchemaInfos,
        Set<String> measurementSet) {
      Map<String, Expression> deviceToQueryFilter = new HashMap<>();
      for (DeviceSchemaInfo deviceSchemaInfo : deviceSchemaInfos) {
        List<Expression> rewrittenPredicates =
            ExpressionAnalyzer.removeWildcardInQueryFilterByDevice(
                queryStatement.getWhereCondition().getPredicate(),
                deviceSchemaInfo,
                measurementSet,
                typeProvider);
        deviceToQueryFilter.put(
            deviceSchemaInfo.getDevicePath().getFullPath(),
            ExpressionUtils.constructQueryFilter(
                rewrittenPredicates.stream().distinct().collect(Collectors.toList())));
      }
      return deviceToQueryFilter;
    }

    private Map<Expression, Set<Expression>> analyzeGroupByLevel(
        QueryStatement queryStatement,
        List<Pair<Expression, String>> outputExpressions,
        Set<Expression> selectExpressions) {
      GroupByLevelController groupByLevelController =
          new GroupByLevelController(queryStatement.getGroupByLevelComponent().getLevels());
      for (Pair<Expression, String> measurementWithAlias : outputExpressions) {
        groupByLevelController.control(measurementWithAlias.left, measurementWithAlias.right);
      }
      Map<Expression, Set<Expression>> rawGroupByLevelExpressions =
          groupByLevelController.getGroupedPathMap();
      // check whether the datatype of paths which has the same output column name are consistent
      // if not, throw a SemanticException
      rawGroupByLevelExpressions.values().forEach(this::checkDataTypeConsistencyInGroupByLevel);

      Map<Expression, Set<Expression>> groupByLevelExpressions = new LinkedHashMap<>();
      ColumnPaginationController paginationController =
          new ColumnPaginationController(
              queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);
      for (Expression groupedExpression : rawGroupByLevelExpressions.keySet()) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
          continue;
        }
        if (paginationController.hasCurLimit()) {
          groupByLevelExpressions.put(
              groupedExpression, rawGroupByLevelExpressions.get(groupedExpression));
          paginationController.consumeLimit();
        } else {
          break;
        }
      }

      // reset outputExpressions & selectExpressions after applying SLIMIT/SOFFSET
      outputExpressions.clear();
      for (Expression groupedExpression : groupByLevelExpressions.keySet()) {
        TSDataType dataType =
            typeProvider.getType(
                new ArrayList<>(groupByLevelExpressions.get(groupedExpression))
                    .get(0)
                    .getExpressionString());
        typeProvider.setType(groupedExpression.getExpressionString(), dataType);
        outputExpressions.add(
            new Pair<>(
                groupedExpression,
                groupByLevelController.getAlias(groupedExpression.getExpressionString())));
      }
      selectExpressions.clear();
      selectExpressions.addAll(
          groupByLevelExpressions.values().stream()
              .flatMap(Set::stream)
              .collect(Collectors.toSet()));
      return groupByLevelExpressions;
    }

    private FilterNullParameter analyzeWithoutNull(
        QueryStatement queryStatement, SchemaTree schemaTree, Set<Expression> selectExpressions) {
      FilterNullParameter filterNullParameter = new FilterNullParameter();
      filterNullParameter.setFilterNullPolicy(
          queryStatement.getFilterNullComponent().getWithoutPolicyType());
      List<Expression> resultFilterNullColumns = new ArrayList<>();
      List<Expression> rawFilterNullColumns =
          queryStatement.getFilterNullComponent().getWithoutNullColumns();
      for (Expression filterNullColumn : rawFilterNullColumns) {
        List<Expression> resultExpressions =
            ExpressionAnalyzer.removeWildcardInExpression(filterNullColumn, schemaTree);
        for (Expression expression : resultExpressions) {
          Expression expressionWithoutAlias =
              ExpressionAnalyzer.removeAliasFromExpression(expression);
          if (!selectExpressions.contains(expressionWithoutAlias)) {
            throw new SemanticException(
                String.format(
                    "The without null column '%s' don't match the columns queried.", expression));
          }
          resultFilterNullColumns.add(expressionWithoutAlias);
        }
      }
      // don't specify columns, by default, it is effective for all columns
      if (rawFilterNullColumns.isEmpty()) {
        resultFilterNullColumns.addAll(selectExpressions);
      }
      filterNullParameter.setFilterNullColumns(resultFilterNullColumns);
      return filterNullParameter;
    }

    private DatasetHeader analyzeOutput(
        QueryStatement queryStatement, List<Pair<Expression, String>> outputExpressions) {
      boolean isIgnoreTimestamp =
          queryStatement.isAggregationQuery() && !queryStatement.isGroupByTime();
      List<ColumnHeader> columnHeaders = new ArrayList<>();
      if (queryStatement.isAlignByDevice()) {
        columnHeaders.add(new ColumnHeader(HeaderConstant.COLUMN_DEVICE, TSDataType.TEXT, null));
        typeProvider.setType(HeaderConstant.COLUMN_DEVICE, TSDataType.TEXT);
      }
      columnHeaders.addAll(
          outputExpressions.stream()
              .map(
                  expressionWithAlias -> {
                    String columnName = expressionWithAlias.left.getExpressionString();
                    String alias = expressionWithAlias.right;
                    return new ColumnHeader(columnName, typeProvider.getType(columnName), alias);
                  })
              .collect(Collectors.toList()));
      return new DatasetHeader(columnHeaders, isIgnoreTimestamp);
    }

    /**
     * Check datatype consistency in ALIGN BY DEVICE.
     *
     * <p>an inconsistent example: select s0 from root.sg1.d1, root.sg1.d2 align by device, return
     * false while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
     */
    private void checkDataTypeConsistencyInAlignByDevice(List<MeasurementPath> measurementPaths) {
      TSDataType checkedDataType = measurementPaths.get(0).getSeriesType();
      for (MeasurementPath path : measurementPaths) {
        if (path.getSeriesType() != checkedDataType) {
          throw new SemanticException(
              "ALIGN BY DEVICE: the data types of the same measurement column should be the same across devices.");
        }
      }
    }

    /** Check datatype consistency in GROUP BY LEVEL. */
    private void checkDataTypeConsistencyInGroupByLevel(Set<Expression> expressions) {
      List<Expression> expressionList = new ArrayList<>(expressions);
      TSDataType checkedDataType =
          typeProvider.getType(expressionList.get(0).getExpressionString());
      for (Expression expression : expressionList) {
        if (typeProvider.getType(expression.getExpressionString()) != checkedDataType) {
          throw new SemanticException(
              "GROUP BY LEVEL: the data types of the same output column should be the same.");
        }
      }
    }

    private void checkDataTypeConsistencyInFill(Expression fillColumn, Literal fillValue) {
      TSDataType checkedDataType = typeProvider.getType(fillColumn.getExpressionString());
      if (!fillValue.isDataTypeConsistency(checkedDataType)) {
        // TODO: consider type casting
        throw new SemanticException(
            "FILL: the data type of the fill value should be the same as the output column");
      }
    }

    @Override
    public Analysis visitInsert(InsertStatement insertStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      long[] timeArray = insertStatement.getTimes();
      PartialPath devicePath = insertStatement.getDevice();
      String[] measurements = insertStatement.getMeasurementList();
      if (timeArray.length == 1) {
        // construct insert row statement
        InsertRowStatement insertRowStatement = new InsertRowStatement();
        insertRowStatement.setDevicePath(devicePath);
        insertRowStatement.setTime(timeArray[0]);
        insertRowStatement.setMeasurements(measurements);
        insertRowStatement.setDataTypes(
            new TSDataType[insertStatement.getMeasurementList().length]);
        Object[] values = new Object[insertStatement.getMeasurementList().length];
        System.arraycopy(insertStatement.getValuesList().get(0), 0, values, 0, values.length);
        insertRowStatement.setValues(values);
        insertRowStatement.setNeedInferType(true);
        insertRowStatement.setAligned(insertStatement.isAligned());
        return insertRowStatement.accept(this, context);
      } else {
        // construct insert rows statement
        // construct insert statement
        InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
        List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
        for (int i = 0; i < timeArray.length; i++) {
          InsertRowStatement statement = new InsertRowStatement();
          statement.setDevicePath(devicePath);
          statement.setMeasurements(measurements);
          statement.setTime(timeArray[i]);
          statement.setDataTypes(new TSDataType[insertStatement.getMeasurementList().length]);
          Object[] values = new Object[insertStatement.getMeasurementList().length];
          System.arraycopy(insertStatement.getValuesList().get(i), 0, values, 0, values.length);
          statement.setValues(values);
          statement.setAligned(insertStatement.isAligned());
          statement.setNeedInferType(true);
          insertRowStatementList.add(statement);
        }
        insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
        return insertRowsStatement.accept(this, context);
      }
    }

    @Override
    public Analysis visitCreateTimeseries(
        CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      if (createTimeSeriesStatement.getTags() != null
          && !createTimeSeriesStatement.getTags().isEmpty()
          && createTimeSeriesStatement.getAttributes() != null
          && !createTimeSeriesStatement.getAttributes().isEmpty()) {
        for (String tagKey : createTimeSeriesStatement.getTags().keySet()) {
          if (createTimeSeriesStatement.getAttributes().containsKey(tagKey)) {
            throw new SemanticException(
                String.format(
                    "Tag and attribute shouldn't have the same property key [%s]", tagKey));
          }
        }
      }
      Analysis analysis = new Analysis();
      analysis.setStatement(createTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getOrCreateSchemaPartition(
              new PathPatternTree(createTimeSeriesStatement.getPath()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitCreateAlignedTimeseries(
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement,
        MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      List<String> measurements = createAlignedTimeSeriesStatement.getMeasurements();
      Set<String> measurementsSet = new HashSet<>(measurements);
      if (measurementsSet.size() < measurements.size()) {
        throw new SemanticException(
            "Measurement under an aligned device is not allowed to have the same measurement name");
      }

      Analysis analysis = new Analysis();
      analysis.setStatement(createAlignedTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo =
          partitionFetcher.getOrCreateSchemaPartition(
              new PathPatternTree(
                  createAlignedTimeSeriesStatement.getDevicePath(),
                  createAlignedTimeSeriesStatement.getMeasurements()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      Analysis analysis = new Analysis();
      analysis.setStatement(alterTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(alterTimeSeriesStatement.getPath()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertTabletStatement.getTimePartitionSlots());

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonList(dataPartitionQueryParam));

      Analysis analysis = new Analysis();
      analysis.setStatement(insertTabletStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertRowStatement.getTimePartitionSlots());

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonList(dataPartitionQueryParam));

      Analysis analysis = new Analysis();
      analysis.setStatement(insertRowStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRows(
        InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertRowStatement.getTimePartitionSlots());
        dataPartitionQueryParams.add(dataPartitionQueryParam);
      }

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams);

      Analysis analysis = new Analysis();
      analysis.setStatement(insertRowsStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertMultiTablets(
        InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
      for (InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertTabletStatement.getTimePartitionSlots());
        dataPartitionQueryParams.add(dataPartitionQueryParam);
      }

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams);

      Analysis analysis = new Analysis();
      analysis.setStatement(insertMultiTabletsStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(
          insertRowsOfOneDeviceStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertRowsOfOneDeviceStatement.getTimePartitionSlots());

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonList(dataPartitionQueryParam));

      Analysis analysis = new Analysis();
      analysis.setStatement(insertRowsOfOneDeviceStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitShowTimeSeries(
        ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(showTimeSeriesStatement.getPathPattern()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.showTimeSeriesHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowStorageGroup(
        ShowStorageGroupStatement showStorageGroupStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showStorageGroupStatement);
      analysis.setRespDatasetHeader(HeaderConstant.showStorageGroupHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowTTL(ShowTTLStatement showTTLStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showTTLStatement);
      analysis.setRespDatasetHeader(HeaderConstant.showTTLHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowDevices(
        ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showDevicesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(
                  showDevicesStatement
                      .getPathPattern()
                      .concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(
          showDevicesStatement.hasSgCol()
              ? HeaderConstant.showDevicesWithSgHeader
              : HeaderConstant.showDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountStorageGroup(
        CountStorageGroupStatement countStorageGroupStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countStorageGroupStatement);
      analysis.setRespDatasetHeader(HeaderConstant.countStorageGroupHeader);
      return analysis;
    }

    @Override
    public Analysis visitSchemaFetch(
        SchemaFetchStatement schemaFetchStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(schemaFetchStatement);
      analysis.setSchemaPartitionInfo(schemaFetchStatement.getSchemaPartition());
      return analysis;
    }

    @Override
    public Analysis visitCountDevices(
        CountDevicesStatement countDevicesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countDevicesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(
                  countDevicesStatement
                      .getPartialPath()
                      .concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountTimeSeries(
        CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(countTimeSeriesStatement.getPartialPath()));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countTimeSeriesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountLevelTimeSeries(
        CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countLevelTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(countLevelTimeSeriesStatement.getPartialPath()));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countLevelTimeSeriesHeader);
      return analysis;
    }
  }
}
