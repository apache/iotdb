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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.exception.sql.MeasurementNotExistException;
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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
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
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.LastPointFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;

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
    public Analysis visitExplain(ExplainStatement explainStatement, MPPQueryContext context) {
      Analysis analysis = visitQuery(explainStatement.getQueryStatement(), context);
      analysis.setStatement(explainStatement);
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    @Override
    public Analysis visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      try {
        // check for semantic errors
        queryStatement.semanticCheck();

        // concat path and construct path pattern tree
        PathPatternTree patternTree = new PathPatternTree();
        queryStatement =
            (QueryStatement) new ConcatPathRewriter().rewrite(queryStatement, patternTree);
        analysis.setStatement(queryStatement);

        // request schema fetch API
        logger.info("{} fetch query schema...", getLogHeader());
        SchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
        logger.info("{} fetch schema done", getLogHeader());
        // If there is no leaf node in the schema tree, the query should be completed immediately
        if (schemaTree.isEmpty()) {
          analysis.setFinishQueryAfterAnalyze(true);
          return analysis;
        }

        // extract global time filter from query filter and determine if there is a value filter
        Pair<Filter, Boolean> resultPair = analyzeGlobalTimeFilter(queryStatement);
        Filter globalTimeFilter = resultPair.left;
        boolean hasValueFilter = resultPair.right;
        analysis.setGlobalTimeFilter(globalTimeFilter);
        analysis.setHasValueFilter(hasValueFilter);

        if (queryStatement.isLastQuery()) {
          if (hasValueFilter) {
            throw new SemanticException("Only time filters are supported in LAST query");
          }

          return analyzeLast(analysis, schemaTree.getAllMeasurement(), schemaTree);
        }

        // Example 1: select s1, s1 + s2 as t, udf(udf(s1)) from root.sg.d1
        //   outputExpressions: [<root.sg.d1.s1,null>, <root.sg.d1.s1 + root.sg.d1.s2,t>,
        //                       <udf(udf(root.sg.d1.s1)),null>]
        //   transformExpressions: [root.sg.d1.s1, root.sg.d1.s1 + root.sg.d1.s2,
        //                       udf(udf(root.sg.d1.s1))]
        //   sourceExpressions: {root.sg.d1 -> [root.sg.d1.s1, root.sg.d1.s2]}
        //
        // Example 2: select s1, s2, s3 as t from root.sg.* align by device
        //   outputExpressions: [<s1,null>, <s2,null>, <s1,t>]
        //   transformExpressions: [root.sg.d1.s1, root.sg.d1.s2, root.sg.d1.s3,
        //                       root.sg.d2.s1, root.sg.d2.s2]
        //   sourceExpressions: {root.sg.d1 -> [root.sg.d1.s1, root.sg.d1.s2, root.sg.d1.s2],
        //                       root.sg.d2 -> [root.sg.d2.s1, root.sg.d2.s2]}
        //
        // Example 3: select sum(s1) + 1 as t, count(s2) from root.sg.d1
        //   outputExpressions: [<sum(root.sg.d1.s1) + 1,t>, <count(root.sg.d1.s2),t>]
        //   transformExpressions: [sum(root.sg.d1.s1) + 1, count(root.sg.d1.s2)]
        //   aggregationExpressions: {root.sg.d1 -> [sum(root.sg.d1.s1), count(root.sg.d1.s2)]}
        //   sourceExpressions: {root.sg.d1 -> [sum(root.sg.d1.s1), count(root.sg.d1.s2)]}
        //
        // Example 4: select sum(s1) + 1 as t, count(s2) from root.sg.d1 where s1 > 1
        //   outputExpressions: [<sum(root.sg.d1.s1) + 1,t>, <count(root.sg.d1.s2),t>]
        //   transformExpressions: [sum(root.sg.d1.s1) + 1, count(root.sg.d1.s2)]
        //   aggregationExpressions: {root.sg.d1 -> [sum(root.sg.d1.s1), count(root.sg.d1.s2)]}
        //   sourceExpressions: {root.sg.d1 -> [root.sg.d1.s1, root.sg.d1.s2]}
        List<Pair<Expression, String>> outputExpressions;
        if (queryStatement.isAlignByDevice()) {
          Map<String, Set<Expression>> deviceToTransformExpressions = new HashMap<>();

          // all selected device
          Set<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

          Map<String, Set<String>> deviceToMeasurementsMap = new HashMap<>();
          outputExpressions =
              analyzeSelect(
                  queryStatement,
                  schemaTree,
                  deviceList,
                  deviceToTransformExpressions,
                  deviceToMeasurementsMap);

          Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
          List<String> allMeasurements =
              outputExpressions.stream()
                  .map(Pair::getLeft)
                  .map(Expression::getExpressionString)
                  .distinct()
                  .collect(Collectors.toList());
          for (String deviceName : deviceToMeasurementsMap.keySet()) {
            List<String> measurementsUnderDevice =
                new ArrayList<>(deviceToMeasurementsMap.get(deviceName));
            List<Integer> indexes = new ArrayList<>();
            for (String measurement : measurementsUnderDevice) {
              indexes.add(
                  allMeasurements.indexOf(measurement) + 1); // add 1 to skip the device column
            }
            deviceToMeasurementIndexesMap.put(deviceName, indexes);
          }
          analysis.setDeviceToMeasurementIndexesMap(deviceToMeasurementIndexesMap);

          Map<String, Set<Expression>> deviceToSourceExpressions = new HashMap<>();
          boolean isValueFilterAggregation = queryStatement.isAggregationQuery() && hasValueFilter;

          Map<String, Boolean> deviceToIsRawDataSource = new HashMap<>();

          Map<String, Set<Expression>> deviceToAggregationExpressions = new HashMap<>();
          Map<String, Set<Expression>> deviceToAggregationTransformExpressions = new HashMap<>();
          for (String deviceName : deviceToTransformExpressions.keySet()) {
            Set<Expression> transformExpressions = deviceToTransformExpressions.get(deviceName);
            Set<Expression> aggregationExpressions = new LinkedHashSet<>();
            Set<Expression> aggregationTransformExpressions = new LinkedHashSet<>();

            boolean isHasRawDataInputAggregation = false;
            if (queryStatement.isAggregationQuery()) {
              // true if nested expressions and UDFs exist in aggregation function
              // i.e. select sum(s1 + 1) from root.sg.d1 align by device
              isHasRawDataInputAggregation =
                  analyzeAggregation(
                      transformExpressions,
                      aggregationExpressions,
                      aggregationTransformExpressions);
              deviceToAggregationExpressions.put(deviceName, aggregationExpressions);
              deviceToAggregationTransformExpressions.put(
                  deviceName, aggregationTransformExpressions);
            }

            boolean isRawDataSource =
                !queryStatement.isAggregationQuery()
                    || isValueFilterAggregation
                    || isHasRawDataInputAggregation;

            for (Expression expression : transformExpressions) {
              updateSource(
                  expression,
                  deviceToSourceExpressions.computeIfAbsent(
                      deviceName, key -> new LinkedHashSet<>()),
                  isRawDataSource);
            }
            deviceToIsRawDataSource.put(deviceName, isRawDataSource);
          }
          analysis.setDeviceToAggregationExpressions(deviceToAggregationExpressions);
          analysis.setDeviceToAggregationTransformExpressions(
              deviceToAggregationTransformExpressions);
          analysis.setDeviceToIsRawDataSource(deviceToIsRawDataSource);

          if (queryStatement.getWhereCondition() != null) {
            Map<String, Expression> deviceToQueryFilter = new HashMap<>();
            Iterator<PartialPath> deviceIterator = deviceList.iterator();
            while (deviceIterator.hasNext()) {
              PartialPath devicePath = deviceIterator.next();
              Expression queryFilter = null;
              try {
                queryFilter = analyzeWhereSplitByDevice(queryStatement, devicePath, schemaTree);
              } catch (SemanticException e) {
                if (e instanceof MeasurementNotExistException) {
                  logger.warn(e.getMessage());
                  deviceIterator.remove();
                  deviceToSourceExpressions.remove(devicePath.getFullPath());
                  continue;
                }
                throw e;
              }
              deviceToQueryFilter.put(devicePath.getFullPath(), queryFilter);
              queryFilter.inferTypes(typeProvider);
              updateSource(
                  queryFilter,
                  deviceToSourceExpressions.computeIfAbsent(
                      devicePath.getFullPath(), key -> new LinkedHashSet<>()),
                  true);
            }
            analysis.setDeviceToQueryFilter(deviceToQueryFilter);
          }
          analysis.setDeviceToSourceExpressions(deviceToSourceExpressions);
          analysis.setDeviceToTransformExpressions(deviceToTransformExpressions);
        } else {
          outputExpressions = analyzeSelect(queryStatement, schemaTree);
          Set<Expression> transformExpressions =
              outputExpressions.stream()
                  .map(Pair::getLeft)
                  .collect(Collectors.toCollection(LinkedHashSet::new));

          if (queryStatement.isGroupByLevel()) {
            // map from grouped expression to set of input expressions
            Map<Expression, Expression> rawPathToGroupedPathMap = new HashMap<>();
            Map<Expression, Set<Expression>> groupByLevelExpressions =
                analyzeGroupByLevel(
                    queryStatement,
                    outputExpressions,
                    transformExpressions,
                    rawPathToGroupedPathMap);
            analysis.setGroupByLevelExpressions(groupByLevelExpressions);
            analysis.setRawPathToGroupedPathMap(rawPathToGroupedPathMap);
          }

          // true if nested expressions and UDFs exist in aggregation function
          // i.e. select sum(s1 + 1) from root.sg.d1
          boolean isHasRawDataInputAggregation = false;
          if (queryStatement.isAggregationQuery()) {
            Set<Expression> aggregationExpressions = new HashSet<>();
            Set<Expression> aggregationTransformExpressions = new HashSet<>();
            isHasRawDataInputAggregation =
                analyzeAggregation(
                    transformExpressions, aggregationExpressions, aggregationTransformExpressions);
            analysis.setAggregationExpressions(aggregationExpressions);
            analysis.setAggregationTransformExpressions(aggregationTransformExpressions);
          }

          // generate sourceExpression according to transformExpressions
          Set<Expression> sourceExpressions = new HashSet<>();
          boolean isValueFilterAggregation = queryStatement.isAggregationQuery() && hasValueFilter;
          boolean isRawDataSource =
              !queryStatement.isAggregationQuery()
                  || isValueFilterAggregation
                  || isHasRawDataInputAggregation;
          for (Expression expression : transformExpressions) {
            updateSource(expression, sourceExpressions, isRawDataSource);
          }

          if (queryStatement.getWhereCondition() != null) {
            Expression queryFilter = analyzeWhere(queryStatement, schemaTree);

            // update sourceExpression according to queryFilter
            queryFilter.inferTypes(typeProvider);
            updateSource(queryFilter, sourceExpressions, isRawDataSource);
            analysis.setQueryFilter(queryFilter);
          }
          analysis.setRawDataSource(isRawDataSource);
          analysis.setSourceExpressions(sourceExpressions);
          analysis.setTransformExpressions(transformExpressions);
        }

        if (queryStatement.isGroupByTime()) {
          analysis.setGroupByTimeParameter(
              new GroupByTimeParameter(queryStatement.getGroupByTimeComponent()));
        }

        if (queryStatement.getFilterNullComponent() != null) {
          FilterNullParameter filterNullParameter = new FilterNullParameter();
          filterNullParameter.setFilterNullPolicy(
              queryStatement.getFilterNullComponent().getWithoutPolicyType());
          List<Expression> resultFilterNullColumns;
          if (queryStatement.isAlignByDevice()) {
            resultFilterNullColumns =
                analyzeWithoutNullAlignByDevice(
                    queryStatement,
                    outputExpressions.stream().map(Pair::getLeft).collect(Collectors.toSet()));
          } else {
            resultFilterNullColumns =
                analyzeWithoutNull(queryStatement, schemaTree, analysis.getTransformExpressions());
          }
          filterNullParameter.setFilterNullColumns(resultFilterNullColumns);
          analysis.setFilterNullParameter(filterNullParameter);
        }

        if (queryStatement.getFillComponent() != null) {
          FillComponent fillComponent = queryStatement.getFillComponent();
          List<Expression> fillColumnList =
              outputExpressions.stream().map(Pair::getLeft).distinct().collect(Collectors.toList());
          if (fillComponent.getFillPolicy() == FillPolicy.VALUE) {
            for (Expression fillColumn : fillColumnList) {
              TSDataType checkedDataType = typeProvider.getType(fillColumn.getExpressionString());
              if (!fillComponent.getFillValue().isDataTypeConsistency(checkedDataType)) {
                throw new SemanticException(
                    "FILL: the data type of the fill value should be the same as the output column");
              }
            }
          } else if (fillComponent.getFillPolicy() == FillPolicy.LINEAR) {
            for (Expression fillColumn : fillColumnList) {
              TSDataType checkedDataType = typeProvider.getType(fillColumn.getExpressionString());
              if (!checkedDataType.isNumeric()) {
                throw new SemanticException(
                    String.format(
                        "FILL: dataType %s doesn't support linear fill.", checkedDataType));
              }
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
        Set<String> deviceSet = new HashSet<>();
        if (queryStatement.isAlignByDevice()) {
          deviceSet = analysis.getDeviceToSourceExpressions().keySet();
        } else {
          for (Expression expression : analysis.getSourceExpressions()) {
            deviceSet.add(ExpressionAnalyzer.getDeviceNameInSourceExpression(expression));
          }
        }
        DataPartition dataPartition = fetchDataPartitionByDevices(deviceSet, schemaTree);
        analysis.setDataPartitionInfo(dataPartition);
      } catch (StatementAnalyzeException e) {
        logger.error("Meet error when analyzing the query statement: ", e);
        throw new StatementAnalyzeException(
            "Meet error when analyzing the query statement: " + e.getMessage());
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
        if (hasAlias && !queryStatement.isGroupByLevel() && resultExpressions.size() > 1) {
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
            if (queryStatement.isGroupByLevel()
                && resultColumn.getExpression() instanceof FunctionExpression) {
              queryStatement
                  .getGroupByLevelComponent()
                  .updateIsCountStar((FunctionExpression) resultColumn.getExpression());
            }
            ExpressionAnalyzer.updateTypeProvider(expressionWithoutAlias, typeProvider);
            expressionWithoutAlias.inferTypes(typeProvider);
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
      }
      return outputExpressions;
    }

    private Set<PartialPath> analyzeFrom(QueryStatement queryStatement, SchemaTree schemaTree) {
      // device path patterns in FROM clause
      List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();

      Set<PartialPath> deviceList = new LinkedHashSet<>();
      for (PartialPath devicePattern : devicePatternList) {
        // get all matched devices
        deviceList.addAll(
            schemaTree.getMatchedDevices(devicePattern).stream()
                .map(DeviceSchemaInfo::getDevicePath)
                .collect(Collectors.toList()));
      }
      return deviceList;
    }

    private List<Pair<Expression, String>> analyzeSelect(
        QueryStatement queryStatement,
        SchemaTree schemaTree,
        Set<PartialPath> deviceList,
        Map<String, Set<Expression>> deviceToTransformExpressions,
        Map<String, Set<String>> deviceToMeasurementsMap) {
      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      ColumnPaginationController paginationController =
          new ColumnPaginationController(
              queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);

      for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
        Expression selectExpression = resultColumn.getExpression();
        boolean hasAlias = resultColumn.hasAlias();

        // measurement expression after removing wildcard
        // use LinkedHashMap for order-preserving
        Map<Expression, Map<String, Expression>> measurementToDeviceTransformExpressions =
            new LinkedHashMap<>();
        for (PartialPath device : deviceList) {
          List<Expression> transformExpressions =
              ExpressionAnalyzer.concatDeviceAndRemoveWildcard(
                  selectExpression, device, schemaTree, typeProvider);
          for (Expression transformExpression : transformExpressions) {
            measurementToDeviceTransformExpressions
                .computeIfAbsent(
                    ExpressionAnalyzer.getMeasurementExpression(transformExpression),
                    key -> new LinkedHashMap<>())
                .put(
                    device.getFullPath(),
                    ExpressionAnalyzer.removeAliasFromExpression(transformExpression));
          }
        }

        if (hasAlias && measurementToDeviceTransformExpressions.keySet().size() > 1) {
          throw new SemanticException(
              String.format(
                  "alias '%s' can only be matched with one time series", resultColumn.getAlias()));
        }

        for (Expression measurementExpression : measurementToDeviceTransformExpressions.keySet()) {
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            Map<String, Expression> deviceToTransformExpressionOfOneMeasurement =
                measurementToDeviceTransformExpressions.get(measurementExpression);
            deviceToTransformExpressionOfOneMeasurement
                .values()
                .forEach(expression -> expression.inferTypes(typeProvider));
            // check whether the datatype of paths which has the same measurement name are
            // consistent
            // if not, throw a SemanticException
            checkDataTypeConsistencyInAlignByDevice(
                new ArrayList<>(deviceToTransformExpressionOfOneMeasurement.values()));

            // add outputExpressions
            Expression measurementExpressionWithoutAlias =
                ExpressionAnalyzer.removeAliasFromExpression(measurementExpression);
            String alias =
                !Objects.equals(measurementExpressionWithoutAlias, measurementExpression)
                    ? measurementExpression.getExpressionString()
                    : null;
            alias = hasAlias ? resultColumn.getAlias() : alias;
            ExpressionAnalyzer.updateTypeProvider(measurementExpressionWithoutAlias, typeProvider);
            measurementExpressionWithoutAlias.inferTypes(typeProvider);
            outputExpressions.add(new Pair<>(measurementExpressionWithoutAlias, alias));

            // add deviceToTransformExpressions
            for (String deviceName : deviceToTransformExpressionOfOneMeasurement.keySet()) {
              Expression transformExpression =
                  deviceToTransformExpressionOfOneMeasurement.get(deviceName);
              ExpressionAnalyzer.updateTypeProvider(transformExpression, typeProvider);
              transformExpression.inferTypes(typeProvider);
              deviceToTransformExpressions
                  .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
                  .add(ExpressionAnalyzer.removeAliasFromExpression(transformExpression));
              deviceToMeasurementsMap
                  .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
                  .add(measurementExpressionWithoutAlias.getExpressionString());
            }
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
      }

      return outputExpressions;
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
        Filter groupByFilter = initGroupByFilter(groupByTimeComponent);
        if (globalTimeFilter == null) {
          globalTimeFilter = groupByFilter;
        } else {
          // TODO: optimize the filter
          globalTimeFilter = FilterFactory.and(globalTimeFilter, groupByFilter);
        }
      }
      return new Pair<>(globalTimeFilter, hasValueFilter);
    }

    private GroupByFilter initGroupByFilter(GroupByTimeComponent groupByTimeComponent) {
      if (groupByTimeComponent.isIntervalByMonth() || groupByTimeComponent.isSlidingStepByMonth()) {
        return new GroupByMonthFilter(
            groupByTimeComponent.getInterval(),
            groupByTimeComponent.getSlidingStep(),
            groupByTimeComponent.getStartTime(),
            groupByTimeComponent.getEndTime(),
            groupByTimeComponent.isSlidingStepByMonth(),
            groupByTimeComponent.isIntervalByMonth(),
            TimeZone.getTimeZone("+00:00"));
      } else {
        return new GroupByFilter(
            groupByTimeComponent.getInterval(),
            groupByTimeComponent.getSlidingStep(),
            groupByTimeComponent.getStartTime(),
            groupByTimeComponent.getEndTime());
      }
    }

    private void updateSource(
        Expression selectExpr, Set<Expression> sourceExpressions, boolean isRawDataSource) {
      sourceExpressions.addAll(
          ExpressionAnalyzer.searchSourceExpressions(selectExpr, isRawDataSource));
    }

    private boolean analyzeAggregation(
        Set<Expression> transformExpressions,
        Set<Expression> aggregationExpressions,
        Set<Expression> aggregationTransformExpressions) {
      // true if nested expressions and UDFs exist in aggregation function
      // i.e. select sum(s1 + 1) from root.sg.d1 align by device
      boolean isHasRawDataInputAggregation = false;
      for (Expression expression : transformExpressions) {
        for (Expression aggregationExpression :
            ExpressionAnalyzer.searchAggregationExpressions(expression)) {
          aggregationExpressions.add(aggregationExpression);
          aggregationTransformExpressions.addAll(aggregationExpression.getExpressions());
        }
      }
      for (Expression aggregationTransformExpression : aggregationTransformExpressions) {
        if (ExpressionAnalyzer.checkIsNeedTransform(aggregationTransformExpression)) {
          isHasRawDataInputAggregation = true;
          break;
        }
      }
      return isHasRawDataInputAggregation;
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

    private Expression analyzeWhereSplitByDevice(
        QueryStatement queryStatement, PartialPath devicePath, SchemaTree schemaTree) {
      List<Expression> rewrittenPredicates =
          ExpressionAnalyzer.removeWildcardInQueryFilterByDevice(
              queryStatement.getWhereCondition().getPredicate(),
              devicePath,
              schemaTree,
              typeProvider);
      return ExpressionUtils.constructQueryFilter(
          rewrittenPredicates.stream().distinct().collect(Collectors.toList()));
    }

    private Map<Expression, Set<Expression>> analyzeGroupByLevel(
        QueryStatement queryStatement,
        List<Pair<Expression, String>> outputExpressions,
        Set<Expression> transformExpressions,
        Map<Expression, Expression> rawPathToGroupedPathMap) {
      GroupByLevelController groupByLevelController =
          new GroupByLevelController(
              queryStatement.getGroupByLevelComponent().getLevels(), typeProvider);
      for (int i = 0; i < outputExpressions.size(); i++) {
        Pair<Expression, String> measurementWithAlias = outputExpressions.get(i);
        boolean isCountStar = queryStatement.getGroupByLevelComponent().isCountStar(i);
        groupByLevelController.control(
            isCountStar, measurementWithAlias.left, measurementWithAlias.right);
      }
      Map<Expression, Set<Expression>> rawGroupByLevelExpressions =
          groupByLevelController.getGroupedPathMap();
      rawPathToGroupedPathMap.putAll(groupByLevelController.getRawPathToGroupedPathMap());

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

      // reset outputExpressions & transformExpressions after applying SLIMIT/SOFFSET
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
      transformExpressions.clear();
      transformExpressions.addAll(
          groupByLevelExpressions.values().stream()
              .flatMap(Set::stream)
              .collect(Collectors.toSet()));
      return groupByLevelExpressions;
    }

    private List<Expression> analyzeWithoutNullAlignByDevice(
        QueryStatement queryStatement, Set<Expression> outputExpressions) {
      List<Expression> resultFilterNullColumns = new ArrayList<>();
      List<Expression> rawFilterNullColumns =
          queryStatement.getFilterNullComponent().getWithoutNullColumns();

      // don't specify columns, by default, it is effective for all columns
      if (rawFilterNullColumns.isEmpty()) {
        resultFilterNullColumns.addAll(outputExpressions);
        return resultFilterNullColumns;
      }

      for (Expression filterNullColumn : rawFilterNullColumns) {
        if (!outputExpressions.contains(filterNullColumn)) {
          throw new SemanticException(
              String.format(
                  "The without null column '%s' don't match the columns queried.",
                  filterNullColumn));
        }
        resultFilterNullColumns.add(filterNullColumn);
      }
      return resultFilterNullColumns;
    }

    private List<Expression> analyzeWithoutNull(
        QueryStatement queryStatement,
        SchemaTree schemaTree,
        Set<Expression> transformExpressions) {
      List<Expression> resultFilterNullColumns = new ArrayList<>();
      List<Expression> rawFilterNullColumns =
          queryStatement.getFilterNullComponent().getWithoutNullColumns();

      // don't specify columns, by default, it is effective for all columns
      if (rawFilterNullColumns.isEmpty()) {
        resultFilterNullColumns.addAll(transformExpressions);
        return resultFilterNullColumns;
      }

      for (Expression filterNullColumn : rawFilterNullColumns) {
        List<Expression> resultExpressions =
            ExpressionAnalyzer.removeWildcardInExpression(filterNullColumn, schemaTree);
        for (Expression expression : resultExpressions) {
          Expression expressionWithoutAlias =
              ExpressionAnalyzer.removeAliasFromExpression(expression);
          if (!transformExpressions.contains(expressionWithoutAlias)) {
            throw new SemanticException(
                String.format(
                    "The without null column '%s' don't match the columns queried.",
                    filterNullColumn));
          }
          resultFilterNullColumns.add(expressionWithoutAlias);
        }
      }
      return resultFilterNullColumns;
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

    private Analysis analyzeLast(
        Analysis analysis, List<MeasurementPath> allSelectedPath, SchemaTree schemaTree) {
      Set<Expression> sourceExpressions =
          allSelectedPath.stream()
              .map(TimeSeriesOperand::new)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      sourceExpressions.forEach(
          expression -> ExpressionAnalyzer.updateTypeProvider(expression, typeProvider));
      analysis.setSourceExpressions(sourceExpressions);

      analysis.setRespDatasetHeader(HeaderConstant.LAST_QUERY_HEADER);
      typeProvider.setType(HeaderConstant.COLUMN_TIMESERIES, TSDataType.TEXT);
      typeProvider.setType(HeaderConstant.COLUMN_VALUE, TSDataType.TEXT);
      typeProvider.setType(HeaderConstant.COLUMN_TIMESERIES_DATATYPE, TSDataType.TEXT);

      Set<String> deviceSet =
          allSelectedPath.stream().map(MeasurementPath::getDevice).collect(Collectors.toSet());
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (String devicePath : deviceSet) {
        DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
        queryParam.setDevicePath(devicePath);
        sgNameToQueryParamsMap
            .computeIfAbsent(
                schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
            .add(queryParam);
      }
      DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    private DataPartition fetchDataPartitionByDevices(
        Set<String> deviceSet, SchemaTree schemaTree) {
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (String devicePath : deviceSet) {
        DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
        queryParam.setDevicePath(devicePath);
        sgNameToQueryParamsMap
            .computeIfAbsent(
                schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
            .add(queryParam);
      }
      return partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
    }

    /**
     * Check datatype consistency in ALIGN BY DEVICE.
     *
     * <p>an inconsistent example: select s0 from root.sg1.d1, root.sg1.d2 align by device, return
     * false while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
     */
    private void checkDataTypeConsistencyInAlignByDevice(List<Expression> expressions) {
      TSDataType checkedDataType = typeProvider.getType(expressions.get(0).getExpressionString());
      for (Expression expression : expressions) {
        if (typeProvider.getType(expression.getExpressionString()) != checkedDataType) {
          throw new SemanticException(
              "ALIGN BY DEVICE: the data types of the same measurement column should be the same across devices.");
        }
      }
    }

    @Override
    public Analysis visitLastPointFetch(
        LastPointFetchStatement statement, MPPQueryContext context) {
      context.setQueryType(QueryType.READ);

      Analysis analysis = new Analysis();
      analysis.setStatement(statement);

      SchemaTree schemaTree = new SchemaTree();
      schemaTree.setStorageGroups(statement.getStorageGroups());

      return analyzeLast(analysis, statement.getSelectedPaths(), schemaTree);
    }

    @Override
    public Analysis visitInsert(InsertStatement insertStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      insertStatement.semanticCheck();
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

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendFullPath(createTimeSeriesStatement.getPath());
      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getOrCreateSchemaPartition(patternTree);
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

      PathPatternTree pathPatternTree = new PathPatternTree();
      for (String measurement : createAlignedTimeSeriesStatement.getMeasurements()) {
        pathPatternTree.appendFullPath(
            createAlignedTimeSeriesStatement.getDevicePath(), measurement);
      }

      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo = partitionFetcher.getOrCreateSchemaPartition(pathPatternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitInternalCreateTimeseries(
        InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement,
        MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      Analysis analysis = new Analysis();
      analysis.setStatement(internalCreateTimeSeriesStatement);

      PathPatternTree pathPatternTree = new PathPatternTree();
      for (String measurement : internalCreateTimeSeriesStatement.getMeasurements()) {
        pathPatternTree.appendFullPath(
            internalCreateTimeSeriesStatement.getDevicePath(), measurement);
      }

      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo = partitionFetcher.getOrCreateSchemaPartition(pathPatternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitCreateMultiTimeseries(
        CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      Analysis analysis = new Analysis();
      analysis.setStatement(createMultiTimeSeriesStatement);

      PathPatternTree patternTree = new PathPatternTree();
      for (PartialPath path : createMultiTimeSeriesStatement.getPaths()) {
        patternTree.appendFullPath(path);
      }
      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getOrCreateSchemaPartition(patternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      Analysis analysis = new Analysis();
      analysis.setStatement(alterTimeSeriesStatement);

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendFullPath(alterTimeSeriesStatement.getPath());
      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
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

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(showTimeSeriesStatement.getPathPattern());
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);

      if (showTimeSeriesStatement.isOrderByHeat()) {
        patternTree.constructTree();
        // request schema fetch API
        logger.info("{} fetch query schema...", getLogHeader());
        SchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
        logger.info("{} fetch schema done", getLogHeader());
        List<MeasurementPath> allSelectedPath = schemaTree.getAllMeasurement();

        Set<Expression> sourceExpressions =
            allSelectedPath.stream()
                .map(TimeSeriesOperand::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        analysis.setSourceExpressions(sourceExpressions);

        Set<String> deviceSet =
            allSelectedPath.stream().map(MeasurementPath::getDevice).collect(Collectors.toSet());
        Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
        for (String devicePath : deviceSet) {
          DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
          queryParam.setDevicePath(devicePath);
          sgNameToQueryParamsMap
              .computeIfAbsent(
                  schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
              .add(queryParam);
        }
        DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
        analysis.setDataPartitionInfo(dataPartition);
      }

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

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(
          showDevicesStatement.getPathPattern().concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(
          showDevicesStatement.hasSgCol()
              ? HeaderConstant.showDevicesWithSgHeader
              : HeaderConstant.showDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowCluster(
        ShowClusterStatement showClusterStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showClusterStatement);
      analysis.setRespDatasetHeader(HeaderConstant.showClusterHeader);
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

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(
          countDevicesStatement.getPartialPath().concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountTimeSeries(
        CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countTimeSeriesStatement);

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(countTimeSeriesStatement.getPartialPath());
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countTimeSeriesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountLevelTimeSeries(
        CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countLevelTimeSeriesStatement);

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(countLevelTimeSeriesStatement.getPartialPath());
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countLevelTimeSeriesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countStatement);

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(countStatement.getPartialPath());
      SchemaNodeManagementPartition schemaNodeManagementPartition =
          partitionFetcher.getSchemaNodeManagementPartitionWithLevel(
              patternTree, countStatement.getLevel());

      if (schemaNodeManagementPartition == null) {
        return analysis;
      }
      if (!schemaNodeManagementPartition.getMatchedNode().isEmpty()
          && schemaNodeManagementPartition.getSchemaPartition().getSchemaPartitionMap().size()
              == 0) {
        analysis.setFinishQueryAfterAnalyze(true);
      }
      analysis.setMatchedNodes(schemaNodeManagementPartition.getMatchedNode());
      analysis.setSchemaPartitionInfo(schemaNodeManagementPartition.getSchemaPartition());
      analysis.setRespDatasetHeader(HeaderConstant.countNodesHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowChildPaths(
        ShowChildPathsStatement showChildPathsStatement, MPPQueryContext context) {
      return visitSchemaNodeManagementPartition(
          showChildPathsStatement,
          showChildPathsStatement.getPartialPath(),
          HeaderConstant.showChildPathsHeader);
    }

    @Override
    public Analysis visitShowChildNodes(
        ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
      return visitSchemaNodeManagementPartition(
          showChildNodesStatement,
          showChildNodesStatement.getPartialPath(),
          HeaderConstant.showChildNodesHeader);
    }

    private Analysis visitSchemaNodeManagementPartition(
        Statement statement, PartialPath path, DatasetHeader header) {
      Analysis analysis = new Analysis();
      analysis.setStatement(statement);

      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(path);
      SchemaNodeManagementPartition schemaNodeManagementPartition =
          partitionFetcher.getSchemaNodeManagementPartition(patternTree);

      if (schemaNodeManagementPartition == null) {
        return analysis;
      }
      if (!schemaNodeManagementPartition.getMatchedNode().isEmpty()
          && schemaNodeManagementPartition.getSchemaPartition().getSchemaPartitionMap().size()
              == 0) {
        analysis.setFinishQueryAfterAnalyze(true);
      }
      analysis.setMatchedNodes(schemaNodeManagementPartition.getMatchedNode());
      analysis.setSchemaPartitionInfo(schemaNodeManagementPartition.getSchemaPartition());
      analysis.setRespDatasetHeader(header);
      return analysis;
    }

    @Override
    public Analysis visitDeleteData(
        DeleteDataStatement deleteDataStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      Analysis analysis = new Analysis();
      analysis.setStatement(deleteDataStatement);

      PathPatternTree patternTree = new PathPatternTree();
      for (PartialPath pathPattern : deleteDataStatement.getPathList()) {
        patternTree.appendPathPattern(pathPattern);
      }

      SchemaPartition schemaPartition = partitionFetcher.getSchemaPartition(patternTree);

      SchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree, schemaPartition);
      analysis.setSchemaTree(schemaTree);

      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();

      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
          schemaPartition.getSchemaPartitionMap();

      // todo keep the behaviour consistency of cluster and standalone,
      // the behaviour of standalone fetcher and LocalConfigNode is not consistent with that of
      // cluster mode's
      if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
        for (String storageGroup : schemaPartitionMap.keySet()) {
          sgNameToQueryParamsMap.put(
              storageGroup,
              schemaPartitionMap.get(storageGroup).keySet().stream()
                  .map(DataPartitionQueryParam::new)
                  .collect(Collectors.toList()));
        }
      } else {
        // the StandalonePartitionFetcher and LocalConfigNode now doesn't support partition fetch
        // via slotId
        schemaTree
            .getMatchedDevices(new PartialPath(ALL_RESULT_NODES))
            .forEach(
                deviceSchemaInfo -> {
                  PartialPath devicePath = deviceSchemaInfo.getDevicePath();
                  DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
                  queryParam.setDevicePath(devicePath.getFullPath());
                  sgNameToQueryParamsMap
                      .computeIfAbsent(
                          schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
                      .add(queryParam);
                });
      }

      DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitClearCache(
        ClearCacheStatement clearCacheStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(clearCacheStatement);
      analysis.setFinishQueryAfterAnalyze(true);

      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      BloomFilterCache.getInstance().clear();

      return analysis;
    }
  }
}
