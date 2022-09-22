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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.metadata.template.TemplateImcompatibeException;
import org.apache.iotdb.db.exception.sql.MeasurementNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InternalCreateTimeSeriesStatement;
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
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkTypeStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant.COLUMN_DEVICE;

/** This visitor is used to analyze each type of Statement and returns the {@link Analysis}. */
public class AnalyzeVisitor extends StatementVisitor<Analysis, MPPQueryContext> {

  private static final Logger logger = LoggerFactory.getLogger(Analyzer.class);

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private final MPPQueryContext context;

  public AnalyzeVisitor(
      IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher, MPPQueryContext context) {
    this.context = context;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
  }

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
      logger.info("[StartFetchSchema]");
      ISchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
      logger.info("[EndFetchSchema]");
      // If there is no leaf node in the schema tree, the query should be completed immediately
      if (schemaTree.isEmpty()) {
        if (queryStatement.isLastQuery()) {
          analysis.setRespDatasetHeader(DatasetHeaderFactory.getLastQueryHeader());
        }
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
        analysis.setMergeOrderParameter(analyzeOrderBy(queryStatement));
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
        Expression deviceExpression =
            new TimeSeriesOperand(new MeasurementPath(COLUMN_DEVICE, TSDataType.TEXT));
        Set<Expression> transformInput = new LinkedHashSet<>();
        transformInput.add(deviceExpression);
        Set<Expression> transformOutput = new LinkedHashSet<>();
        transformOutput.add(deviceExpression);

        // all selected device
        Set<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

        Map<String, Set<String>> deviceToMeasurementsMap = new HashMap<>();
        outputExpressions =
            analyzeSelect(
                analysis,
                schemaTree,
                deviceList,
                deviceToTransformExpressions,
                deviceToMeasurementsMap,
                transformInput);

        transformOutput.addAll(
            outputExpressions.stream().map(Pair::getLeft).collect(Collectors.toList()));

        if (queryStatement.hasHaving()) {
          Expression havingExpression =
              analyzeHaving(
                  analysis,
                  schemaTree,
                  deviceList,
                  deviceToTransformExpressions,
                  deviceToMeasurementsMap,
                  transformInput);
          analyzeExpression(analysis, havingExpression);

          // used for planFilter after planDeviceView
          analysis.setHavingExpression(havingExpression);
        }

        List<String> allMeasurements =
            transformInput.stream()
                .map(Expression::getExpressionString)
                .collect(Collectors.toList());

        Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
        for (String deviceName : deviceToMeasurementsMap.keySet()) {
          List<String> measurementsUnderDevice =
              new ArrayList<>(deviceToMeasurementsMap.get(deviceName));
          List<Integer> indexes = new ArrayList<>();
          for (String measurement : measurementsUnderDevice) {
            indexes.add(allMeasurements.indexOf(measurement));
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
                    transformExpressions, aggregationExpressions, aggregationTransformExpressions);
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
                deviceToSourceExpressions.computeIfAbsent(deviceName, key -> new LinkedHashSet<>()),
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
            analyzeExpression(analysis, queryFilter);
            updateSource(
                queryFilter,
                deviceToSourceExpressions.computeIfAbsent(
                    devicePath.getFullPath(), key -> new LinkedHashSet<>()),
                true);
          }
          analysis.setDeviceToQueryFilter(deviceToQueryFilter);
        }
        analysis.setTransformInput(transformInput);
        analysis.setTransformOutput(transformOutput);
        analysis.setDeviceToSourceExpressions(deviceToSourceExpressions);
        analysis.setDeviceToTransformExpressions(deviceToTransformExpressions);
      } else {
        outputExpressions = analyzeSelect(analysis, schemaTree);
        Set<Expression> transformExpressions =
            outputExpressions.stream()
                .map(Pair::getLeft)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // cast functionName to lowercase in havingExpression
        Expression havingExpression =
            queryStatement.hasHaving()
                ? ExpressionAnalyzer.removeAliasFromExpression(
                    queryStatement.getHavingCondition().getPredicate())
                : null;

        // get removeWildcard Expressions in Having
        // used to analyzeAggregation in Having expression and updateSource
        Set<Expression> transformExpressionsInHaving =
            queryStatement.hasHaving()
                ? new HashSet<>(
                    ExpressionAnalyzer.removeWildcardInFilter(
                        havingExpression,
                        queryStatement.getFromComponent().getPrefixPaths(),
                        schemaTree,
                        false))
                : null;

        if (queryStatement.isGroupByLevel()) {
          // map from grouped expression to set of input expressions
          Map<Expression, Expression> rawPathToGroupedPathMap = new HashMap<>();
          Map<Expression, Set<Expression>> groupByLevelExpressions =
              analyzeGroupByLevel(
                  analysis, outputExpressions, transformExpressions, rawPathToGroupedPathMap);
          analysis.setGroupByLevelExpressions(groupByLevelExpressions);
          analysis.setRawPathToGroupedPathMap(rawPathToGroupedPathMap);
        }

        // true if nested expressions and UDFs exist in aggregation function
        // i.e. select sum(s1 + 1) from root.sg.d1
        boolean isHasRawDataInputAggregation = false;
        if (queryStatement.isAggregationQuery()) {
          Set<Expression> aggregationExpressions = new HashSet<>();
          Set<Expression> aggregationTransformExpressions = new HashSet<>();
          List<Expression> aggregationExpressionsInHaving =
              new ArrayList<>(); // as input of GroupByLevelController
          isHasRawDataInputAggregation =
              analyzeAggregation(
                  transformExpressions, aggregationExpressions, aggregationTransformExpressions);
          if (queryStatement.hasHaving()) {
            isHasRawDataInputAggregation |=
                analyzeAggregationInHaving(
                    transformExpressionsInHaving,
                    aggregationExpressionsInHaving,
                    aggregationExpressions,
                    aggregationTransformExpressions);

            havingExpression = // construct Filter from Having
                analyzeHaving(
                    analysis,
                    analysis.getGroupByLevelExpressions(),
                    transformExpressionsInHaving,
                    aggregationExpressionsInHaving);
            analyzeExpression(analysis, havingExpression);
            analysis.setHavingExpression(havingExpression);
          }
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

        if (queryStatement.hasHaving()) {
          for (Expression expression : transformExpressionsInHaving) {
            analyzeExpression(analysis, expression);
            updateSource(expression, sourceExpressions, isRawDataSource);
          }
        }

        if (queryStatement.getWhereCondition() != null) {
          Expression queryFilter = analyzeWhere(queryStatement, schemaTree);

          // update sourceExpression according to queryFilter
          analyzeExpression(analysis, queryFilter);
          updateSource(queryFilter, sourceExpressions, isRawDataSource);
          analysis.setQueryFilter(queryFilter);
        }
        analysis.setRawDataSource(isRawDataSource);
        analysis.setSourceExpressions(sourceExpressions);
        analysis.setTransformExpressions(transformExpressions);
      }

      if (queryStatement.isGroupByTime()) {
        GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
        if ((groupByTimeComponent.isIntervalByMonth()
                || groupByTimeComponent.isSlidingStepByMonth())
            && queryStatement.getResultTimeOrder() == Ordering.DESC) {
          throw new SemanticException("Group by month doesn't support order by time desc now.");
        }
        analysis.setGroupByTimeParameter(new GroupByTimeParameter(groupByTimeComponent));
      }

      if (queryStatement.getFillComponent() != null) {
        FillComponent fillComponent = queryStatement.getFillComponent();
        analysis.setFillDescriptor(
            new FillDescriptor(fillComponent.getFillPolicy(), fillComponent.getFillValue()));
      }

      // generate result set header according to output expressions
      DatasetHeader datasetHeader = analyzeOutput(analysis, outputExpressions);
      analysis.setOutputExpressions(outputExpressions);
      analysis.setRespDatasetHeader(datasetHeader);

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
    } catch (StatementAnalyzeException | IllegalPathException e) {
      logger.error("Meet error when analyzing the query statement: ", e);
      throw new StatementAnalyzeException(
          "Meet error when analyzing the query statement: " + e.getMessage());
    }
    return analysis;
  }

  private List<Pair<Expression, String>> analyzeSelect(Analysis analysis, ISchemaTree schemaTree) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    boolean isGroupByLevel = queryStatement.isGroupByLevel();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(),
            queryStatement.getSeriesOffset(),
            queryStatement.isLastQuery() || isGroupByLevel);

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
          if (isGroupByLevel) {
            analyzeExpression(analysis, expression);
            outputExpressions.add(new Pair<>(expression, resultColumn.getAlias()));
            if (resultColumn.getExpression() instanceof FunctionExpression) {
              queryStatement
                  .getGroupByLevelComponent()
                  .updateIsCountStar((FunctionExpression) resultColumn.getExpression());
            }
          } else {
            Expression expressionWithoutAlias =
                ExpressionAnalyzer.removeAliasFromExpression(expression);
            String alias =
                !Objects.equals(expressionWithoutAlias, expression)
                    ? expression.getExpressionString()
                    : null;
            alias = hasAlias ? resultColumn.getAlias() : alias;
            analyzeExpression(analysis, expressionWithoutAlias);
            outputExpressions.add(new Pair<>(expressionWithoutAlias, alias));
          }
          paginationController.consumeLimit();
        } else {
          break;
        }
      }
    }
    return outputExpressions;
  }

  private Set<PartialPath> analyzeFrom(QueryStatement queryStatement, ISchemaTree schemaTree) {
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
      Analysis analysis,
      ISchemaTree schemaTree,
      Set<PartialPath> deviceList,
      Map<String, Set<Expression>> deviceToTransformExpressions,
      Map<String, Set<String>> deviceToMeasurementsMap,
      Set<Expression> transformInput) {

    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
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
            ExpressionAnalyzer.concatDeviceAndRemoveWildcard(selectExpression, device, schemaTree);
        if (queryStatement.isAggregationQuery()) {
          // extract aggregation in transformExpressions as input of Transform node after DeviceView
          // node
          Set<Expression> aggregationExpressions = analyzeAggregation(transformExpressions);
          for (Expression aggregationExpression : aggregationExpressions) {
            Expression measurementExpression =
                ExpressionAnalyzer.getMeasurementExpression(aggregationExpression);
            transformInput.add(measurementExpression);
            analyzeExpression(analysis, measurementExpression);
          }
        }
        for (Expression transformExpression : transformExpressions) {
          Expression measurementExpression =
              ExpressionAnalyzer.getMeasurementExpression(transformExpression);
          measurementToDeviceTransformExpressions
              .computeIfAbsent(measurementExpression, key -> new LinkedHashMap<>())
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
              .forEach(expression -> analyzeExpression(analysis, expression));
          // check whether the datatype of paths which has the same measurement name are
          // consistent
          // if not, throw a SemanticException
          checkDataTypeConsistencyInAlignByDevice(
              analysis, new ArrayList<>(deviceToTransformExpressionOfOneMeasurement.values()));

          // add outputExpressions
          Expression measurementExpressionWithoutAlias =
              ExpressionAnalyzer.removeAliasFromExpression(measurementExpression);
          String alias =
              !Objects.equals(measurementExpressionWithoutAlias, measurementExpression)
                  ? measurementExpression.getExpressionString()
                  : null;
          alias = hasAlias ? resultColumn.getAlias() : alias;
          analyzeExpression(analysis, measurementExpressionWithoutAlias);
          outputExpressions.add(new Pair<>(measurementExpressionWithoutAlias, alias));

          // add deviceToTransformExpressions
          for (String deviceName : deviceToTransformExpressionOfOneMeasurement.keySet()) {
            Expression transformExpression =
                deviceToTransformExpressionOfOneMeasurement.get(deviceName);
            Expression transformExpressionWithoutAlias =
                ExpressionAnalyzer.removeAliasFromExpression(transformExpression);
            analyzeExpression(analysis, transformExpressionWithoutAlias);
            deviceToTransformExpressions
                .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
                .add(transformExpressionWithoutAlias);
            if (queryStatement.isAggregationQuery()) {
              deviceToMeasurementsMap
                  .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
                  .addAll(
                      ExpressionAnalyzer.searchAggregationExpressions(
                              measurementExpressionWithoutAlias)
                          .stream()
                          .map(Expression::getExpressionString)
                          .collect(Collectors.toList()));
            } else {
              List<Expression> sourceExpressions =
                  ExpressionAnalyzer.searchSourceExpressions(
                      measurementExpressionWithoutAlias, true);
              transformInput.addAll(sourceExpressions);
              deviceToMeasurementsMap
                  .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
                  .addAll(
                      sourceExpressions.stream()
                          .map(Expression::getExpressionString)
                          .collect(Collectors.toList()));
            }
          }
          paginationController.consumeLimit();
        } else {
          break;
        }
      }
    }

    return outputExpressions;
  }

  private Expression analyzeHaving(
      Analysis analysis,
      ISchemaTree schemaTree,
      Set<PartialPath> deviceList,
      Map<String, Set<Expression>> deviceToTransformExpressions,
      Map<String, Set<String>> deviceToMeasurementsMap,
      Set<Expression> transformInput) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    Expression havingExpression =
        ExpressionAnalyzer.removeAliasFromExpression(
            queryStatement.getHavingCondition().getPredicate());
    Set<Expression> measurementsInHaving = new HashSet<>();

    // measurement expression after removing wildcard
    // use LinkedHashMap for order-preserving
    Map<Expression, Map<String, Expression>> measurementToDeviceTransformExpressions =
        new LinkedHashMap<>();
    for (PartialPath device : deviceList) {
      List<Expression> transformExpressionsInHaving =
          ExpressionAnalyzer.concatDeviceAndRemoveWildcard(havingExpression, device, schemaTree);

      measurementsInHaving.addAll(
          transformExpressionsInHaving.stream()
              .map(
                  transformExpression ->
                      ExpressionAnalyzer.getMeasurementExpression(transformExpression))
              .collect(Collectors.toList()));

      Set<Expression> aggregationExpressionsInHaving =
          analyzeAggregation(transformExpressionsInHaving);

      for (Expression aggregationExpressionInHaving : aggregationExpressionsInHaving) {
        Expression measurementExpression =
            ExpressionAnalyzer.getMeasurementExpression(aggregationExpressionInHaving);
        transformInput.add(measurementExpression);
        analyzeExpression(analysis, measurementExpression);
        measurementToDeviceTransformExpressions
            .computeIfAbsent(measurementExpression, key -> new LinkedHashMap<>())
            .put(device.getFullPath(), aggregationExpressionInHaving);
      }
    }

    for (Expression measurementExpression : measurementToDeviceTransformExpressions.keySet()) {

      Map<String, Expression> deviceToTransformExpressionOfOneMeasurement =
          measurementToDeviceTransformExpressions.get(measurementExpression);
      deviceToTransformExpressionOfOneMeasurement
          .values()
          .forEach(expression -> analyzeExpression(analysis, expression));

      // check whether the datatype of paths which has the same measurement name are
      // consistent
      // if not, throw a SemanticException
      checkDataTypeConsistencyInAlignByDevice(
          analysis, new ArrayList<>(deviceToTransformExpressionOfOneMeasurement.values()));

      analyzeExpression(analysis, measurementExpression);

      // add deviceToTransformExpressions
      for (String deviceName : deviceToTransformExpressionOfOneMeasurement.keySet()) {
        Expression transformExpression =
            deviceToTransformExpressionOfOneMeasurement.get(deviceName);

        deviceToTransformExpressions
            .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
            .add(transformExpression);
        deviceToMeasurementsMap
            .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
            .add(measurementExpression.getExpressionString());
      }
    }

    return ExpressionUtils.constructQueryFilter(
        measurementsInHaving.stream().distinct().collect(Collectors.toList()));
  }

  private Pair<Filter, Boolean> analyzeGlobalTimeFilter(QueryStatement queryStatement) {
    Filter globalTimeFilter = null;
    boolean hasValueFilter = false;
    if (queryStatement.getWhereCondition() != null) {
      Expression predicate = queryStatement.getWhereCondition().getPredicate();
      WhereCondition whereCondition = queryStatement.getWhereCondition();
      Pair<Filter, Boolean> resultPair =
          ExpressionAnalyzer.transformToGlobalTimeFilter(predicate, true, true);
      predicate = ExpressionAnalyzer.evaluatePredicate(predicate);

      // set where condition to null if predicate is true
      if (predicate.getExpressionType().equals(ExpressionType.CONSTANT)
          && Boolean.parseBoolean(predicate.getExpressionString())) {
        queryStatement.setWhereCondition(null);
      } else {
        whereCondition.setPredicate(predicate);
      }
      globalTimeFilter = resultPair.left;
      hasValueFilter = resultPair.right;
    }
    if (queryStatement.isGroupByTime()) {
      GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
      Filter groupByFilter = initGroupByFilter(groupByTimeComponent);
      if (globalTimeFilter == null) {
        globalTimeFilter = groupByFilter;
      } else {
        globalTimeFilter = FilterFactory.and(globalTimeFilter, groupByFilter);
      }
    }
    return new Pair<>(globalTimeFilter, hasValueFilter);
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

  private boolean analyzeAggregationInHaving(
      Set<Expression> expressions,
      List<Expression> aggregationExpressionsInHaving,
      Set<Expression> aggregationExpressions,
      Set<Expression> aggregationTransformExpressions) {
    // true if nested expressions and UDFs exist in aggregation function
    // i.e. select sum(s1 + 1) from root.sg.d1 align by device
    boolean isHasRawDataInputAggregation = false;
    for (Expression expression : expressions) {
      for (Expression aggregationExpression :
          ExpressionAnalyzer.searchAggregationExpressions(expression)) {
        aggregationExpressionsInHaving.add(aggregationExpression);
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

  private Set<Expression> analyzeAggregation(List<Expression> inputExpressions) {
    Set<Expression> aggregationExpressions = new HashSet<>();
    for (Expression inputExpression : inputExpressions) {
      for (Expression aggregationExpression :
          ExpressionAnalyzer.searchAggregationExpressions(inputExpression)) {
        aggregationExpressions.add(aggregationExpression);
      }
    }
    return aggregationExpressions;
  }

  private Expression analyzeWhere(QueryStatement queryStatement, ISchemaTree schemaTree) {
    List<Expression> rewrittenPredicates =
        ExpressionAnalyzer.removeWildcardInFilter(
            queryStatement.getWhereCondition().getPredicate(),
            queryStatement.getFromComponent().getPrefixPaths(),
            schemaTree,
            true);
    return ExpressionUtils.constructQueryFilter(
        rewrittenPredicates.stream().distinct().collect(Collectors.toList()));
  }

  private Expression analyzeWhereSplitByDevice(
      QueryStatement queryStatement, PartialPath devicePath, ISchemaTree schemaTree) {
    List<Expression> rewrittenPredicates =
        ExpressionAnalyzer.removeWildcardInFilterByDevice(
            queryStatement.getWhereCondition().getPredicate(), devicePath, schemaTree, true);
    return ExpressionUtils.constructQueryFilter(
        rewrittenPredicates.stream().distinct().collect(Collectors.toList()));
  }

  private Expression analyzeHaving(
      Analysis analysis,
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      Set<Expression> transformExpressionsInHaving,
      List<Expression> aggregationExpressionsInHaving) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();

    if (queryStatement.isGroupByLevel()) {
      Map<Expression, Expression> rawPathToGroupedPathMapInHaving =
          analyzeGroupByLevelInHaving(
              analysis, aggregationExpressionsInHaving, groupByLevelExpressions);
      List<Expression> convertedPredicates = new ArrayList<>();
      for (Expression expression : transformExpressionsInHaving) {
        Expression convertedPredicate =
            ExpressionAnalyzer.replaceRawPathWithGroupedPath(
                expression, rawPathToGroupedPathMapInHaving);
        convertedPredicates.add(convertedPredicate);
        analyzeExpression(analysis, expression);
      }
      return ExpressionUtils.constructQueryFilter(
          convertedPredicates.stream().distinct().collect(Collectors.toList()));
    }

    return ExpressionUtils.constructQueryFilter(
        transformExpressionsInHaving.stream().distinct().collect(Collectors.toList()));
  }

  private Map<Expression, Expression> analyzeGroupByLevelInHaving(
      Analysis analysis,
      List<Expression> inputExpressions,
      Map<Expression, Set<Expression>> groupByLevelExpressions) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    GroupByLevelController groupByLevelController =
        new GroupByLevelController(queryStatement.getGroupByLevelComponent().getLevels());
    for (Expression inputExpression : inputExpressions) {
      groupByLevelController.control(false, inputExpression, null);
    }
    Map<Expression, Set<Expression>> groupedPathMap = groupByLevelController.getGroupedPathMap();
    groupedPathMap.keySet().forEach(expression -> analyzeExpression(analysis, expression));
    groupByLevelExpressions.putAll(groupedPathMap);
    return groupByLevelController.getRawPathToGroupedPathMap();
  }

  private Map<Expression, Set<Expression>> analyzeGroupByLevel(
      Analysis analysis,
      List<Pair<Expression, String>> outputExpressions,
      Set<Expression> transformExpressions,
      Map<Expression, Expression> rawPathToGroupedPathMap) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    GroupByLevelController groupByLevelController =
        new GroupByLevelController(queryStatement.getGroupByLevelComponent().getLevels());
    for (int i = 0; i < outputExpressions.size(); i++) {
      Pair<Expression, String> expressionAliasPair = outputExpressions.get(i);
      boolean isCountStar = queryStatement.getGroupByLevelComponent().isCountStar(i);
      groupByLevelController.control(
          isCountStar, expressionAliasPair.left, expressionAliasPair.right);
    }
    Map<Expression, Set<Expression>> rawGroupByLevelExpressions =
        groupByLevelController.getGroupedPathMap();
    rawPathToGroupedPathMap.putAll(groupByLevelController.getRawPathToGroupedPathMap());

    Map<Expression, Set<Expression>> groupByLevelExpressions = new LinkedHashMap<>();
    outputExpressions.clear();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);
    for (Expression groupedExpression : rawGroupByLevelExpressions.keySet()) {
      if (paginationController.hasCurOffset()) {
        paginationController.consumeOffset();
        continue;
      }
      if (paginationController.hasCurLimit()) {
        Pair<Expression, String> outputExpression =
            removeAliasFromExpression(
                groupedExpression,
                groupByLevelController.getAlias(groupedExpression.getExpressionString()));
        Expression groupedExpressionWithoutAlias = outputExpression.left;

        Set<Expression> rawExpressions = rawGroupByLevelExpressions.get(groupedExpression);
        rawExpressions.forEach(expression -> analyzeExpression(analysis, expression));

        Set<Expression> rawExpressionsWithoutAlias =
            rawExpressions.stream()
                .map(ExpressionAnalyzer::removeAliasFromExpression)
                .collect(Collectors.toSet());
        rawExpressionsWithoutAlias.forEach(expression -> analyzeExpression(analysis, expression));

        groupByLevelExpressions.put(groupedExpressionWithoutAlias, rawExpressionsWithoutAlias);
        analyzeExpression(analysis, groupedExpressionWithoutAlias);
        outputExpressions.add(outputExpression);
        paginationController.consumeLimit();
      } else {
        break;
      }
    }

    // reset transformExpressions after applying SLIMIT/SOFFSET
    transformExpressions.clear();
    transformExpressions.addAll(
        groupByLevelExpressions.values().stream().flatMap(Set::stream).collect(Collectors.toSet()));
    return groupByLevelExpressions;
  }

  private Pair<Expression, String> removeAliasFromExpression(
      Expression rawExpression, String rawAlias) {
    Expression expressionWithoutAlias = ExpressionAnalyzer.removeAliasFromExpression(rawExpression);
    String alias =
        !Objects.equals(expressionWithoutAlias, rawExpression)
            ? rawExpression.getExpressionString()
            : null;
    alias = rawAlias == null ? alias : rawAlias;
    return new Pair<>(expressionWithoutAlias, alias);
  }

  private DatasetHeader analyzeOutput(
      Analysis analysis, List<Pair<Expression, String>> outputExpressions) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    boolean isIgnoreTimestamp =
        queryStatement.isAggregationQuery() && !queryStatement.isGroupByTime();
    List<ColumnHeader> columnHeaders = new ArrayList<>();
    if (queryStatement.isAlignByDevice()) {
      columnHeaders.add(new ColumnHeader(COLUMN_DEVICE, TSDataType.TEXT, null));
    }
    columnHeaders.addAll(
        outputExpressions.stream()
            .map(
                expressionAliasPair -> {
                  String columnName = expressionAliasPair.left.getExpressionString();
                  String alias = expressionAliasPair.right;
                  return new ColumnHeader(
                      columnName, analysis.getType(expressionAliasPair.left), alias);
                })
            .collect(Collectors.toList()));
    return new DatasetHeader(columnHeaders, isIgnoreTimestamp);
  }

  private Analysis analyzeLast(
      Analysis analysis, List<MeasurementPath> allSelectedPath, ISchemaTree schemaTree) {
    Set<Expression> sourceExpressions;
    List<SortItem> sortItemList = analysis.getMergeOrderParameter().getSortItemList();
    if (sortItemList.size() > 0) {
      checkState(
          sortItemList.size() == 1 && sortItemList.get(0).getSortKey() == SortKey.TIMESERIES,
          "Last queries only support sorting by timeseries now.");
      boolean isAscending = sortItemList.get(0).getOrdering() == Ordering.ASC;
      sourceExpressions =
          allSelectedPath.stream()
              .map(TimeSeriesOperand::new)
              .sorted(
                  (o1, o2) ->
                      isAscending
                          ? o1.getExpressionString().compareTo(o2.getExpressionString())
                          : o2.getExpressionString().compareTo(o1.getExpressionString()))
              .collect(Collectors.toCollection(LinkedHashSet::new));
    } else {
      sourceExpressions =
          allSelectedPath.stream()
              .map(TimeSeriesOperand::new)
              .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    sourceExpressions.forEach(expression -> analyzeExpression(analysis, expression));
    analysis.setSourceExpressions(sourceExpressions);

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getLastQueryHeader());

    Set<String> deviceSet =
        allSelectedPath.stream().map(MeasurementPath::getDevice).collect(Collectors.toSet());
    Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
    for (String devicePath : deviceSet) {
      DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
      queryParam.setDevicePath(devicePath);
      sgNameToQueryParamsMap
          .computeIfAbsent(schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
          .add(queryParam);
    }
    DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
    analysis.setDataPartitionInfo(dataPartition);

    return analysis;
  }

  private DataPartition fetchDataPartitionByDevices(Set<String> deviceSet, ISchemaTree schemaTree) {
    Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
    for (String devicePath : deviceSet) {
      DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
      queryParam.setDevicePath(devicePath);
      sgNameToQueryParamsMap
          .computeIfAbsent(schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
          .add(queryParam);
    }
    return partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
  }

  private OrderByParameter analyzeOrderBy(QueryStatement queryStatement) {
    return new OrderByParameter(queryStatement.getSortItemList());
  }

  private void analyzeExpression(Analysis analysis, Expression expression) {
    ExpressionTypeAnalyzer.analyzeExpression(analysis, expression);
  }

  /**
   * Check datatype consistency in ALIGN BY DEVICE.
   *
   * <p>an inconsistent example: select s0 from root.sg1.d1, root.sg1.d2 align by device, return
   * false while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
   */
  private void checkDataTypeConsistencyInAlignByDevice(
      Analysis analysis, List<Expression> expressions) {
    TSDataType checkedDataType = analysis.getType(expressions.get(0));
    for (Expression expression : expressions) {
      if (analysis.getType(expression) != checkedDataType) {
        throw new SemanticException(
            "ALIGN BY DEVICE: the data types of the same measurement column should be the same across devices.");
      }
    }
  }

  @Override
  public Analysis visitInsert(InsertStatement insertStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    insertStatement.semanticCheck();
    long[] timeArray = insertStatement.getTimes();
    PartialPath devicePath = insertStatement.getDevice();
    String[] measurementList = insertStatement.getMeasurementList();
    if (timeArray.length == 1) {
      // construct insert row statement
      InsertRowStatement insertRowStatement = new InsertRowStatement();
      insertRowStatement.setDevicePath(devicePath);
      insertRowStatement.setTime(timeArray[0]);
      insertRowStatement.setMeasurements(measurementList);
      insertRowStatement.setDataTypes(new TSDataType[measurementList.length]);
      Object[] values = new Object[measurementList.length];
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
        String[] measurements = new String[measurementList.length];
        System.arraycopy(measurementList, 0, measurements, 0, measurements.length);
        statement.setMeasurements(measurements);
        statement.setTime(timeArray[i]);
        statement.setDataTypes(new TSDataType[measurementList.length]);
        Object[] values = new Object[measurementList.length];
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
              String.format("Tag and attribute shouldn't have the same property key [%s]", tagKey));
        }
      }
    }
    Analysis analysis = new Analysis();
    analysis.setStatement(createTimeSeriesStatement);

    checkIsTemplateCompatible(
        createTimeSeriesStatement.getPath(), createTimeSeriesStatement.getAlias());

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(createTimeSeriesStatement.getPath());
    SchemaPartition schemaPartitionInfo = partitionFetcher.getOrCreateSchemaPartition(patternTree);
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  private void checkIsTemplateCompatible(PartialPath timeseriesPath, String alias) {
    Pair<Template, PartialPath> templateInfo = schemaFetcher.checkTemplateSetInfo(timeseriesPath);
    if (templateInfo != null) {
      if (templateInfo.left.hasSchema(timeseriesPath.getMeasurement())) {
        throw new RuntimeException(
            new TemplateImcompatibeException(
                timeseriesPath.getFullPath(),
                templateInfo.left.getName(),
                timeseriesPath.getMeasurement()));
      }

      if (alias != null && templateInfo.left.hasSchema(alias)) {
        throw new RuntimeException(
            new TemplateImcompatibeException(
                timeseriesPath.getDevicePath().concatNode(alias).getFullPath(),
                templateInfo.left.getName(),
                alias));
      }
    }
  }

  private void checkIsTemplateCompatible(
      PartialPath devicePath, List<String> measurements, List<String> aliasList) {
    Pair<Template, PartialPath> templateInfo = schemaFetcher.checkTemplateSetInfo(devicePath);
    if (templateInfo != null) {
      Template template = templateInfo.left;
      for (String measurement : measurements) {
        if (template.hasSchema(measurement)) {
          throw new RuntimeException(
              new TemplateImcompatibeException(
                  devicePath.concatNode(measurement).getFullPath(),
                  templateInfo.left.getName(),
                  measurement));
        }
      }

      if (aliasList == null) {
        return;
      }

      for (String alias : aliasList) {
        if (template.hasSchema(alias)) {
          throw new RuntimeException(
              new TemplateImcompatibeException(
                  devicePath.concatNode(alias).getFullPath(), templateInfo.left.getName(), alias));
        }
      }
    }
  }

  @Override
  public Analysis visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    List<String> measurements = createAlignedTimeSeriesStatement.getMeasurements();
    Set<String> measurementsSet = new HashSet<>(measurements);
    if (measurementsSet.size() < measurements.size()) {
      throw new SemanticException(
          "Measurement under an aligned device is not allowed to have the same measurement name");
    }

    Analysis analysis = new Analysis();
    analysis.setStatement(createAlignedTimeSeriesStatement);

    checkIsTemplateCompatible(
        createAlignedTimeSeriesStatement.getDevicePath(),
        createAlignedTimeSeriesStatement.getMeasurements(),
        createAlignedTimeSeriesStatement.getAliasList());

    PathPatternTree pathPatternTree = new PathPatternTree();
    for (String measurement : createAlignedTimeSeriesStatement.getMeasurements()) {
      pathPatternTree.appendFullPath(createAlignedTimeSeriesStatement.getDevicePath(), measurement);
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

    checkIsTemplateCompatible(
        internalCreateTimeSeriesStatement.getDevicePath(),
        internalCreateTimeSeriesStatement.getMeasurements(),
        null);

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

    List<PartialPath> timeseriesPathList = createMultiTimeSeriesStatement.getPaths();
    List<String> aliasList = createMultiTimeSeriesStatement.getAliasList();
    for (int i = 0; i < timeseriesPathList.size(); i++) {
      checkIsTemplateCompatible(
          timeseriesPathList.get(i), aliasList == null ? null : aliasList.get(i));
    }

    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : createMultiTimeSeriesStatement.getPaths()) {
      patternTree.appendFullPath(path);
    }
    SchemaPartition schemaPartitionInfo = partitionFetcher.getOrCreateSchemaPartition(patternTree);
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  @Override
  public Analysis visitAlterTimeseries(
      AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setStatement(alterTimeSeriesStatement);

    if (alterTimeSeriesStatement.getAlias() != null) {
      checkIsTemplateCompatible(
          alterTimeSeriesStatement.getPath(), alterTimeSeriesStatement.getAlias());
    }

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
    dataPartitionQueryParam.setTimePartitionSlotList(insertTabletStatement.getTimePartitionSlots());

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
    for (InsertRowStatement insertRowStatement : insertRowsStatement.getInsertRowStatementList()) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertRowStatement.getTimePartitionSlots());
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
  public Analysis visitLoadFile(LoadTsFileStatement loadTsFileStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    Map<String, Long> device2MinTime = new HashMap<>();
    Map<String, Long> device2MaxTime = new HashMap<>();
    Map<String, Map<MeasurementSchema, File>> device2Schemas = new HashMap<>();
    Map<String, Pair<Boolean, File>> device2IsAligned = new HashMap<>();

    // analyze tsfile metadata
    for (File tsFile : loadTsFileStatement.getTsFiles()) {
      try {
        TsFileResource resource =
            analyzeTsFile(
                loadTsFileStatement,
                tsFile,
                device2MinTime,
                device2MaxTime,
                device2Schemas,
                device2IsAligned);
        loadTsFileStatement.addTsFileResource(resource);
      } catch (Exception e) {
        logger.error(String.format("Parse file %s to resource error.", tsFile.getPath()), e);
        throw new SemanticException(
            String.format("Parse file %s to resource error", tsFile.getPath()));
      }
    }

    // auto create and verify schema
    if (loadTsFileStatement.isVerifySchema() || loadTsFileStatement.isAutoCreateSchema()) {
      try {
        if (loadTsFileStatement.isVerifySchema()) {
          verifyLoadingMeasurements(device2Schemas);
        }
        autoCreateSg(loadTsFileStatement.getSgLevel(), device2Schemas);
        ISchemaTree schemaTree = autoCreateSchema(device2Schemas, device2IsAligned);
        if (loadTsFileStatement.isVerifySchema()) {
          verifySchema(schemaTree, device2Schemas, device2IsAligned);
        }
      } catch (Exception e) {
        logger.error("Auto create or verify schema error.", e);
        throw new SemanticException(
            String.format(
                "Auto create or verify schema error when executing statement %s.",
                loadTsFileStatement));
      }
    }

    // construct partition info
    List<DataPartitionQueryParam> params = new ArrayList<>();
    for (Map.Entry<String, Long> entry : device2MinTime.entrySet()) {
      List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
      String device = entry.getKey();
      long endTime = device2MaxTime.get(device);
      long interval = StorageEngineV2.getTimePartitionInterval();
      long time = (entry.getValue() / interval) * interval;
      for (; time <= endTime; time += interval) {
        timePartitionSlots.add(StorageEngineV2.getTimePartitionSlot(time));
      }

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(device);
      dataPartitionQueryParam.setTimePartitionSlotList(timePartitionSlots);
      params.add(dataPartitionQueryParam);
    }

    DataPartition dataPartition = partitionFetcher.getOrCreateDataPartition(params);

    Analysis analysis = new Analysis();
    analysis.setStatement(loadTsFileStatement);
    analysis.setDataPartitionInfo(dataPartition);

    return analysis;
  }

  private TsFileResource analyzeTsFile(
      LoadTsFileStatement statement,
      File tsFile,
      Map<String, Long> device2MinTime,
      Map<String, Long> device2MaxTime,
      Map<String, Map<MeasurementSchema, File>> device2Schemas,
      Map<String, Pair<Boolean, File>> device2IsAligned)
      throws IOException, VerifyMetadataException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      Map<String, List<TimeseriesMetadata>> device2Metadata = reader.getAllTimeseriesMetadata(true);

      if (statement.isAutoCreateSchema() || statement.isVerifySchema()) {
        // construct schema
        for (Map.Entry<String, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
          String device = entry.getKey();
          List<TimeseriesMetadata> timeseriesMetadataList = entry.getValue();
          boolean isAligned = false;
          for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
            TSDataType dataType = timeseriesMetadata.getTSDataType();
            if (!dataType.equals(TSDataType.VECTOR)) {
              ChunkHeader chunkHeader =
                  getChunkHeaderByTimeseriesMetadata(reader, timeseriesMetadata);
              MeasurementSchema measurementSchema =
                  new MeasurementSchema(
                      timeseriesMetadata.getMeasurementId(),
                      dataType,
                      chunkHeader.getEncodingType(),
                      chunkHeader.getCompressionType());
              device2Schemas
                  .computeIfAbsent(device, o -> new HashMap<>())
                  .put(measurementSchema, tsFile);
            } else {
              isAligned = true;
            }
          }
          boolean finalIsAligned = isAligned;
          if (!device2IsAligned
              .computeIfAbsent(device, o -> new Pair<>(finalIsAligned, tsFile))
              .left
              .equals(isAligned)) {
            throw new VerifyMetadataException(
                String.format(
                    "Device %s has different aligned definition in tsFile %s and other TsFile.",
                    device, tsFile.getParentFile()));
          }
        }
      }

      // construct TsFileResource
      TsFileResource resource = new TsFileResource(tsFile);
      FileLoaderUtils.updateTsFileResource(device2Metadata, resource);
      resource.updatePlanIndexes(reader.getMinPlanIndex());
      resource.updatePlanIndexes(reader.getMaxPlanIndex());

      // construct device time range
      for (String device : resource.getDevices()) {
        device2MinTime.put(
            device,
            Math.min(
                device2MinTime.getOrDefault(device, Long.MAX_VALUE),
                resource.getStartTime(device)));
        device2MaxTime.put(
            device,
            Math.max(
                device2MaxTime.getOrDefault(device, Long.MIN_VALUE), resource.getEndTime(device)));
      }

      resource.setStatus(TsFileResourceStatus.CLOSED);
      return resource;
    }
  }

  private ChunkHeader getChunkHeaderByTimeseriesMetadata(
      TsFileSequenceReader reader, TimeseriesMetadata timeseriesMetadata) throws IOException {
    IChunkMetadata chunkMetadata = timeseriesMetadata.getChunkMetadataList().get(0);
    reader.position(chunkMetadata.getOffsetOfChunkHeader());
    return reader.readChunkHeader(reader.readMarker());
  }

  private void autoCreateSg(int sgLevel, Map<String, Map<MeasurementSchema, File>> device2Schemas)
      throws VerifyMetadataException, LoadFileException, IllegalPathException {
    sgLevel += 1; // e.g. "root.sg" means sgLevel = 1, "root.sg.test" means sgLevel=2
    Set<PartialPath> sgSet = new HashSet<>();
    for (String device : device2Schemas.keySet()) {
      PartialPath devicePath = new PartialPath(device);

      String[] nodes = devicePath.getNodes();
      String[] sgNodes = new String[sgLevel];
      if (nodes.length < sgLevel) {
        throw new VerifyMetadataException(
            String.format("Sg level %d is longer than device %s.", sgLevel, device));
      }
      for (int i = 0; i < sgLevel; i++) {
        sgNodes[i] = nodes[i];
      }
      PartialPath sgPath = new PartialPath(sgNodes);
      sgSet.add(sgPath);
    }

    for (PartialPath sgPath : sgSet) {
      SetStorageGroupStatement statement = new SetStorageGroupStatement();
      statement.setStorageGroupPath(sgPath);
      executeSetStorageGroupStatement(statement);
    }
  }

  private void executeSetStorageGroupStatement(Statement statement) throws LoadFileException {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    ExecutionResult result =
        Coordinator.getInstance()
            .execute(
                statement,
                queryId,
                null,
                "",
                partitionFetcher,
                schemaFetcher,
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && result.status.code != TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode()) {
      logger.error(String.format("Set Storage group error, statement: %s.", statement));
      logger.error(String.format("Set storage group result status : %s.", result.status));
      throw new LoadFileException(
          String.format("Can not execute set storage group statement: %s", statement));
    }
  }

  private ISchemaTree autoCreateSchema(
      Map<String, Map<MeasurementSchema, File>> device2Schemas,
      Map<String, Pair<Boolean, File>> device2IsAligned)
      throws IllegalPathException {
    List<PartialPath> deviceList = new ArrayList<>();
    List<String[]> measurementList = new ArrayList<>();
    List<TSDataType[]> dataTypeList = new ArrayList<>();
    List<TSEncoding[]> encodingsList = new ArrayList<>();
    List<CompressionType[]> compressionTypesList = new ArrayList<>();
    List<Boolean> isAlignedList = new ArrayList<>();

    for (Map.Entry<String, Map<MeasurementSchema, File>> entry : device2Schemas.entrySet()) {
      int measurementSize = entry.getValue().size();
      String[] measurements = new String[measurementSize];
      TSDataType[] tsDataTypes = new TSDataType[measurementSize];
      TSEncoding[] encodings = new TSEncoding[measurementSize];
      CompressionType[] compressionTypes = new CompressionType[measurementSize];

      int index = 0;
      for (MeasurementSchema measurementSchema : entry.getValue().keySet()) {
        measurements[index] = measurementSchema.getMeasurementId();
        tsDataTypes[index] = measurementSchema.getType();
        encodings[index] = measurementSchema.getEncodingType();
        compressionTypes[index++] = measurementSchema.getCompressor();
      }

      deviceList.add(new PartialPath(entry.getKey()));
      measurementList.add(measurements);
      dataTypeList.add(tsDataTypes);
      encodingsList.add(encodings);
      compressionTypesList.add(compressionTypes);
      isAlignedList.add(device2IsAligned.get(entry.getKey()).left);
    }

    return SchemaValidator.validate(
        deviceList,
        measurementList,
        dataTypeList,
        encodingsList,
        compressionTypesList,
        isAlignedList);
  }

  private void verifyLoadingMeasurements(Map<String, Map<MeasurementSchema, File>> device2Schemas)
      throws VerifyMetadataException {
    for (Map.Entry<String, Map<MeasurementSchema, File>> deviceEntry : device2Schemas.entrySet()) {
      Map<String, MeasurementSchema> id2Schema = new HashMap<>();
      Map<MeasurementSchema, File> schema2TsFile = deviceEntry.getValue();
      for (Map.Entry<MeasurementSchema, File> entry : schema2TsFile.entrySet()) {
        String measurementId = entry.getKey().getMeasurementId();
        if (!id2Schema.containsKey(measurementId)) {
          id2Schema.put(measurementId, entry.getKey());
        } else {
          MeasurementSchema conflictSchema = id2Schema.get(measurementId);
          String msg =
              String.format(
                  "Measurement %s Conflict, TsFile %s has measurement: %s, TsFile %s has measurement %s.",
                  deviceEntry.getKey() + measurementId,
                  entry.getValue().getPath(),
                  entry.getKey(),
                  schema2TsFile.get(conflictSchema).getPath(),
                  conflictSchema);
          logger.error(msg);
          throw new VerifyMetadataException(msg);
        }
      }
    }
  }

  private void verifySchema(
      ISchemaTree schemaTree,
      Map<String, Map<MeasurementSchema, File>> device2Schemas,
      Map<String, Pair<Boolean, File>> device2IsAligned)
      throws VerifyMetadataException, IllegalPathException {
    for (Map.Entry<String, Map<MeasurementSchema, File>> entry : device2Schemas.entrySet()) {
      String device = entry.getKey();
      MeasurementSchema[] tsFileSchemas =
          entry.getValue().keySet().toArray(new MeasurementSchema[0]);
      DeviceSchemaInfo schemaInfo =
          schemaTree.searchDeviceSchemaInfo(
              new PartialPath(device),
              Arrays.stream(tsFileSchemas)
                  .map(MeasurementSchema::getMeasurementId)
                  .collect(Collectors.toList()));
      if (schemaInfo.isAligned() != device2IsAligned.get(device).left) {
        throw new VerifyMetadataException(
            device,
            "Is aligned",
            device2IsAligned.get(device).left.toString(),
            device2IsAligned.get(device).right.getPath(),
            String.valueOf(schemaInfo.isAligned()));
      }
      List<MeasurementSchema> originSchemaList = schemaInfo.getMeasurementSchemaList();
      int measurementSize = originSchemaList.size();
      for (int j = 0; j < measurementSize; j++) {
        MeasurementSchema originSchema = originSchemaList.get(j);
        MeasurementSchema tsFileSchema = tsFileSchemas[j];
        String measurementPath =
            device + TsFileConstant.PATH_SEPARATOR + originSchema.getMeasurementId();
        if (!tsFileSchema.getType().equals(originSchema.getType())) {
          throw new VerifyMetadataException(
              measurementPath,
              "Datatype",
              tsFileSchema.getType().name(),
              entry.getValue().get(tsFileSchema).getPath(),
              originSchema.getType().name());
        }
        if (!tsFileSchema.getEncodingType().equals(originSchema.getEncodingType())) {
          throw new VerifyMetadataException(
              measurementPath,
              "Encoding",
              tsFileSchema.getEncodingType().name(),
              entry.getValue().get(tsFileSchema).getPath(),
              originSchema.getEncodingType().name());
        }
        if (!tsFileSchema.getCompressor().equals(originSchema.getCompressor())) {
          throw new VerifyMetadataException(
              measurementPath,
              "Compress type",
              tsFileSchema.getCompressor().name(),
              entry.getValue().get(tsFileSchema).getPath(),
              originSchema.getCompressor().name());
        }
      }
    }
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

    Map<Integer, Template> templateMap =
        schemaFetcher.checkAllRelatedTemplate(showTimeSeriesStatement.getPathPattern());
    analysis.setRelatedTemplateInfo(templateMap);

    if (showTimeSeriesStatement.isOrderByHeat()) {
      patternTree.constructTree();
      // request schema fetch API
      logger.info("[StartFetchSchema]");
      ISchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
      logger.info("[EndFetchSchema]]");
      List<MeasurementPath> allSelectedPath = schemaTree.getAllMeasurement();

      Set<Expression> sourceExpressions =
          allSelectedPath.stream()
              .map(TimeSeriesOperand::new)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      analysis.setSourceExpressions(sourceExpressions);
      sourceExpressions.forEach(expression -> analyzeExpression(analysis, expression));

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

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowTimeSeriesHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showStorageGroupStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowStorageGroupHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowTTL(ShowTTLStatement showTTLStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showTTLStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowTTLHeader());
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
            ? DatasetHeaderFactory.getShowDevicesWithSgHeader()
            : DatasetHeaderFactory.getShowDevicesHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowCluster(
      ShowClusterStatement showClusterStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showClusterStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowClusterHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(countStorageGroupStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountStorageGroupHeader());
    return analysis;
  }

  @Override
  public Analysis visitSchemaFetch(
      SchemaFetchStatement schemaFetchStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(schemaFetchStatement);

    SchemaPartition schemaPartition =
        partitionFetcher.getSchemaPartition(schemaFetchStatement.getPatternTree());
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
    }

    return analysis;
  }

  @Override
  public Analysis visitCountDevices(
      CountDevicesStatement countDevicesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(countDevicesStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(
        countDevicesStatement.getPathPattern().concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountDevicesHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountTimeSeries(
      CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(countTimeSeriesStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(countTimeSeriesStatement.getPathPattern());
    SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);

    Map<Integer, Template> templateMap =
        schemaFetcher.checkAllRelatedTemplate(countTimeSeriesStatement.getPathPattern());
    analysis.setRelatedTemplateInfo(templateMap);

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountTimeSeriesHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountLevelTimeSeries(
      CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(countLevelTimeSeriesStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(countLevelTimeSeriesStatement.getPathPattern());
    SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountLevelTimeSeriesHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(countStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(countStatement.getPathPattern());
    SchemaNodeManagementPartition schemaNodeManagementPartition =
        partitionFetcher.getSchemaNodeManagementPartitionWithLevel(
            patternTree, countStatement.getLevel());

    if (schemaNodeManagementPartition == null) {
      return analysis;
    }
    if (!schemaNodeManagementPartition.getMatchedNode().isEmpty()
        && schemaNodeManagementPartition.getSchemaPartition().getSchemaPartitionMap().size() == 0) {
      analysis.setFinishQueryAfterAnalyze(true);
    }
    analysis.setMatchedNodes(schemaNodeManagementPartition.getMatchedNode());
    analysis.setSchemaPartitionInfo(schemaNodeManagementPartition.getSchemaPartition());
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountNodesHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, MPPQueryContext context) {
    return visitSchemaNodeManagementPartition(
        showChildPathsStatement,
        showChildPathsStatement.getPartialPath(),
        DatasetHeaderFactory.getShowChildPathsHeader());
  }

  @Override
  public Analysis visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
    return visitSchemaNodeManagementPartition(
        showChildNodesStatement,
        showChildNodesStatement.getPartialPath(),
        DatasetHeaderFactory.getShowChildNodesHeader());
  }

  @Override
  public Analysis visitShowVersion(
      ShowVersionStatement showVersionStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showVersionStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowVersionHeader());
    analysis.setFinishQueryAfterAnalyze(true);
    return analysis;
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
        && schemaNodeManagementPartition.getSchemaPartition().getSchemaPartitionMap().size() == 0) {
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

    ISchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
    analysis.setSchemaTree(schemaTree);

    Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();

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

    DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
    analysis.setDataPartitionInfo(dataPartition);

    if (dataPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
    }

    return analysis;
  }

  @Override
  public Analysis visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, MPPQueryContext context) {

    context.setQueryType(QueryType.WRITE);
    List<List<String>> measurementsList = createTemplateStatement.getMeasurements();
    for (List measurements : measurementsList) {
      Set<String> measurementsSet = new HashSet<>(measurements);
      if (measurementsSet.size() < measurements.size()) {
        throw new SemanticException(
            "Measurement under an aligned device is not allowed to have the same measurement name");
      }
    }
    Analysis analysis = new Analysis();
    analysis.setStatement(createTemplateStatement);
    return analysis;
  }

  @Override
  public Analysis visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement,
      MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showNodesInSchemaTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowNodesInSchemaTemplateHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showSchemaTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowSchemaTemplateHeader());
    return analysis;
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
      long startTime =
          groupByTimeComponent.isLeftCRightO()
              ? groupByTimeComponent.getStartTime()
              : groupByTimeComponent.getStartTime() + 1;
      long endTime =
          groupByTimeComponent.isLeftCRightO()
              ? groupByTimeComponent.getEndTime()
              : groupByTimeComponent.getEndTime() + 1;
      return new GroupByFilter(
          groupByTimeComponent.getInterval(),
          groupByTimeComponent.getSlidingStep(),
          startTime,
          endTime);
    }
  }

  @Override
  public Analysis visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setStatement(setSchemaTemplateStatement);
    return analysis;
  }

  @Override
  public Analysis visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showPathSetTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowPathSetTemplateHeader());
    return analysis;
  }

  @Override
  public Analysis visitActivateTemplate(
      ActivateTemplateStatement activateTemplateStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setStatement(activateTemplateStatement);

    PartialPath activatePath = activateTemplateStatement.getPath();

    Pair<Template, PartialPath> templateSetInfo = schemaFetcher.checkTemplateSetInfo(activatePath);
    if (templateSetInfo == null) {
      throw new StatementAnalyzeException(
          new MetadataException(
              String.format(
                  "Path [%s] has not been set any template.", activatePath.getFullPath())));
    }
    analysis.setTemplateSetInfo(
        new Pair<>(templateSetInfo.left, Collections.singletonList(templateSetInfo.right)));

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(activatePath.concatNode(ONE_LEVEL_PATH_WILDCARD));
    SchemaPartition partition = partitionFetcher.getOrCreateSchemaPartition(patternTree);

    analysis.setSchemaPartitionInfo(partition);

    return analysis;
  }

  @Override
  public Analysis visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showPathsUsingTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowPathsUsingTemplateHeader());

    Pair<Template, List<PartialPath>> templateSetInfo =
        schemaFetcher.getAllPathsSetTemplate(showPathsUsingTemplateStatement.getTemplateName());
    analysis.setTemplateSetInfo(templateSetInfo);
    if (templateSetInfo == null
        || templateSetInfo.right == null
        || templateSetInfo.right.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    PathPatternTree patternTree = new PathPatternTree();
    templateSetInfo.right.forEach(
        path -> {
          patternTree.appendPathPattern(path);
          patternTree.appendPathPattern(path.concatNode(MULTI_LEVEL_PATH_WILDCARD));
        });

    SchemaPartition partition = partitionFetcher.getOrCreateSchemaPartition(patternTree);
    analysis.setSchemaPartitionInfo(partition);
    if (partition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    return analysis;
  }

  @Override
  public Analysis visitShowPipeSinkType(
      ShowPipeSinkTypeStatement showPipeSinkTypeStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setStatement(showPipeSinkTypeStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowPipeSinkTypeHeader());
    analysis.setFinishQueryAfterAnalyze(true);
    return analysis;
  }
}
