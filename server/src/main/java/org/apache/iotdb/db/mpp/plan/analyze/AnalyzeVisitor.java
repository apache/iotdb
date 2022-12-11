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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.metadata.template.TemplateImcompatibeException;
import org.apache.iotdb.db.exception.sql.MeasurementNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.IntoComponent;
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
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.TimeRange;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ALLOWED_SCHEMA_PROPS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.DEADBAND;
import static org.apache.iotdb.commons.conf.IoTDBConstant.LOSS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant.DEVICE;
import static org.apache.iotdb.db.mpp.plan.analyze.SelectIntoUtils.constructTargetDevice;
import static org.apache.iotdb.db.mpp.plan.analyze.SelectIntoUtils.constructTargetMeasurement;
import static org.apache.iotdb.db.mpp.plan.analyze.SelectIntoUtils.constructTargetPath;

/** This visitor is used to analyze each type of Statement and returns the {@link Analysis}. */
public class AnalyzeVisitor extends StatementVisitor<Analysis, MPPQueryContext> {

  private static final Logger logger = LoggerFactory.getLogger(Analyzer.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  public AnalyzeVisitor(IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
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
      logger.debug("[StartFetchSchema]");
      ISchemaTree schemaTree;
      if (queryStatement.isGroupByTag()) {
        schemaTree = schemaFetcher.fetchSchemaWithTags(patternTree);
      } else {
        schemaTree = schemaFetcher.fetchSchema(patternTree);
      }
      logger.debug("[EndFetchSchema]");
      // If there is no leaf node in the schema tree, the query should be completed immediately
      if (schemaTree.isEmpty()) {
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

      // extract global time filter from query filter and determine if there is a value filter
      analyzeGlobalTimeFilter(analysis, queryStatement);

      if (queryStatement.isLastQuery()) {
        if (analysis.hasValueFilter()) {
          throw new SemanticException("Only time filters are supported in LAST query");
        }
        analyzeOrderBy(analysis, queryStatement);
        return analyzeLast(analysis, schemaTree.getAllMeasurement(), schemaTree);
      }

      List<Pair<Expression, String>> outputExpressions;
      if (queryStatement.isAlignByDevice()) {
        Set<PartialPath> deviceSet = analyzeFrom(queryStatement, schemaTree);
        outputExpressions = analyzeSelect(analysis, queryStatement, schemaTree, deviceSet);

        Map<String, Set<Expression>> deviceToAggregationExpressions = new HashMap<>();
        analyzeHaving(
            analysis, queryStatement, schemaTree, deviceSet, deviceToAggregationExpressions);
        analyzeDeviceToAggregation(analysis, queryStatement, deviceToAggregationExpressions);
        analysis.setDeviceToAggregationExpressions(deviceToAggregationExpressions);

        analyzeDeviceToWhere(analysis, queryStatement, schemaTree, deviceSet);
        analyzeDeviceToSourceTransform(analysis, queryStatement);

        analyzeDeviceToSource(analysis, queryStatement);
        analyzeDeviceView(analysis, queryStatement, outputExpressions);

        analyzeInto(analysis, queryStatement, deviceSet, outputExpressions);
      } else {
        Map<Integer, List<Pair<Expression, String>>> outputExpressionMap =
            analyzeSelect(analysis, queryStatement, schemaTree);
        outputExpressions = new ArrayList<>();
        outputExpressionMap.values().forEach(outputExpressions::addAll);
        analyzeHaving(analysis, queryStatement, schemaTree);
        analyzeGroupByLevel(analysis, queryStatement, outputExpressionMap, outputExpressions);
        analyzeGroupByTag(analysis, queryStatement, outputExpressions, schemaTree);
        Set<Expression> selectExpressions =
            outputExpressions.stream()
                .map(Pair::getLeft)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        analysis.setSelectExpressions(selectExpressions);

        analyzeAggregation(analysis, queryStatement);

        analyzeWhere(analysis, queryStatement, schemaTree);
        analyzeSourceTransform(analysis, queryStatement);

        analyzeSource(analysis, queryStatement);

        analyzeInto(analysis, queryStatement, outputExpressions);
      }

      analyzeGroupBy(analysis, queryStatement);

      analyzeFill(analysis, queryStatement);

      // generate result set header according to output expressions
      analyzeOutput(analysis, queryStatement, outputExpressions);

      // fetch partition information
      analyzeDataPartition(analysis, queryStatement, schemaTree);

    } catch (StatementAnalyzeException e) {
      logger.warn("Meet error when analyzing the query statement: ", e);
      throw new StatementAnalyzeException(
          "Meet error when analyzing the query statement: " + e.getMessage());
    }
    return analysis;
  }

  private void analyzeGlobalTimeFilter(Analysis analysis, QueryStatement queryStatement) {
    Filter globalTimeFilter = null;
    boolean hasValueFilter = false;
    if (queryStatement.getWhereCondition() != null) {
      WhereCondition whereCondition = queryStatement.getWhereCondition();
      Expression predicate = whereCondition.getPredicate();

      Pair<Filter, Boolean> resultPair =
          ExpressionAnalyzer.extractGlobalTimeFilter(predicate, true, true);
      globalTimeFilter = resultPair.left;
      hasValueFilter = resultPair.right;

      predicate = ExpressionAnalyzer.evaluatePredicate(predicate);

      // set where condition to null if predicate is true or time filter.
      if (!hasValueFilter
          || (predicate.getExpressionType().equals(ExpressionType.CONSTANT)
              && Boolean.parseBoolean(predicate.getExpressionString()))) {
        queryStatement.setWhereCondition(null);
      } else {
        whereCondition.setPredicate(predicate);
      }
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
    analysis.setGlobalTimeFilter(globalTimeFilter);
    analysis.setHasValueFilter(hasValueFilter);
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

    analysis.setSourceExpressions(sourceExpressions);

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getLastQueryHeader());

    Set<String> deviceSet =
        allSelectedPath.stream().map(MeasurementPath::getDevice).collect(Collectors.toSet());

    Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
        getTimePartitionSlotList(analysis.getGlobalTimeFilter());

    DataPartition dataPartition;

    // there is no satisfied time range
    if (res.left.isEmpty() && !res.right.left) {
      dataPartition =
          new DataPartition(
              Collections.emptyMap(),
              CONFIG.getSeriesPartitionExecutorClass(),
              CONFIG.getSeriesPartitionSlotNum());
    } else {
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (String devicePath : deviceSet) {
        DataPartitionQueryParam queryParam =
            new DataPartitionQueryParam(devicePath, res.left, res.right.left, res.right.right);
        sgNameToQueryParamsMap
            .computeIfAbsent(schemaTree.getBelongedDatabase(devicePath), key -> new ArrayList<>())
            .add(queryParam);
      }

      if (res.right.left || res.right.right) {
        dataPartition =
            partitionFetcher.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
      } else {
        dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      }
    }

    analysis.setDataPartitionInfo(dataPartition);

    return analysis;
  }

  private Map<Integer, List<Pair<Expression, String>>> analyzeSelect(
      Analysis analysis, QueryStatement queryStatement, ISchemaTree schemaTree) {
    Map<Integer, List<Pair<Expression, String>>> outputExpressionMap = new HashMap<>();
    boolean isGroupByLevel = queryStatement.isGroupByLevel();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(),
            queryStatement.getSeriesOffset(),
            queryStatement.isLastQuery() || isGroupByLevel);
    int columnIndex = 0;
    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      boolean hasAlias = resultColumn.hasAlias();
      List<Expression> resultExpressions =
          ExpressionAnalyzer.removeWildcardInExpression(resultColumn.getExpression(), schemaTree);
      if (hasAlias
          && !queryStatement.isGroupByLevel()
          && !queryStatement.isGroupByTag()
          && resultExpressions.size() > 1) {
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
            queryStatement
                .getGroupByLevelComponent()
                .updateIsCountStar(resultColumn.getExpression());
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
      outputExpressionMap.put(columnIndex++, outputExpressions);
    }
    return outputExpressionMap;
  }

  private Set<PartialPath> analyzeFrom(QueryStatement queryStatement, ISchemaTree schemaTree) {
    // device path patterns in FROM clause
    List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();

    Set<PartialPath> deviceSet = new LinkedHashSet<>();
    for (PartialPath devicePattern : devicePatternList) {
      // get all matched devices
      deviceSet.addAll(
          schemaTree.getMatchedDevices(devicePattern).stream()
              .map(DeviceSchemaInfo::getDevicePath)
              .collect(Collectors.toList()));
    }
    return deviceSet;
  }

  private List<Pair<Expression, String>> analyzeSelect(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      Set<PartialPath> deviceSet) {
    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    Map<String, Set<Expression>> deviceToSelectExpressions = new HashMap<>();

    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      Expression selectExpression = resultColumn.getExpression();
      boolean hasAlias = resultColumn.hasAlias();

      // select expression after removing wildcard
      // use LinkedHashMap for order-preserving
      Map<Expression, Map<String, Expression>> measurementToDeviceSelectExpressions =
          new LinkedHashMap<>();
      for (PartialPath device : deviceSet) {
        List<Expression> selectExpressionsOfOneDevice =
            ExpressionAnalyzer.concatDeviceAndRemoveWildcard(selectExpression, device, schemaTree);
        for (Expression expression : selectExpressionsOfOneDevice) {
          Expression measurementExpression =
              ExpressionAnalyzer.getMeasurementExpression(expression);
          measurementToDeviceSelectExpressions
              .computeIfAbsent(measurementExpression, key -> new LinkedHashMap<>())
              .put(device.getFullPath(), ExpressionAnalyzer.removeAliasFromExpression(expression));
        }
      }

      if (hasAlias && measurementToDeviceSelectExpressions.keySet().size() > 1) {
        throw new SemanticException(
            String.format(
                "alias '%s' can only be matched with one time series", resultColumn.getAlias()));
      }

      for (Expression measurementExpression : measurementToDeviceSelectExpressions.keySet()) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
          continue;
        }
        if (paginationController.hasCurLimit()) {
          Map<String, Expression> deviceToSelectExpressionsOfOneMeasurement =
              measurementToDeviceSelectExpressions.get(measurementExpression);
          deviceToSelectExpressionsOfOneMeasurement
              .values()
              .forEach(expression -> analyzeExpression(analysis, expression));
          // check whether the datatype of paths which has the same measurement name are
          // consistent
          // if not, throw a SemanticException
          checkDataTypeConsistencyInAlignByDevice(
              analysis, new ArrayList<>(deviceToSelectExpressionsOfOneMeasurement.values()));

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

          // add deviceToSelectExpressions
          for (String deviceName : deviceToSelectExpressionsOfOneMeasurement.keySet()) {
            Expression expression = deviceToSelectExpressionsOfOneMeasurement.get(deviceName);
            Expression expressionWithoutAlias =
                ExpressionAnalyzer.removeAliasFromExpression(expression);
            analyzeExpression(analysis, expressionWithoutAlias);
            deviceToSelectExpressions
                .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
                .add(expressionWithoutAlias);
          }
          paginationController.consumeLimit();
        } else {
          break;
        }
      }
    }

    analysis.setDeviceToSelectExpressions(deviceToSelectExpressions);
    return outputExpressions;
  }

  private void analyzeHaving(
      Analysis analysis, QueryStatement queryStatement, ISchemaTree schemaTree) {
    if (!queryStatement.hasHaving()) {
      return;
    }

    // get removeWildcard Expressions in Having
    List<Expression> conJunctions =
        ExpressionAnalyzer.removeWildcardInFilter(
            queryStatement.getHavingCondition().getPredicate(),
            queryStatement.getFromComponent().getPrefixPaths(),
            schemaTree,
            false);
    Expression havingExpression =
        ExpressionUtils.constructQueryFilter(
            conJunctions.stream().distinct().collect(Collectors.toList()));
    TSDataType outputType = analyzeExpression(analysis, havingExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(
          String.format(
              "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: %s.",
              outputType));
    }
    analysis.setHavingExpression(havingExpression);
  }

  private void analyzeHaving(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      Set<PartialPath> deviceSet,
      Map<String, Set<Expression>> deviceToAggregationExpressions) {
    if (!queryStatement.hasHaving()) {
      return;
    }

    Expression havingExpression = queryStatement.getHavingCondition().getPredicate();
    Set<Expression> conJunctions = new HashSet<>();

    for (PartialPath device : deviceSet) {
      List<Expression> expressionsInHaving =
          ExpressionAnalyzer.concatDeviceAndRemoveWildcard(havingExpression, device, schemaTree);

      conJunctions.addAll(
          expressionsInHaving.stream()
              .map(ExpressionAnalyzer::getMeasurementExpression)
              .collect(Collectors.toList()));

      for (Expression expression : expressionsInHaving) {
        Set<Expression> aggregationExpressions = new LinkedHashSet<>();
        for (Expression aggregationExpression :
            ExpressionAnalyzer.searchAggregationExpressions(expression)) {
          analyzeExpression(analysis, aggregationExpression);
          aggregationExpressions.add(aggregationExpression);
        }
        deviceToAggregationExpressions
            .computeIfAbsent(device.getFullPath(), key -> new LinkedHashSet<>())
            .addAll(aggregationExpressions);
      }
    }

    havingExpression = ExpressionUtils.constructQueryFilter(new ArrayList<>(conJunctions));
    TSDataType outputType = analyzeExpression(analysis, havingExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(
          String.format(
              "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: %s.",
              outputType));
    }
    analysis.setHavingExpression(havingExpression);
  }

  private void analyzeGroupByLevel(
      Analysis analysis,
      QueryStatement queryStatement,
      Map<Integer, List<Pair<Expression, String>>> outputExpressionMap,
      List<Pair<Expression, String>> outputExpressions) {
    if (!queryStatement.isGroupByLevel()) {
      return;
    }

    GroupByLevelController groupByLevelController =
        new GroupByLevelController(queryStatement.getGroupByLevelComponent().getLevels());

    List<Expression> groupedSelectExpressions = new LinkedList<>();

    for (List<Pair<Expression, String>> outputExpressionList : outputExpressionMap.values()) {
      Set<Expression> groupedSelectExpressionSet = new LinkedHashSet<>();
      for (int i = 0; i < outputExpressionList.size(); i++) {
        Pair<Expression, String> expressionAliasPair = outputExpressionList.get(i);
        boolean isCountStar = queryStatement.getGroupByLevelComponent().isCountStar(i);
        Expression groupedExpression =
            groupByLevelController.control(
                isCountStar, expressionAliasPair.left, expressionAliasPair.right);
        groupedSelectExpressionSet.add(groupedExpression);
      }
      groupedSelectExpressions.addAll(groupedSelectExpressionSet);
    }

    LinkedHashMap<Expression, Set<Expression>> groupByLevelExpressions = new LinkedHashMap<>();
    if (queryStatement.hasHaving()) {
      // update havingExpression
      Expression havingExpression = groupByLevelController.control(analysis.getHavingExpression());
      analyzeExpression(analysis, havingExpression);
      analysis.setHavingExpression(havingExpression);
      updateGroupByLevelExpressions(
          analysis,
          havingExpression,
          groupByLevelExpressions,
          groupByLevelController.getGroupedExpressionToRawExpressionsMap());
    }

    outputExpressions.clear();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);
    for (Expression groupedExpression : groupedSelectExpressions) {
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
        analyzeExpression(analysis, groupedExpressionWithoutAlias);
        outputExpressions.add(outputExpression);
        updateGroupByLevelExpressions(
            analysis,
            groupedExpression,
            groupByLevelExpressions,
            groupByLevelController.getGroupedExpressionToRawExpressionsMap());
        paginationController.consumeLimit();
      } else {
        break;
      }
    }

    checkDataTypeConsistencyInGroupByLevel(analysis, groupByLevelExpressions);
    analysis.setCrossGroupByExpressions(groupByLevelExpressions);
  }

  private void checkDataTypeConsistencyInGroupByLevel(
      Analysis analysis, Map<Expression, Set<Expression>> groupByLevelExpressions) {
    for (Expression groupedAggregationExpression : groupByLevelExpressions.keySet()) {
      TSDataType checkedDataType = analysis.getType(groupedAggregationExpression);
      for (Expression rawAggregationExpression :
          groupByLevelExpressions.get(groupedAggregationExpression)) {
        if (analysis.getType(rawAggregationExpression) != checkedDataType) {
          throw new SemanticException(
              String.format(
                  "GROUP BY LEVEL: the data types of the same output column[%s] should be the same.",
                  groupedAggregationExpression));
        }
      }
    }
  }

  private void updateGroupByLevelExpressions(
      Analysis analysis,
      Expression expression,
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      Map<Expression, Set<Expression>> groupedExpressionToRawExpressionsMap) {
    for (Expression groupedAggregationExpression :
        ExpressionAnalyzer.searchAggregationExpressions(expression)) {
      Set<Expression> groupedExpressionSet =
          groupedExpressionToRawExpressionsMap.get(groupedAggregationExpression).stream()
              .map(ExpressionAnalyzer::removeAliasFromExpression)
              .collect(Collectors.toSet());
      Expression groupedAggregationExpressionWithoutAlias =
          ExpressionAnalyzer.removeAliasFromExpression(groupedAggregationExpression);

      analyzeExpression(analysis, groupedAggregationExpressionWithoutAlias);
      groupedExpressionSet.forEach(
          groupedExpression -> analyzeExpression(analysis, groupedExpression));

      groupByLevelExpressions
          .computeIfAbsent(groupedAggregationExpressionWithoutAlias, key -> new HashSet<>())
          .addAll(groupedExpressionSet);
    }
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

  /**
   * This method is used to analyze GROUP BY TAGS query.
   *
   * <p>TODO: support slimit/soffset/value filter
   */
  private void analyzeGroupByTag(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions,
      ISchemaTree schemaTree) {
    if (!queryStatement.isGroupByTag()) {
      return;
    }
    if (analysis.hasValueFilter()) {
      throw new SemanticException("Only time filters are supported in GROUP BY TAGS query");
    }
    if (queryStatement.hasHaving()) {
      throw new SemanticException("Having clause is not supported yet in GROUP BY TAGS query");
    }
    Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
        tagValuesToGroupedTimeseriesOperands = new HashMap<>();
    LinkedHashMap<Expression, Set<Expression>> groupByTagOutputExpressions = new LinkedHashMap<>();
    List<String> tagKeys = queryStatement.getGroupByTagComponent().getTagKeys();
    List<MeasurementPath> allSelectedPath = schemaTree.getAllMeasurement();
    Map<MeasurementPath, Map<String, String>> queriedTagMap = new HashMap<>();
    allSelectedPath.forEach(v -> queriedTagMap.put(v, v.getTagMap()));

    for (Pair<Expression, String> outputExpressionAndAlias : outputExpressions) {
      if (!(outputExpressionAndAlias.getLeft() instanceof FunctionExpression
          && outputExpressionAndAlias.getLeft().getExpressions().get(0) instanceof TimeSeriesOperand
          && outputExpressionAndAlias.getLeft().isBuiltInAggregationFunctionExpression())) {
        throw new SemanticException(
            outputExpressionAndAlias.getLeft()
                + " can't be used in group by tag. It will be supported in the future.");
      }
      FunctionExpression outputExpression = (FunctionExpression) outputExpressionAndAlias.getLeft();
      MeasurementPath measurementPath =
          (MeasurementPath)
              ((TimeSeriesOperand) outputExpression.getExpressions().get(0)).getPath();
      MeasurementPath fakePath = null;
      try {
        fakePath =
            new MeasurementPath(measurementPath.getMeasurement(), measurementPath.getSeriesType());
      } catch (IllegalPathException e) {
        // do nothing
      }
      Expression measurementExpression = new TimeSeriesOperand(fakePath);
      Expression groupedExpression =
          new FunctionExpression(
              outputExpression.getFunctionName(),
              outputExpression.getFunctionAttributes(),
              Collections.singletonList(measurementExpression));
      groupByTagOutputExpressions
          .computeIfAbsent(groupedExpression, v -> new HashSet<>())
          .add(outputExpression);
      Map<String, String> tagMap = queriedTagMap.get(measurementPath);
      List<String> tagValues = new ArrayList<>();
      for (String tagKey : tagKeys) {
        tagValues.add(tagMap.get(tagKey));
      }
      tagValuesToGroupedTimeseriesOperands
          .computeIfAbsent(tagValues, key -> new LinkedHashMap<>())
          .computeIfAbsent(groupedExpression, key -> new ArrayList<>())
          .add(outputExpression.getExpressions().get(0));
    }

    outputExpressions.clear();
    for (String tagKey : tagKeys) {
      Expression tagKeyExpression =
          TimeSeriesOperand.constructColumnHeaderExpression(tagKey, TSDataType.TEXT);
      analyzeExpression(analysis, tagKeyExpression);
      outputExpressions.add(new Pair<>(tagKeyExpression, null));
    }
    for (Expression groupByTagOutputExpression : groupByTagOutputExpressions.keySet()) {
      // TODO: support alias
      analyzeExpression(analysis, groupByTagOutputExpression);
      outputExpressions.add(new Pair<>(groupByTagOutputExpression, null));
    }
    analysis.setTagKeys(queryStatement.getGroupByTagComponent().getTagKeys());
    analysis.setTagValuesToGroupedTimeseriesOperands(tagValuesToGroupedTimeseriesOperands);
    analysis.setCrossGroupByExpressions(groupByTagOutputExpressions);
  }

  private void analyzeDeviceToAggregation(
      Analysis analysis,
      QueryStatement queryStatement,
      Map<String, Set<Expression>> deviceToAggregationExpressions) {
    if (!queryStatement.isAggregationQuery()) {
      return;
    }

    Map<String, Set<Expression>> deviceToSelectExpressions =
        analysis.getDeviceToSelectExpressions();
    for (String deviceName : deviceToSelectExpressions.keySet()) {
      Set<Expression> selectExpressions = deviceToSelectExpressions.get(deviceName);
      Set<Expression> aggregationExpressions = new LinkedHashSet<>();
      for (Expression expression : selectExpressions) {
        aggregationExpressions.addAll(ExpressionAnalyzer.searchAggregationExpressions(expression));
      }
      deviceToAggregationExpressions
          .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
          .addAll(aggregationExpressions);
    }
  }

  private void analyzeAggregation(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.isAggregationQuery()) {
      return;
    }

    if (queryStatement.isGroupByLevel() || queryStatement.isGroupByTag()) {
      Set<Expression> aggregationExpressions =
          analysis.getCrossGroupByExpressions().values().stream()
              .flatMap(Set::stream)
              .collect(Collectors.toSet());
      analysis.setAggregationExpressions(aggregationExpressions);
    } else {
      Set<Expression> aggregationExpressions = new HashSet<>();
      for (Expression expression : analysis.getSelectExpressions()) {
        aggregationExpressions.addAll(ExpressionAnalyzer.searchAggregationExpressions(expression));
      }
      if (queryStatement.hasHaving()) {
        aggregationExpressions.addAll(
            ExpressionAnalyzer.searchAggregationExpressions(analysis.getHavingExpression()));
      }
      analysis.setAggregationExpressions(aggregationExpressions);
    }
  }

  private void analyzeDeviceToSourceTransform(Analysis analysis, QueryStatement queryStatement) {
    Map<String, Set<Expression>> deviceToSourceTransformExpressions = new HashMap<>();
    if (queryStatement.isAggregationQuery()) {
      Map<String, Set<Expression>> deviceToAggregationExpressions =
          analysis.getDeviceToAggregationExpressions();
      for (String deviceName : deviceToAggregationExpressions.keySet()) {
        Set<Expression> aggregationExpressions = deviceToAggregationExpressions.get(deviceName);
        Set<Expression> sourceTransformExpressions = new LinkedHashSet<>();
        for (Expression expression : aggregationExpressions) {
          sourceTransformExpressions.addAll(expression.getExpressions());
        }
        deviceToSourceTransformExpressions.put(deviceName, sourceTransformExpressions);
      }
    } else {
      deviceToSourceTransformExpressions = analysis.getDeviceToSelectExpressions();
    }
    analysis.setDeviceToSourceTransformExpressions(deviceToSourceTransformExpressions);
  }

  private void analyzeSourceTransform(Analysis analysis, QueryStatement queryStatement) {
    Set<Expression> sourceTransformExpressions = new HashSet<>();
    if (queryStatement.isAggregationQuery()) {
      for (Expression expression : analysis.getAggregationExpressions()) {
        sourceTransformExpressions.addAll(expression.getExpressions());
      }
    } else {
      sourceTransformExpressions = analysis.getSelectExpressions();
    }
    analysis.setSourceTransformExpressions(sourceTransformExpressions);
  }

  private void analyzeDeviceToSource(Analysis analysis, QueryStatement queryStatement) {
    Map<String, Set<Expression>> deviceToSourceExpressions = new HashMap<>();
    Map<String, Set<Expression>> deviceToSourceTransformExpressions =
        analysis.getDeviceToSourceTransformExpressions();
    for (String deviceName : deviceToSourceTransformExpressions.keySet()) {
      Set<Expression> sourceTransformExpressions =
          deviceToSourceTransformExpressions.get(deviceName);
      Set<Expression> sourceExpressions = new LinkedHashSet<>();
      for (Expression expression : sourceTransformExpressions) {
        sourceExpressions.addAll(ExpressionAnalyzer.searchSourceExpressions(expression));
      }
      deviceToSourceExpressions.put(deviceName, sourceExpressions);
    }
    if (queryStatement.hasWhere()) {
      Map<String, Expression> deviceToWhereExpression = analysis.getDeviceToWhereExpression();
      for (String deviceName : deviceToWhereExpression.keySet()) {
        Expression whereExpression = deviceToWhereExpression.get(deviceName);
        deviceToSourceExpressions
            .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
            .addAll(ExpressionAnalyzer.searchSourceExpressions(whereExpression));
      }
    }
    analysis.setDeviceToSourceExpressions(deviceToSourceExpressions);
  }

  private void analyzeSource(Analysis analysis, QueryStatement queryStatement) {
    Set<Expression> sourceExpressions = new HashSet<>();
    for (Expression expression : analysis.getSourceTransformExpressions()) {
      sourceExpressions.addAll(ExpressionAnalyzer.searchSourceExpressions(expression));
    }
    if (queryStatement.hasWhere()) {
      sourceExpressions.addAll(
          ExpressionAnalyzer.searchSourceExpressions(analysis.getWhereExpression()));
    }
    analysis.setSourceExpressions(sourceExpressions);
  }

  private void analyzeDeviceToWhere(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      Set<PartialPath> deviceSet) {
    if (!queryStatement.hasWhere()) {
      return;
    }

    Map<String, Expression> deviceToWhereExpression = new HashMap<>();
    Iterator<PartialPath> deviceIterator = deviceSet.iterator();
    while (deviceIterator.hasNext()) {
      PartialPath devicePath = deviceIterator.next();
      Expression whereExpression;
      try {
        whereExpression = analyzeWhereSplitByDevice(queryStatement, devicePath, schemaTree);
      } catch (SemanticException e) {
        if (e instanceof MeasurementNotExistException) {
          logger.warn(e.getMessage());
          deviceIterator.remove();
          analysis.getDeviceToSelectExpressions().remove(devicePath.getFullPath());
          if (queryStatement.isAggregationQuery()) {
            analysis.getDeviceToAggregationExpressions().remove(devicePath.getFullPath());
          }
          continue;
        }
        throw e;
      }

      TSDataType outputType = analyzeExpression(analysis, whereExpression);
      if (outputType != TSDataType.BOOLEAN) {
        throw new SemanticException(
            String.format(
                "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: %s.",
                outputType));
      }

      deviceToWhereExpression.put(devicePath.getFullPath(), whereExpression);
    }
    analysis.setDeviceToWhereExpression(deviceToWhereExpression);
  }

  private void analyzeWhere(
      Analysis analysis, QueryStatement queryStatement, ISchemaTree schemaTree) {
    if (!queryStatement.hasWhere()) {
      return;
    }
    List<Expression> conJunctions =
        ExpressionAnalyzer.removeWildcardInFilter(
            queryStatement.getWhereCondition().getPredicate(),
            queryStatement.getFromComponent().getPrefixPaths(),
            schemaTree,
            true);
    Expression whereExpression =
        ExpressionUtils.constructQueryFilter(
            conJunctions.stream().distinct().collect(Collectors.toList()));
    TSDataType outputType = analyzeExpression(analysis, whereExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(
          String.format(
              "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: %s.",
              outputType));
    }
    analysis.setWhereExpression(whereExpression);
  }

  private Expression analyzeWhereSplitByDevice(
      QueryStatement queryStatement, PartialPath devicePath, ISchemaTree schemaTree) {
    List<Expression> conJunctions =
        ExpressionAnalyzer.removeWildcardInFilterByDevice(
            queryStatement.getWhereCondition().getPredicate(), devicePath, schemaTree, true);
    return ExpressionUtils.constructQueryFilter(
        conJunctions.stream().distinct().collect(Collectors.toList()));
  }

  private void analyzeDeviceView(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions) {
    Expression deviceExpression =
        new TimeSeriesOperand(new MeasurementPath(new PartialPath(DEVICE, false), TSDataType.TEXT));

    Set<Expression> selectExpressions = new LinkedHashSet<>();
    selectExpressions.add(deviceExpression);
    selectExpressions.addAll(
        outputExpressions.stream()
            .map(Pair::getLeft)
            .collect(Collectors.toCollection(LinkedHashSet::new)));
    analysis.setSelectExpressions(selectExpressions);

    Set<Expression> deviceViewOutputExpressions = new LinkedHashSet<>();
    if (queryStatement.isAggregationQuery()) {
      deviceViewOutputExpressions.add(deviceExpression);
      for (Expression selectExpression : selectExpressions) {
        deviceViewOutputExpressions.addAll(
            ExpressionAnalyzer.searchAggregationExpressions(selectExpression));
      }
      if (queryStatement.hasHaving()) {
        deviceViewOutputExpressions.addAll(
            ExpressionAnalyzer.searchAggregationExpressions(analysis.getHavingExpression()));
      }
    } else {
      deviceViewOutputExpressions = selectExpressions;
    }
    analysis.setDeviceViewOutputExpressions(deviceViewOutputExpressions);

    List<String> deviceViewOutputColumns =
        deviceViewOutputExpressions.stream()
            .map(Expression::getExpressionString)
            .collect(Collectors.toList());

    Map<String, Set<String>> deviceToOutputColumnsMap = new LinkedHashMap<>();
    Map<String, Set<Expression>> deviceToOutputExpressions =
        queryStatement.isAggregationQuery()
            ? analysis.getDeviceToAggregationExpressions()
            : analysis.getDeviceToSourceTransformExpressions();
    for (String deviceName : deviceToOutputExpressions.keySet()) {
      Set<Expression> outputExpressionsUnderDevice = deviceToOutputExpressions.get(deviceName);
      Set<String> outputColumns = new LinkedHashSet<>();
      for (Expression expression : outputExpressionsUnderDevice) {
        outputColumns.add(ExpressionAnalyzer.getMeasurementExpression(expression).toString());
      }
      deviceToOutputColumnsMap.put(deviceName, outputColumns);
    }

    Map<String, List<Integer>> deviceViewInputIndexesMap = new HashMap<>();
    for (String deviceName : deviceToOutputColumnsMap.keySet()) {
      List<String> outputsUnderDevice = new ArrayList<>(deviceToOutputColumnsMap.get(deviceName));
      List<Integer> indexes = new ArrayList<>();
      for (String output : outputsUnderDevice) {
        int index = deviceViewOutputColumns.indexOf(output);
        checkState(
            index >= 1, "output column '%s' is not stored in %s", output, deviceViewOutputColumns);
        indexes.add(index);
      }
      deviceViewInputIndexesMap.put(deviceName, indexes);
    }
    analysis.setDeviceViewInputIndexesMap(deviceViewInputIndexesMap);
  }

  private void analyzeOutput(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions) {
    if (queryStatement.isSelectInto()) {
      analysis.setRespDatasetHeader(
          DatasetHeaderFactory.getSelectIntoHeader(queryStatement.isAlignByDevice()));
      return;
    }

    boolean isIgnoreTimestamp =
        queryStatement.isAggregationQuery() && !queryStatement.isGroupByTime();
    List<ColumnHeader> columnHeaders = new ArrayList<>();
    if (queryStatement.isAlignByDevice()) {
      columnHeaders.add(new ColumnHeader(DEVICE, TSDataType.TEXT, null));
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
    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaders, isIgnoreTimestamp));
  }

  private void analyzeOrderBy(Analysis analysis, QueryStatement queryStatement) {
    analysis.setMergeOrderParameter(new OrderByParameter(queryStatement.getSortItemList()));
  }

  private TSDataType analyzeExpression(Analysis analysis, Expression expression) {
    ExpressionTypeAnalyzer.analyzeExpression(analysis, expression);
    return analysis.getType(expression);
  }

  private void analyzeGroupBy(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.isGroupByTime()) {
      return;
    }

    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    if ((groupByTimeComponent.isIntervalByMonth() || groupByTimeComponent.isSlidingStepByMonth())
        && queryStatement.getResultTimeOrder() == Ordering.DESC) {
      throw new SemanticException("Group by month doesn't support order by time desc now.");
    }
    if (!queryStatement.isCqQueryBody()
        && (groupByTimeComponent.getStartTime() == 0 && groupByTimeComponent.getEndTime() == 0)) {
      throw new SemanticException(
          "The query time range should be specified in the GROUP BY TIME clause.");
    }
    analysis.setGroupByTimeParameter(new GroupByTimeParameter(groupByTimeComponent));
  }

  private void analyzeFill(Analysis analysis, QueryStatement queryStatement) {
    if (queryStatement.getFillComponent() == null) {
      return;
    }

    FillComponent fillComponent = queryStatement.getFillComponent();
    analysis.setFillDescriptor(
        new FillDescriptor(fillComponent.getFillPolicy(), fillComponent.getFillValue()));
  }

  private void analyzeDataPartition(
      Analysis analysis, QueryStatement queryStatement, ISchemaTree schemaTree) {
    Set<String> deviceSet = new HashSet<>();
    if (queryStatement.isAlignByDevice()) {
      deviceSet = analysis.getDeviceToSourceExpressions().keySet();
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
    Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
        getTimePartitionSlotList(globalTimeFilter);
    // there is no satisfied time range
    if (res.left.isEmpty() && !res.right.left) {
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
  }

  /**
   * get TTimePartitionSlot list about this time filter
   *
   * @return List<TTimePartitionSlot>, if contains (-oo, XXX] time range, res.right.left = true; if
   *     contains [XX, +oo), res.right.right = true
   */
  public static Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> getTimePartitionSlotList(
      Filter timeFilter) {
    if (timeFilter == null) {
      // (-oo, +oo)
      return new Pair<>(Collections.emptyList(), new Pair<>(true, true));
    }
    List<TimeRange> timeRangeList = timeFilter.getTimeRanges();
    if (timeRangeList.isEmpty()) {
      // no satisfied time range
      return new Pair<>(Collections.emptyList(), new Pair<>(false, false));
    } else if (timeRangeList.size() == 1
        && (timeRangeList.get(0).getMin() == Long.MIN_VALUE
            && timeRangeList.get(timeRangeList.size() - 1).getMax() == Long.MAX_VALUE)) {
      // (-oo, +oo)
      return new Pair<>(Collections.emptyList(), new Pair<>(true, true));
    }

    boolean needLeftAll, needRightAll;
    long startTime, endTime;
    TTimePartitionSlot timePartitionSlot;
    int index = 0, size = timeRangeList.size();

    if (timeRangeList.get(0).getMin() == Long.MIN_VALUE) {
      needLeftAll = true;
      startTime =
          (timeRangeList.get(0).getMax() / TimePartitionUtils.timePartitionInterval)
              * TimePartitionUtils.timePartitionInterval; // included
      endTime = startTime + TimePartitionUtils.timePartitionInterval; // excluded
      timePartitionSlot = TimePartitionUtils.getTimePartition(timeRangeList.get(0).getMax());
    } else {
      startTime =
          (timeRangeList.get(0).getMin() / TimePartitionUtils.timePartitionInterval)
              * TimePartitionUtils.timePartitionInterval; // included
      endTime = startTime + TimePartitionUtils.timePartitionInterval; // excluded
      timePartitionSlot = TimePartitionUtils.getTimePartition(timeRangeList.get(0).getMin());
      needLeftAll = false;
    }

    if (timeRangeList.get(size - 1).getMax() == Long.MAX_VALUE) {
      needRightAll = true;
      size--;
    } else {
      needRightAll = false;
    }

    List<TTimePartitionSlot> result = new ArrayList<>();
    while (index < size) {
      long curLeft = timeRangeList.get(index).getMin();
      long curRight = timeRangeList.get(index).getMax();
      if (curLeft >= endTime) {
        result.add(timePartitionSlot);
        // next init
        endTime =
            (curLeft / TimePartitionUtils.timePartitionInterval + 1)
                * TimePartitionUtils.timePartitionInterval;
        timePartitionSlot = TimePartitionUtils.getTimePartition(curLeft);
      } else if (curRight >= endTime) {
        result.add(timePartitionSlot);
        // next init
        timePartitionSlot = new TTimePartitionSlot(endTime);
        endTime = endTime + TimePartitionUtils.timePartitionInterval;
      } else {
        index++;
      }
    }
    result.add(timePartitionSlot);

    if (needRightAll) {
      TTimePartitionSlot lastTimePartitionSlot =
          TimePartitionUtils.getTimePartition(timeRangeList.get(timeRangeList.size() - 1).getMin());
      if (lastTimePartitionSlot.startTime != timePartitionSlot.startTime) {
        result.add(lastTimePartitionSlot);
      }
    }
    return new Pair<>(result, new Pair<>(needLeftAll, needRightAll));
  }

  private void analyzeInto(
      Analysis analysis,
      QueryStatement queryStatement,
      Set<PartialPath> deviceSet,
      List<Pair<Expression, String>> outputExpressions) {
    if (!queryStatement.isSelectInto()) {
      return;
    }
    queryStatement.setOrderByComponent(null);

    List<PartialPath> sourceDevices = new ArrayList<>(deviceSet);
    List<Expression> sourceColumns =
        outputExpressions.stream()
            .map(Pair::getLeft)
            .collect(Collectors.toCollection(ArrayList::new));

    IntoComponent intoComponent = queryStatement.getIntoComponent();
    intoComponent.validate(sourceDevices, sourceColumns);

    DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor = new DeviceViewIntoPathDescriptor();
    IntoComponent.IntoDeviceMeasurementIterator intoDeviceMeasurementIterator =
        intoComponent.getIntoDeviceMeasurementIterator();
    for (PartialPath sourceDevice : sourceDevices) {
      PartialPath deviceTemplate = intoDeviceMeasurementIterator.getDeviceTemplate();
      boolean isAlignedDevice = intoDeviceMeasurementIterator.isAlignedDevice();
      PartialPath targetDevice = constructTargetDevice(sourceDevice, deviceTemplate);
      deviceViewIntoPathDescriptor.specifyDeviceAlignment(targetDevice.toString(), isAlignedDevice);

      for (Expression sourceColumn : sourceColumns) {
        String measurementTemplate = intoDeviceMeasurementIterator.getMeasurementTemplate();
        String targetMeasurement;
        if (sourceColumn instanceof TimeSeriesOperand) {
          targetMeasurement =
              constructTargetMeasurement(
                  sourceDevice.concatNode(sourceColumn.toString()), measurementTemplate);
        } else {
          targetMeasurement = measurementTemplate;
        }
        deviceViewIntoPathDescriptor.specifyTargetDeviceMeasurement(
            sourceDevice, targetDevice, sourceColumn.toString(), targetMeasurement);
        intoDeviceMeasurementIterator.nextMeasurement();
      }

      intoDeviceMeasurementIterator.nextDevice();
    }
    deviceViewIntoPathDescriptor.validate();
    analysis.setDeviceViewIntoPathDescriptor(deviceViewIntoPathDescriptor);
  }

  private void analyzeInto(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions) {
    if (!queryStatement.isSelectInto()) {
      return;
    }
    queryStatement.setOrderByComponent(null);

    List<Expression> sourceColumns =
        outputExpressions.stream()
            .map(Pair::getLeft)
            .collect(Collectors.toCollection(ArrayList::new));

    IntoComponent intoComponent = queryStatement.getIntoComponent();
    intoComponent.validate(sourceColumns);

    IntoPathDescriptor intoPathDescriptor = new IntoPathDescriptor();
    IntoComponent.IntoPathIterator intoPathIterator = intoComponent.getIntoPathIterator();
    for (Expression sourceColumn : sourceColumns) {
      PartialPath deviceTemplate = intoPathIterator.getDeviceTemplate();
      String measurementTemplate = intoPathIterator.getMeasurementTemplate();
      boolean isAlignedDevice = intoPathIterator.isAlignedDevice();

      PartialPath targetPath;
      if (sourceColumn instanceof TimeSeriesOperand) {
        PartialPath sourcePath = ((TimeSeriesOperand) sourceColumn).getPath();
        targetPath = constructTargetPath(sourcePath, deviceTemplate, measurementTemplate);
      } else {
        targetPath = deviceTemplate.concatNode(measurementTemplate);
      }
      intoPathDescriptor.specifyTargetPath(sourceColumn.toString(), targetPath);
      intoPathDescriptor.specifyDeviceAlignment(
          targetPath.getDevicePath().toString(), isAlignedDevice);
      intoPathIterator.next();
    }
    intoPathDescriptor.validate();
    analysis.setIntoPathDescriptor(intoPathDescriptor);
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
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement =
          new InsertRowsOfOneDeviceStatement();
      List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
      for (int i = 0; i < timeArray.length; i++) {
        InsertRowStatement statement = new InsertRowStatement();
        statement.setDevicePath(devicePath);
        String[] measurements = new String[measurementList.length];
        System.arraycopy(measurementList, 0, measurements, 0, measurements.length);
        statement.setMeasurements(measurements);
        statement.setTime(timeArray[i]);
        TSDataType[] dataTypes = new TSDataType[measurementList.length];
        statement.setDataTypes(dataTypes);
        Object[] values = new Object[measurementList.length];
        System.arraycopy(insertStatement.getValuesList().get(i), 0, values, 0, values.length);
        statement.setValues(values);
        statement.setAligned(insertStatement.isAligned());
        statement.setNeedInferType(true);
        insertRowStatementList.add(statement);
      }
      insertRowsOfOneDeviceStatement.setInsertRowStatementList(insertRowStatementList);
      return insertRowsOfOneDeviceStatement.accept(this, context);
    }
  }

  @Override
  public Analysis visitCreateTimeseries(
      CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    if (createTimeSeriesStatement.getPath().getNodeLength() < 3) {
      throw new RuntimeException(
          new IllegalPathException(createTimeSeriesStatement.getPath().getFullPath()));
    }
    analyzeSchemaProps(createTimeSeriesStatement.getProps());
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

  private void analyzeSchemaProps(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    Map<String, String> caseChangeMap = new HashMap<>();
    for (String key : props.keySet()) {
      caseChangeMap.put(key.toLowerCase(Locale.ROOT), key);
    }
    for (String lowerCaseKey : caseChangeMap.keySet()) {
      if (!ALLOWED_SCHEMA_PROPS.contains(lowerCaseKey)) {
        throw new SemanticException(
            new MetadataException(
                String.format("%s is not a legal prop.", caseChangeMap.get(lowerCaseKey))));
      }
      props.put(lowerCaseKey, props.remove(caseChangeMap.get(lowerCaseKey)));
    }
    if (props.containsKey(DEADBAND)) {
      props.put(LOSS, props.remove(DEADBAND));
    }
  }

  private void analyzeSchemaProps(List<Map<String, String>> propsList) {
    if (propsList == null) {
      return;
    }
    for (Map<String, String> props : propsList) {
      analyzeSchemaProps(props);
    }
  }

  @Override
  public Analysis visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    if (createAlignedTimeSeriesStatement.getDevicePath().getNodeLength() < 2) {
      throw new RuntimeException(
          new IllegalPathException(createAlignedTimeSeriesStatement.getDevicePath().getFullPath()));
    }
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

    analyzeSchemaProps(createMultiTimeSeriesStatement.getPropsList());

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

    return getAnalysisForWriting(
        insertTabletStatement, Collections.singletonList(dataPartitionQueryParam));
  }

  @Override
  public Analysis visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
    dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
    dataPartitionQueryParam.setTimePartitionSlotList(insertRowStatement.getTimePartitionSlots());

    return getAnalysisForWriting(
        insertRowStatement, Collections.singletonList(dataPartitionQueryParam));
  }

  @Override
  public Analysis visitInsertRows(
      InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    Map<String, Set<TTimePartitionSlot>> dataPartitionQueryParamMap = new HashMap<>();
    for (InsertRowStatement insertRowStatement : insertRowsStatement.getInsertRowStatementList()) {
      Set<TTimePartitionSlot> timePartitionSlotSet =
          dataPartitionQueryParamMap.computeIfAbsent(
              insertRowStatement.getDevicePath().getFullPath(), k -> new HashSet());
      timePartitionSlotSet.addAll(insertRowStatement.getTimePartitionSlots());
    }

    List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
    for (Map.Entry<String, Set<TTimePartitionSlot>> entry : dataPartitionQueryParamMap.entrySet()) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(entry.getKey());
      dataPartitionQueryParam.setTimePartitionSlotList(new ArrayList<>(entry.getValue()));
      dataPartitionQueryParams.add(dataPartitionQueryParam);
    }

    return getAnalysisForWriting(insertRowsStatement, dataPartitionQueryParams);
  }

  @Override
  public Analysis visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    Map<String, Set<TTimePartitionSlot>> dataPartitionQueryParamMap = new HashMap<>();
    for (InsertTabletStatement insertTabletStatement :
        insertMultiTabletsStatement.getInsertTabletStatementList()) {
      Set<TTimePartitionSlot> timePartitionSlotSet =
          dataPartitionQueryParamMap.computeIfAbsent(
              insertTabletStatement.getDevicePath().getFullPath(), k -> new HashSet());
      timePartitionSlotSet.addAll(insertTabletStatement.getTimePartitionSlots());
    }

    List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
    for (Map.Entry<String, Set<TTimePartitionSlot>> entry : dataPartitionQueryParamMap.entrySet()) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(entry.getKey());
      dataPartitionQueryParam.setTimePartitionSlotList(new ArrayList<>(entry.getValue()));
      dataPartitionQueryParams.add(dataPartitionQueryParam);
    }

    return getAnalysisForWriting(insertMultiTabletsStatement, dataPartitionQueryParams);
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

    return getAnalysisForWriting(
        insertRowsOfOneDeviceStatement, Collections.singletonList(dataPartitionQueryParam));
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
        logger.warn(String.format("Parse file %s to resource error.", tsFile.getPath()), e);
        throw new SemanticException(
            String.format("Parse file %s to resource error", tsFile.getPath()));
      }
    }

    // auto create and verify schema
    try {
      if (loadTsFileStatement.isVerifySchema()) {
        verifyLoadingMeasurements(device2Schemas);
      }
      if (loadTsFileStatement.isAutoCreateDatabase()) {
        autoCreateSg(loadTsFileStatement.getSgLevel(), device2Schemas);
      }
      ISchemaTree schemaTree =
          autoCreateSchema(
              device2Schemas,
              device2IsAligned); // schema fetcher will not auto create if config set
      // isAutoCreateSchemaEnabled is false.
      if (loadTsFileStatement.isVerifySchema()) {
        verifySchema(schemaTree, device2Schemas, device2IsAligned);
      }
    } catch (Exception e) {
      logger.warn("Auto create or verify schema error.", e);
      throw new SemanticException(
          String.format(
              "Auto create or verify schema error when executing statement %s.",
              loadTsFileStatement));
    }

    // construct partition info
    List<DataPartitionQueryParam> params = new ArrayList<>();
    for (Map.Entry<String, Long> entry : device2MinTime.entrySet()) {
      List<TTimePartitionSlot> timePartitionSlots = new ArrayList<>();
      String device = entry.getKey();
      long endTime = device2MaxTime.get(device);
      long interval = TimePartitionUtils.timePartitionInterval;
      long time = (entry.getValue() / interval) * interval;
      for (; time <= endTime; time += interval) {
        timePartitionSlots.add(TimePartitionUtils.getTimePartition(time));
      }

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(device);
      dataPartitionQueryParam.setTimePartitionSlotList(timePartitionSlots);
      params.add(dataPartitionQueryParam);
    }

    return getAnalysisForWriting(loadTsFileStatement, params);
  }

  /** get analysis according to statement and params */
  private Analysis getAnalysisForWriting(
      Statement statement, List<DataPartitionQueryParam> dataPartitionQueryParams) {
    Analysis analysis = new Analysis();
    analysis.setStatement(statement);

    DataPartition dataPartition =
        partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams);
    if (dataPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailMessage(
          "Database not exists and failed to create automatically because enable_auto_create_schema is FALSE.");
    }
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

      if (IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()
          || statement.isVerifySchema()) {
        // construct schema
        for (Map.Entry<String, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
          String device = entry.getKey();
          List<TimeseriesMetadata> timeseriesMetadataList = entry.getValue();
          boolean isAligned = false;
          for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
            TSDataType dataType = timeseriesMetadata.getTSDataType();
            if (!dataType.equals(TSDataType.VECTOR)) {
              Pair<CompressionType, TSEncoding> pair =
                  reader.readTimeseriesCompressionTypeAndEncoding(timeseriesMetadata);
              MeasurementSchema measurementSchema =
                  new MeasurementSchema(
                      timeseriesMetadata.getMeasurementId(),
                      dataType,
                      pair.getRight(),
                      pair.getLeft());
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
      if (!resource.resourceFileExists()) {
        FileLoaderUtils.updateTsFileResource(
            device2Metadata, resource); // serialize it in LoadSingleTsFileNode
        resource.updatePlanIndexes(reader.getMinPlanIndex());
        resource.updatePlanIndexes(reader.getMaxPlanIndex());
      } else {
        resource.deserialize();
      }

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
      System.arraycopy(nodes, 0, sgNodes, 0, sgLevel);
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
    long queryId = SessionManager.getInstance().requestQueryId();
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
        && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      logger.warn(
          "Create Database error, statement: {}, result status is: {}", statement, result.status);
      throw new LoadFileException(
          String.format("Can not execute create database statement: %s", statement));
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
          logger.warn(msg);
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
      logger.debug("[StartFetchSchema]");
      ISchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
      logger.debug("[EndFetchSchema]]");
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
            .computeIfAbsent(schemaTree.getBelongedDatabase(devicePath), key -> new ArrayList<>())
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
    if (showClusterStatement.isDetails()) {
      analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowClusterDetailsHeader());
    } else {
      analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowClusterHeader());
    }
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
                      schemaTree.getBelongedDatabase(devicePath), key -> new ArrayList<>())
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

    if (templateSetInfo == null
        || templateSetInfo.right == null
        || templateSetInfo.right.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    analysis.setTemplateSetInfo(templateSetInfo);

    PathPatternTree patternTree = new PathPatternTree();
    PartialPath rawPathPattern = showPathsUsingTemplateStatement.getPathPattern();
    List<PartialPath> specifiedPatternList = new ArrayList<>();
    templateSetInfo.right.forEach(
        setPath -> {
          for (PartialPath specifiedPattern : rawPathPattern.alterPrefixPath(setPath)) {
            patternTree.appendPathPattern(specifiedPattern);
            specifiedPatternList.add(specifiedPattern);
          }
        });

    if (specifiedPatternList.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    analysis.setSpecifiedTemplateRelatedPathPatternList(specifiedPatternList);

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
