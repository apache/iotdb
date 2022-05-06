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
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LastQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
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

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Analyze the statement and generate Analysis. */
public class Analyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Analyzer.class);

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
        queryStatement.selfCheck();

        // concat path and construct path pattern tree
        PathPatternTree patternTree = new PathPatternTree();
        QueryStatement rewrittenStatement =
            (QueryStatement) new ConcatPathRewriter().rewrite(queryStatement, patternTree);
        analysis.setStatement(rewrittenStatement);

        // request schema fetch API
        SchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);
        // If there is no leaf node in the schema tree, the query should be completed immediately
        if (schemaTree.isEmpty()) {
          analysis.setRespDatasetHeader(new DatasetHeader(new ArrayList<>(), false));
          return analysis;
        }

        List<Pair<Expression, String>> outputExpressions;
        Set<Expression> selectExpressions = new HashSet<>();
        List<DeviceSchemaInfo> deviceSchemaInfos = new ArrayList<>();
        if (!queryStatement.isAlignByDevice()) {
          outputExpressions = analyzeSelect(queryStatement, schemaTree);
          selectExpressions =
              outputExpressions.stream().map(Pair::getLeft).collect(Collectors.toSet());
        } else {
          outputExpressions =
              analyzeFrom(queryStatement, schemaTree, deviceSchemaInfos, selectExpressions);
        }

        if (queryStatement.isGroupByLevel()) {
          Validate.isTrue(!queryStatement.isAlignByDevice());
          Map<Expression, Set<Expression>> groupByLevelExpressions =
              analyzeGroupByLevel(
                  (AggregationQueryStatement) queryStatement, outputExpressions, selectExpressions);
          analysis.setGroupByLevelExpressions(groupByLevelExpressions);
        }

        Filter globalTimeFilter = analyzeGlobalTimeFilter(queryStatement);
        analysis.setGlobalTimeFilter(globalTimeFilter);

        if (queryStatement.getWhereCondition() != null) {
          if (!queryStatement.isAlignByDevice()) {
            Expression queryFilter = analyzeWhere(queryStatement, schemaTree);
            analysis.setQueryFilter(queryFilter);
          } else {
            Map<String, Expression> deviceToQueryFilter =
                analyzeWhereSplitByDevice(queryStatement, deviceSchemaInfos);
            analysis.setDeviceToQueryFilter(deviceToQueryFilter);
          }
        }

        if (queryStatement.getFilterNullComponent() != null) {
          FilterNullParameter filterNullParameter =
              analyzeWithoutNull(queryStatement, schemaTree, selectExpressions);
          analysis.setFilterNullParameter(filterNullParameter);
        }

        if (queryStatement.getFillComponent() != null) {
          List<FillDescriptor> fillDescriptorList = analyzeFill(queryStatement, outputExpressions);
          analysis.setFillDescriptorList(fillDescriptorList);
        }

        Map<String, Set<Expression>> sourceExpressions = new HashMap<>();
        for (Expression selectExpr : selectExpressions) {
          for (Expression sourceExpression :
              ExpressionAnalyzer.searchSourceExpressions(selectExpr)) {
            sourceExpressions
                .computeIfAbsent(
                    ExpressionAnalyzer.getDeviceName(sourceExpression), key -> new HashSet<>())
                .add(sourceExpression);
          }
        }
        analysis.setSourceExpressions(sourceExpressions);

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
        LOGGER.error("Meet error when analyzing the query statement: ", e);
        throw new StatementAnalyzeException("Meet error when analyzing the query statement");
      }
      return analysis;
    }

    private Filter analyzeGlobalTimeFilter(QueryStatement queryStatement) {
      Filter globalTimeFilter = null;
      if (queryStatement.getWhereCondition() != null) {
        globalTimeFilter =
            ExpressionAnalyzer.transformToGlobalTimeFilter(
                queryStatement.getWhereCondition().getPredicate());
      }
      if (queryStatement.isGroupByTime()) {
        GroupByTimeComponent groupByTimeComponent =
            ((AggregationQueryStatement) queryStatement).getGroupByTimeComponent();
        Filter groupByFilter =
            new GroupByFilter(
                groupByTimeComponent.getInterval(),
                groupByTimeComponent.getSlidingStep(),
                groupByTimeComponent.getStartTime(),
                groupByTimeComponent.getEndTime());
        if (globalTimeFilter == null) {
          globalTimeFilter = groupByFilter;
        } else {
          globalTimeFilter = FilterFactory.and(globalTimeFilter, groupByFilter);
        }
      }
      return globalTimeFilter;
    }

    private List<Pair<Expression, String>> analyzeSelect(
        QueryStatement queryStatement, SchemaTree schemaTree) {
      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      ColumnPaginationController paginationController =
          new ColumnPaginationController(
              queryStatement.getSeriesLimit(),
              queryStatement.getSeriesOffset(),
              queryStatement instanceof LastQueryStatement || queryStatement.isGroupByLevel());

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
            ExpressionAnalyzer.setTypeProvider(expression, typeProvider);
            outputExpressions.add(
                new Pair<>(expression, hasAlias ? resultColumn.getAlias() : null));
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
        Set<Expression> selectExpressions) {
      List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();
      Set<String> measurementSet = new HashSet<>();
      List<Pair<Expression, String>> measurementWithAliasList =
          analyzeMeasurements(queryStatement, measurementSet);

      List<MeasurementPath> allSelectedPaths = new ArrayList<>();
      for (PartialPath devicePattern : devicePatternList) {
        List<DeviceSchemaInfo> deviceSchemaInfos = schemaTree.getMatchedDevices(devicePattern);
        allDeviceSchemaInfos.addAll(deviceSchemaInfos);
        for (DeviceSchemaInfo deviceSchema : deviceSchemaInfos) {
          allSelectedPaths.addAll(deviceSchema.getMeasurements(measurementSet));
        }
      }
      Map<String, List<MeasurementPath>> measurementNameToPathsMap = new HashMap<>();
      for (MeasurementPath measurementPath : allSelectedPaths) {
        measurementNameToPathsMap
            .computeIfAbsent(measurementPath.getMeasurement(), key -> new ArrayList<>())
            .add(measurementPath);
      }
      measurementNameToPathsMap.values().forEach(Analyzer::checkDataTypeConsistency);

      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      ColumnPaginationController paginationController =
          new ColumnPaginationController(
              queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset(), false);

      for (Pair<Expression, String> measurementWithAlias : measurementWithAliasList) {
        String measurement =
            ExpressionAnalyzer.getPathInLeafExpression(measurementWithAlias.left).toString();
        if (measurementNameToPathsMap.containsKey(measurement)) {
          List<MeasurementPath> measurementPaths = measurementNameToPathsMap.get(measurement);
          TSDataType dataType = measurementPaths.get(0).getSeriesType();
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            outputExpressions.add(measurementWithAlias);
            typeProvider.setType(measurementWithAlias.left.getExpressionString(), dataType);
            for (MeasurementPath measurementPath : measurementPaths) {
              Expression tmpExpression =
                  ExpressionAnalyzer.replacePathInExpression(
                      measurementWithAlias.left, measurementPath);
              typeProvider.setType(tmpExpression.getExpressionString(), dataType);
              selectExpressions.add(tmpExpression);
            }
            paginationController.consumeLimit();
          } else {
            break;
          }
        } else if (measurement.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
          for (String measurementName : measurementNameToPathsMap.keySet()) {
            List<MeasurementPath> measurementPaths = measurementNameToPathsMap.get(measurementName);
            TSDataType dataType = measurementPaths.get(0).getSeriesType();
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              Expression replacedMeasurement =
                  ExpressionAnalyzer.replacePathInExpression(
                      measurementWithAlias.left, measurementName);
              typeProvider.setType(replacedMeasurement.getExpressionString(), dataType);
              outputExpressions.add(new Pair<>(replacedMeasurement, measurementWithAlias.right));
              for (MeasurementPath measurementPath : measurementPaths) {
                Expression tmpExpression =
                    ExpressionAnalyzer.replacePathInExpression(
                        measurementWithAlias.left, measurementPath);
                typeProvider.setType(tmpExpression.getExpressionString(), dataType);
                selectExpressions.add(tmpExpression);
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

    private List<Pair<Expression, String>> analyzeMeasurements(
        QueryStatement queryStatement, Set<String> measurementSet) {
      List<Pair<Expression, String>> measurementWithAliasList =
          queryStatement.getSelectComponent().getResultColumns().stream()
              .map(
                  resultColumn ->
                      ExpressionAnalyzer.getMeasurementWithAliasInExpression(
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

    private Expression analyzeWhere(QueryStatement queryStatement, SchemaTree schemaTree) {
      List<Expression> rewrittenPredicates =
          ExpressionAnalyzer.removeWildcardInQueryFilter(
              queryStatement.getWhereCondition().getPredicate(),
              queryStatement.getFromComponent().getPrefixPaths(),
              schemaTree,
              typeProvider);
      if (rewrittenPredicates.size() == 1) {
        return rewrittenPredicates.get(0);
      } else {
        return ExpressionAnalyzer.constructBinaryFilterTreeWithAnd(
            rewrittenPredicates.stream().distinct().collect(Collectors.toList()));
      }
    }

    private Map<String, Expression> analyzeWhereSplitByDevice(
        QueryStatement queryStatement, List<DeviceSchemaInfo> deviceSchemaInfos) {
      Map<String, Expression> deviceToQueryFilter = new HashMap<>();
      for (DeviceSchemaInfo deviceSchemaInfo : deviceSchemaInfos) {
        List<Expression> rewrittenPredicates =
            ExpressionAnalyzer.removeWildcardInQueryFilterByDevice(
                queryStatement.getWhereCondition().getPredicate(), deviceSchemaInfo, typeProvider);
        if (rewrittenPredicates.size() == 1) {
          deviceToQueryFilter.put(
              deviceSchemaInfo.getDevicePath().getFullPath(), rewrittenPredicates.get(0));
        } else {
          deviceToQueryFilter.put(
              deviceSchemaInfo.getDevicePath().getFullPath(),
              ExpressionAnalyzer.constructBinaryFilterTreeWithAnd(
                  rewrittenPredicates.stream().distinct().collect(Collectors.toList())));
        }
      }
      return deviceToQueryFilter;
    }

    private Map<Expression, Set<Expression>> analyzeGroupByLevel(
        AggregationQueryStatement queryStatement,
        List<Pair<Expression, String>> outputExpressions,
        Set<Expression> selectExpressions) {
      GroupByLevelController groupByLevelController =
          new GroupByLevelController(queryStatement.getGroupByLevelComponent().getLevels());
      for (Pair<Expression, String> measurementWithAlias : outputExpressions) {
        groupByLevelController.control(measurementWithAlias.left, measurementWithAlias.right);
      }

      Map<Expression, Set<Expression>> groupByLevelExpressions = new LinkedHashMap<>();
      Map<Expression, Set<Expression>> rawGroupByLevelExpressions =
          groupByLevelController.getGroupedPathMap();
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
      outputExpressions.clear();
      for (Expression groupedExpression : groupByLevelExpressions.keySet()) {
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
          if (!selectExpressions.contains(expression)) {
            throw new SemanticException(
                String.format(
                    "The without null column '%s' don't match the columns queried.", expression));
          }
          resultFilterNullColumns.add(expression);
        }
      }
      filterNullParameter.setFilterNullColumns(resultFilterNullColumns);
      return filterNullParameter;
    }

    private List<FillDescriptor> analyzeFill(
        QueryStatement queryStatement, List<Pair<Expression, String>> outputExpressions) {
      // TODO: support more powerful FILL
      FillComponent fillComponent = queryStatement.getFillComponent();
      return outputExpressions.stream()
          .map(Pair::getLeft)
          .map(
              expression ->
                  new FillDescriptor(
                      fillComponent.getFillPolicy(), fillComponent.getFillValue(), expression))
          .collect(Collectors.toList());
    }

    private DatasetHeader analyzeOutput(
        QueryStatement queryStatement, List<Pair<Expression, String>> outputExpressions) {
      boolean isIgnoreTimestamp =
          queryStatement instanceof AggregationQueryStatement && !queryStatement.isGroupByTime();
      List<ColumnHeader> columnHeaders =
          outputExpressions.stream()
              .map(
                  expressionWithAlias -> {
                    String columnName = expressionWithAlias.left.getExpressionString();
                    String alias = expressionWithAlias.right;
                    return new ColumnHeader(columnName, typeProvider.getType(columnName), alias);
                  })
              .collect(Collectors.toList());
      return new DatasetHeader(columnHeaders, isIgnoreTimestamp);
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

  /**
   * Check datatype consistency in ALIGN BY DEVICE.
   *
   * <p>an inconsistent example: select s0 from root.sg1.d1, root.sg1.d2 align by device, return
   * false while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
   */
  private static void checkDataTypeConsistency(List<MeasurementPath> measurementPaths) {
    Validate.isTrue(measurementPaths.size() > 0);
    TSDataType checkedDataType = measurementPaths.get(0).getSeriesType();
    for (MeasurementPath path : measurementPaths) {
      if (path.getSeriesType() != checkedDataType) {
        throw new SemanticException(
            "ALIGN BY DEVICE: the data types of the same measurement column should be the same across devices.");
      }
    }
  }
}
