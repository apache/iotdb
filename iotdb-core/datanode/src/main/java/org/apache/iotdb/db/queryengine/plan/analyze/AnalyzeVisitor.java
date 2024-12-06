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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ainode.GetModelInfoException;
import org.apache.iotdb.db.exception.metadata.table.TableAlreadyExistsException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIncompatibleException;
import org.apache.iotdb.db.exception.metadata.view.UnsupportedViewException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.execution.operator.window.WindowType;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.BottomInferenceWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.CountInferenceWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.CountInferenceWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.HeadInferenceWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindowType;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.TailInferenceWindow;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileToTableModelAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileToTreeModelAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExistUnknownTypeInExpression;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByConditionParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByCountParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupBySessionParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByVariationParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByConditionComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByCountComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupBySessionComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByVariationComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowCurrentTimestampStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.ShowLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ALLOWED_SCHEMA_PROPS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.DEADBAND;
import static org.apache.iotdb.commons.conf.IoTDBConstant.LOSS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.DEVICE;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.ENDTIME;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.PARTITION_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.SCHEMA_FETCHER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.TREE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils.removeLogicalView;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils.validateSchema;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.bindSchemaForExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.concatDeviceAndBindSchemaForExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.concatDeviceAndBindSchemaForHaving;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.getMeasurementExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.normalizeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchAggregationExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.searchSourceExpressions;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer.toLowerCaseExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer.analyzeExpression;
import static org.apache.iotdb.db.queryengine.plan.analyze.SelectIntoUtils.constructTargetDevice;
import static org.apache.iotdb.db.queryengine.plan.analyze.SelectIntoUtils.constructTargetMeasurement;
import static org.apache.iotdb.db.queryengine.plan.analyze.SelectIntoUtils.constructTargetPath;
import static org.apache.iotdb.db.queryengine.plan.analyze.SelectIntoUtils.constructTargetPathWithoutPlaceHolder;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetInGroupByTimeForDevice;
import static org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor.parseNodeString;
import static org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.GetSourcePathsVisitor.getSourcePaths;
import static org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet.ANALYSIS;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME_HEADER;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROOT_DOT;

/** This visitor is used to analyze each type of Statement and returns the {@link Analysis}. */
public class AnalyzeVisitor extends StatementVisitor<Analysis, MPPQueryContext> {

  private static final Logger logger = LoggerFactory.getLogger(AnalyzeVisitor.class);

  static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  static final Expression DEVICE_EXPRESSION =
      TimeSeriesOperand.constructColumnHeaderExpression(DEVICE, TSDataType.TEXT);

  public static final Expression END_TIME_EXPRESSION =
      TimeSeriesOperand.constructColumnHeaderExpression(ENDTIME, TSDataType.INT64);

  private static final String INFERENCE_COLUMN_NAME = "output";

  private final List<String> lastQueryColumnNames =
      new ArrayList<>(Arrays.asList("TIME", "TIMESERIES", "VALUE", "DATATYPE"));

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private final IModelFetcher modelFetcher;

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  public AnalyzeVisitor(IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
    this.modelFetcher = ModelFetcher.getInstance();
  }

  @Override
  public Analysis visitNode(StatementNode node, MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  public Analysis visitExplain(ExplainStatement explainStatement, MPPQueryContext context) {
    Analysis analysis = visitQuery(explainStatement.getQueryStatement(), context);
    analysis.setRealStatement(explainStatement);
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setDatabaseName(context.getDatabaseName().orElse(null));
    return analysis;
  }

  @Override
  public Analysis visitExplainAnalyze(
      ExplainAnalyzeStatement explainAnalyzeStatement, MPPQueryContext context) {
    Analysis analysis = visitQuery(explainAnalyzeStatement.getQueryStatement(), context);
    context.setExplainAnalyze(true);
    analysis.setRealStatement(explainAnalyzeStatement);
    analysis.setRespDatasetHeader(
        new DatasetHeader(
            Collections.singletonList(
                new ColumnHeader(ColumnHeaderConstant.EXPLAIN_ANALYZE, TSDataType.TEXT, null)),
            true));
    analysis.setDatabaseName(context.getDatabaseName().orElse(null));
    return analysis;
  }

  @Override
  public Analysis visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setLastLevelUseWildcard(queryStatement.isLastLevelUseWildcard());

    try {
      // check for semantic errors
      queryStatement.semanticCheck();

      // fetch model inference information and check
      analyzeModelInference(analysis, queryStatement);

      ISchemaTree schemaTree = analyzeSchema(queryStatement, analysis, context);

      // If there is no leaf node in the schema tree, the query should be completed immediately
      if (schemaTree.isEmpty()) {
        return finishQuery(queryStatement, analysis);
      }

      // extract global time filter from query filter and determine if there is a value filter
      analyzeGlobalTimeFilter(analysis, queryStatement);

      if (queryStatement.isLastQuery()) {
        return analyzeLastQuery(queryStatement, analysis, schemaTree, context);
      }

      List<Pair<Expression, String>> outputExpressions;
      if (queryStatement.isAlignByDevice()) {
        if (TemplatedAnalyze.canBuildPlanUseTemplate(
            analysis, queryStatement, partitionFetcher, schemaTree, context)) {
          return analysis;
        }

        List<PartialPath> deviceList = analyzeFrom(queryStatement, schemaTree);

        if (canPushDownLimitOffsetInGroupByTimeForDevice(queryStatement)) {
          // remove the device which won't appear in resultSet after limit/offset
          deviceList = pushDownLimitOffsetInGroupByTimeForDevice(deviceList, queryStatement);
        }

        outputExpressions =
            analyzeSelect(analysis, queryStatement, schemaTree, deviceList, context);
        if (outputExpressions.isEmpty()) {
          return finishQuery(queryStatement, analysis);
        }

        analyzeDeviceToWhere(analysis, queryStatement, schemaTree, deviceList, context);
        if (deviceList.isEmpty()) {
          return finishQuery(queryStatement, analysis, outputExpressions);
        }
        analysis.setDeviceList(deviceList);

        analyzeDeviceToGroupBy(analysis, queryStatement, schemaTree, deviceList, context);
        analyzeDeviceToOrderBy(analysis, queryStatement, schemaTree, deviceList, context);
        analyzeHaving(analysis, queryStatement, schemaTree, deviceList, context);

        analyzeDeviceToAggregation(analysis, queryStatement);
        analyzeDeviceToSourceTransform(analysis, queryStatement);
        analyzeDeviceToSource(analysis, queryStatement);

        analyzeDeviceViewOutput(analysis, queryStatement);
        analyzeDeviceViewInput(analysis, queryStatement);

        analyzeInto(analysis, queryStatement, deviceList, outputExpressions, context);
      } else {
        // analyze output expressions
        if (queryStatement.isGroupByLevel()) {
          GroupByLevelHelper groupByLevelHelper =
              new GroupByLevelHelper(queryStatement.getGroupByLevelComponent().getLevels());

          outputExpressions =
              analyzeGroupByLevelSelect(
                  analysis, queryStatement, schemaTree, groupByLevelHelper, context);
          if (outputExpressions.isEmpty()) {
            return finishQuery(queryStatement, analysis);
          }
          analysis.setOutputExpressions(outputExpressions);
          setSelectExpressions(analysis, queryStatement, outputExpressions);

          analyzeGroupByLevelHaving(
              analysis, queryStatement, schemaTree, groupByLevelHelper, context);

          analyzeGroupByLevelOrderBy(
              analysis, queryStatement, schemaTree, groupByLevelHelper, context);

          checkDataTypeConsistencyInGroupByLevel(
              analysis, groupByLevelHelper.getGroupByLevelExpressions());
          analysis.setCrossGroupByExpressions(groupByLevelHelper.getGroupByLevelExpressions());
        } else {
          outputExpressions = analyzeSelect(analysis, queryStatement, schemaTree, context);

          analyzeGroupByTag(analysis, queryStatement, outputExpressions);

          if (outputExpressions.isEmpty()) {
            return finishQuery(queryStatement, analysis);
          }
          analysis.setOutputExpressions(outputExpressions);
          setSelectExpressions(analysis, queryStatement, outputExpressions);

          analyzeHaving(analysis, queryStatement, schemaTree, context);

          analyzeOrderBy(analysis, queryStatement, schemaTree, context);
        }

        // analyze aggregation
        analyzeAggregation(analysis, queryStatement);

        // analyze aggregation input
        analyzeGroupBy(analysis, queryStatement, schemaTree, context);
        analyzeWhere(analysis, queryStatement, schemaTree, context);
        if (analysis.getWhereExpression() != null
            && analysis.getWhereExpression().equals(ConstantOperand.FALSE)) {
          return finishQuery(queryStatement, analysis, outputExpressions);
        }
        analyzeSourceTransform(analysis, outputExpressions, queryStatement);

        // analyze series scan
        analyzeSource(analysis, queryStatement);

        // analyze into paths
        analyzeInto(analysis, queryStatement, outputExpressions, context);
      }

      analyzeGroupByTime(analysis, queryStatement);
      context.generateGlobalTimeFilter(analysis);

      analyzeFill(analysis, queryStatement);

      // generate result set header according to output expressions
      analyzeOutput(analysis, queryStatement, outputExpressions);

      // fetch partition information
      analyzeDataPartition(analysis, queryStatement, schemaTree, context);

    } catch (StatementAnalyzeException e) {
      throw new StatementAnalyzeException(
          "Meet error when analyzing the query statement: " + e.getMessage());
    }
    return analysis;
  }

  // check if there is proper model to inference for MODEL_NAME, there is no need to do the
  // following analyze if there isn't.
  private void analyzeModelInference(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.hasModelInference()) {
      return;
    }

    // Get model metadata from configNode and do some check
    String modelId = queryStatement.getModelName();
    TSStatus status = modelFetcher.fetchModel(modelId, analysis);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new GetModelInfoException(status.getMessage());
    }
    ModelInformation modelInformation = analysis.getModelInformation();
    if (modelInformation == null || !modelInformation.available()) {
      throw new SemanticException("Model " + modelId + " is not active");
    }

    // set inference window if there is
    if (queryStatement.isSetInferenceWindow()) {
      InferenceWindow window = queryStatement.getInferenceWindow();
      if (InferenceWindowType.HEAD == window.getType()) {
        long windowSize = ((HeadInferenceWindow) window).getWindowSize();
        checkWindowSize(windowSize, modelInformation);
        if (queryStatement.hasLimit() && queryStatement.getRowLimit() < windowSize) {
          throw new SemanticException(
              "Limit in Sql should be larger than window size in inference");
        }
        // optimize head window by limitNode
        queryStatement.setRowLimit(windowSize);
      } else if (InferenceWindowType.TAIL == window.getType()) {
        long windowSize = ((TailInferenceWindow) window).getWindowSize();
        checkWindowSize(windowSize, modelInformation);
        InferenceWindowParameter inferenceWindowParameter =
            new BottomInferenceWindowParameter(windowSize);
        analysis
            .getModelInferenceDescriptor()
            .setInferenceWindowParameter(inferenceWindowParameter);
      } else if (InferenceWindowType.COUNT == window.getType()) {
        CountInferenceWindow countInferenceWindow = (CountInferenceWindow) window;
        checkWindowSize(countInferenceWindow.getInterval(), modelInformation);
        InferenceWindowParameter inferenceWindowParameter =
            new CountInferenceWindowParameter(
                countInferenceWindow.getInterval(), countInferenceWindow.getStep());
        analysis
            .getModelInferenceDescriptor()
            .setInferenceWindowParameter(inferenceWindowParameter);
      }
    }

    // set inference attributes if there is
    if (queryStatement.hasInferenceAttributes()) {
      analysis
          .getModelInferenceDescriptor()
          .setInferenceAttributes(queryStatement.getInferenceAttributes());
    }
  }

  private void checkWindowSize(long windowSize, ModelInformation modelInformation) {
    if (modelInformation.isBuiltIn()) {
      return;
    }

    if (modelInformation.getInputShape()[0] != windowSize) {
      throw new SemanticException(
          String.format(
              "Window output %d is not equal to input size of model %d",
              windowSize, modelInformation.getInputShape()[0]));
    }
  }

  private ISchemaTree analyzeSchema(
      QueryStatement queryStatement, Analysis analysis, MPPQueryContext context) {
    // concat path and construct path pattern tree
    ConcatPathRewriter concatPathRewriter = new ConcatPathRewriter();
    queryStatement =
        (QueryStatement)
            concatPathRewriter.rewrite(
                queryStatement, new PathPatternTree(queryStatement.useWildcard()), context);
    analysis.setRealStatement(queryStatement);

    // request schema fetch API
    long startTime = System.nanoTime();
    ISchemaTree schemaTree;
    try {
      logger.debug("[StartFetchSchema]");
      PathPatternTree authorizedPatternTree = queryStatement.getAuthorityScope();
      // If the authority scope of query statement contains full path, we should fetch schema
      // without template. Otherwise, the result ISchemaTree may contain template series that is
      // not authorized to access.
      boolean allWildcardLeaf =
          !authorizedPatternTree.isContainFullPath() && authorizedPatternTree.isContainWildcard();
      if (queryStatement.isGroupByTag()) {
        schemaTree =
            schemaFetcher.fetchSchemaWithTags(
                concatPathRewriter.getPatternTree(), allWildcardLeaf, context);
      } else {
        schemaTree =
            schemaFetcher.fetchSchema(
                concatPathRewriter.getPatternTree(), allWildcardLeaf, context);
      }

      // make sure paths in logical view is fetched
      updateSchemaTreeByViews(analysis, schemaTree, context);
    } finally {
      logger.debug("[EndFetchSchema]");
      long schemaFetchCost = System.nanoTime() - startTime;
      context.setFetchSchemaCost(schemaFetchCost);
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TREE_TYPE, SCHEMA_FETCHER, schemaFetchCost);
    }

    analysis.setSchemaTree(schemaTree);
    return schemaTree;
  }

  private Analysis finishQuery(QueryStatement queryStatement, Analysis analysis) {
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

  private Analysis finishQuery(
      QueryStatement queryStatement,
      Analysis analysis,
      List<Pair<Expression, String>> outputExpressions) {
    analyzeOutput(analysis, queryStatement, outputExpressions);
    analysis.setFinishQueryAfterAnalyze(true);
    return analysis;
  }

  private void analyzeGlobalTimeFilter(Analysis analysis, QueryStatement queryStatement) {
    Expression globalTimePredicate = null;
    boolean hasValueFilter = false;
    if (queryStatement.getWhereCondition() != null) {
      WhereCondition whereCondition = queryStatement.getWhereCondition();
      Expression predicate = whereCondition.getPredicate();

      Pair<Expression, Boolean> resultPair =
          PredicateUtils.extractGlobalTimePredicate(predicate, true, true);
      globalTimePredicate = resultPair.left;
      if (globalTimePredicate != null) {
        globalTimePredicate = PredicateUtils.predicateRemoveNot(globalTimePredicate);
      }
      hasValueFilter = resultPair.right;

      predicate = PredicateUtils.simplifyPredicate(predicate);

      // set where condition to null if predicate is true or time filter.
      if (!hasValueFilter || predicate.equals(ConstantOperand.TRUE)) {
        queryStatement.setWhereCondition(null);
      } else {
        whereCondition.setPredicate(predicate);
      }
    }
    analysis.setGlobalTimePredicate(globalTimePredicate);
    analysis.setHasValueFilter(hasValueFilter);
  }

  private Analysis analyzeLastQuery(
      QueryStatement queryStatement,
      Analysis analysis,
      ISchemaTree schemaTree,
      MPPQueryContext context) {
    if (analysis.hasValueFilter()) {
      throw new SemanticException("Only time filters are supported in LAST query");
    }
    analyzeLastOrderBy(analysis, queryStatement);

    List<Expression> selectExpressions = new ArrayList<>();
    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      selectExpressions.add(resultColumn.getExpression());
    }
    analyzeLastSource(analysis, selectExpressions, schemaTree, context);

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getLastQueryHeader());

    // fetch partition information
    analyzeDataPartition(analysis, queryStatement, schemaTree, context);

    return analysis;
  }

  private void analyzeLastSource(
      Analysis analysis,
      List<Expression> selectExpressions,
      ISchemaTree schemaTree,
      MPPQueryContext context) {
    Set<Expression> sourceExpressions = new LinkedHashSet<>();
    Set<Expression> lastQueryBaseExpressions = new LinkedHashSet<>();
    Map<Expression, List<Expression>> lastQueryNonWritableViewSourceExpressionMap = null;

    for (Expression selectExpression : selectExpressions) {
      for (Expression lastQuerySourceExpression :
          bindSchemaForExpression(selectExpression, schemaTree, context)) {
        if (lastQuerySourceExpression instanceof TimeSeriesOperand) {
          lastQueryBaseExpressions.add(lastQuerySourceExpression);
          sourceExpressions.add(lastQuerySourceExpression);
        } else {
          if (lastQueryNonWritableViewSourceExpressionMap == null) {
            lastQueryNonWritableViewSourceExpressionMap = new HashMap<>();
          }
          List<Expression> sourceExpressionsOfNonWritableView =
              searchSourceExpressions(lastQuerySourceExpression);
          lastQueryNonWritableViewSourceExpressionMap.putIfAbsent(
              lastQuerySourceExpression, sourceExpressionsOfNonWritableView);
          sourceExpressions.addAll(sourceExpressionsOfNonWritableView);
        }
      }
    }

    analysis.setSourceExpressions(sourceExpressions);
    analysis.setLastQueryBaseExpressions(lastQueryBaseExpressions);
    analysis.setLastQueryNonWritableViewSourceExpressionMap(
        lastQueryNonWritableViewSourceExpressionMap);
  }

  private void updateSchemaTreeByViews(
      Analysis analysis, ISchemaTree originSchemaTree, MPPQueryContext context) {
    if (!originSchemaTree.hasLogicalViewMeasurement()) {
      return;
    }

    PathPatternTree patternTree = new PathPatternTree();
    boolean needToReFetch = false;
    boolean useLogicalView = false;
    try {
      for (MeasurementPath measurementPath :
          originSchemaTree.searchMeasurementPaths(ALL_MATCH_PATTERN).left) {
        if (measurementPath.getMeasurementSchema().isLogicalView()) {
          useLogicalView = true;
          LogicalViewSchema logicalViewSchema =
              (LogicalViewSchema) measurementPath.getMeasurementSchema();
          ViewExpression viewExpression = logicalViewSchema.getExpression();
          List<PartialPath> pathsNeedToReFetch = getSourcePaths(viewExpression);
          for (PartialPath path : pathsNeedToReFetch) {
            patternTree.appendFullPath(path);
            needToReFetch = true;
          }
        }
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    analysis.setUseLogicalView(useLogicalView);
    if (useLogicalView
        && analysis.getTreeStatement() instanceof QueryStatement
        && (((QueryStatement) analysis.getTreeStatement()).isGroupByTag())) {
      throw new SemanticException("Views cannot be used in GROUP BY TAGS query yet.");
    }

    if (needToReFetch) {
      ISchemaTree viewSchemaTree = this.schemaFetcher.fetchSchema(patternTree, true, context);
      originSchemaTree.mergeSchemaTree(viewSchemaTree);
      Set<String> allDatabases = viewSchemaTree.getDatabases();
      allDatabases.addAll(originSchemaTree.getDatabases());
      originSchemaTree.setDatabases(allDatabases);
    }
  }

  /** process select component for align by time + group by level. */
  private List<Pair<Expression, String>> analyzeGroupByLevelSelect(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      GroupByLevelHelper groupByLevelHelper,
      MPPQueryContext queryContext) {
    Map<Integer, Set<Pair<Expression, String>>> outputExpressionMap = new HashMap<>();
    int columnIndex = 0;

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      Set<Pair<Expression, String>> outputExpressionSet = new LinkedHashSet<>();

      List<Expression> resultExpressions =
          bindSchemaForExpression(resultColumn.getExpression(), schemaTree, queryContext);
      boolean isCountStar =
          resultColumn.getExpression().getExpressionType().equals(ExpressionType.FUNCTION)
              && ((FunctionExpression) resultColumn.getExpression()).isCountStar();

      for (Expression resultExpression : resultExpressions) {
        Expression outputExpression =
            groupByLevelHelper.applyLevels(
                isCountStar, resultExpression, resultColumn.getAlias(), analysis);
        Expression normalizedOutputExpression = normalizeExpression(outputExpression);
        analyzeExpressionType(analysis, normalizedOutputExpression);
        outputExpressionSet.add(
            new Pair<>(
                normalizedOutputExpression,
                analyzeAlias(
                    groupByLevelHelper.getAlias(outputExpression.getExpressionString()),
                    outputExpression,
                    normalizedOutputExpression,
                    queryStatement)));
      }

      outputExpressionMap.put(columnIndex++, outputExpressionSet);
    }

    // construct output expressions
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());
    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();

    for (Set<Pair<Expression, String>> outputExpressionSet : outputExpressionMap.values()) {
      for (Pair<Expression, String> outputExpression : outputExpressionSet) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
        } else if (paginationController.hasCurLimit()) {
          outputExpressions.add(outputExpression);
          groupByLevelHelper.updateGroupByLevelExpressions(outputExpression.left);
          paginationController.consumeLimit();
        } else {
          break;
        }
      }
    }
    return new ArrayList<>(outputExpressions);
  }

  /** process select component for align by time. */
  private List<Pair<Expression, String>> analyzeSelect(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      MPPQueryContext queryContext) {
    Map<Integer, List<Pair<Expression, String>>> outputExpressionMap = new HashMap<>();

    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());

    Set<String> aliasSet = new HashSet<>();

    int columnIndex = 0;

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
      List<Expression> resultExpressions =
          bindSchemaForExpression(resultColumn.getExpression(), schemaTree, queryContext);

      for (Expression resultExpression : resultExpressions) {
        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
        } else if (paginationController.hasCurLimit()) {
          checkAliasUniqueness(resultColumn.getAlias(), aliasSet);

          Expression normalizedExpression = normalizeExpression(resultExpression);
          analyzeExpressionType(analysis, normalizedExpression);
          outputExpressions.add(
              new Pair<>(
                  normalizedExpression,
                  analyzeAlias(
                      resultColumn.getAlias(),
                      resultExpression,
                      normalizedExpression,
                      queryStatement)));
          paginationController.consumeLimit();
        } else {
          break;
        }
      }

      outputExpressionMap.put(columnIndex++, outputExpressions);
    }

    // construct output expressions
    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    outputExpressionMap.values().forEach(outputExpressions::addAll);
    return outputExpressions;
  }

  private List<PartialPath> analyzeFrom(QueryStatement queryStatement, ISchemaTree schemaTree) {
    // device path patterns in FROM clause
    List<PartialPath> devicePatternList = queryStatement.getFromComponent().getPrefixPaths();

    Set<PartialPath> deviceSet = new HashSet<>();
    for (PartialPath devicePattern : devicePatternList) {
      // get all matched devices
      deviceSet.addAll(
          schemaTree.getMatchedDevices(devicePattern).stream()
              .map(DeviceSchemaInfo::getDevicePath)
              .collect(Collectors.toList()));
    }

    return queryStatement.getResultDeviceOrder() == Ordering.ASC
        ? deviceSet.stream().sorted().collect(Collectors.toList())
        : deviceSet.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
  }

  /** process select component for align by device. */
  private List<Pair<Expression, String>> analyzeSelect(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceList,
      MPPQueryContext queryContext) {
    List<Pair<Expression, String>> outputExpressions = new ArrayList<>();
    Map<IDeviceID, Set<Expression>> deviceToSelectExpressions = new HashMap<>();
    ColumnPaginationController paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      Expression selectExpression = resultColumn.getExpression();

      // select expression after removing wildcard
      // use LinkedHashMap for order-preserving
      Map<Expression, Map<IDeviceID, Expression>> measurementToDeviceSelectExpressions =
          new LinkedHashMap<>();
      for (PartialPath device : deviceList) {
        List<Expression> selectExpressionsOfOneDevice =
            concatDeviceAndBindSchemaForExpression(
                selectExpression, device, schemaTree, queryContext);
        if (selectExpressionsOfOneDevice.isEmpty()) {
          continue;
        }

        updateMeasurementToDeviceSelectExpressions(
            analysis,
            measurementToDeviceSelectExpressions,
            device.getIDeviceIDAsFullDevice(),
            selectExpressionsOfOneDevice);
      }

      checkAliasUniqueness(resultColumn.getAlias(), measurementToDeviceSelectExpressions);

      for (Map.Entry<Expression, Map<IDeviceID, Expression>> entry :
          measurementToDeviceSelectExpressions.entrySet()) {
        Expression measurementExpression = entry.getKey();
        Map<IDeviceID, Expression> deviceToSelectExpressionsOfOneMeasurement = entry.getValue();

        if (paginationController.hasCurOffset()) {
          paginationController.consumeOffset();
        } else if (paginationController.hasCurLimit()) {
          deviceToSelectExpressionsOfOneMeasurement
              .values()
              .forEach(expression -> analyzeExpressionType(analysis, expression));
          // check whether the datatype of paths which has the same measurement name are
          // consistent; if not, throw a SemanticException
          checkDataTypeConsistencyInAlignByDevice(
              analysis, new ArrayList<>(deviceToSelectExpressionsOfOneMeasurement.values()));

          // add outputExpressions
          Expression lowerCaseMeasurementExpression = toLowerCaseExpression(measurementExpression);
          analyzeExpressionType(analysis, lowerCaseMeasurementExpression);

          outputExpressions.add(
              new Pair<>(
                  lowerCaseMeasurementExpression,
                  analyzeAlias(
                      resultColumn.getAlias(),
                      measurementExpression,
                      lowerCaseMeasurementExpression,
                      queryStatement)));

          // add deviceToSelectExpressions
          updateDeviceToSelectExpressions(
              analysis, deviceToSelectExpressions, deviceToSelectExpressionsOfOneMeasurement);

          paginationController.consumeLimit();
        } else {
          break;
        }
      }
    }

    // remove devices without measurements to compute
    Set<PartialPath> noMeasurementDevices = new HashSet<>();
    for (PartialPath device : deviceList) {
      if (!deviceToSelectExpressions.containsKey(device.getIDeviceIDAsFullDevice())) {
        noMeasurementDevices.add(device);
      }
    }
    deviceList.removeAll(noMeasurementDevices);

    // when the select expression of any device is empty,
    // the where expression map also need remove this device
    if (analysis.getDeviceToWhereExpression() != null) {
      noMeasurementDevices.forEach(
          devicePath ->
              analysis.getDeviceToWhereExpression().remove(devicePath.getIDeviceIDAsFullDevice()));
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

  private void updateMeasurementToDeviceSelectExpressions(
      Analysis analysis,
      Map<Expression, Map<IDeviceID, Expression>> measurementToDeviceSelectExpressions,
      IDeviceID device,
      List<Expression> selectExpressionsOfOneDevice) {
    for (Expression expression : selectExpressionsOfOneDevice) {
      Expression measurementExpression =
          ExpressionAnalyzer.getMeasurementExpression(expression, analysis);
      measurementToDeviceSelectExpressions
          .computeIfAbsent(measurementExpression, key -> new LinkedHashMap<>())
          .put(device, ExpressionAnalyzer.toLowerCaseExpression(expression));
    }
  }

  private void updateDeviceToSelectExpressions(
      Analysis analysis,
      Map<IDeviceID, Set<Expression>> deviceToSelectExpressions,
      Map<IDeviceID, Expression> deviceToSelectExpressionsOfOneMeasurement) {

    for (Map.Entry<IDeviceID, Expression> entry :
        deviceToSelectExpressionsOfOneMeasurement.entrySet()) {
      IDeviceID deviceID = entry.getKey();
      Expression expression = entry.getValue();

      Expression lowerCaseExpression = toLowerCaseExpression(expression);
      analyzeExpressionType(analysis, lowerCaseExpression);
      deviceToSelectExpressions
          .computeIfAbsent(deviceID, key -> new LinkedHashSet<>())
          .add(lowerCaseExpression);
    }
  }

  private String analyzeAlias(
      String resultColumnAlias,
      Expression rawExpression,
      Expression normalizedExpression,
      QueryStatement queryStatement) {
    if (resultColumnAlias != null) {
      // use alias as output symbol
      return resultColumnAlias;
    }

    if (queryStatement.isCountTimeAggregation()) {
      return COUNT_TIME_HEADER;
    }

    if (!Objects.equals(normalizedExpression, rawExpression)) {
      return rawExpression.getOutputSymbol();
    }

    return null;
  }

  private void analyzeHavingBase(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      UnaryOperator<Expression> havingExpressionAnalyzer,
      MPPQueryContext queryContext) {
    // get removeWildcard Expressions in Having
    List<Expression> conJunctions =
        ExpressionAnalyzer.bindSchemaForPredicate(
            queryStatement.getHavingCondition().getPredicate(),
            queryStatement.getFromComponent().getPrefixPaths(),
            schemaTree,
            true,
            queryContext);
    Expression havingExpression =
        PredicateUtils.combineConjuncts(
            conJunctions.stream().distinct().collect(Collectors.toList()));
    havingExpression = havingExpressionAnalyzer.apply(havingExpression);

    TSDataType outputType = analyzeExpressionType(analysis, havingExpression);
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
      MPPQueryContext queryContext) {
    if (!queryStatement.hasHaving()) {
      return;
    }

    analyzeHavingBase(
        analysis,
        queryStatement,
        schemaTree,
        ExpressionAnalyzer::normalizeExpression,
        queryContext);
  }

  private void analyzeGroupByLevelHaving(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      GroupByLevelHelper groupByLevelHelper,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasHaving()) {
      return;
    }

    analyzeHavingBase(
        analysis,
        queryStatement,
        schemaTree,
        havingExpression ->
            PredicateUtils.removeDuplicateConjunct(
                groupByLevelHelper.applyLevels(havingExpression, analysis)),
        queryContext);
    // update groupByLevelExpressions
    groupByLevelHelper.updateGroupByLevelExpressions(analysis.getHavingExpression());
  }

  private void analyzeHaving(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceSet,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasHaving()) {
      return;
    }

    // two maps to be updated
    Map<IDeviceID, Set<Expression>> deviceToAggregationExpressions =
        analysis.getDeviceToAggregationExpressions();
    Map<IDeviceID, Set<Expression>> deviceToOutputExpressions =
        analysis.getDeviceToOutputExpressions();

    Expression havingExpression = queryStatement.getHavingCondition().getPredicate();
    Set<Expression> conJunctions = new HashSet<>();

    for (PartialPath device : deviceSet) {
      List<Expression> expressionsInHaving =
          concatDeviceAndBindSchemaForHaving(havingExpression, device, schemaTree, queryContext);

      conJunctions.addAll(
          expressionsInHaving.stream()
              .map(expression -> getMeasurementExpression(expression, analysis))
              .collect(Collectors.toList()));

      for (Expression expression : expressionsInHaving) {
        Set<Expression> aggregationExpressions = new LinkedHashSet<>();
        Set<Expression> normalizedAggregationExpressions = new LinkedHashSet<>();
        for (Expression aggregationExpression : searchAggregationExpressions(expression)) {
          Expression normalizedAggregationExpression = normalizeExpression(aggregationExpression);

          if (!new ExistUnknownTypeInExpression().process(aggregationExpression, null).isEmpty()) {
            continue;
          }

          analyzeExpressionType(analysis, aggregationExpression);
          analyzeExpressionType(analysis, normalizedAggregationExpression);

          aggregationExpressions.add(aggregationExpression);
          normalizedAggregationExpressions.add(normalizedAggregationExpression);
        }
        deviceToOutputExpressions
            .computeIfAbsent(device.getIDeviceIDAsFullDevice(), key -> new LinkedHashSet<>())
            .addAll(aggregationExpressions);
        deviceToAggregationExpressions
            .computeIfAbsent(device.getIDeviceID(), key -> new LinkedHashSet<>())
            .addAll(normalizedAggregationExpressions);
      }
    }

    havingExpression = PredicateUtils.combineConjuncts(new ArrayList<>(conJunctions));
    TSDataType outputType = analyzeExpressionType(analysis, havingExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(
          String.format(
              "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: %s.",
              outputType));
    }
    analysis.setDeviceToAggregationExpressions(deviceToAggregationExpressions);
    analysis.setHavingExpression(havingExpression);
  }

  private void checkDataTypeConsistencyInGroupByLevel(
      Analysis analysis, Map<Expression, Set<Expression>> groupByLevelExpressions) {
    for (Map.Entry<Expression, Set<Expression>> groupedExpressionRawExpressionsEntry :
        groupByLevelExpressions.entrySet()) {
      Expression groupedAggregationExpression = groupedExpressionRawExpressionsEntry.getKey();
      Set<Expression> rawAggregationExpressions = groupedExpressionRawExpressionsEntry.getValue();

      TSDataType checkedDataType = analysis.getType(groupedAggregationExpression);
      for (Expression rawAggregationExpression : rawAggregationExpressions) {
        if (analysis.getType(rawAggregationExpression) != checkedDataType) {
          throw new SemanticException(
              String.format(
                  "GROUP BY LEVEL: the data types of the same output column[%s] should be the same.",
                  groupedAggregationExpression));
        }
      }
    }
  }

  private void setSelectExpressions(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions) {
    Set<Expression> selectExpressions = new LinkedHashSet<>();
    if (queryStatement.isOutputEndTime()) {
      selectExpressions.add(END_TIME_EXPRESSION);
    }
    for (Pair<Expression, String> outputExpressionAndAlias : outputExpressions) {
      Expression outputExpression = outputExpressionAndAlias.left;
      selectExpressions.add(outputExpression);
    }
    analysis.setSelectExpressions(selectExpressions);
  }

  /**
   * This method is used to analyze GROUP BY TAGS query.
   *
   * <p>TODO: support slimit/soffset/value filter
   */
  private void analyzeGroupByTag(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions) {
    if (!queryStatement.isGroupByTag()) {
      return;
    }
    if (analysis.hasValueFilter()) {
      throw new SemanticException("Only time filters are supported in GROUP BY TAGS query");
    }

    List<String> tagKeys = queryStatement.getGroupByTagComponent().getTagKeys();
    Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
        tagValuesToGroupedTimeseriesOperands = new HashMap<>();
    LinkedHashMap<Expression, Set<Expression>> outputExpressionToRawExpressionsMap =
        new LinkedHashMap<>();

    for (Pair<Expression, String> outputExpressionAndAlias : outputExpressions) {
      FunctionExpression rawExpression = (FunctionExpression) outputExpressionAndAlias.getLeft();
      FunctionExpression measurementExpression =
          (FunctionExpression) getMeasurementExpression(rawExpression, analysis);
      outputExpressionToRawExpressionsMap
          .computeIfAbsent(measurementExpression, v -> new HashSet<>())
          .add(rawExpression);

      Map<String, String> tagMap =
          ((MeasurementPath) ((TimeSeriesOperand) rawExpression.getExpressions().get(0)).getPath())
              .getTagMap();
      List<String> tagValues = new ArrayList<>();
      for (String tagKey : tagKeys) {
        tagValues.add(tagMap.get(tagKey));
      }
      tagValuesToGroupedTimeseriesOperands
          .computeIfAbsent(tagValues, key -> new LinkedHashMap<>())
          .computeIfAbsent(measurementExpression, key -> new ArrayList<>())
          .add(rawExpression.getExpressions().get(0));
    }

    // update outputExpressions
    outputExpressions.clear();
    for (String tagKey : tagKeys) {
      Expression tagKeyExpression =
          TimeSeriesOperand.constructColumnHeaderExpression(tagKey, TSDataType.TEXT);
      analyzeExpressionType(analysis, tagKeyExpression);
      outputExpressions.add(new Pair<>(tagKeyExpression, null));
    }
    for (Expression outputExpression : outputExpressionToRawExpressionsMap.keySet()) {
      // TODO: support alias
      analyzeExpressionType(analysis, outputExpression);
      outputExpressions.add(new Pair<>(outputExpression, null));
    }
    analysis.setTagKeys(queryStatement.getGroupByTagComponent().getTagKeys());
    analysis.setTagValuesToGroupedTimeseriesOperands(tagValuesToGroupedTimeseriesOperands);
    analysis.setCrossGroupByExpressions(outputExpressionToRawExpressionsMap);
  }

  private void analyzeDeviceToAggregation(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.isAggregationQuery()) {
      return;
    }

    updateDeviceToAggregationAndOutputExpressions(
        analysis, analysis.getDeviceToSelectExpressions());

    if (queryStatement.hasOrderByExpression()) {
      updateDeviceToAggregationAndOutputExpressions(
          analysis, analysis.getDeviceToOrderByExpressions());
    }
  }

  private void updateDeviceToAggregationAndOutputExpressions(
      Analysis analysis, Map<IDeviceID, Set<Expression>> deviceToExpressions) {
    // two maps to be updated
    Map<IDeviceID, Set<Expression>> deviceToAggregationExpressions =
        analysis.getDeviceToAggregationExpressions();
    Map<IDeviceID, Set<Expression>> deviceToOutputExpressions =
        analysis.getDeviceToOutputExpressions();

    for (Map.Entry<IDeviceID, Set<Expression>> deviceExpressionsEntry :
        deviceToExpressions.entrySet()) {
      IDeviceID deviceID = deviceExpressionsEntry.getKey();
      Set<Expression> expressionSet = deviceExpressionsEntry.getValue();

      for (Expression expression : expressionSet) {
        for (Expression aggregationExpression : searchAggregationExpressions(expression)) {
          Expression normalizedAggregationExpression = normalizeExpression(aggregationExpression);

          analyzeExpressionType(analysis, normalizedAggregationExpression);

          deviceToOutputExpressions
              .computeIfAbsent(deviceID, key -> new LinkedHashSet<>())
              .add(aggregationExpression);
          deviceToAggregationExpressions
              .computeIfAbsent(deviceID, key -> new LinkedHashSet<>())
              .add(normalizedAggregationExpression);
        }
      }
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
              .map(
                  expression -> {
                    Expression normalizedExpression = normalizeExpression(expression);
                    analyzeExpressionType(analysis, normalizedExpression);
                    return normalizedExpression;
                  })
              .collect(Collectors.toSet());
      analysis.setAggregationExpressions(aggregationExpressions);
      return;
    }

    Set<Expression> aggregationExpressions = new HashSet<>();
    for (Expression expression : analysis.getSelectExpressions()) {
      aggregationExpressions.addAll(searchAggregationExpressions(expression));
    }
    if (queryStatement.hasHaving()) {
      aggregationExpressions.addAll(searchAggregationExpressions(analysis.getHavingExpression()));
    }
    if (queryStatement.hasOrderByExpression()) {
      for (Expression expression : analysis.getOrderByExpressions()) {
        aggregationExpressions.addAll(searchAggregationExpressions(expression));
      }
    }
    analysis.setAggregationExpressions(aggregationExpressions);
  }

  private void analyzeDeviceToSourceTransform(Analysis analysis, QueryStatement queryStatement) {
    if (queryStatement.isAggregationQuery()) {
      Map<IDeviceID, Set<Expression>> deviceToSourceTransformExpressions =
          analysis.getDeviceToSourceTransformExpressions();
      Map<IDeviceID, Set<Expression>> deviceToAggregationExpressions =
          analysis.getDeviceToAggregationExpressions();

      for (Map.Entry<IDeviceID, Set<Expression>> entry :
          deviceToAggregationExpressions.entrySet()) {
        IDeviceID deviceID = entry.getKey();
        Set<Expression> aggregationExpressions = entry.getValue();

        Set<Expression> sourceTransformExpressions =
            deviceToSourceTransformExpressions.computeIfAbsent(
                deviceID, k -> new LinkedHashSet<>());

        for (Expression expression : aggregationExpressions) {
          // if count_time aggregation exist, it can exist only one count_time(*)
          if (queryStatement.isCountTimeAggregation()) {
            for (Expression countTimeSourceExpression :
                ((FunctionExpression) expression).getCountTimeExpressions()) {

              analyzeExpressionType(analysis, countTimeSourceExpression);
              sourceTransformExpressions.add(countTimeSourceExpression);
            }
          } else {
            // We just process first input Expression of COUNT_IF,
            // keep other input Expressions as origin
            if (SqlConstant.COUNT_IF.equalsIgnoreCase(
                ((FunctionExpression) expression).getFunctionName())) {
              sourceTransformExpressions.add(expression.getExpressions().get(0));
            } else {
              sourceTransformExpressions.addAll(expression.getExpressions());
            }
          }
        }

        if (queryStatement.hasGroupByExpression()) {
          sourceTransformExpressions.add(analysis.getDeviceToGroupByExpression().get(deviceID));
        }
      }
    } else {
      updateDeviceToSourceTransformAndOutputExpressions(
          analysis, analysis.getDeviceToSelectExpressions());
      if (queryStatement.hasOrderByExpression()) {
        updateDeviceToSourceTransformAndOutputExpressions(
            analysis, analysis.getDeviceToOrderByExpressions());
      }
    }
  }

  private void updateDeviceToSourceTransformAndOutputExpressions(
      Analysis analysis, Map<IDeviceID, Set<Expression>> deviceToExpressions) {
    // two maps to be updated
    Map<IDeviceID, Set<Expression>> deviceToSourceTransformExpressions =
        analysis.getDeviceToSourceTransformExpressions();
    Map<IDeviceID, Set<Expression>> deviceToOutputExpressions =
        analysis.getDeviceToOutputExpressions();

    for (Map.Entry<IDeviceID, Set<Expression>> deviceExpressionsEntry :
        deviceToExpressions.entrySet()) {
      IDeviceID deviceID = deviceExpressionsEntry.getKey();
      Set<Expression> expressions = deviceExpressionsEntry.getValue();

      Set<Expression> normalizedExpressions = new LinkedHashSet<>();
      for (Expression expression : expressions) {
        Expression normalizedExpression = normalizeExpression(expression);
        analyzeExpressionType(analysis, normalizedExpression);

        normalizedExpressions.add(normalizedExpression);
      }
      deviceToOutputExpressions
          .computeIfAbsent(deviceID, key -> new LinkedHashSet<>())
          .addAll(expressions);
      deviceToSourceTransformExpressions
          .computeIfAbsent(deviceID, key -> new LinkedHashSet<>())
          .addAll(normalizedExpressions);
    }
  }

  private void analyzeSourceTransform(
      Analysis analysis,
      List<Pair<Expression, String>> outputExpressions,
      QueryStatement queryStatement) {
    Set<Expression> sourceTransformExpressions = analysis.getSourceTransformExpressions();

    if (queryStatement.isAggregationQuery()) {
      if (queryStatement.isCountTimeAggregation()) {

        for (Pair<Expression, String> pair : outputExpressions) {
          FunctionExpression countTimeExpression = (FunctionExpression) pair.left;
          for (Expression countTimeSourceExpression :
              countTimeExpression.getCountTimeExpressions()) {
            analyzeExpressionType(analysis, countTimeSourceExpression);
            sourceTransformExpressions.add(countTimeSourceExpression);
          }
        }

        // count_time only returns one result
        Pair<Expression, String> firstCountTimeExpression = outputExpressions.get(0);
        outputExpressions.clear();
        outputExpressions.add(firstCountTimeExpression);

      } else {
        for (Expression aggExpression : analysis.getAggregationExpressions()) {
          // for COUNT_IF, only the first Expression of input need to transform
          if (SqlConstant.COUNT_IF.equalsIgnoreCase(
              ((FunctionExpression) aggExpression).getFunctionName())) {
            sourceTransformExpressions.add(aggExpression.getExpressions().get(0));
          } else {
            sourceTransformExpressions.addAll(aggExpression.getExpressions());
          }
        }
      }

      if (queryStatement.hasGroupByExpression()) {
        sourceTransformExpressions.add(analysis.getGroupByExpression());
      }
    } else {
      sourceTransformExpressions.addAll(analysis.getSelectExpressions());
      if (queryStatement.hasOrderByExpression()) {
        sourceTransformExpressions.addAll(analysis.getOrderByExpressions());
      }
    }
  }

  private void analyzeDeviceToSource(Analysis analysis, QueryStatement queryStatement) {
    Map<IDeviceID, Set<Expression>> deviceToSourceExpressions = new HashMap<>();
    Map<IDeviceID, Set<Expression>> deviceToSourceTransformExpressions =
        analysis.getDeviceToSourceTransformExpressions();

    for (Map.Entry<IDeviceID, Set<Expression>> entry :
        deviceToSourceTransformExpressions.entrySet()) {
      IDeviceID deviceName = entry.getKey();
      Set<Expression> sourceTransformExpressions = entry.getValue();

      Set<Expression> sourceExpressions = new LinkedHashSet<>();
      sourceTransformExpressions.forEach(
          expression -> sourceExpressions.addAll(searchSourceExpressions(expression)));

      deviceToSourceExpressions.put(deviceName, sourceExpressions);
    }

    if (queryStatement.hasWhere()) {
      Map<IDeviceID, Expression> deviceToWhereExpression = analysis.getDeviceToWhereExpression();
      for (Map.Entry<IDeviceID, Expression> deviceWhereExpressionEntry :
          deviceToWhereExpression.entrySet()) {
        IDeviceID deviceName = deviceWhereExpressionEntry.getKey();
        Expression whereExpression = deviceWhereExpressionEntry.getValue();
        deviceToSourceExpressions
            .computeIfAbsent(deviceName, key -> new LinkedHashSet<>())
            .addAll(searchSourceExpressions(whereExpression));
      }
    }

    Map<IDeviceID, IDeviceID> outputDeviceToQueriedDevicesMap = new LinkedHashMap<>();
    for (Map.Entry<IDeviceID, Set<Expression>> entry : deviceToSourceExpressions.entrySet()) {
      IDeviceID deviceName = entry.getKey();
      Set<Expression> sourceExpressionsUnderDevice = entry.getValue();
      Set<IDeviceID> queriedDevices = new HashSet<>();
      // conversion
      for (Expression expression : sourceExpressionsUnderDevice) {
        queriedDevices.add(ExpressionAnalyzer.getDeviceNameInSourceExpression(expression));
      }
      if (queriedDevices.size() > 1) {
        throw new SemanticException(
            "Cross-device queries are not supported in ALIGN BY DEVICE queries.");
      }
      outputDeviceToQueriedDevicesMap.put(deviceName, queriedDevices.iterator().next());
    }

    analysis.setDeviceToSourceExpressions(deviceToSourceExpressions);
    analysis.setOutputDeviceToQueriedDevicesMap(outputDeviceToQueriedDevicesMap);
  }

  private void analyzeSource(Analysis analysis, QueryStatement queryStatement) {
    Set<Expression> sourceExpressions = analysis.getSourceExpressions();
    if (sourceExpressions == null) {
      sourceExpressions = new HashSet<>();
      analysis.setSourceExpressions(sourceExpressions);
    }

    for (Expression expression : analysis.getSourceTransformExpressions()) {
      sourceExpressions.addAll(searchSourceExpressions(expression));
    }
    Expression whereExpression = analysis.getWhereExpression();
    if (whereExpression != null) {
      sourceExpressions.addAll(searchSourceExpressions(analysis.getWhereExpression()));
    }
  }

  public static final String WHERE_WRONG_TYPE_ERROR_MSG =
      "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: %s.";

  private void analyzeDeviceToWhere(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceSet,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasWhere()) {
      return;
    }

    Map<IDeviceID, Expression> deviceToWhereExpression = new HashMap<>();
    Iterator<PartialPath> deviceIterator = deviceSet.iterator();
    boolean hasValueFilter = false;
    while (deviceIterator.hasNext()) {
      PartialPath devicePath = deviceIterator.next();
      Expression whereExpression =
          analyzeWhereSplitByDevice(queryStatement, devicePath, schemaTree, queryContext);
      if (whereExpression.equals(ConstantOperand.FALSE)) {
        deviceIterator.remove();
      } else if (whereExpression.equals(ConstantOperand.TRUE)) {
        deviceToWhereExpression.put(devicePath.getIDeviceIDAsFullDevice(), null);
      } else {
        TSDataType outputType = analyzeExpressionType(analysis, whereExpression);
        if (outputType != TSDataType.BOOLEAN) {
          throw new SemanticException(String.format(WHERE_WRONG_TYPE_ERROR_MSG, outputType));
        }
        deviceToWhereExpression.put(devicePath.getIDeviceIDAsFullDevice(), whereExpression);
        hasValueFilter = true;
      }
    }
    analysis.setDeviceToWhereExpression(deviceToWhereExpression);
    analysis.setHasValueFilter(hasValueFilter);
  }

  private void analyzeWhere(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasWhere()) {
      return;
    }
    List<Expression> conJunctions =
        ExpressionAnalyzer.bindSchemaForPredicate(
            queryStatement.getWhereCondition().getPredicate(),
            queryStatement.getFromComponent().getPrefixPaths(),
            schemaTree,
            true,
            queryContext);
    Expression whereExpression = convertConJunctionsToWhereExpression(conJunctions);
    if (whereExpression.equals(ConstantOperand.TRUE)) {
      analysis.setWhereExpression(null);
      analysis.setHasValueFilter(false);
      return;
    }

    TSDataType outputType = analyzeExpressionType(analysis, whereExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(String.format(WHERE_WRONG_TYPE_ERROR_MSG, outputType));
    }
    analysis.setWhereExpression(whereExpression);
  }

  private Expression analyzeWhereSplitByDevice(
      final QueryStatement queryStatement,
      final PartialPath devicePath,
      final ISchemaTree schemaTree,
      final MPPQueryContext queryContext) {
    List<Expression> conJunctions =
        ExpressionAnalyzer.concatDeviceAndBindSchemaForPredicate(
            queryStatement.getWhereCondition().getPredicate(),
            devicePath,
            schemaTree,
            true,
            queryContext);
    return convertConJunctionsToWhereExpression(conJunctions);
  }

  private Expression convertConJunctionsToWhereExpression(List<Expression> conJunctions) {
    Expression predicate =
        PredicateUtils.combineConjuncts(
            conJunctions.stream().distinct().collect(Collectors.toList()));
    predicate = PredicateUtils.simplifyPredicate(predicate);
    predicate = normalizeExpression(predicate);
    return predicate;
  }

  private void analyzeDeviceViewOutput(Analysis analysis, QueryStatement queryStatement) {
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
      deviceViewOutputExpressions.addAll(selectExpressions);
      if (queryStatement.hasOrderByExpression()) {
        deviceViewOutputExpressions.addAll(analysis.getOrderByExpressions());
      }
    }
    analysis.setDeviceViewOutputExpressions(deviceViewOutputExpressions);
    analysis.setDeviceViewSpecialProcess(
        analyzeDeviceViewSpecialProcess(deviceViewOutputExpressions, queryStatement, analysis));
  }

  static boolean analyzeDeviceViewSpecialProcess(
      Set<Expression> deviceViewOutputExpressions,
      QueryStatement queryStatement,
      Analysis analysis) {
    if (queryStatement.isAggregationQuery()
        || analysis.getWhereExpression() != null
            && ExpressionAnalyzer.isDeviceViewNeedSpecialProcess(
                analysis.getWhereExpression(), analysis)) {
      return true;
    }
    for (Expression expression : deviceViewOutputExpressions) {
      if (ExpressionAnalyzer.isDeviceViewNeedSpecialProcess(expression, analysis)) {
        return true;
      }
    }
    return false;
  }

  private void analyzeDeviceViewInput(Analysis analysis, QueryStatement queryStatement) {
    List<String> deviceViewOutputColumns =
        analysis.getDeviceViewOutputExpressions().stream()
            .map(Expression::getOutputSymbol)
            .collect(Collectors.toList());

    Map<IDeviceID, Set<String>> deviceToOutputColumnsMap = new LinkedHashMap<>();
    Map<IDeviceID, Set<Expression>> deviceToOutputExpressions =
        analysis.getDeviceToOutputExpressions();
    for (Map.Entry<IDeviceID, Set<Expression>> deviceOutputExpressionEntry :
        deviceToOutputExpressions.entrySet()) {
      Set<Expression> outputExpressionsUnderDevice = deviceOutputExpressionEntry.getValue();
      checkDeviceViewInputUniqueness(outputExpressionsUnderDevice);

      Set<String> outputColumns = new LinkedHashSet<>();
      if (queryStatement.isOutputEndTime()) {
        outputColumns.add(ENDTIME);
      }
      for (Expression expression : outputExpressionsUnderDevice) {
        outputColumns.add(getMeasurementExpression(expression, analysis).getOutputSymbol());
      }
      deviceToOutputColumnsMap.put(deviceOutputExpressionEntry.getKey(), outputColumns);
    }

    Map<IDeviceID, List<Integer>> deviceViewInputIndexesMap = new HashMap<>();
    for (Map.Entry<IDeviceID, Set<String>> deviceOutputColumnsEntry :
        deviceToOutputColumnsMap.entrySet()) {
      IDeviceID deviceID = deviceOutputColumnsEntry.getKey();
      List<String> outputsUnderDevice = new ArrayList<>(deviceOutputColumnsEntry.getValue());

      List<Integer> indexes = new ArrayList<>();
      for (String output : outputsUnderDevice) {
        int index = deviceViewOutputColumns.indexOf(output);
        checkState(
            index >= 1, "output column '%s' is not stored in %s", output, deviceViewOutputColumns);
        indexes.add(index);
      }
      deviceViewInputIndexesMap.put(deviceID, indexes);
    }
    analysis.setDeviceViewInputIndexesMap(deviceViewInputIndexesMap);
  }

  private void checkDeviceViewInputUniqueness(Set<Expression> outputExpressionsUnderDevice) {
    Set<Expression> normalizedOutputExpressionsUnderDevice =
        outputExpressionsUnderDevice.stream()
            .map(ExpressionAnalyzer::normalizeExpression)
            .collect(Collectors.toSet());
    if (normalizedOutputExpressionsUnderDevice.size() < outputExpressionsUnderDevice.size()) {
      throw new SemanticException(
          "Views or measurement aliases representing the same data source "
              + "cannot be queried concurrently in ALIGN BY DEVICE queries.");
    }
  }

  static void analyzeOutput(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions) {
    if (queryStatement.isSelectInto()) {
      analysis.setRespDatasetHeader(
          DatasetHeaderFactory.getSelectIntoHeader(queryStatement.isAlignByDevice()));
      return;
    }

    if (queryStatement.hasModelInference()) {
      ModelInformation modelInformation = analysis.getModelInformation();
      // check input
      checkInputShape(modelInformation, outputExpressions);
      checkInputType(analysis, modelInformation, outputExpressions);

      // set output
      List<ColumnHeader> columnHeaders = new ArrayList<>();
      int[] outputShape = modelInformation.getOutputShape();
      TSDataType[] outputDataType = modelInformation.getOutputDataType();
      for (int i = 0; i < outputShape[1]; i++) {
        columnHeaders.add(new ColumnHeader(INFERENCE_COLUMN_NAME + i, outputDataType[i]));
      }
      analysis
          .getModelInferenceDescriptor()
          .setOutputColumnNames(
              columnHeaders.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList()));
      analysis.setRespDatasetHeader(new DatasetHeader(columnHeaders, true));
      return;
    }

    boolean isIgnoreTimestamp = queryStatement.isAggregationQuery() && !queryStatement.isGroupBy();
    List<ColumnHeader> columnHeaders = new ArrayList<>();
    if (queryStatement.isAlignByDevice()) {
      columnHeaders.add(new ColumnHeader(DEVICE, TSDataType.TEXT, null));
    }
    if (queryStatement.isOutputEndTime()) {
      columnHeaders.add(new ColumnHeader(ENDTIME, TSDataType.INT64, null));
    }
    for (Pair<Expression, String> expressionAliasPair : outputExpressions) {
      columnHeaders.add(
          new ColumnHeader(
              expressionAliasPair.left.getExpressionString(),
              analysis.getType(expressionAliasPair.left),
              expressionAliasPair.right));
    }
    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaders, isIgnoreTimestamp));
  }

  // check if the result of SQL matches the input of model
  private static void checkInputShape(
      ModelInformation modelInformation, List<Pair<Expression, String>> outputExpressions) {
    if (modelInformation.isBuiltIn()) {
      modelInformation.setInputColumnSize(outputExpressions.size());
      return;
    }

    // check inputShape
    int[] inputShape = modelInformation.getInputShape();
    if (inputShape.length != 2) {
      throw new SemanticException(
          String.format(
              "The input shape of model is not correct, the dimension of input shape should be 2, actual dimension is %d",
              inputShape.length));
    }
    int columnNumber = inputShape[1];
    if (columnNumber != outputExpressions.size()) {
      throw new SemanticException(
          String.format(
              "The column number of SQL result does not match the number of model input [%d] for inference",
              columnNumber));
    }
  }

  private static void checkInputType(
      Analysis analysis,
      ModelInformation modelInformation,
      List<Pair<Expression, String>> outputExpressions) {

    if (modelInformation.isBuiltIn()) {
      TSDataType[] inputType = new TSDataType[outputExpressions.size()];
      for (int i = 0; i < outputExpressions.size(); i++) {
        Expression inputExpression = outputExpressions.get(i).left;
        TSDataType inputDataType = analysis.getType(inputExpression);
        if (!inputDataType.isNumeric()) {
          throw new SemanticException(
              String.format(
                  "The type of SQL result column [%s in %d] should be numeric when inference",
                  inputDataType, i));
        }
        inputType[i] = inputDataType;
      }
      modelInformation.setInputDataType(inputType);
      return;
    }

    TSDataType[] inputType = modelInformation.getInputDataType();
    if (inputType.length != modelInformation.getInputShape()[1]) {
      throw new SemanticException(
          String.format(
              "The inputType does not match the input shape [%d] for inference",
              modelInformation.getInputShape()[1]));
    }
    for (int i = 0; i < inputType.length; i++) {
      Expression inputExpression = outputExpressions.get(i).left;
      TSDataType inputDataType = analysis.getType(inputExpression);
      if (inputDataType != inputType[i]) {
        throw new SemanticException(
            String.format(
                "The type of SQL result column [%s in %d] does not match the type of model input [%s] when inference",
                inputDataType, i, inputType[i]));
      }
    }
  }

  // For last query
  private void analyzeLastOrderBy(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.hasOrderBy()) {
      return;
    }

    if (queryStatement.onlyOrderByTimeseries()) {
      analysis.setTimeseriesOrderingForLastQuery(
          queryStatement.getOrderByComponent().getTimeseriesOrder());
    }

    for (SortItem sortItem : queryStatement.getSortItemList()) {
      String sortKey = sortItem.getSortKey();
      if (!lastQueryColumnNames.contains(sortKey.toUpperCase())) {
        throw new SemanticException(
            String.format(
                "%s in order by clause doesn't exist in the result of last query.", sortKey));
      }
    }
  }

  private void analyzeOrderByBase(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      UnaryOperator<List<Expression>> orderByExpressionAnalyzer,
      MPPQueryContext queryContext) {
    Set<Expression> orderByExpressions = new LinkedHashSet<>();
    for (Expression expressionForItem : queryStatement.getExpressionSortItemList()) {
      // Expression in a sortItem only indicates one column
      List<Expression> expressions =
          bindSchemaForExpression(expressionForItem, schemaTree, queryContext);
      if (expressions.isEmpty()) {
        throw new SemanticException(
            String.format(
                "%s in order by clause doesn't exist.", expressionForItem.getExpressionString()));
      }

      expressions = orderByExpressionAnalyzer.apply(expressions);
      if (expressions.size() > 1) {
        throw new SemanticException(
            String.format(
                "%s in order by clause shouldn't refer to more than one timeseries.",
                expressionForItem.getExpressionString()));
      }

      Expression orderByExpression = normalizeExpression(expressions.get(0));
      TSDataType dataType = analyzeExpressionType(analysis, orderByExpression);
      if (!dataType.isComparable()) {
        throw new SemanticException(
            String.format("The data type of %s is not comparable", dataType));
      }
      orderByExpressions.add(orderByExpression);
    }
    analysis.setOrderByExpressions(orderByExpressions);
    queryStatement.updateSortItems(orderByExpressions);
  }

  private void analyzeOrderBy(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasOrderByExpression()) {
      return;
    }

    analyzeOrderByBase(
        analysis, queryStatement, schemaTree, expressions -> expressions, queryContext);
  }

  private void analyzeGroupByLevelOrderBy(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      GroupByLevelHelper groupByLevelHelper,
      MPPQueryContext queryContext) {
    if (!queryStatement.hasOrderByExpression()) {
      return;
    }

    analyzeOrderByBase(
        analysis,
        queryStatement,
        schemaTree,
        expressions -> {
          Set<Expression> groupedExpressions = new HashSet<>();
          for (Expression expression : expressions) {
            groupedExpressions.add(groupByLevelHelper.applyLevels(expression, analysis));
          }
          return new ArrayList<>(groupedExpressions);
        },
        queryContext);
    // update groupByLevelExpressions
    for (Expression orderByExpression : analysis.getOrderByExpressions()) {
      groupByLevelHelper.updateGroupByLevelExpressions(orderByExpression);
    }
  }

  static TSDataType analyzeExpressionType(Analysis analysis, Expression expression) {
    return analyzeExpression(analysis, expression);
  }

  private void analyzeDeviceToGroupBy(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      List<PartialPath> deviceSet,
      MPPQueryContext queryContext) {
    if (queryStatement.getGroupByComponent() == null) {
      return;
    }
    GroupByComponent groupByComponent = queryStatement.getGroupByComponent();
    WindowType windowType = groupByComponent.getWindowType();

    Map<IDeviceID, Expression> deviceToGroupByExpression = new LinkedHashMap<>();
    if (queryStatement.hasGroupByExpression()) {
      Expression expression = groupByComponent.getControlColumnExpression();
      for (PartialPath device : deviceSet) {
        List<Expression> groupByExpressionsOfOneDevice =
            concatDeviceAndBindSchemaForExpression(expression, device, schemaTree, queryContext);

        if (groupByExpressionsOfOneDevice.isEmpty()) {
          throw new SemanticException(
              String.format("%s in group by clause doesn't exist.", expression));
        }
        if (groupByExpressionsOfOneDevice.size() > 1) {
          throw new SemanticException(
              String.format(
                  "%s in group by clause shouldn't refer to more than one timeseries.",
                  expression));
        }
        deviceToGroupByExpression.put(
            device.getIDeviceIDAsFullDevice(),
            normalizeExpression(groupByExpressionsOfOneDevice.get(0)));
      }
    }

    GroupByParameter groupByParameter;
    switch (windowType) {
      case VARIATION_WINDOW:
        double delta = ((GroupByVariationComponent) groupByComponent).getDelta();
        for (Expression expression : deviceToGroupByExpression.values()) {
          checkGroupByVariationExpressionType(analysis, expression, delta);
        }
        groupByParameter = new GroupByVariationParameter(groupByComponent.isIgnoringNull(), delta);
        analysis.setDeviceToGroupByExpression(deviceToGroupByExpression);
        break;
      case CONDITION_WINDOW:
        Expression keepExpression =
            ((GroupByConditionComponent) groupByComponent).getKeepExpression();
        for (Expression expression : deviceToGroupByExpression.values()) {
          checkGroupByConditionExpressionType(analysis, expression, keepExpression);
        }
        groupByParameter =
            new GroupByConditionParameter(groupByComponent.isIgnoringNull(), keepExpression);
        analysis.setDeviceToGroupByExpression(deviceToGroupByExpression);
        break;
      case SESSION_WINDOW:
        groupByParameter =
            new GroupBySessionParameter(
                ((GroupBySessionComponent) groupByComponent).getTimeInterval());
        break;
      case COUNT_WINDOW:
        groupByParameter =
            new GroupByCountParameter(
                ((GroupByCountComponent) groupByComponent).getCountNumber(),
                groupByComponent.isIgnoringNull());
        analysis.setDeviceToGroupByExpression(deviceToGroupByExpression);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported window type");
    }
    analysis.setGroupByParameter(groupByParameter);
  }

  private void analyzeDeviceToOrderBy(
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
      deviceToOrderByExpressions.put(
          device.getIDeviceIDAsFullDevice(), orderByExpressionsForOneDevice);
    }

    analysis.setOrderByExpressions(deviceViewOrderByExpression);
    queryStatement.updateSortItems(deviceViewOrderByExpression);
    analysis.setDeviceToSortItems(deviceToSortItems);
    analysis.setDeviceToOrderByExpressions(deviceToOrderByExpressions);
  }

  private void analyzeGroupBy(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      MPPQueryContext queryContext) {

    if (queryStatement.getGroupByComponent() == null) {
      return;
    }
    GroupByComponent groupByComponent = queryStatement.getGroupByComponent();
    WindowType windowType = groupByComponent.getWindowType();

    Expression groupByExpression = null;
    if (queryStatement.hasGroupByExpression()) {
      groupByExpression = groupByComponent.getControlColumnExpression();
      // Expression in group by variation clause only indicates one column
      List<Expression> expressions =
          bindSchemaForExpression(groupByExpression, schemaTree, queryContext);
      if (expressions.isEmpty()) {
        throw new SemanticException(
            String.format(
                "%s in group by clause doesn't exist.", groupByExpression.getExpressionString()));
      }
      if (expressions.size() > 1) {
        throw new SemanticException(
            String.format(
                "%s in group by clause shouldn't refer to more than one timeseries.",
                groupByExpression.getExpressionString()));
      }
      // Aggregation expression shouldn't exist in group by clause.
      List<Expression> aggregationExpression = searchAggregationExpressions(expressions.get(0));
      if (aggregationExpression != null && !aggregationExpression.isEmpty()) {
        throw new SemanticException("Aggregation expression shouldn't exist in group by clause");
      }
      groupByExpression = normalizeExpression(expressions.get(0));
    }

    if (windowType == WindowType.VARIATION_WINDOW) {
      double delta = ((GroupByVariationComponent) groupByComponent).getDelta();
      checkGroupByVariationExpressionType(analysis, groupByExpression, delta);
      GroupByParameter groupByParameter =
          new GroupByVariationParameter(groupByComponent.isIgnoringNull(), delta);
      analysis.setGroupByExpression(groupByExpression);
      analysis.setGroupByParameter(groupByParameter);
    } else if (windowType == WindowType.CONDITION_WINDOW) {
      Expression keepExpression =
          ((GroupByConditionComponent) groupByComponent).getKeepExpression();
      checkGroupByConditionExpressionType(analysis, groupByExpression, keepExpression);
      GroupByParameter groupByParameter =
          new GroupByConditionParameter(groupByComponent.isIgnoringNull(), keepExpression);
      analysis.setGroupByExpression(groupByExpression);
      analysis.setGroupByParameter(groupByParameter);
    } else if (windowType == WindowType.SESSION_WINDOW) {
      long interval = ((GroupBySessionComponent) groupByComponent).getTimeInterval();
      GroupByParameter groupByParameter = new GroupBySessionParameter(interval);
      analysis.setGroupByParameter(groupByParameter);
    } else if (windowType == WindowType.COUNT_WINDOW) {
      GroupByParameter groupByParameter =
          new GroupByCountParameter(
              ((GroupByCountComponent) groupByComponent).getCountNumber(),
              groupByComponent.isIgnoringNull());
      analyzeExpressionType(analysis, groupByExpression);
      analysis.setGroupByExpression(groupByExpression);
      analysis.setGroupByParameter(groupByParameter);
    } else {
      throw new SemanticException("Unsupported window type");
    }
  }

  private void checkGroupByVariationExpressionType(
      Analysis analysis, Expression groupByExpression, double delta) {
    TSDataType type = analyzeExpressionType(analysis, groupByExpression);
    if (delta != 0 && !type.isNumeric()) {
      throw new SemanticException("Only support numeric type when delta != 0");
    }
  }

  private void checkGroupByConditionExpressionType(
      Analysis analysis, Expression groupByExpression, Expression keepExpression) {
    TSDataType type = analyzeExpressionType(analysis, groupByExpression);
    if (type != TSDataType.BOOLEAN) {
      throw new SemanticException("Only support boolean type in predict of group by series");
    }

    // check keep Expression
    if (keepExpression instanceof CompareBinaryExpression) {
      Expression leftExpression = ((CompareBinaryExpression) keepExpression).getLeftExpression();
      Expression rightExpression = ((CompareBinaryExpression) keepExpression).getRightExpression();
      if (!(leftExpression instanceof TimeSeriesOperand
          && leftExpression.getExpressionString().equalsIgnoreCase("keep")
          && rightExpression instanceof ConstantOperand)) {
        throw new SemanticException(
            String.format(
                "Please check the keep condition ([%s]), "
                    + "it need to be a constant or a compare expression constructed by 'keep' and a long number.",
                keepExpression.getExpressionString()));
      }
      return;
    }
    if (!(keepExpression instanceof ConstantOperand)) {
      throw new SemanticException(
          String.format(
              "Please check the keep condition ([%s]), "
                  + "it need to be a constant or a compare expression constructed by 'keep' and a long number.",
              keepExpression.getExpressionString()));
    }
  }

  static void analyzeGroupByTime(Analysis analysis, QueryStatement queryStatement) {
    if (!queryStatement.isGroupByTime()) {
      return;
    }

    if (queryStatement.isResultSetEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
    }

    GroupByTimeComponent groupByTimeComponent = queryStatement.getGroupByTimeComponent();
    if ((groupByTimeComponent.getInterval().containsMonth()
            || groupByTimeComponent.getSlidingStep().containsMonth())
        && queryStatement.getResultTimeOrder() == Ordering.DESC) {
      throw new SemanticException("Group by month doesn't support order by time desc now.");
    }
    if (!queryStatement.isCqQueryBody()
        && (groupByTimeComponent.getStartTime() == 0 && groupByTimeComponent.getEndTime() == 0)) {
      throw new SemanticException(
          "The query time range should be specified in the GROUP BY TIME clause.");
    }
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter(groupByTimeComponent);
    analysis.setGroupByTimeParameter(groupByTimeParameter);

    Expression globalTimePredicate = analysis.getGlobalTimePredicate();
    Expression groupByTimePredicate = ExpressionFactory.groupByTime(groupByTimeParameter);
    if (globalTimePredicate == null) {
      globalTimePredicate = groupByTimePredicate;
    } else {
      globalTimePredicate = ExpressionFactory.and(globalTimePredicate, groupByTimePredicate);
    }
    analysis.setGlobalTimePredicate(globalTimePredicate);
  }

  static void analyzeFill(Analysis analysis, QueryStatement queryStatement) {
    if (queryStatement.getFillComponent() == null) {
      return;
    }

    FillComponent fillComponent = queryStatement.getFillComponent();
    analysis.setFillDescriptor(
        new FillDescriptor(
            fillComponent.getFillPolicy(),
            fillComponent.getFillValue(),
            fillComponent.getTimeDurationThreshold()));
  }

  private void analyzeDataPartition(
      Analysis analysis,
      QueryStatement queryStatement,
      ISchemaTree schemaTree,
      MPPQueryContext context) {
    Set<IDeviceID> deviceSet = new HashSet<>();
    if (queryStatement.isAlignByDevice()) {
      // conversion
      deviceSet = new HashSet<>(analysis.getOutputDeviceToQueriedDevicesMap().values());
    } else {
      for (Expression expression : analysis.getSourceExpressions()) {
        deviceSet.add(ExpressionAnalyzer.getDeviceNameInSourceExpression(expression));
      }
    }
    DataPartition dataPartition = fetchDataPartitionByDevices(deviceSet, schemaTree, context);
    analysis.setDataPartitionInfo(dataPartition);
  }

  private DataPartition fetchDataPartitionByDevices(
      Set<IDeviceID> deviceSet, ISchemaTree schemaTree, MPPQueryContext context) {
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
      long partitionFetchCost = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance()
          .recordPlanCost(TREE_TYPE, PARTITION_FETCHER, partitionFetchCost);
      context.setFetchPartitionCost(partitionFetchCost);
    }
  }

  /**
   * get TTimePartitionSlot list about this time filter
   *
   * @return List<TTimePartitionSlot>, if contains (-oo, XXX] time range, res.right.left = true; if
   *     contains [XX, +oo), res.right.right = true
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> getTimePartitionSlotList(
      Filter timeFilter, MPPQueryContext context) {
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

    boolean needLeftAll;
    boolean needRightAll;
    long endTime;
    TTimePartitionSlot timePartitionSlot;
    int index = 0;
    int size = timeRangeList.size();

    if (timeRangeList.get(0).getMin() == Long.MIN_VALUE) {
      needLeftAll = true;
      endTime = TimePartitionUtils.getTimePartitionUpperBound(timeRangeList.get(0).getMax());
      timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(timeRangeList.get(0).getMax());
    } else {
      endTime = TimePartitionUtils.getTimePartitionUpperBound(timeRangeList.get(0).getMin());
      timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(timeRangeList.get(0).getMin());
      needLeftAll = false;
    }

    if (timeRangeList.get(size - 1).getMax() == Long.MAX_VALUE) {
      needRightAll = true;
      size--;
    } else {
      needRightAll = false;
    }

    List<TTimePartitionSlot> result = new ArrayList<>();
    TimeRange currentTimeRange = timeRangeList.get(index);
    reserveMemoryForTimePartitionSlot(
        currentTimeRange.getMax(), currentTimeRange.getMin(), context);
    while (index < size) {
      long curLeft = timeRangeList.get(index).getMin();
      long curRight = timeRangeList.get(index).getMax();
      if (curLeft >= endTime) {
        result.add(timePartitionSlot);
        // next init
        endTime = TimePartitionUtils.getTimePartitionUpperBound(curLeft);
        timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(curLeft);
      } else if (curRight >= endTime) {
        result.add(timePartitionSlot);
        // next init
        timePartitionSlot = new TTimePartitionSlot(endTime);
        endTime = endTime + TimePartitionUtils.getTimePartitionInterval();
      } else {
        index++;
        if (index < size) {
          currentTimeRange = timeRangeList.get(index);
          reserveMemoryForTimePartitionSlot(
              currentTimeRange.getMax(), currentTimeRange.getMin(), context);
        }
      }
    }
    result.add(timePartitionSlot);

    if (needRightAll) {
      TTimePartitionSlot lastTimePartitionSlot =
          TimePartitionUtils.getTimePartitionSlot(
              timeRangeList.get(timeRangeList.size() - 1).getMin());
      if (lastTimePartitionSlot.startTime != timePartitionSlot.startTime) {
        result.add(lastTimePartitionSlot);
      }
    }
    return new Pair<>(result, new Pair<>(needLeftAll, needRightAll));
  }

  private static void reserveMemoryForTimePartitionSlot(
      long maxTime, long minTime, MPPQueryContext context) {
    if (maxTime == Long.MAX_VALUE || minTime == Long.MIN_VALUE) {
      return;
    }
    long size = TimePartitionUtils.getEstimateTimePartitionSize(minTime, maxTime);
    context.reserveMemoryForFrontEnd(
        RamUsageEstimator.shallowSizeOfInstance(TTimePartitionSlot.class) * size);
  }

  private void analyzeInto(
      Analysis analysis,
      QueryStatement queryStatement,
      List<PartialPath> deviceSet,
      List<Pair<Expression, String>> outputExpressions,
      MPPQueryContext context) {
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
    PathPatternTree targetPathTree = new PathPatternTree();
    IntoComponent.IntoDeviceMeasurementIterator intoDeviceMeasurementIterator =
        intoComponent.getIntoDeviceMeasurementIterator();
    for (PartialPath sourceDevice : sourceDevices) {
      PartialPath deviceTemplate = intoDeviceMeasurementIterator.getDeviceTemplate();
      boolean isAlignedDevice = intoDeviceMeasurementIterator.isAlignedDevice();
      PartialPath targetDevice = constructTargetDevice(sourceDevice, deviceTemplate);
      deviceViewIntoPathDescriptor.specifyDeviceAlignment(targetDevice.toString(), isAlignedDevice);

      for (Pair<Expression, String> pair : outputExpressions) {
        Expression sourceColumn = pair.left;
        String measurementTemplate = intoDeviceMeasurementIterator.getMeasurementTemplate();
        String targetMeasurement;
        if (sourceColumn instanceof TimeSeriesOperand) {
          targetMeasurement =
              constructTargetMeasurement(
                  sourceDevice.concatAsMeasurementPath(
                      pair.right == null ? sourceColumn.getExpressionString() : pair.right),
                  measurementTemplate);
        } else {
          targetMeasurement = parseNodeString(measurementTemplate);
        }
        deviceViewIntoPathDescriptor.specifyTargetDeviceMeasurement(
            sourceDevice, targetDevice, sourceColumn.getExpressionString(), targetMeasurement);

        targetPathTree.appendFullPath(targetDevice, targetMeasurement);
        deviceViewIntoPathDescriptor.recordSourceColumnDataType(
            sourceColumn.getExpressionString(), analysis.getType(sourceColumn));

        intoDeviceMeasurementIterator.nextMeasurement();
      }

      intoDeviceMeasurementIterator.nextDevice();
    }
    deviceViewIntoPathDescriptor.validate();

    // fetch schema of target paths
    long startTime = System.nanoTime();
    ISchemaTree targetSchemaTree = schemaFetcher.fetchSchema(targetPathTree, true, context);
    QueryPlanCostMetricSet.getInstance()
        .recordPlanCost(TREE_TYPE, SCHEMA_FETCHER, System.nanoTime() - startTime);
    deviceViewIntoPathDescriptor.bindType(targetSchemaTree);

    analysis.setDeviceViewIntoPathDescriptor(deviceViewIntoPathDescriptor);
  }

  private void analyzeInto(
      Analysis analysis,
      QueryStatement queryStatement,
      List<Pair<Expression, String>> outputExpressions,
      MPPQueryContext context) {
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
    PathPatternTree targetPathTree = new PathPatternTree();
    IntoComponent.IntoPathIterator intoPathIterator = intoComponent.getIntoPathIterator();
    for (Pair<Expression, String> pair : outputExpressions) {
      Expression sourceExpression = pair.left;
      // if it's really view path, it should start with root.
      // otherwise it should just be an alias
      String viewPath = pair.right;
      PartialPath deviceTemplate = intoPathIterator.getDeviceTemplate();
      String measurementTemplate = intoPathIterator.getMeasurementTemplate();
      boolean isAlignedDevice = intoPathIterator.isAlignedDevice();

      PartialPath sourcePath;
      String sourceColumn = sourceExpression.getExpressionString();
      PartialPath targetPath;
      if (sourceExpression instanceof TimeSeriesOperand) {
        if (viewPath != null) {
          try {
            // if it's really view path, it should start with root.
            if (viewPath.startsWith(ROOT_DOT)) {
              sourcePath = new MeasurementPath(viewPath);
            } else {
              // otherwise it should just be an alias
              sourcePath = new PartialPath(viewPath);
            }
          } catch (IllegalPathException e) {
            throw new SemanticException(
                String.format(
                    "View path %s of source column %s is illegal path", viewPath, sourceColumn));
          }
        } else {
          sourcePath = ((TimeSeriesOperand) sourceExpression).getPath();
        }
        targetPath = constructTargetPath(sourcePath, deviceTemplate, measurementTemplate);
      } else {
        targetPath = constructTargetPathWithoutPlaceHolder(deviceTemplate, measurementTemplate);
      }
      intoPathDescriptor.specifyTargetPath(sourceColumn, viewPath, targetPath);
      intoPathDescriptor.specifyDeviceAlignment(
          targetPath.getDevicePath().toString(), isAlignedDevice);

      targetPathTree.appendFullPath(targetPath);
      intoPathDescriptor.recordSourceColumnDataType(
          sourceColumn, analysis.getType(sourceExpression));

      intoPathIterator.next();
    }
    intoPathDescriptor.validate();

    // fetch schema of target paths
    long startTime = System.nanoTime();
    ISchemaTree targetSchemaTree = schemaFetcher.fetchSchema(targetPathTree, true, context);
    updateSchemaTreeByViews(analysis, targetSchemaTree, context);
    QueryPlanCostMetricSet.getInstance()
        .recordPlanCost(TREE_TYPE, SCHEMA_FETCHER, System.nanoTime() - startTime);
    intoPathDescriptor.bindType(targetSchemaTree);

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

  private void checkAliasUniqueness(String alias, Set<String> aliasSet) {
    if (alias != null) {
      if (aliasSet.contains(alias)) {
        throw new SemanticException(
            String.format("alias '%s' can only be matched with one time series", alias));
      }
      aliasSet.add(alias);
    }
  }

  private void checkAliasUniqueness(
      String alias,
      Map<Expression, Map<IDeviceID, Expression>> measurementToDeviceSelectExpressions) {
    if (alias != null && measurementToDeviceSelectExpressions.keySet().size() > 1) {
      throw new SemanticException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
  }

  @Override
  public Analysis visitInsert(InsertStatement insertStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
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
      insertRowStatement.setValues(insertStatement.getValuesList().get(0));
      insertRowStatement.setNeedInferType(true);
      insertRowStatement.setAligned(insertStatement.isAligned());
      return insertRowStatement.accept(this, context);
    } else {
      // construct insert rows statement
      // construct insert statement
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement =
          new InsertRowsOfOneDeviceStatement();
      if (!checkSorted(timeArray)) {
        Integer[] index = new Integer[timeArray.length];
        for (int i = 0; i < index.length; i++) {
          index[i] = i;
        }
        Arrays.sort(index, Comparator.comparingLong(o -> timeArray[o]));
        Arrays.sort(timeArray, 0, timeArray.length);
        insertStatement.setValuesList(
            Arrays.stream(index)
                .map(insertStatement.getValuesList()::get)
                .collect(Collectors.toList()));
      }
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
        statement.setValues(insertStatement.getValuesList().get(i));
        statement.setAligned(insertStatement.isAligned());
        statement.setNeedInferType(true);
        insertRowStatementList.add(statement);
      }
      insertRowsOfOneDeviceStatement.setInsertRowStatementList(insertRowStatementList);
      return insertRowsOfOneDeviceStatement.accept(this, context);
    }
  }

  private boolean checkSorted(long[] times) {
    for (int i = 1; i < times.length; i++) {
      if (times[i] < times[i - 1]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Analysis visitCreateTimeseries(
      CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    if (createTimeSeriesStatement.getPath().getNodeLength() < 3) {
      throw new SemanticException(
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
    analysis.setRealStatement(createTimeSeriesStatement);

    checkIsTableCompatible(createTimeSeriesStatement.getPath(), context, true);
    checkIsTemplateCompatible(
        createTimeSeriesStatement.getPath(), createTimeSeriesStatement.getAlias(), context, true);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(createTimeSeriesStatement.getPath());
    SchemaPartition schemaPartitionInfo =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  private void checkIsTemplateCompatible(
      final PartialPath timeSeriesPath,
      final String alias,
      final MPPQueryContext context,
      final boolean takeLock) {
    if (takeLock) {
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TEMPLATE);
    }
    final Pair<Template, PartialPath> templateInfo =
        schemaFetcher.checkTemplateSetAndPreSetInfo(timeSeriesPath, alias);
    if (templateInfo != null) {
      throw new SemanticException(
          new TemplateIncompatibleException(
              timeSeriesPath.getFullPath(), templateInfo.left.getName(), templateInfo.right));
    }
  }

  private void checkIsTemplateCompatible(
      final PartialPath devicePath,
      final List<String> measurements,
      final List<String> aliasList,
      final MPPQueryContext context,
      final boolean takeLock) {
    if (takeLock) {
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TEMPLATE);
    }
    for (int i = 0; i < measurements.size(); i++) {
      final Pair<Template, PartialPath> templateInfo =
          schemaFetcher.checkTemplateSetAndPreSetInfo(
              devicePath.concatAsMeasurementPath(measurements.get(i)),
              aliasList == null ? null : aliasList.get(i));
      if (templateInfo != null) {
        throw new SemanticException(
            new TemplateIncompatibleException(
                devicePath.getFullPath() + measurements,
                templateInfo.left.getName(),
                templateInfo.right));
      }
    }
  }

  private void checkIsTableCompatible(
      final PartialPath timeSeriesPath, final MPPQueryContext context, final boolean takeLock) {
    if (takeLock) {
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TABLE);
    }
    final Pair<String, String> tableInfo =
        DataNodeTableCache.getInstance().checkTableCreateAndPreCreateOnGivenPath(timeSeriesPath);
    if (tableInfo != null) {
      throw new SemanticException(new TableAlreadyExistsException(tableInfo.left, tableInfo.right));
    }
  }

  private void analyzeSchemaProps(final Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    final Map<String, String> caseChangeMap = new HashMap<>();
    for (final String key : props.keySet()) {
      caseChangeMap.put(key.toLowerCase(Locale.ROOT), key);
    }
    for (final Map.Entry<String, String> caseChangeEntry : caseChangeMap.entrySet()) {
      final String lowerCaseKey = caseChangeEntry.getKey();
      if (!ALLOWED_SCHEMA_PROPS.contains(lowerCaseKey)) {
        throw new SemanticException(
            new MetadataException(
                String.format("%s is not a legal prop.", caseChangeEntry.getValue())));
      }
      props.put(lowerCaseKey, props.remove(caseChangeEntry.getValue()));
    }
    if (props.containsKey(DEADBAND)) {
      props.put(LOSS, props.remove(DEADBAND));
    }
  }

  private void analyzeSchemaProps(final List<Map<String, String>> propsList) {
    if (propsList == null) {
      return;
    }
    for (final Map<String, String> props : propsList) {
      analyzeSchemaProps(props);
    }
  }

  @Override
  public Analysis visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    if (createAlignedTimeSeriesStatement.getDevicePath().getNodeLength() < 2) {
      throw new SemanticException(
          new IllegalPathException(createAlignedTimeSeriesStatement.getDevicePath().getFullPath()));
    }
    List<String> measurements = createAlignedTimeSeriesStatement.getMeasurements();
    Set<String> measurementsSet = new HashSet<>(measurements);
    if (measurementsSet.size() < measurements.size()) {
      throw new SemanticException(
          "Measurement under an aligned device is not allowed to have the same measurement name");
    }

    Analysis analysis = new Analysis();
    analysis.setRealStatement(createAlignedTimeSeriesStatement);

    checkIsTableCompatible(createAlignedTimeSeriesStatement.getDevicePath(), context, true);
    checkIsTemplateCompatible(
        createAlignedTimeSeriesStatement.getDevicePath(),
        createAlignedTimeSeriesStatement.getMeasurements(),
        createAlignedTimeSeriesStatement.getAliasList(),
        context,
        true);

    PathPatternTree pathPatternTree = new PathPatternTree();
    for (String measurement : createAlignedTimeSeriesStatement.getMeasurements()) {
      pathPatternTree.appendFullPath(createAlignedTimeSeriesStatement.getDevicePath(), measurement);
    }

    SchemaPartition schemaPartitionInfo;
    schemaPartitionInfo =
        partitionFetcher.getOrCreateSchemaPartition(
            pathPatternTree, context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  @Override
  public Analysis visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement,
      MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    Analysis analysis = new Analysis();
    analysis.setRealStatement(internalCreateTimeSeriesStatement);
    checkIsTableCompatible(internalCreateTimeSeriesStatement.getDevicePath(), context, true);
    checkIsTemplateCompatible(
        internalCreateTimeSeriesStatement.getDevicePath(),
        internalCreateTimeSeriesStatement.getMeasurements(),
        null,
        context,
        true);

    PathPatternTree pathPatternTree = new PathPatternTree();
    for (String measurement : internalCreateTimeSeriesStatement.getMeasurements()) {
      pathPatternTree.appendFullPath(
          internalCreateTimeSeriesStatement.getDevicePath(), measurement);
    }

    SchemaPartition schemaPartitionInfo;
    schemaPartitionInfo =
        partitionFetcher.getOrCreateSchemaPartition(
            pathPatternTree, context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  @Override
  public Analysis visitInternalCreateMultiTimeSeries(
      final InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement,
      final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    final Analysis analysis = new Analysis();
    analysis.setRealStatement(internalCreateMultiTimeSeriesStatement);

    final PathPatternTree pathPatternTree = new PathPatternTree();
    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TABLE);
    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TEMPLATE);
    for (final Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry :
        internalCreateMultiTimeSeriesStatement.getDeviceMap().entrySet()) {
      checkIsTableCompatible(entry.getKey(), context, false);
      checkIsTemplateCompatible(
          entry.getKey(), entry.getValue().right.getMeasurements(), null, context, false);
      pathPatternTree.appendFullPath(entry.getKey().concatNode(ONE_LEVEL_PATH_WILDCARD));
    }

    final SchemaPartition schemaPartitionInfo;
    schemaPartitionInfo =
        partitionFetcher.getOrCreateSchemaPartition(
            pathPatternTree, context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  @Override
  public Analysis visitCreateMultiTimeSeries(
      final CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement,
      final MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    final Analysis analysis = new Analysis();
    analysis.setRealStatement(createMultiTimeSeriesStatement);

    analyzeSchemaProps(createMultiTimeSeriesStatement.getPropsList());

    final List<MeasurementPath> timeseriesPathList = createMultiTimeSeriesStatement.getPaths();
    final List<String> aliasList = createMultiTimeSeriesStatement.getAliasList();

    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TABLE);
    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TEMPLATE);
    for (int i = 0; i < timeseriesPathList.size(); i++) {
      checkIsTableCompatible(timeseriesPathList.get(i), context, false);
      checkIsTemplateCompatible(
          timeseriesPathList.get(i), aliasList == null ? null : aliasList.get(i), context, false);
    }

    final PathPatternTree patternTree = new PathPatternTree();
    for (final PartialPath path : createMultiTimeSeriesStatement.getPaths()) {
      patternTree.appendFullPath(path);
    }
    final SchemaPartition schemaPartitionInfo =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    return analysis;
  }

  @Override
  public Analysis visitAlterTimeSeries(
      AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setRealStatement(alterTimeSeriesStatement);

    Pair<Template, PartialPath> templateInfo =
        schemaFetcher.checkTemplateSetAndPreSetInfo(
            alterTimeSeriesStatement.getPath(), alterTimeSeriesStatement.getAlias());
    if (templateInfo != null) {
      throw new RuntimeException(
          new TemplateIncompatibleException(
              String.format(
                  "Cannot alter template timeseries [%s] since device template [%s] already set on path [%s].",
                  alterTimeSeriesStatement.getPath().getFullPath(),
                  templateInfo.left.getName(),
                  templateInfo.right)));
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
    Analysis analysis = new Analysis();
    AnalyzeUtils.analyzeInsert(
        context,
        insertTabletStatement,
        () -> SchemaValidator.validate(schemaFetcher, insertTabletStatement, context),
        partitionFetcher::getOrCreateDataPartition,
        AnalyzeUtils::computeTreeDataPartitionParams,
        analysis,
        true);
    return analysis;
  }

  @Override
  public Analysis visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    insertRowStatement.semanticCheck();
    Analysis analysis = new Analysis();
    validateSchema(
        analysis,
        insertRowStatement,
        () -> SchemaValidator.validate(schemaFetcher, insertRowStatement, context));
    InsertBaseStatement realInsertStatement = removeLogicalView(analysis, insertRowStatement);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }
    analysis.setRealStatement(realInsertStatement);

    if (realInsertStatement instanceof InsertRowStatement) {
      InsertRowStatement realInsertRowStatement = (InsertRowStatement) realInsertStatement;
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDeviceID(
          realInsertRowStatement.getDevicePath().getIDeviceIDAsFullDevice());
      dataPartitionQueryParam.setTimePartitionSlotList(
          Collections.singletonList(realInsertRowStatement.getTimePartitionSlot()));

      AnalyzeUtils.analyzeDataPartition(
          analysis,
          Collections.singletonList(dataPartitionQueryParam),
          context.getSession().getUserName(),
          partitionFetcher::getOrCreateDataPartition);
    } else {
      AnalyzeUtils.analyzeDataPartition(
          analysis,
          AnalyzeUtils.computeTreeDataPartitionParams(realInsertStatement, context),
          context.getSession().getUserName(),
          partitionFetcher::getOrCreateDataPartition);
    }
    return analysis;
  }

  @Override
  public Analysis visitInsertRows(
      InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    insertRowsStatement.semanticCheck();
    Analysis analysis = new Analysis();
    validateSchema(
        analysis,
        insertRowsStatement,
        () -> SchemaValidator.validate(schemaFetcher, insertRowsStatement, context));
    InsertRowsStatement realInsertRowsStatement =
        (InsertRowsStatement) removeLogicalView(analysis, insertRowsStatement);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }
    analysis.setRealStatement(realInsertRowsStatement);

    AnalyzeUtils.analyzeDataPartition(
        analysis,
        AnalyzeUtils.computeTreeDataPartitionParams(realInsertRowsStatement, context),
        context.getSession().getUserName(),
        partitionFetcher::getOrCreateDataPartition);
    return analysis;
  }

  @Override
  public Analysis visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    insertMultiTabletsStatement.semanticCheck();
    Analysis analysis = new Analysis();
    validateSchema(
        analysis,
        insertMultiTabletsStatement,
        () -> SchemaValidator.validate(schemaFetcher, insertMultiTabletsStatement, context));
    InsertMultiTabletsStatement realStatement =
        (InsertMultiTabletsStatement) removeLogicalView(analysis, insertMultiTabletsStatement);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }
    analysis.setRealStatement(realStatement);

    AnalyzeUtils.analyzeDataPartition(
        analysis,
        AnalyzeUtils.computeTreeDataPartitionParams(realStatement, context),
        context.getSession().getUserName(),
        partitionFetcher::getOrCreateDataPartition);
    return analysis;
  }

  @Override
  public Analysis visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    insertRowsOfOneDeviceStatement.semanticCheck();
    Analysis analysis = new Analysis();
    validateSchema(
        analysis,
        insertRowsOfOneDeviceStatement,
        () -> SchemaValidator.validate(schemaFetcher, insertRowsOfOneDeviceStatement, context));
    InsertBaseStatement realInsertStatement =
        removeLogicalView(analysis, insertRowsOfOneDeviceStatement);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }
    analysis.setRealStatement(realInsertStatement);

    if (realInsertStatement instanceof InsertRowsOfOneDeviceStatement) {
      InsertRowsOfOneDeviceStatement realStatement =
          (InsertRowsOfOneDeviceStatement) realInsertStatement;
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDeviceID(realStatement.getDevicePath().getIDeviceIDAsFullDevice());
      dataPartitionQueryParam.setTimePartitionSlotList(realStatement.getTimePartitionSlots());

      AnalyzeUtils.analyzeDataPartition(
          analysis,
          Collections.singletonList(dataPartitionQueryParam),
          context.getSession().getUserName(),
          partitionFetcher::getOrCreateDataPartition);
    } else {
      AnalyzeUtils.analyzeDataPartition(
          analysis,
          AnalyzeUtils.computeTreeDataPartitionParams(realInsertStatement, context),
          context.getSession().getUserName(),
          partitionFetcher::getOrCreateDataPartition);
    }
    return analysis;
  }

  @Override
  public Analysis visitPipeEnrichedStatement(
      PipeEnrichedStatement pipeEnrichedStatement, MPPQueryContext context) {
    Analysis analysis = pipeEnrichedStatement.getInnerStatement().accept(this, context);
    analysis.setDatabaseName(context.getDatabaseName().orElse(null));

    // statement may be changed because of logical view
    pipeEnrichedStatement.setInnerStatement(analysis.getTreeStatement());
    analysis.setRealStatement(pipeEnrichedStatement);
    return analysis;
  }

  @Override
  public Analysis visitLoadFile(LoadTsFileStatement loadTsFileStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);

    final long startTime = System.nanoTime();
    try (final LoadTsFileAnalyzer loadTsFileAnalyzer = getAnalyzer(loadTsFileStatement, context)) {
      return (Analysis) loadTsFileAnalyzer.analyzeFileByFile(new Analysis());
    } catch (final Exception e) {
      final String exceptionMessage =
          String.format(
              "Failed to execute load tsfile statement %s. Detail: %s",
              loadTsFileStatement,
              e.getMessage() == null ? e.getClass().getName() : e.getMessage());
      logger.warn(exceptionMessage, e);
      final Analysis analysis = new Analysis();
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
      return analysis;
    } finally {
      LoadTsFileCostMetricsSet.getInstance()
          .recordPhaseTimeCost(ANALYSIS, System.nanoTime() - startTime);
    }
  }

  private LoadTsFileAnalyzer getAnalyzer(
      LoadTsFileStatement loadTsFileStatement, MPPQueryContext context) {
    if (Objects.equals(loadTsFileStatement.getModel(), LoadTsFileConfigurator.MODEL_TREE_VALUE)) {
      // Load to tree-model
      return new LoadTsFileToTreeModelAnalyzer(loadTsFileStatement, context);
    } else {
      // Load to table-model
      if (Objects.nonNull(loadTsFileStatement.getDatabase())) {
        return new LoadTsFileToTableModelAnalyzer(
            loadTsFileStatement, LocalExecutionPlanner.getInstance().metadata, context);
      } else {
        throw new SemanticException(
            "Database name must be specified when loading data into the table model.");
      }
    }
  }

  private boolean analyzeTimeseriesRegionScan(
      WhereCondition timeCondition,
      PathPatternTree patternTree,
      Analysis analysis,
      MPPQueryContext context,
      PathPatternTree authorityScope)
      throws IllegalPathException {
    analyzeGlobalTimeConditionInShowMetaData(timeCondition, analysis);
    context.generateGlobalTimeFilter(analysis);
    patternTree.constructTree();
    ISchemaTree schemaTree =
        schemaFetcher.fetchRawSchemaInMeasurementLevel(patternTree, authorityScope, context);

    if (schemaTree.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return false;
    }
    removeLogicViewMeasurement(schemaTree);
    Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesContext =
        new HashMap<>();
    /**
     * Since we fetch raw time series schema without template(The template sequence will be treated
     * as a normal node, not a device+templateId. This means that all nodes are what we need.). We
     * can use ALL_MATCH_PATTERN to get result.
     */
    List<DeviceSchemaInfo> deviceSchemaInfoList = schemaTree.getMatchedDevices(ALL_MATCH_PATTERN);
    Set<IDeviceID> deviceSet = new HashSet<>();
    for (DeviceSchemaInfo deviceSchemaInfo : deviceSchemaInfoList) {
      boolean isAligned = deviceSchemaInfo.isAligned();
      PartialPath devicePath = deviceSchemaInfo.getDevicePath();
      deviceSet.add(devicePath.getIDeviceIDAsFullDevice());
      if (isAligned) {
        List<String> measurementList = new ArrayList<>();
        List<IMeasurementSchema> schemaList = new ArrayList<>();
        List<TimeseriesContext> timeseriesContextList = new ArrayList<>();
        for (IMeasurementSchemaInfo measurementSchemaInfo :
            deviceSchemaInfo.getMeasurementSchemaInfoList()) {
          schemaList.add(measurementSchemaInfo.getSchema());
          measurementList.add(measurementSchemaInfo.getName());
          timeseriesContextList.add(new TimeseriesContext(measurementSchemaInfo));
        }
        AlignedPath alignedPath =
            new AlignedPath(devicePath.getNodes(), measurementList, schemaList);
        deviceToTimeseriesContext
            .computeIfAbsent(devicePath, k -> new HashMap<>())
            .put(alignedPath, timeseriesContextList);
      } else {
        for (IMeasurementSchemaInfo measurementSchemaInfo :
            deviceSchemaInfo.getMeasurementSchemaInfoList()) {
          MeasurementPath measurementPath =
              new MeasurementPath(
                  devicePath.concatNode(measurementSchemaInfo.getName()).getNodes());
          deviceToTimeseriesContext
              .computeIfAbsent(devicePath, k -> new HashMap<>())
              .put(
                  measurementPath,
                  Collections.singletonList(new TimeseriesContext(measurementSchemaInfo)));
        }
      }
    }

    analysis.setDeviceToTimeseriesSchemas(deviceToTimeseriesContext);
    // fetch Data partition
    DataPartition dataPartition = fetchDataPartitionByDevices(deviceSet, schemaTree, context);
    analysis.setDataPartitionInfo(dataPartition);
    return true;
  }

  @Override
  public Analysis visitShowTimeSeries(
      ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showTimeSeriesStatement);

    if (showTimeSeriesStatement.getPathPattern().getNodeLength() <= 1) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowTimeSeriesHeader());
      return analysis;
    }

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(showTimeSeriesStatement.getPathPattern());

    if (showTimeSeriesStatement.hasTimeCondition()) {
      try {
        // If there is time condition in SHOW TIMESERIES, we need to scan the raw data
        boolean hasSchema =
            analyzeTimeseriesRegionScan(
                showTimeSeriesStatement.getTimeCondition(),
                patternTree,
                analysis,
                context,
                showTimeSeriesStatement.getAuthorityScope());
        if (!hasSchema) {
          analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowTimeSeriesHeader());
          return analysis;
        }
      } catch (IllegalPathException e) {
        throw new StatementAnalyzeException(e.getMessage());
      }

    } else {
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);

      Map<Integer, Template> templateMap =
          schemaFetcher.checkAllRelatedTemplate(showTimeSeriesStatement.getPathPattern());
      analysis.setRelatedTemplateInfo(templateMap);
    }

    if (showTimeSeriesStatement.isOrderByHeat()) {
      patternTree.constructTree();
      // request schema fetch API
      logger.debug("[StartFetchSchema]");
      ISchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree, true, context);
      updateSchemaTreeByViews(analysis, schemaTree, context);
      logger.debug("[EndFetchSchema]]");

      analyzeLastSource(
          analysis,
          Collections.singletonList(
              new TimeSeriesOperand(showTimeSeriesStatement.getPathPattern())),
          schemaTree,
          context);
      analyzeDataPartition(analysis, new QueryStatement(), schemaTree, context);
    }

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowTimeSeriesHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowStorageGroup(
      ShowDatabaseStatement showDatabaseStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showDatabaseStatement);
    analysis.setRespDatasetHeader(
        DatasetHeaderFactory.getShowStorageGroupHeader(showDatabaseStatement.isDetailed()));
    return analysis;
  }

  @Override
  public Analysis visitShowTTL(ShowTTLStatement showTTLStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showTTLStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowTTLHeader());
    return analysis;
  }

  private void analyzeGlobalTimeConditionInShowMetaData(
      WhereCondition timeCondition, Analysis analysis) {
    Expression predicate = timeCondition.getPredicate();
    Pair<Expression, Boolean> resultPair =
        PredicateUtils.extractGlobalTimePredicate(predicate, true, true);
    if (resultPair.right) {
      throw new SemanticException(
          "Value Filter can't exist in the condition of SHOW/COUNT clause, only time condition supported");
    }
    if (resultPair.left == null) {
      throw new SemanticException(
          "Time condition can't be empty in the condition of SHOW/COUNT clause");
    }
    Expression globalTimePredicate = resultPair.left;
    globalTimePredicate = PredicateUtils.predicateRemoveNot(globalTimePredicate);
    analysis.setGlobalTimePredicate(globalTimePredicate);
  }

  private void removeLogicViewMeasurement(ISchemaTree schemaTree) {
    if (!schemaTree.hasLogicalViewMeasurement()) {
      return;
    }
    schemaTree.removeLogicalView();
  }

  private void analyzeDeviceRegionScan(
      WhereCondition timeCondition,
      PartialPath pattern,
      PathPatternTree authorityScope,
      Analysis analysis,
      MPPQueryContext context) {
    // If there is time condition in SHOW DEVICES, we need to scan the raw data
    analyzeGlobalTimeConditionInShowMetaData(timeCondition, analysis);
    context.generateGlobalTimeFilter(analysis);
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(pattern);
    ISchemaTree schemaTree =
        schemaFetcher.fetchRawSchemaInDeviceLevel(patternTree, authorityScope, context);
    if (schemaTree.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return;
    }

    // fetch Data partition

    Map<PartialPath, DeviceContext> devicePathsToInfoMap =
        schemaTree.getMatchedDevices(pattern).stream()
            .collect(Collectors.toMap(DeviceSchemaInfo::getDevicePath, DeviceContext::new));
    analysis.setDevicePathToContextMap(devicePathsToInfoMap);
    DataPartition dataPartition =
        fetchDataPartitionByDevices(
            devicePathsToInfoMap.keySet().stream()
                .map(PartialPath::getIDeviceIDAsFullDevice)
                .collect(Collectors.toSet()),
            schemaTree,
            context);
    analysis.setDataPartitionInfo(dataPartition);
  }

  @Override
  public Analysis visitShowDevices(
      ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showDevicesStatement);

    if (showDevicesStatement.hasTimeCondition()) {
      analyzeDeviceRegionScan(
          showDevicesStatement.getTimeCondition(),
          showDevicesStatement.getPathPattern(),
          showDevicesStatement.getAuthorityScope(),
          analysis,
          context);
    } else {
      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(
          showDevicesStatement.getPathPattern().concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    }
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
    analysis.setRealStatement(showClusterStatement);
    if (showClusterStatement.isDetails()) {
      analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowClusterDetailsHeader());
    } else {
      analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowClusterHeader());
    }
    return analysis;
  }

  @Override
  public Analysis visitCountStorageGroup(
      CountDatabaseStatement countDatabaseStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(countDatabaseStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountStorageGroupHeader());
    return analysis;
  }

  @Override
  public Analysis visitSeriesSchemaFetch(
      SeriesSchemaFetchStatement seriesSchemaFetchStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(seriesSchemaFetchStatement);

    SchemaPartition schemaPartition =
        partitionFetcher.getSchemaPartition(seriesSchemaFetchStatement.getPatternTree());
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
    }

    return analysis;
  }

  @Override
  public Analysis visitDeviceSchemaFetch(
      DeviceSchemaFetchStatement deviceSchemaFetchStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(deviceSchemaFetchStatement);

    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : deviceSchemaFetchStatement.getPaths()) {
      patternTree.appendPathPattern(path.concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    }
    patternTree.constructTree();
    SchemaPartition schemaPartition = partitionFetcher.getSchemaPartition(patternTree);
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
    analysis.setRealStatement(countDevicesStatement);

    if (countDevicesStatement.hasTimeCondition()) {
      analyzeDeviceRegionScan(
          countDevicesStatement.getTimeCondition(),
          countDevicesStatement.getPathPattern(),
          countDevicesStatement.getAuthorityScope(),
          analysis,
          context);
    } else {
      PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendPathPattern(
          countDevicesStatement.getPathPattern().concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    }

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountDevicesHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountTimeSeries(
      CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(countTimeSeriesStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(countTimeSeriesStatement.getPathPattern());

    if (countTimeSeriesStatement.hasTimeCondition()) {
      try {
        boolean hasSchema =
            analyzeTimeseriesRegionScan(
                countTimeSeriesStatement.getTimeCondition(),
                patternTree,
                analysis,
                context,
                countTimeSeriesStatement.getAuthorityScope());
        if (!hasSchema) {
          analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountTimeSeriesHeader());
          return analysis;
        }
      } catch (IllegalPathException e) {
        throw new StatementAnalyzeException(e.getMessage());
      }
    } else {
      SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      Map<Integer, Template> templateMap =
          schemaFetcher.checkAllRelatedTemplate(countTimeSeriesStatement.getPathPattern());
      analysis.setRelatedTemplateInfo(templateMap);
    }
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountTimeSeriesHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountLevelTimeSeries(
      CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(countLevelTimeSeriesStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(countLevelTimeSeriesStatement.getPathPattern());
    SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);

    analysis.setSchemaPartitionInfo(schemaPartitionInfo);
    Map<Integer, Template> templateMap =
        schemaFetcher.checkAllRelatedTemplate(countLevelTimeSeriesStatement.getPathPattern());
    analysis.setRelatedTemplateInfo(templateMap);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getCountLevelTimeSeriesHeader());
    return analysis;
  }

  @Override
  public Analysis visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(countStatement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(countStatement.getPathPattern());
    SchemaNodeManagementPartition schemaNodeManagementPartition =
        partitionFetcher.getSchemaNodeManagementPartitionWithLevel(
            patternTree, countStatement.getAuthorityScope(), countStatement.getLevel());

    if (schemaNodeManagementPartition == null) {
      return analysis;
    }
    if (!schemaNodeManagementPartition.getMatchedNode().isEmpty()
        && schemaNodeManagementPartition.getSchemaPartition().getSchemaPartitionMap().isEmpty()) {
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
        showChildPathsStatement.getAuthorityScope(),
        DatasetHeaderFactory.getShowChildPathsHeader());
  }

  @Override
  public Analysis visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
    return visitSchemaNodeManagementPartition(
        showChildNodesStatement,
        showChildNodesStatement.getPartialPath(),
        showChildNodesStatement.getAuthorityScope(),
        DatasetHeaderFactory.getShowChildNodesHeader());
  }

  @Override
  public Analysis visitShowVersion(
      ShowVersionStatement showVersionStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showVersionStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowVersionHeader());
    analysis.setFinishQueryAfterAnalyze(true);
    return analysis;
  }

  private Analysis visitSchemaNodeManagementPartition(
      Statement statement, PartialPath path, PathPatternTree scope, DatasetHeader header) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(statement);

    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(path);
    SchemaNodeManagementPartition schemaNodeManagementPartition =
        partitionFetcher.getSchemaNodeManagementPartition(patternTree, scope);

    if (schemaNodeManagementPartition == null) {
      return analysis;
    }
    if (!schemaNodeManagementPartition.getMatchedNode().isEmpty()
        && schemaNodeManagementPartition.getSchemaPartition().getSchemaPartitionMap().isEmpty()) {
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
    analysis.setRealStatement(deleteDataStatement);

    PathPatternTree patternTree = new PathPatternTree();
    deleteDataStatement.getPathList().forEach(patternTree::appendPathPattern);

    ISchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree, true, context);
    Set<IDeviceID> deduplicatedDeviceIDs = new HashSet<>();

    if (schemaTree.hasLogicalViewMeasurement()) {
      updateSchemaTreeByViews(analysis, schemaTree, context);

      Set<MeasurementPath> deletePatternSet = new HashSet<>(deleteDataStatement.getPathList());
      IMeasurementSchema measurementSchema;
      LogicalViewSchema logicalViewSchema;
      PartialPath sourcePathOfAliasSeries;
      for (MeasurementPath measurementPath :
          schemaTree.searchMeasurementPaths(ALL_MATCH_PATTERN).left) {
        measurementSchema = measurementPath.getMeasurementSchema();
        if (measurementSchema.isLogicalView()) {
          logicalViewSchema = (LogicalViewSchema) measurementSchema;
          if (logicalViewSchema.isWritable()) {
            sourcePathOfAliasSeries = logicalViewSchema.getSourcePathIfWritable();
            deletePatternSet.add(new MeasurementPath(sourcePathOfAliasSeries.getNodes()));
            deduplicatedDeviceIDs.add(sourcePathOfAliasSeries.getIDeviceID());
          }
          deletePatternSet.remove(measurementPath);
        } else {
          deduplicatedDeviceIDs.add(measurementPath.getIDeviceID());
        }
      }
      deleteDataStatement.setPathList(new ArrayList<>(deletePatternSet));
    } else {
      for (PartialPath devicePattern : patternTree.getAllDevicePaths()) {
        schemaTree
            .getMatchedDevices(devicePattern)
            .forEach(
                deviceSchemaInfo ->
                    deduplicatedDeviceIDs.add(
                        deviceSchemaInfo.getDevicePath().getIDeviceIDAsFullDevice()));
      }
    }
    analysis.setSchemaTree(schemaTree);

    Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();

    deduplicatedDeviceIDs.forEach(
        deviceID -> {
          DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
          queryParam.setDeviceID(deviceID);
          sgNameToQueryParamsMap
              .computeIfAbsent(schemaTree.getBelongedDatabase(deviceID), key -> new ArrayList<>())
              .add(queryParam);
        });

    DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
    analysis.setDataPartitionInfo(dataPartition);
    analysis.setFinishQueryAfterAnalyze(dataPartition.isEmpty());

    return analysis;
  }

  @Override
  public Analysis visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, MPPQueryContext context) {

    context.setQueryType(QueryType.WRITE);
    List<String> measurements = createTemplateStatement.getMeasurements();
    Set<String> measurementsSet = new HashSet<>(measurements);
    if (measurementsSet.size() < measurements.size()) {
      throw new SemanticException(
          "Measurement under template is not allowed to have the same measurement name");
    }
    Analysis analysis = new Analysis();
    analysis.setRealStatement(createTemplateStatement);
    return analysis;
  }

  @Override
  public Analysis visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement,
      MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showNodesInSchemaTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowNodesInSchemaTemplateHeader());
    return analysis;
  }

  @Override
  public Analysis visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showSchemaTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowSchemaTemplateHeader());
    return analysis;
  }

  @Override
  public Analysis visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setRealStatement(setSchemaTemplateStatement);
    return analysis;
  }

  @Override
  public Analysis visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showPathSetTemplateStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowPathSetTemplateHeader());
    return analysis;
  }

  @Override
  public Analysis visitActivateTemplate(
      ActivateTemplateStatement activateTemplateStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setRealStatement(activateTemplateStatement);

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
    SchemaPartition partition =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());

    analysis.setSchemaPartitionInfo(partition);

    return analysis;
  }

  @Override
  public Analysis visitBatchActivateTemplate(
      BatchActivateTemplateStatement batchActivateTemplateStatement, MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setRealStatement(batchActivateTemplateStatement);

    Map<PartialPath, Pair<Template, PartialPath>> deviceTemplateSetInfoMap =
        new HashMap<>(batchActivateTemplateStatement.getDevicePathList().size());
    for (PartialPath devicePath : batchActivateTemplateStatement.getDevicePathList()) {
      Pair<Template, PartialPath> templateSetInfo = schemaFetcher.checkTemplateSetInfo(devicePath);
      if (templateSetInfo == null) {
        throw new StatementAnalyzeException(
            new MetadataException(
                String.format(
                    "Path [%s] has not been set any template.", devicePath.getFullPath())));
      }
      deviceTemplateSetInfoMap.put(devicePath, templateSetInfo);
    }
    analysis.setDeviceTemplateSetInfoMap(deviceTemplateSetInfoMap);

    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath devicePath : batchActivateTemplateStatement.getDevicePathList()) {
      // the devicePath is a path without wildcard
      patternTree.appendFullPath(devicePath.concatAsMeasurementPath(ONE_LEVEL_PATH_WILDCARD));
    }
    SchemaPartition partition =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());

    analysis.setSchemaPartitionInfo(partition);

    return analysis;
  }

  @Override
  public Analysis visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement internalBatchActivateTemplateStatement,
      MPPQueryContext context) {
    context.setQueryType(QueryType.WRITE);
    Analysis analysis = new Analysis();
    analysis.setRealStatement(internalBatchActivateTemplateStatement);

    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath activatePath :
        internalBatchActivateTemplateStatement.getDeviceMap().keySet()) {
      // the devicePath is a path without wildcard
      patternTree.appendFullPath(activatePath.concatNode(ONE_LEVEL_PATH_WILDCARD));
    }
    SchemaPartition partition =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());

    analysis.setSchemaPartitionInfo(partition);

    return analysis;
  }

  @Override
  public Analysis visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showPathsUsingTemplateStatement);
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

    SchemaPartition partition = partitionFetcher.getSchemaPartition(patternTree);
    analysis.setSchemaPartitionInfo(partition);
    if (partition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    return analysis;
  }

  @Override
  public Analysis visitShowQueries(
      ShowQueriesStatement showQueriesStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showQueriesStatement);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowQueriesHeader());
    analysis.setVirtualSource(true);

    List<TDataNodeLocation> allRunningDataNodeLocations = getRunningDataNodeLocations();
    if (allRunningDataNodeLocations.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze(true);
    }
    // TODO Constant folding optimization for Where Predicate after True/False Constant introduced
    if (allRunningDataNodeLocations.isEmpty()) {
      throw new StatementAnalyzeException("no Running DataNodes");
    }
    analysis.setRunningDataNodeLocations(allRunningDataNodeLocations);

    Set<Expression> sourceExpressions = new HashSet<>();
    for (ColumnHeader columnHeader : analysis.getRespDatasetHeader().getColumnHeaders()) {
      sourceExpressions.add(
          TimeSeriesOperand.constructColumnHeaderExpression(
              columnHeader.getColumnName(), columnHeader.getColumnType()));
    }
    analysis.setSourceExpressions(sourceExpressions);
    sourceExpressions.forEach(expression -> analyzeExpressionType(analysis, expression));

    analyzeWhere(analysis, showQueriesStatement);

    analysis.setMergeOrderParameter(new OrderByParameter(showQueriesStatement.getSortItemList()));

    return analysis;
  }

  private List<TDataNodeLocation> getRunningDataNodeLocations() {
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetDataNodeLocationsResp showDataNodesResp = client.getRunningDataNodeLocations();
      if (showDataNodesResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new StatementAnalyzeException(
            "An error occurred when executing getRunningDataNodeLocations():"
                + showDataNodesResp.getStatus().getMessage());
      }
      return showDataNodesResp.getDataNodeLocationList();
    } catch (ClientManagerException | TException e) {
      throw new StatementAnalyzeException(
          "An error occurred when executing getRunningDataNodeLocations():" + e.getMessage());
    }
  }

  private void analyzeWhere(Analysis analysis, ShowQueriesStatement showQueriesStatement) {
    WhereCondition whereCondition = showQueriesStatement.getWhereCondition();
    if (whereCondition == null) {
      return;
    }

    Expression whereExpression =
        ExpressionAnalyzer.bindTypeForTimeSeriesOperand(
            whereCondition.getPredicate(), ColumnHeaderConstant.showQueriesColumnHeaders);

    TSDataType outputType = analyzeExpressionType(analysis, whereExpression);
    if (outputType != TSDataType.BOOLEAN) {
      throw new SemanticException(String.format(WHERE_WRONG_TYPE_ERROR_MSG, outputType));
    }

    analysis.setWhereExpression(whereExpression);
  }

  // Region view

  // Create Logical View
  @Override
  public Analysis visitCreateLogicalView(
      CreateLogicalViewStatement createLogicalViewStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    context.setQueryType(QueryType.WRITE);
    analysis.setRealStatement(createLogicalViewStatement);

    if (createLogicalViewStatement.getViewExpressions() == null) {
      // Analyze query in statement
      QueryStatement queryStatement = createLogicalViewStatement.getQueryStatement();
      if (queryStatement != null) {
        Pair<List<Expression>, Analysis> queryAnalysisPair =
            this.analyzeQueryInLogicalViewStatement(analysis, queryStatement, context);
        if (queryAnalysisPair.right.isFinishQueryAfterAnalyze()) {
          return analysis;
        } else if (queryAnalysisPair.left != null) {
          try {
            createLogicalViewStatement.setSourceExpressions(queryAnalysisPair.left);
          } catch (UnsupportedViewException e) {
            analysis.setFinishQueryAfterAnalyze(true);
            analysis.setFailStatus(RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
            return analysis;
          }
        }
      }
      // Only check and use source paths when view expressions are not set because there
      // is no need to check source when renaming views and the check may not be satisfied
      // when the statement is generated by pipe

      // Use source and into item to generate target views
      // If expressions are filled the target paths must be filled likewise
      createLogicalViewStatement.parseIntoItemIfNecessary();

      checkSourcePathsInCreateLogicalView(analysis, createLogicalViewStatement);
      if (analysis.isFinishQueryAfterAnalyze()) {
        return analysis;
      }

      // Make sure there is no view in source
      List<Expression> sourceExpressionList = createLogicalViewStatement.getSourceExpressionList();
      checkViewsInSource(analysis, sourceExpressionList, context);
      if (analysis.isFinishQueryAfterAnalyze()) {
        return analysis;
      }
    }

    // Check target paths.
    checkTargetPathsInCreateLogicalView(analysis, createLogicalViewStatement, context);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }

    // Set schema partition info, this info will be used to split logical plan node.
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath thisFullPath : createLogicalViewStatement.getTargetPathList()) {
      patternTree.appendFullPath(thisFullPath);
    }
    SchemaPartition schemaPartitionInfo =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);

    return analysis;
  }

  private Pair<List<Expression>, Analysis> analyzeQueryInLogicalViewStatement(
      Analysis analysis, QueryStatement queryStatement, MPPQueryContext context) {
    Analysis queryAnalysis = this.visitQuery(queryStatement, context);
    analysis.setSchemaTree(queryAnalysis.getSchemaTree());
    // get all expression from resultColumns
    List<Pair<Expression, String>> outputExpressions = queryAnalysis.getOutputExpressions();
    if (queryAnalysis.isFailed()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(queryAnalysis.getFailStatus());
      return new Pair<>(null, analysis);
    }
    if (outputExpressions == null) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              "Columns in the query statement is empty. Please check your SQL."));
      return new Pair<>(null, analysis);
    }
    if (queryAnalysis.useLogicalView()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              "Can not create a view based on existing views. Check the query in your SQL."));
      return new Pair<>(null, analysis);
    }
    List<Expression> expressionList = new ArrayList<>();
    for (Pair<Expression, String> thisPair : outputExpressions) {
      expressionList.add(thisPair.left);
    }
    return new Pair<>(expressionList, analysis);
  }

  private void checkSourcePathsInCreateLogicalView(
      Analysis analysis, CreateLogicalViewStatement createLogicalViewStatement) {
    Pair<Boolean, String> checkResult =
        createLogicalViewStatement.checkSourcePathsIfNotUsingQueryStatement();
    if (Boolean.FALSE.equals(checkResult.left)) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.ILLEGAL_PATH.getStatusCode(),
              "The path " + checkResult.right + " is illegal."));
      return;
    }

    List<PartialPath> targetPathList = createLogicalViewStatement.getTargetPathList();
    if (createLogicalViewStatement.getSourceExpressionList().size() != targetPathList.size()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              String.format(
                  "The number of target paths (%d) and sources (%d) are miss matched! Please check your SQL.",
                  createLogicalViewStatement.getTargetPathList().size(),
                  createLogicalViewStatement.getSourceExpressionList().size())));
    }
  }

  private void checkViewsInSource(
      Analysis analysis, List<Expression> sourceExpressionList, MPPQueryContext context) {
    List<PartialPath> pathsNeedCheck = new ArrayList<>();
    for (Expression expression : sourceExpressionList) {
      if (expression instanceof TimeSeriesOperand) {
        pathsNeedCheck.add(((TimeSeriesOperand) expression).getPath());
      }
    }
    Pair<ISchemaTree, Integer> schemaOfNeedToCheck =
        fetchSchemaOfPathsAndCount(pathsNeedCheck, analysis, context);
    if (schemaOfNeedToCheck.right != pathsNeedCheck.size()) {
      // Some source paths is not exist, and could not fetch schema.
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              "Can not create a view based on non-exist time series."));
      return;
    }
    Pair<List<PartialPath>, PartialPath> viewInSourceCheckResult =
        findAllViewsInPaths(pathsNeedCheck, schemaOfNeedToCheck.left);
    if (viewInSourceCheckResult.right != null) {
      // Some source paths is not exist
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              "Path "
                  + viewInSourceCheckResult.right.toString()
                  + " does not exist! You can not create a view based on non-exist time series."));
      return;
    }
    if (!viewInSourceCheckResult.left.isEmpty()) {
      // Some source paths is logical view
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              "Can not create a view based on existing views."));
    }
  }

  /**
   * Compute how many paths exist, get the schema tree and the number of existed paths.
   *
   * @return a pair of ISchemaTree, and the number of exist paths.
   */
  private Pair<ISchemaTree, Integer> fetchSchemaOfPathsAndCount(
      List<PartialPath> pathList, Analysis analysis, MPPQueryContext context) {
    ISchemaTree schemaTree = analysis.getSchemaTree();
    if (schemaTree == null) {
      // source is not represented by query, thus has not done fetch schema.
      PathPatternTree pathPatternTree = new PathPatternTree();
      for (PartialPath path : pathList) {
        pathPatternTree.appendPathPattern(path);
      }
      schemaTree = this.schemaFetcher.fetchSchema(pathPatternTree, true, context);
    }

    // search each path, make sure they all exist.
    int numOfExistPaths = 0;
    for (PartialPath path : pathList) {
      Pair<List<MeasurementPath>, Integer> pathPair = schemaTree.searchMeasurementPaths(path);
      numOfExistPaths += !pathPair.left.isEmpty() ? 1 : 0;
    }
    return new Pair<>(schemaTree, numOfExistPaths);
  }

  /**
   * @param pathList the paths you want to check
   * @param schemaTree the given schema tree
   * @return if all paths you give can be found in schema tree, return a pair of view paths and
   *     null; else return view paths and the non-exist path.
   */
  private Pair<List<PartialPath>, PartialPath> findAllViewsInPaths(
      List<PartialPath> pathList, ISchemaTree schemaTree) {
    List<PartialPath> result = new ArrayList<>();
    for (PartialPath path : pathList) {
      Pair<List<MeasurementPath>, Integer> measurementPathList =
          schemaTree.searchMeasurementPaths(path);
      if (measurementPathList.left.isEmpty()) {
        return new Pair<>(result, path);
      }
      for (MeasurementPath measurementPath : measurementPathList.left) {
        if (measurementPath.getMeasurementSchema().isLogicalView()) {
          result.add(measurementPath);
        }
      }
    }
    return new Pair<>(result, null);
  }

  private void checkTargetPathsInCreateLogicalView(
      final Analysis analysis,
      final CreateLogicalViewStatement createLogicalViewStatement,
      final MPPQueryContext context) {
    final Pair<Boolean, String> checkResult = createLogicalViewStatement.checkTargetPaths();
    if (Boolean.FALSE.equals(checkResult.left)) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.ILLEGAL_PATH.getStatusCode(),
              "The path " + checkResult.right + " is illegal."));
      return;
    }
    // Make sure there are no redundant paths in targets. Note that redundant paths in source
    // are legal.
    final List<PartialPath> targetPathList = createLogicalViewStatement.getTargetPathList();
    final Set<String> targetStringSet = new HashSet<>();
    for (final PartialPath path : targetPathList) {
      final boolean repeatPathNotExist = targetStringSet.add(path.toString());
      if (!repeatPathNotExist) {
        analysis.setFinishQueryAfterAnalyze(true);
        analysis.setFailStatus(
            RpcUtils.getStatus(
                TSStatusCode.ILLEGAL_PATH.getStatusCode(),
                String.format("Path [%s] is redundant in target paths.", path)));
        return;
      }
    }
    // Make sure all paths are not under any templates
    try {
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TABLE);
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.TIMESERIES_VS_TEMPLATE);
      for (final PartialPath path : createLogicalViewStatement.getTargetPathList()) {
        checkIsTableCompatible(path, context, false);
        checkIsTemplateCompatible(path, null, context, false);
      }
    } catch (final Exception e) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
              "Can not create view under template."));
    }
  }

  @Override
  public Analysis visitShowLogicalView(
      final ShowLogicalViewStatement showLogicalViewStatement, final MPPQueryContext context) {
    context.setQueryType(QueryType.READ);
    final Analysis analysis = new Analysis();
    analysis.setRealStatement(showLogicalViewStatement);

    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(showLogicalViewStatement.getPathPattern());
    final SchemaPartition schemaPartitionInfo = partitionFetcher.getSchemaPartition(patternTree);
    analysis.setSchemaPartitionInfo(schemaPartitionInfo);

    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowLogicalViewHeader());
    return analysis;
  }

  // endregion view

  @Override
  public Analysis visitShowCurrentTimestamp(
      ShowCurrentTimestampStatement showCurrentTimestampStatement, MPPQueryContext context) {
    Analysis analysis = new Analysis();
    analysis.setRealStatement(showCurrentTimestampStatement);
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setRespDatasetHeader(DatasetHeaderFactory.getShowCurrentTimestampHeader());
    return analysis;
  }
}
