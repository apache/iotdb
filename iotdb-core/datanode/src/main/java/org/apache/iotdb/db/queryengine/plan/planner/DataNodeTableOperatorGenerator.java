/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.calc.execution.operator.process.LimitOperator;
import org.apache.iotdb.calc.execution.operator.process.OffsetOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.LastDescAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.calc.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.calc.plan.planner.TableOperatorGenerator;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TableDateBinTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TableSingleTimeWindowIterator;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.execution.operator.EmptyDataOperator;
import org.apache.iotdb.db.queryengine.execution.operator.ExplainAnalyzeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableIntoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.TableCopyToOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.InnerTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.TableLeftOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.queryengine.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaCountOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaQueryScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.DevicePredicateFilter;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.SchemaSourceFactory;
import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractAggTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.CteScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DefaultAggTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DeviceIteratorScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.LastQueryAggTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeAlignedDeviceViewAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeAlignedDeviceViewScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeNonAlignedDeviceViewAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeToTableViewAdaptorOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CteScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.NonAlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.idcolumn.FourOrHigherLevelDBExtractor;
import org.apache.tsfile.file.metadata.idcolumn.ThreeLevelDBExtractor;
import org.apache.tsfile.file.metadata.idcolumn.TwoLevelDBExtractor;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.calc.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.isTimeColumn;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.AVG;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.COUNT;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.EXTREME;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.FIRST_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.FIRST_BY_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.LAST_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.LAST_BY_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.MAX;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.MIN;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.SUM;
import static org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator.DELIMITER_BETWEEN_ID;
import static org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator.DOWNSTREAM_PLAN_NODE_ID;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaContentSupplierFactory.getSupplier;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.isFilterGtOrGe;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions.updateFilterUsingTTL;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class DataNodeTableOperatorGenerator
    extends TableOperatorGenerator<LocalExecutionPlanContext, Metadata>
    implements PlanVisitor<Operator, LocalExecutionPlanContext> {

  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  public DataNodeTableOperatorGenerator(Metadata metadata) {
    super(metadata);
  }

  @Override
  protected String getSortTmpDir(CommonOperatorContext operatorContext) {
    OperatorContext dataNodeOperatorContext = (OperatorContext) operatorContext;
    dataNodeOperatorContext.getDriverContext().setHaveTmpFile(true);
    dataNodeOperatorContext.getDriverContext().getFragmentInstanceContext().setMayHaveTmpFile(true);
    return IoTDBDescriptor.getInstance().getConfig().getSortTmpDir()
        + File.separator
        + dataNodeOperatorContext
            .getDriverContext()
            .getFragmentInstanceContext()
            .getId()
            .getFullId()
        + File.separator
        + dataNodeOperatorContext.getDriverContext().getPipelineId()
        + File.separator;
  }

  @Override
  protected OperatorContext addOperatorContext(
      LocalExecutionPlanContext context, PlanNodeId planNodeId, String operatorType) {
    return context
        .getDriverContext()
        .addOperatorContext(context.getNextOperatorId(), planNodeId, operatorType);
  }

  @Override
  public Operator visitCteScan(CteScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), CteScanOperator.class.getSimpleName());
    return new CteScanOperator(
        operatorContext,
        node.getPlanNodeId(),
        node.getDataStore(),
        context.getFragmentInstanceId().getQueryId());
  }

  @Override
  public Operator visitIdentitySink(IdentitySinkNode node, LocalExecutionPlanContext context) {
    context.addExchangeSumNum(1);
    OperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), IdentitySinkOperator.class.getSimpleName());
    String downStreamPlanNodeId =
        node.getDownStreamChannelLocationList().stream()
            .map(DownStreamChannelLocation::getRemotePlanNodeId)
            .collect(Collectors.joining(DELIMITER_BETWEEN_ID));
    if (!downStreamPlanNodeId.isEmpty()) {
      operatorContext.recordSpecifiedInfo(DOWNSTREAM_PLAN_NODE_ID, downStreamPlanNodeId);
    }
    checkArgument(
        MPP_DATA_EXCHANGE_MANAGER != null, "MPP_DATA_EXCHANGE_MANAGER should not be null");
    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
    ISinkHandle sinkHandle =
        MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
            node.getDownStreamChannelLocationList(),
            downStreamChannelIndex,
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localInstanceId.toThrift(),
            node.getPlanNodeId().getId(),
            context.getInstanceContext());

    if (node.getChildren().size() == 1) {
      Operator child = node.getChildren().get(0).accept(this, context);
      List<Operator> children = new ArrayList<>(1);
      children.add(child);
      sinkHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
      context.getDriverContext().setSink(sinkHandle);
      return new IdentitySinkOperator(
          operatorContext, children, downStreamChannelIndex, sinkHandle);
    } else {
      throw new IllegalStateException(
          "IdentitySinkNode should only have one child in table model.");
    }
  }

  @Override
  public Operator visitTableExchange(ExchangeNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), ExchangeOperator.class.getSimpleName());

    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    FragmentInstanceId remoteInstanceId = node.getUpstreamInstanceId();

    TEndPoint upstreamEndPoint = node.getUpstreamEndpoint();
    boolean isSameNode = isSameNode(upstreamEndPoint);
    ISourceHandle sourceHandle =
        isSameNode
            ? MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandleForFragment(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                node.getUpstreamPlanNodeId().getId(),
                remoteInstanceId.toThrift(),
                node.getIndexOfUpstreamSinkHandle(),
                context.getInstanceContext()::failed,
                context.getInstanceContext().isHighestPriority())
            : MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                node.getIndexOfUpstreamSinkHandle(),
                upstreamEndPoint,
                remoteInstanceId.toThrift(),
                context.getInstanceContext()::failed,
                context.getInstanceContext().isHighestPriority());
    if (!isSameNode) {
      context.addExchangeSumNum(1);
    }
    sourceHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    ExchangeOperator exchangeOperator =
        new ExchangeOperator(operatorContext, sourceHandle, node.getUpstreamPlanNodeId());
    context.addExchangeOperator(exchangeOperator);
    return exchangeOperator;
  }

  public static IDeviceID.TreeDeviceIdColumnValueExtractor createTreeDeviceIdColumnValueExtractor(
      String treeDBName) {
    try {
      PartialPath db = new PartialPath(treeDBName);
      int dbLevel = db.getNodes().length;
      // For the path of 'root.**', we can only get the root level in this place
      // In this case, we still need to support deviceId such as 'root.db'
      // The relevant deviceId must be two level db, but we can't get it now
      if (dbLevel == 1 || dbLevel == 2) {
        return new TwoLevelDBExtractor(treeDBName.length());
      } else if (dbLevel == 3) {
        return new ThreeLevelDBExtractor(treeDBName.length());
      } else if (dbLevel >= 4) {
        return new FourOrHigherLevelDBExtractor(dbLevel);
      } else {
        throw new IllegalArgumentException(
            "tree db name should at least be two level: " + treeDBName);
      }
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public Operator visitTreeNonAlignedDeviceViewScan(
      TreeNonAlignedDeviceViewScanNode node, LocalExecutionPlanContext context) {

    boolean containsFieldColumn = false;
    for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
      if (entry.getValue().getColumnCategory() == FIELD) {
        containsFieldColumn = true;
        break;
      }
    }
    TsTable tsTable =
        DataNodeTableCache.getInstance()
            .getTable(
                node.getQualifiedObjectName().getDatabaseName(),
                node.getQualifiedObjectName().getObjectName());
    if (!containsFieldColumn) {
      Map<Symbol, ColumnSchema> newAssignments = new LinkedHashMap<>(node.getAssignments());
      for (TsTableColumnSchema columnSchema : tsTable.getColumnList()) {
        if (columnSchema.getColumnCategory() == FIELD) {
          newAssignments.put(
              new Symbol(columnSchema.getColumnName()),
              new ColumnSchema(
                  columnSchema.getColumnName(),
                  TypeFactory.getType(columnSchema.getDataType()),
                  false,
                  columnSchema.getColumnCategory()));
          containsFieldColumn = true;
        }
      }
      node.setAssignments(newAssignments);
    }
    // For non-aligned series, scan cannot be performed when no field columns
    // can be obtained, so an empty result set is returned.
    if (!containsFieldColumn || node.getDeviceEntries().isEmpty()) {
      OperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), EmptyDataOperator.class.getSimpleName());
      return new EmptyDataOperator(operatorContext);
    }
    String treePrefixPath = DataNodeTreeViewSchemaUtils.getPrefixPath(tsTable);
    IDeviceID.TreeDeviceIdColumnValueExtractor extractor =
        createTreeDeviceIdColumnValueExtractor(treePrefixPath);
    long viewTTL = tsTable.getCachedTableTTL();

    DeviceIteratorScanOperator.TreeNonAlignedDeviceViewScanParameters parameter =
        constructTreeNonAlignedDeviceViewScanOperatorParameter(
            node,
            context,
            TreeNonAlignedDeviceViewScanNode.class.getSimpleName(),
            node.getMeasurementColumnNameMap(),
            extractor,
            viewTTL);

    DeviceIteratorScanOperator treeNonAlignedDeviceIteratorScanOperator =
        new DeviceIteratorScanOperator(
            parameter.context, parameter.deviceEntries, parameter.generator);
    addSource(
        treeNonAlignedDeviceIteratorScanOperator,
        context,
        node,
        parameter.measurementColumnNames,
        parameter.measurementSchemas,
        parameter.allSensors,
        TreeNonAlignedDeviceViewScanNode.class.getSimpleName());

    if (!parameter.generator.keepOffsetAndLimitOperatorAfterDeviceIterator()) {
      return treeNonAlignedDeviceIteratorScanOperator;
    }
    Operator operator = treeNonAlignedDeviceIteratorScanOperator;
    if (node.getPushDownOffset() > 0) {
      operator = new OffsetOperator(parameter.context, node.getPushDownOffset(), operator);
    }
    if (node.getPushDownLimit() > 0) {
      operator = new LimitOperator(parameter.context, node.getPushDownLimit(), operator);
    }
    return operator;
  }

  private DeviceIteratorScanOperator.TreeNonAlignedDeviceViewScanParameters
      constructTreeNonAlignedDeviceViewScanOperatorParameter(
          TreeNonAlignedDeviceViewScanNode node,
          LocalExecutionPlanContext context,
          String className,
          Map<String, String> fieldColumnsRenameMap,
          IDeviceID.TreeDeviceIdColumnValueExtractor extractor,
          long viewTTL) {
    if (node.isPushLimitToEachDevice() && node.getPushDownOffset() > 0) {
      throw new IllegalArgumentException(
          "PushDownOffset should not be set when isPushLimitToEachDevice is true.");
    }
    CommonTableScanOperatorParameters commonParameter =
        new CommonTableScanOperatorParameters(node, fieldColumnsRenameMap, true);
    List<IMeasurementSchema> measurementSchemas = commonParameter.measurementSchemas;
    List<Symbol> measurementSchemaIndex2Symbols = commonParameter.measurementSchemaIndex2Symbol;
    List<String> measurementColumnNames = commonParameter.measurementColumnNames;
    List<ColumnSchema> fullColumnSchemas = commonParameter.columnSchemas;
    List<Symbol> symbolInputs = commonParameter.symbolInputs;
    int[] columnsIndexArray = commonParameter.columnsIndexArray;

    boolean isSingleColumn = measurementSchemas.size() == 1;

    OperatorContext operatorContext = addOperatorContext(context, node.getPlanNodeId(), className);

    Set<String> allSensors = new HashSet<>(measurementColumnNames);

    DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator deviceChildOperatorTreeGenerator =
        new DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator() {

          private Operator operator;
          private List<SeriesScanOptions> seriesScanOptionsList;
          private List<Operator> seriesScanOperators;
          private FilterAndProjectOperator filterAndProjectOperator;
          private OffsetOperator reuseOffsetOperator;
          private LimitOperator reuseLimitOperator;
          private Operator startCloseInternalOperator;

          private List<Expression> cannotPushDownConjuncts;
          private boolean removeUpperOffsetAndLimitOperator;

          private final long INSTANCE_SIZE =
              RamUsageEstimator.shallowSizeOfInstance(this.getClass());

          @Override
          public long ramBytesUsed() {
            return INSTANCE_SIZE
                + (seriesScanOptionsList == null
                    ? 0L
                    : seriesScanOptionsList.stream()
                        .mapToLong(seriesScanOption -> seriesScanOption.ramBytesUsed())
                        .sum());
          }

          @Override
          public boolean keepOffsetAndLimitOperatorAfterDeviceIterator() {
            calculateSeriesScanOptionsList();
            return !removeUpperOffsetAndLimitOperator && !node.isPushLimitToEachDevice();
          }

          @Override
          public void generateCurrentDeviceOperatorTree(
              DeviceEntry deviceEntry, boolean needAdaptor) {
            calculateSeriesScanOptionsList();
            if (needAdaptor) {
              operator = constructTreeToTableViewAdaptorOperator(deviceEntry);
            } else {
              seriesScanOperators = new ArrayList<>(measurementSchemas.size());
              operator = constructAndJoinScanOperators(deviceEntry);
            }
            boolean needToPruneColumn =
                node.getAssignments().size() != node.getOutputSymbols().size();
            if (isSingleColumn) {
              operator = needToPruneColumn ? getFilterAndProjectOperator(operator) : operator;
              return;
            }
            if (!cannotPushDownConjuncts.isEmpty() || needToPruneColumn) {
              operator = getFilterAndProjectOperator(operator);
            }
            if (!node.isPushLimitToEachDevice() || removeUpperOffsetAndLimitOperator) {
              return;
            }
            if (node.getPushDownLimit() > 0) {
              operator = new LimitOperator(operatorContext, node.getPushDownLimit(), operator);
            }
          }

          private void calculateSeriesScanOptionsList() {
            if (seriesScanOptionsList != null) {
              return;
            }
            seriesScanOptionsList = new ArrayList<>(measurementSchemas.size());
            cannotPushDownConjuncts = new ArrayList<>();
            Map<Symbol, List<Expression>> pushDownConjunctsForEachMeasurement = new HashMap<>();
            if (node.getPushDownPredicate() != null) {
              List<Expression> conjuncts = IrUtils.extractConjuncts(node.getPushDownPredicate());
              for (Expression conjunct : conjuncts) {
                Set<Symbol> symbols = SymbolsExtractor.extractUnique(conjunct);
                boolean containsMultiDataSource = symbols.size() > 1;
                if (containsMultiDataSource) {
                  cannotPushDownConjuncts.add(conjunct);
                  continue;
                }
                Symbol symbol = symbols.iterator().next();
                pushDownConjunctsForEachMeasurement
                    .computeIfAbsent(symbol, k -> new ArrayList<>())
                    .add(conjunct);
              }
            }

            // Using getSeriesScanOptionsBuilder to create SeriesScanBuilder will cause multiple
            // calls to setTimeFilterForTableModel and generate a deeply nested Or filter.
            // Therefore, a separate setting is made here
            Filter timeFilter = null;
            if (node.getTimePredicate().isPresent()) {
              Expression timePredicate = node.getTimePredicate().get();
              timeFilter =
                  timePredicate.accept(
                      new ConvertPredicateToTimeFilterVisitor(
                          context.getZoneId(), TimestampPrecisionUtils.currPrecision),
                      null);
              context
                  .getDriverContext()
                  .getFragmentInstanceContext()
                  .setTimeFilterForTableModel(timeFilter);
            }

            boolean canPushDownLimit = cannotPushDownConjuncts.isEmpty();
            // only use full outer time join
            boolean canPushDownLimitToAllSeriesScanOptions =
                canPushDownLimit && pushDownConjunctsForEachMeasurement.isEmpty();
            // the left child of LeftOuterTimeJoinOperator is SeriesScanOperator
            boolean pushDownOffsetAndLimitToLeftChildSeriesScanOperator =
                canPushDownLimit && pushDownConjunctsForEachMeasurement.size() == 1;
            // the left child of LeftOuterTimeJoinOperator is InnerTimeJoinOperator
            boolean pushDownOffsetAndLimitAfterInnerJoinOperator =
                canPushDownLimit && pushDownConjunctsForEachMeasurement.size() > 1;
            removeUpperOffsetAndLimitOperator =
                pushDownOffsetAndLimitToLeftChildSeriesScanOperator
                    || pushDownOffsetAndLimitAfterInnerJoinOperator
                    || isSingleColumn;
            for (Symbol symbol : measurementSchemaIndex2Symbols) {
              List<Expression> pushDownPredicatesForCurrentMeasurement =
                  pushDownConjunctsForEachMeasurement.get(symbol);
              Expression pushDownPredicateForCurrentMeasurement =
                  isSingleColumn
                      ? node.getPushDownPredicate()
                      : (pushDownPredicatesForCurrentMeasurement == null
                          ? null
                          : IrUtils.combineConjuncts(pushDownPredicatesForCurrentMeasurement));
              SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
              // time filter may be stateful, so we need to copy it
              builder.withGlobalTimeFilter(timeFilter == null ? null : timeFilter.copy());
              builder.withIsTableViewForTreeModel(true).withAllSensors(allSensors);
              if (pushDownPredicateForCurrentMeasurement != null) {
                builder.withPushDownFilter(
                    convertPredicateToFilter(
                        pushDownPredicateForCurrentMeasurement,
                        Collections.singletonMap(symbol.getName(), 0),
                        commonParameter.columnSchemaMap,
                        commonParameter.timeColumnName,
                        context.getZoneId(),
                        TimestampPrecisionUtils.currPrecision));
              }
              if (isSingleColumn
                  || (pushDownOffsetAndLimitToLeftChildSeriesScanOperator
                      && pushDownPredicateForCurrentMeasurement != null)) {
                builder.withPushDownLimit(node.getPushDownLimit());
                builder.withPushLimitToEachDevice(node.isPushLimitToEachDevice());
              }

              // In the case of single column, both offset and limit are pushed down to the
              // SeriesScanOperator
              if (!isSingleColumn && canPushDownLimitToAllSeriesScanOptions) {
                builder.withPushDownLimit(node.getPushDownLimit() + node.getPushDownOffset());
              }
              if (isSingleColumn
                  || (pushDownOffsetAndLimitToLeftChildSeriesScanOperator
                      && pushDownPredicateForCurrentMeasurement != null)) {
                builder.withPushDownOffset(
                    node.isPushLimitToEachDevice() ? 0 : node.getPushDownOffset());
              }
              SeriesScanOptions options = builder.build();
              options.setTTLForTableView(viewTTL);
              seriesScanOptionsList.add(options);
            }
          }

          private Operator constructTreeToTableViewAdaptorOperator(DeviceEntry deviceEntry) {
            seriesScanOperators = new ArrayList<>(measurementSchemas.size());
            operator = constructAndJoinScanOperators(deviceEntry);
            return new TreeToTableViewAdaptorOperator(
                operatorContext,
                deviceEntry,
                columnsIndexArray,
                fullColumnSchemas,
                operator,
                extractor);
          }

          private Operator constructAndJoinScanOperators(DeviceEntry deviceEntry) {
            List<Operator> childrenWithPushDownPredicate = new ArrayList<>();
            List<TSDataType> innerJoinDataTypeList = new ArrayList<>();
            List<Operator> childrenWithoutPushDownPredicate = new ArrayList<>();
            List<TSDataType> fullOuterTimeJoinDataTypeList = new ArrayList<>();
            Map<InputLocation, Integer> leftOuterJoinColumnIndexMap = new HashMap<>();
            for (int i = 0; i < measurementSchemas.size(); i++) {
              IMeasurementSchema measurementSchema = measurementSchemas.get(i);
              NonAlignedFullPath path =
                  new NonAlignedFullPath(deviceEntry.getDeviceID(), measurementSchema);
              SeriesScanOptions seriesScanOptions = seriesScanOptionsList.get(i);
              Operator seriesScanOperator =
                  new SeriesScanOperator(
                      operatorContext,
                      node.getPlanNodeId(),
                      path,
                      node.getScanOrder(),
                      seriesScanOptions);
              seriesScanOperators.add(seriesScanOperator);
              if (seriesScanOptions.getPushDownFilter() != null) {
                childrenWithPushDownPredicate.add(seriesScanOperator);
                innerJoinDataTypeList.add(measurementSchema.getType());
                leftOuterJoinColumnIndexMap.put(
                    new InputLocation(0, childrenWithPushDownPredicate.size() - 1), i);
              } else {
                childrenWithoutPushDownPredicate.add(seriesScanOperator);
                fullOuterTimeJoinDataTypeList.add(measurementSchema.getType());
                leftOuterJoinColumnIndexMap.put(
                    new InputLocation(1, childrenWithoutPushDownPredicate.size() - 1), i);
              }
            }
            Operator leftChild =
                generateInnerTimeJoinOperator(childrenWithPushDownPredicate, innerJoinDataTypeList);
            Operator rightChild =
                generateFullOuterTimeJoinOperator(
                    childrenWithoutPushDownPredicate, fullOuterTimeJoinDataTypeList);
            return generateLeftOuterTimeJoinOperator(
                leftChild,
                rightChild,
                childrenWithPushDownPredicate.size(),
                leftOuterJoinColumnIndexMap,
                IMeasurementSchema.getDataTypeList(measurementSchemas));
          }

          private Operator generateInnerTimeJoinOperator(
              List<Operator> operators, List<TSDataType> dataTypes) {
            if (operators.isEmpty()) {
              return null;
            }
            if (operators.size() == 1) {
              return operators.get(0);
            }
            Map<InputLocation, Integer> outputColumnMap = new HashMap<>();
            for (int i = 0; i < operators.size(); i++) {
              outputColumnMap.put(new InputLocation(i, 0), i);
            }
            Operator currentOperator =
                new InnerTimeJoinOperator(
                    operatorContext,
                    operators,
                    dataTypes,
                    node.getScanOrder() == Ordering.ASC
                        ? new AscTimeComparator()
                        : new DescTimeComparator(),
                    outputColumnMap);
            boolean addOffsetAndLimitOperatorAfterLeftChild =
                operators.size() > 1 && cannotPushDownConjuncts.isEmpty();
            if (addOffsetAndLimitOperatorAfterLeftChild) {
              if (node.getPushDownOffset() > 0) {
                currentOperator = getReuseOffsetOperator(currentOperator);
              }
              if (node.getPushDownLimit() > 0) {
                currentOperator = getReuseLimitOperator(currentOperator);
              }
            }
            return currentOperator;
          }

          private Operator generateFullOuterTimeJoinOperator(
              List<Operator> operators, List<TSDataType> dataTypes) {
            if (operators.isEmpty()) {
              return null;
            }
            if (operators.size() == 1) {
              return operators.get(0);
            }
            List<ColumnMerger> columnMergers = new ArrayList<>(operators.size());
            for (int i = 0; i < operators.size(); i++) {
              columnMergers.add(
                  new SingleColumnMerger(
                      new InputLocation(i, 0),
                      node.getScanOrder() == Ordering.ASC
                          ? new AscTimeComparator()
                          : new DescTimeComparator()));
            }
            return new FullOuterTimeJoinOperator(
                operatorContext,
                operators,
                node.getScanOrder(),
                dataTypes,
                columnMergers,
                node.getScanOrder() == Ordering.ASC
                    ? new AscTimeComparator()
                    : new DescTimeComparator());
          }

          private Operator generateLeftOuterTimeJoinOperator(
              Operator left,
              Operator right,
              int leftColumnCount,
              Map<InputLocation, Integer> outputColumnMap,
              List<TSDataType> dataTypes) {
            if (left == null) {
              return right;
            } else if (right == null) {
              return left;
            } else {
              return new TableLeftOuterTimeJoinOperator(
                  operatorContext,
                  left,
                  right,
                  leftColumnCount,
                  outputColumnMap,
                  dataTypes,
                  node.getScanOrder() == Ordering.ASC
                      ? new AscTimeComparator()
                      : new DescTimeComparator());
            }
          }

          private Operator getReuseOffsetOperator(Operator child) {
            this.reuseOffsetOperator =
                reuseOffsetOperator == null
                    ? new OffsetOperator(operatorContext, node.getPushDownOffset(), child)
                    : new OffsetOperator(reuseOffsetOperator, child);
            return this.reuseOffsetOperator;
          }

          private Operator getReuseLimitOperator(Operator child) {
            this.reuseLimitOperator =
                reuseLimitOperator == null
                    ? new LimitOperator(operatorContext, node.getPushDownLimit(), child)
                    : new LimitOperator(reuseLimitOperator, child);
            return this.reuseLimitOperator;
          }

          private Operator getFilterAndProjectOperator(Operator childOperator) {
            startCloseInternalOperator = childOperator;
            if (filterAndProjectOperator != null) {
              return new FilterAndProjectOperator(filterAndProjectOperator, childOperator);
            }
            List<TSDataType> inputDataTypeList = new ArrayList<>(fullColumnSchemas.size());
            Map<Symbol, List<InputLocation>> symbolInputLocationMap =
                new HashMap<>(fullColumnSchemas.size());
            for (int i = 0; i < fullColumnSchemas.size(); i++) {
              ColumnSchema columnSchema = fullColumnSchemas.get(i);
              symbolInputLocationMap
                  .computeIfAbsent(
                      new Symbol(symbolInputs.get(i).getName()), key -> new ArrayList<>())
                  .add(new InputLocation(0, i));
              inputDataTypeList.add(getTSDataType(columnSchema.getType()));
            }
            Expression combinedCannotPushDownPredicates =
                cannotPushDownConjuncts.isEmpty()
                    ? null
                    : IrUtils.combineConjuncts(cannotPushDownConjuncts);
            filterAndProjectOperator =
                (FilterAndProjectOperator)
                    DataNodeTableOperatorGenerator.this.constructFilterAndProjectOperator(
                        Optional.ofNullable(combinedCannotPushDownPredicates),
                        childOperator,
                        node.getOutputSymbols().stream()
                            .map(Symbol::toSymbolReference)
                            .toArray(Expression[]::new),
                        inputDataTypeList,
                        symbolInputLocationMap,
                        node.getPlanNodeId(),
                        context);
            return filterAndProjectOperator;
          }

          @Override
          public Operator getCurrentDeviceRootOperator() {
            return operator;
          }

          @Override
          public List<Operator> getCurrentDeviceDataSourceOperators() {
            return seriesScanOperators;
          }

          @Override
          public Operator getCurrentDeviceStartCloseOperator() {
            return startCloseInternalOperator == null ? operator : startCloseInternalOperator;
          }
        };

    return new DeviceIteratorScanOperator.TreeNonAlignedDeviceViewScanParameters(
        allSensors,
        operatorContext,
        node.getDeviceEntries(),
        measurementColumnNames,
        measurementSchemas,
        deviceChildOperatorTreeGenerator);
  }

  private static class CommonTableScanOperatorParameters {

    List<Symbol> outputColumnNames;
    List<ColumnSchema> columnSchemas;
    List<Symbol> symbolInputs;
    int[] columnsIndexArray;
    Map<Symbol, ColumnSchema> columnSchemaMap;
    Map<Symbol, Integer> tagAndAttributeColumnsIndexMap;
    List<String> measurementColumnNames;
    Map<String, Integer> measurementColumnsIndexMap;
    String timeColumnName;
    List<IMeasurementSchema> measurementSchemas;
    List<Symbol> measurementSchemaIndex2Symbol;
    int measurementColumnCount;
    int idx;

    private CommonTableScanOperatorParameters(
        DeviceTableScanNode node,
        Map<String, String> fieldColumnsRenameMap,
        boolean keepNonOutputMeasurementColumns) {
      outputColumnNames = node.getOutputSymbols();
      int outputColumnCount =
          keepNonOutputMeasurementColumns ? node.getAssignments().size() : outputColumnNames.size();
      columnSchemas = new ArrayList<>(outputColumnCount);
      symbolInputs = new ArrayList<>(outputColumnCount);
      columnsIndexArray = new int[outputColumnCount];
      columnSchemaMap = node.getAssignments();
      tagAndAttributeColumnsIndexMap = node.getTagAndAttributeIndexMap();
      measurementColumnNames = new ArrayList<>();
      measurementColumnsIndexMap = new HashMap<>();
      measurementSchemas = new ArrayList<>();
      measurementSchemaIndex2Symbol = new ArrayList<>();
      measurementColumnCount = 0;
      idx = 0;

      boolean addedTimeColumn = false;
      for (Symbol columnName : outputColumnNames) {
        ColumnSchema schema =
            requireNonNull(columnSchemaMap.get(columnName), columnName + " is null");

        symbolInputs.add(columnName);
        switch (schema.getColumnCategory()) {
          case TAG:
          case ATTRIBUTE:
            columnsIndexArray[idx++] =
                requireNonNull(
                    tagAndAttributeColumnsIndexMap.get(columnName), columnName + " is null");
            columnSchemas.add(schema);
            break;
          case FIELD:
            columnsIndexArray[idx++] = measurementColumnCount;
            measurementColumnCount++;

            String realMeasurementName =
                fieldColumnsRenameMap.getOrDefault(schema.getName(), schema.getName());

            measurementColumnNames.add(realMeasurementName);
            measurementSchemas.add(
                new MeasurementSchema(realMeasurementName, getTSDataType(schema.getType())));
            measurementSchemaIndex2Symbol.add(columnName);
            columnSchemas.add(schema);
            measurementColumnsIndexMap.put(columnName.getName(), measurementColumnCount - 1);
            break;
          case TIME:
            columnsIndexArray[idx++] = -1;
            columnSchemas.add(schema);
            timeColumnName = columnName.getName();
            addedTimeColumn = true;
            break;
          default:
            throw new IllegalArgumentException(
                "Unexpected column category: " + schema.getColumnCategory());
        }
      }
      Set<Symbol> outputSet = new HashSet<>(outputColumnNames);
      for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
        if (!outputSet.contains(entry.getKey()) && entry.getValue().getColumnCategory() == FIELD) {
          if (keepNonOutputMeasurementColumns) {
            columnSchemas.add(entry.getValue());
            columnsIndexArray[idx++] = measurementColumnCount;
            symbolInputs.add(entry.getKey());
          }
          measurementColumnCount++;
          String realMeasurementName =
              fieldColumnsRenameMap.getOrDefault(
                  entry.getValue().getName(), entry.getValue().getName());

          measurementColumnNames.add(realMeasurementName);
          measurementSchemas.add(
              new MeasurementSchema(
                  realMeasurementName, getTSDataType(entry.getValue().getType())));
          measurementSchemaIndex2Symbol.add(entry.getKey());
          measurementColumnsIndexMap.put(entry.getKey().getName(), measurementColumnCount - 1);
        } else if (entry.getValue().getColumnCategory() == TIME) {
          timeColumnName = entry.getKey().getName();
          // for non aligned series table view scan, here the time column will not be obtained
          // through this structure, but we need to ensure that the length of columnSchemas is
          // consistent with the length of columnsIndexArray
          if (keepNonOutputMeasurementColumns && !addedTimeColumn) {
            columnSchemas.add(entry.getValue());
            columnsIndexArray[idx++] = -1;
            symbolInputs.add(entry.getKey());
          }
        }
      }
    }
  }

  @Override
  public Operator visitTreeAlignedDeviceViewScan(
      TreeAlignedDeviceViewScanNode node, LocalExecutionPlanContext context) {
    QualifiedObjectName qualifiedObjectName = node.getQualifiedObjectName();
    TsTable tsTable =
        DataNodeTableCache.getInstance()
            .getTable(qualifiedObjectName.getDatabaseName(), qualifiedObjectName.getObjectName());
    IDeviceID.TreeDeviceIdColumnValueExtractor idColumnValueExtractor =
        createTreeDeviceIdColumnValueExtractor(DataNodeTreeViewSchemaUtils.getPrefixPath(tsTable));

    AbstractTableScanOperator.AbstractTableScanOperatorParameter parameter =
        constructAbstractTableScanOperatorParameter(
            node,
            context,
            TreeAlignedDeviceViewScanOperator.class.getSimpleName(),
            node.getMeasurementColumnNameMap(),
            tsTable.getCachedTableTTL());

    TreeAlignedDeviceViewScanOperator treeAlignedDeviceViewScanOperator =
        new TreeAlignedDeviceViewScanOperator(parameter, idColumnValueExtractor);

    addSource(
        treeAlignedDeviceViewScanOperator,
        context,
        node,
        parameter.measurementColumnNames,
        parameter.measurementSchemas,
        parameter.allSensors,
        TreeAlignedDeviceViewScanNode.class.getSimpleName());

    return treeAlignedDeviceViewScanOperator;
  }

  private void addSource(
      AbstractDataSourceOperator sourceOperator,
      LocalExecutionPlanContext context,
      DeviceTableScanNode node,
      List<String> measurementColumnNames,
      List<IMeasurementSchema> measurementSchemas,
      Set<String> allSensors,
      String planNodeName) {

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(sourceOperator);

    for (int i = 0, size = node.getDeviceEntries().size(); i < size; i++) {
      DeviceEntry deviceEntry = node.getDeviceEntries().get(i);
      if (deviceEntry == null) {
        throw new IllegalStateException(
            "Device entries of index " + i + " in " + planNodeName + " is empty");
      }
      if (deviceEntry instanceof NonAlignedDeviceEntry) {
        for (IMeasurementSchema schema : measurementSchemas) {
          NonAlignedFullPath nonAlignedFullPath =
              new NonAlignedFullPath(deviceEntry.getDeviceID(), schema);
          ((DataDriverContext) context.getDriverContext()).addPath(nonAlignedFullPath);
        }
      } else {
        AlignedFullPath alignedPath =
            constructAlignedPath(
                deviceEntry, measurementColumnNames, measurementSchemas, allSensors);
        ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
      }
    }

    context.getDriverContext().setInputDriver(true);
  }

  private AbstractTableScanOperator.AbstractTableScanOperatorParameter
      constructAbstractTableScanOperatorParameter(
          DeviceTableScanNode node,
          LocalExecutionPlanContext context,
          String className,
          Map<String, String> fieldColumnsRenameMap,
          long viewTTL) {

    CommonTableScanOperatorParameters commonParameter =
        new CommonTableScanOperatorParameters(node, fieldColumnsRenameMap, false);
    List<IMeasurementSchema> measurementSchemas = commonParameter.measurementSchemas;
    List<String> measurementColumnNames = commonParameter.measurementColumnNames;
    List<ColumnSchema> columnSchemas = commonParameter.columnSchemas;
    int[] columnsIndexArray = commonParameter.columnsIndexArray;
    SeriesScanOptions seriesScanOptions =
        buildSeriesScanOptions(
            context,
            commonParameter.columnSchemaMap,
            measurementColumnNames,
            commonParameter.measurementColumnsIndexMap,
            commonParameter.timeColumnName,
            node.getTimePredicate(),
            node.getPushDownLimit(),
            node.getPushDownOffset(),
            node.isPushLimitToEachDevice(),
            node.getPushDownPredicate());
    seriesScanOptions.setTTLForTableView(viewTTL);
    seriesScanOptions.setIsTableViewForTreeModel(node instanceof TreeDeviceViewScanNode);

    OperatorContext operatorContext = addOperatorContext(context, node.getPlanNodeId(), className);

    int maxTsBlockLineNum = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    if (context.getTypeProvider().getTemplatedInfo() != null) {
      maxTsBlockLineNum =
          (int)
              Math.min(
                  context.getTypeProvider().getTemplatedInfo().getLimitValue(), maxTsBlockLineNum);
    }

    Set<String> allSensors = new HashSet<>(measurementColumnNames);
    // for time column
    allSensors.add("");

    return new AbstractTableScanOperator.AbstractTableScanOperatorParameter(
        allSensors,
        operatorContext,
        node.getPlanNodeId(),
        columnSchemas,
        columnsIndexArray,
        node.getDeviceEntries(),
        node.getScanOrder(),
        seriesScanOptions,
        measurementColumnNames,
        measurementSchemas,
        maxTsBlockLineNum);
  }

  // used for TableScanOperator
  private AbstractTableScanOperator.AbstractTableScanOperatorParameter
      constructAbstractTableScanOperatorParameter(
          DeviceTableScanNode node, LocalExecutionPlanContext context) {
    return constructAbstractTableScanOperatorParameter(
        node,
        context,
        TableScanOperator.class.getSimpleName(),
        Collections.emptyMap(),
        Long.MAX_VALUE);
  }

  @Override
  public Operator visitTreeDeviceViewScan(
      TreeDeviceViewScanNode node, LocalExecutionPlanContext context) {
    if (node.getDeviceEntries().isEmpty() || node.getTreeDBName() == null) {
      OperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), EmptyDataOperator.class.getSimpleName());
      return new EmptyDataOperator(operatorContext);
    }
    throw new IllegalArgumentException("Valid TreeDeviceViewScanNode is not expected here.");
  }

  @Override
  public Operator visitDeviceTableScan(
      DeviceTableScanNode node, LocalExecutionPlanContext context) {

    AbstractTableScanOperator.AbstractTableScanOperatorParameter parameter =
        constructAbstractTableScanOperatorParameter(node, context);

    TableScanOperator tableScanOperator = new TableScanOperator(parameter);

    context.getInstanceContext().collectTable(node.getQualifiedObjectName().getObjectName());
    addSource(
        tableScanOperator,
        context,
        node,
        parameter.measurementColumnNames,
        parameter.measurementSchemas,
        parameter.allSensors,
        DeviceTableScanNode.class.getSimpleName());

    return tableScanOperator;
  }

  private SeriesScanOptions.Builder getSeriesScanOptionsBuilder(
      LocalExecutionPlanContext context, @NotNull Expression timePredicate) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();

    Filter timeFilter =
        timePredicate.accept(
            new ConvertPredicateToTimeFilterVisitor(
                context.getZoneId(), TimestampPrecisionUtils.currPrecision),
            null);
    context.getDriverContext().getFragmentInstanceContext().setTimeFilterForTableModel(timeFilter);
    // time filter may be stateful, so we need to copy it
    scanOptionsBuilder.withGlobalTimeFilter(timeFilter.copy());

    return scanOptionsBuilder;
  }

  @Override
  public Operator visitInformationSchemaTableScan(
      final InformationSchemaTableScanNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        addOperatorContext(
            context,
            node.getPlanNodeId(),
            InformationSchemaTableScanOperator.class.getSimpleName());

    final List<TSDataType> dataTypes =
        node.getOutputSymbols().stream()
            .map(symbol -> getTSDataType(context.getTypeProvider().getTableModelType(symbol)))
            .collect(Collectors.toList());

    return new InformationSchemaTableScanOperator(
        operatorContext,
        node.getPlanNodeId(),
        getSupplier(operatorContext, dataTypes, getSessionInfo(context).getUserEntity(), node));
  }

  @Override
  public Operator visitCountMerge(
      final CountSchemaMergeNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), CountMergeOperator.class.getSimpleName());
    final List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (final PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    return new CountMergeOperator(operatorContext, children);
  }

  @Override
  public Operator visitTableDeviceFetch(
      final TableDeviceFetchNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getTableDeviceFetchSource(
            node.getDatabase(),
            node.getTableName(),
            node.getDeviceIdList(),
            node.getColumnHeaderList()));
  }

  @Override
  public Operator visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node, final LocalExecutionPlanContext context) {
    // Query scan use filterNode directly
    final TsTable table =
        DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTableName());
    final SchemaQueryScanOperator<IDeviceSchemaInfo> operator =
        new SchemaQueryScanOperator<>(
            node.getPlanNodeId(),
            addOperatorContext(
                context, node.getPlanNodeId(), SchemaQueryScanOperator.class.getSimpleName()),
            SchemaSourceFactory.getTableDeviceQuerySource(
                node.getDatabase(),
                table,
                node.getTagDeterminedFilterList(),
                node.getColumnHeaderList(),
                node.getColumnHeaderList().stream()
                    .map(columnHeader -> table.getColumnSchema(columnHeader.getColumnName()))
                    .collect(Collectors.toList()),
                null,
                node.isNeedAligned()));
    operator.setLimit(node.getLimit());
    return operator;
  }

  @Override
  public Operator visitTableDeviceQueryCount(
      final TableDeviceQueryCountNode node, final LocalExecutionPlanContext context) {
    final String database = node.getDatabase();
    final TsTable table = DataNodeTableCache.getInstance().getTable(database, node.getTableName());
    final List<TsTableColumnSchema> columnSchemaList =
        node.getColumnHeaderList().stream()
            .map(columnHeader -> table.getColumnSchema(columnHeader.getColumnName()))
            .collect(Collectors.toList());

    // In "count" we have to reuse filter operator per "next"
    final List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();
    return new SchemaCountOperator<>(
        node.getPlanNodeId(),
        addOperatorContext(
            context, node.getPlanNodeId(), SchemaCountOperator.class.getSimpleName()),
        SchemaSourceFactory.getTableDeviceQuerySource(
            database,
            table,
            node.getTagDeterminedFilterList(),
            node.getColumnHeaderList(),
            columnSchemaList,
            Objects.nonNull(node.getTagFuzzyPredicate())
                ? new DevicePredicateFilter(
                    filterLeafColumnTransformerList,
                    new ColumnTransformerBuilder()
                        .process(
                            node.getTagFuzzyPredicate(),
                            new ColumnTransformerBuilder.Context(
                                getSessionInfo(context),
                                filterLeafColumnTransformerList,
                                makeLayout(Collections.singletonList(node)),
                                new HashMap<>(),
                                ImmutableMap.of(),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                0,
                                context.getTypeProvider(),
                                metadata)),
                    columnSchemaList,
                    database,
                    table)
                : null,
            false));
  }

  @Override
  public Operator visitAggregationTreeDeviceViewScan(
      AggregationTreeDeviceViewScanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException(
        "The AggregationTreeDeviceViewScanNode should has been transferred to its child class node");
  }

  @Override
  public Operator visitAlignedAggregationTreeDeviceViewScan(
      AlignedAggregationTreeDeviceViewScanNode node, LocalExecutionPlanContext context) {
    QualifiedObjectName qualifiedObjectName = node.getQualifiedObjectName();
    TsTable tsTable =
        DataNodeTableCache.getInstance()
            .getTable(qualifiedObjectName.getDatabaseName(), qualifiedObjectName.getObjectName());
    IDeviceID.TreeDeviceIdColumnValueExtractor idColumnValueExtractor =
        createTreeDeviceIdColumnValueExtractor(DataNodeTreeViewSchemaUtils.getPrefixPath(tsTable));

    AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter parameter =
        constructAbstractAggTableScanOperatorParameter(
            node,
            context,
            TreeAlignedDeviceViewAggregationScanOperator.class.getSimpleName(),
            node.getMeasurementColumnNameMap(),
            tsTable.getCachedTableTTL());

    TreeAlignedDeviceViewAggregationScanOperator treeAlignedDeviceViewAggregationScanOperator =
        new TreeAlignedDeviceViewAggregationScanOperator(parameter, idColumnValueExtractor);

    addSource(
        treeAlignedDeviceViewAggregationScanOperator,
        context,
        node,
        parameter.getMeasurementColumnNames(),
        parameter.getMeasurementSchemas(),
        parameter.getAllSensors(),
        AlignedAggregationTreeDeviceViewScanNode.class.getSimpleName());
    return treeAlignedDeviceViewAggregationScanOperator;
  }

  @Override
  public Operator visitNonAlignedAggregationTreeDeviceViewScan(
      NonAlignedAggregationTreeDeviceViewScanNode node, LocalExecutionPlanContext context) {
    QualifiedObjectName qualifiedObjectName = node.getQualifiedObjectName();
    TsTable tsTable =
        DataNodeTableCache.getInstance()
            .getTable(qualifiedObjectName.getDatabaseName(), qualifiedObjectName.getObjectName());
    IDeviceID.TreeDeviceIdColumnValueExtractor idColumnValueExtractor =
        createTreeDeviceIdColumnValueExtractor(DataNodeTreeViewSchemaUtils.getPrefixPath(tsTable));

    AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter parameter =
        constructAbstractAggTableScanOperatorParameter(
            node,
            context,
            TreeNonAlignedDeviceViewAggregationScanOperator.class.getSimpleName(),
            node.getMeasurementColumnNameMap(),
            tsTable.getCachedTableTTL());

    // construct source operator (generator)
    TreeNonAlignedDeviceViewScanNode scanNode =
        new TreeNonAlignedDeviceViewScanNode(
            node.getPlanNodeId(),
            node.getQualifiedObjectName(),
            // the outputSymbols of TreeNonAlignedDeviceViewAggregationScanOperator is not equals
            // with TreeNonAlignedDeviceViewScanNode
            parameter.getOutputSymbols(),
            node.getAssignments(),
            node.getDeviceEntries(),
            node.getTagAndAttributeIndexMap(),
            node.getScanOrder(),
            node.getTimePredicate().orElse(null),
            node.getPushDownPredicate(),
            node.getPushDownLimit(),
            node.getPushDownOffset(),
            node.isPushLimitToEachDevice(),
            true,
            node.getTreeDBName(),
            node.getMeasurementColumnNameMap());

    Operator sourceOperator = visitTreeNonAlignedDeviceViewScan(scanNode, context);
    if (!(sourceOperator instanceof EmptyDataOperator)) {
      // Use deviceChildOperatorTreeGenerator directly, we will control switch of devices in
      // TreeNonAlignedDeviceViewAggregationScanOperator
      TreeNonAlignedDeviceViewAggregationScanOperator aggTableScanOperator =
          new TreeNonAlignedDeviceViewAggregationScanOperator(
              parameter,
              idColumnValueExtractor,
              ((DeviceIteratorScanOperator) sourceOperator).getDeviceChildOperatorTreeGenerator());

      addSource(
          aggTableScanOperator,
          context,
          node,
          parameter.getMeasurementColumnNames(),
          parameter.getMeasurementSchemas(),
          parameter.getAllSensors(),
          NonAlignedAggregationTreeDeviceViewScanNode.class.getSimpleName());
      return aggTableScanOperator;
    } else {
      // source data is empty, return directly
      return sourceOperator;
    }
  }

  private AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter
      constructAbstractAggTableScanOperatorParameter(
          AggregationTableScanNode node,
          LocalExecutionPlanContext context,
          String className,
          Map<String, String> fieldColumnsRenameMap,
          long tableViewTTL) {

    List<String> measurementColumnNames = new ArrayList<>();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    Map<String, Integer> measurementColumnsIndexMap = new HashMap<>();

    List<TableAggregator> aggregators = new ArrayList<>(node.getAggregations().size());
    List<Integer> aggregatorInputChannels =
        new ArrayList<>(
            (int)
                node.getAggregations().values().stream()
                    .mapToLong(aggregation -> aggregation.getArguments().size())
                    .sum());
    int aggDistinctArgumentCount =
        (int)
            node.getAggregations().values().stream()
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .map(Symbol::from)
                .distinct()
                .count();
    List<ColumnSchema> aggColumnSchemas = new ArrayList<>(aggDistinctArgumentCount);
    Map<Symbol, Integer> aggColumnLayout = new HashMap<>(aggDistinctArgumentCount);
    int[] aggColumnsIndexArray = new int[aggDistinctArgumentCount];

    List<Symbol> outputSymbols = new ArrayList<>();

    String timeColumnName = null;
    int channel = 0;
    int measurementColumnCount = 0;
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      for (Expression argument : entry.getValue().getArguments()) {
        Symbol symbol = Symbol.from(argument);
        ColumnSchema schema =
            requireNonNull(node.getAssignments().get(symbol), symbol + " is null");
        if (!aggColumnLayout.containsKey(symbol)) {
          switch (schema.getColumnCategory()) {
            case TAG:
            case ATTRIBUTE:
              aggColumnsIndexArray[channel] =
                  requireNonNull(
                      node.getTagAndAttributeIndexMap().get(symbol), symbol + " is null");
              break;
            case FIELD:
              aggColumnsIndexArray[channel] = measurementColumnCount;
              measurementColumnCount++;
              String realMeasurementName =
                  fieldColumnsRenameMap.getOrDefault(schema.getName(), schema.getName());
              measurementColumnNames.add(realMeasurementName);
              measurementSchemas.add(
                  new MeasurementSchema(realMeasurementName, getTSDataType(schema.getType())));
              measurementColumnsIndexMap.put(symbol.getName(), measurementColumnCount - 1);
              outputSymbols.add(symbol);
              break;
            case TIME:
              aggColumnsIndexArray[channel] = -1;
              timeColumnName = symbol.getName();
              break;
            default:
              throw new IllegalArgumentException(
                  "Unexpected column category: " + schema.getColumnCategory());
          }

          aggColumnSchemas.add(schema);
          aggregatorInputChannels.add(channel);
          aggColumnLayout.put(symbol, channel++);
        } else {
          aggregatorInputChannels.add(aggColumnLayout.get(symbol));
        }
      }
    }

    for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
      if (!aggColumnLayout.containsKey(entry.getKey())
          && entry.getValue().getColumnCategory() == FIELD) {
        measurementColumnCount++;
        String realMeasurementName =
            fieldColumnsRenameMap.getOrDefault(
                entry.getValue().getName(), entry.getValue().getName());
        measurementColumnNames.add(realMeasurementName);
        measurementSchemas.add(
            new MeasurementSchema(realMeasurementName, getTSDataType(entry.getValue().getType())));
        measurementColumnsIndexMap.put(entry.getKey().getName(), measurementColumnCount - 1);
      } else if (entry.getValue().getColumnCategory() == TIME) {
        timeColumnName = entry.getKey().getName();
      }
    }

    boolean[] ret = checkStatisticAndScanOrder(node, timeColumnName);
    boolean canUseStatistic = ret[0];
    boolean scanAscending = ret[1];

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      aggregators.add(
          buildAggregator(
              aggColumnLayout,
              entry.getKey(),
              entry.getValue(),
              node.getStep(),
              context.getTypeProvider(),
              scanAscending,
              true,
              timeColumnName,
              measurementColumnsIndexMap.keySet()));
    }

    ITableTimeRangeIterator timeRangeIterator = null;
    List<ColumnSchema> groupingKeySchemas = null;
    int[] groupingKeyIndex = null;
    if (!node.getGroupingKeys().isEmpty()) {
      groupingKeySchemas = new ArrayList<>(node.getGroupingKeys().size());
      groupingKeyIndex = new int[node.getGroupingKeys().size()];
      for (int i = 0; i < node.getGroupingKeys().size(); i++) {
        Symbol groupingKey = node.getGroupingKeys().get(i);

        if (node.getTagAndAttributeIndexMap().containsKey(groupingKey)) {
          groupingKeySchemas.add(node.getAssignments().get(groupingKey));
          groupingKeyIndex[i] = node.getTagAndAttributeIndexMap().get(groupingKey);
        } else {
          if (node.getProjection() != null
              && !node.getProjection().getMap().isEmpty()
              && node.getProjection().contains(groupingKey)) {
            FunctionCall dateBinFunc = (FunctionCall) node.getProjection().get(groupingKey);
            List<Expression> arguments = dateBinFunc.getArguments();
            DateBinFunctionColumnTransformer dateBinTransformer =
                new DateBinFunctionColumnTransformer(
                    TIMESTAMP,
                    ((LongLiteral) arguments.get(0)).getParsedValue(),
                    ((LongLiteral) arguments.get(1)).getParsedValue(),
                    null,
                    ((LongLiteral) arguments.get(3)).getParsedValue(),
                    context.getZoneId());
            timeRangeIterator = new TableDateBinTimeRangeIterator(dateBinTransformer);
          } else {
            throw new IllegalStateException(
                "grouping key must be ID or Attribute in AggregationTableScan");
          }
        }
      }
    }
    if (timeRangeIterator == null) {
      if (node.getGroupingKeys().isEmpty()) {
        // global aggregation, has no group by, output init value if no data
        timeRangeIterator =
            new TableSingleTimeWindowIterator(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
      } else {
        // aggregation with group by, only has data the result will not be empty
        timeRangeIterator = new TableSingleTimeWindowIterator();
      }
    }

    final OperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), className);
    SeriesScanOptions seriesScanOptions =
        buildSeriesScanOptions(
            context,
            node.getAssignments(),
            measurementColumnNames,
            measurementColumnsIndexMap,
            timeColumnName,
            node.getTimePredicate(),
            node.getPushDownLimit(),
            node.getPushDownOffset(),
            node.isPushLimitToEachDevice(),
            node.getPushDownPredicate());
    seriesScanOptions.setTTLForTableView(tableViewTTL);
    seriesScanOptions.setIsTableViewForTreeModel(node instanceof AggregationTreeDeviceViewScanNode);

    Set<String> allSensors = new HashSet<>(measurementColumnNames);
    allSensors.add(""); // for time column
    context.getDriverContext().setInputDriver(true);

    return new AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter(
        node.getPlanNodeId(),
        operatorContext,
        aggColumnSchemas,
        aggColumnsIndexArray,
        node.getDeviceEntries(),
        node.getDeviceEntries().size(),
        seriesScanOptions,
        measurementColumnNames,
        allSensors,
        measurementSchemas,
        aggregators,
        groupingKeySchemas,
        groupingKeyIndex,
        timeRangeIterator,
        scanAscending,
        canUseStatistic,
        aggregatorInputChannels,
        timeColumnName,
        outputSymbols);
  }

  // used for AggregationTableScanNode
  private AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter
      constructAbstractAggTableScanOperatorParameter(
          AggregationTableScanNode node, LocalExecutionPlanContext context) {
    return constructAbstractAggTableScanOperatorParameter(
        node,
        context,
        AbstractAggTableScanOperator.class.getSimpleName(),
        Collections.emptyMap(),
        Long.MAX_VALUE);
  }

  @Override
  public Operator visitAggregationTableScan(
      AggregationTableScanNode node, LocalExecutionPlanContext context) {

    AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter parameter =
        constructAbstractAggTableScanOperatorParameter(node, context);

    OptimizeType optimizeType =
        canUseLastCacheOptimize(
            parameter.getTableAggregators(), node, parameter.getTimeColumnName());
    if (optimizeType != OptimizeType.NOOP) {
      return constructLastQueryAggTableScanOperator(
          node, parameter, optimizeType == OptimizeType.LAST_ROW, context);
    } else {
      DefaultAggTableScanOperator aggTableScanOperator = new DefaultAggTableScanOperator(parameter);

      context.getInstanceContext().collectTable(node.getQualifiedObjectName().getObjectName());
      addSource(
          aggTableScanOperator,
          context,
          node,
          parameter.getMeasurementColumnNames(),
          parameter.getMeasurementSchemas(),
          parameter.getAllSensors(),
          AggregationTableScanNode.class.getSimpleName());
      return aggTableScanOperator;
    }
  }

  private LastQueryAggTableScanOperator constructLastQueryAggTableScanOperator(
      AggregationTableScanNode node,
      AbstractAggTableScanOperator.AbstractAggTableScanOperatorParameter parameter,
      boolean isLastRowOptimize,
      LocalExecutionPlanContext context) {
    List<Integer> hitCachesIndexes = new ArrayList<>();
    List<Pair<OptionalLong, TsPrimitiveType[]>> lastRowCacheResults = null;
    List<TimeValuePair[]> lastValuesCacheResults = null;
    List<DeviceEntry> cachedDeviceEntries = new ArrayList<>();
    List<DeviceEntry> unCachedDeviceEntries = new ArrayList<>();
    long tableTTL =
        DataNodeTTLCache.getInstance()
            .getTTLForTable(
                node.getQualifiedObjectName().getDatabaseName(),
                node.getQualifiedObjectName().getObjectName());
    Filter updateTimeFilter =
        updateFilterUsingTTL(parameter.getSeriesScanOptions().getGlobalTimeFilter(), tableTTL);
    if (isLastRowOptimize) {
      lastRowCacheResults = new ArrayList<>();
      for (int i = 0; i < node.getDeviceEntries().size(); i++) {
        Optional<Pair<OptionalLong, TsPrimitiveType[]>> lastByResult =
            TableDeviceSchemaCache.getInstance()
                .getLastRow(
                    node.getQualifiedObjectName().getDatabaseName(),
                    node.getDeviceEntries().get(i).getDeviceID(),
                    "",
                    parameter.getMeasurementColumnNames());
        boolean allHitCache = true;
        if (lastByResult.isPresent() && lastByResult.get().getLeft().isPresent()) {
          for (int j = 0; j < lastByResult.get().getRight().length; j++) {
            TsPrimitiveType tsPrimitiveType = lastByResult.get().getRight()[j];
            if (tsPrimitiveType == null
                || (updateTimeFilter != null
                    && !LastQueryUtil.satisfyFilter(
                        updateTimeFilter,
                        new TimeValuePair(
                            lastByResult.get().getLeft().getAsLong(), tsPrimitiveType)))) {
              // the process logic is different from tree model which examine if
              // `isFilterGtOrGe(seriesScanOptions.getGlobalTimeFilter())`, set
              // `lastByResult.get().getRight()[j] = EMPTY_PRIMITIVE_TYPE`,
              // but it should skip in table model
              allHitCache = false;
              break;
            }
          }
        } else {
          allHitCache = false;
        }

        DeviceEntry deviceEntry = node.getDeviceEntries().get(i);
        if (!allHitCache) {
          AlignedFullPath alignedPath =
              constructAlignedPath(
                  deviceEntry,
                  parameter.getMeasurementColumnNames(),
                  parameter.getMeasurementSchemas(),
                  parameter.getAllSensors());
          ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
          unCachedDeviceEntries.add(deviceEntry);
          addUncachedDeviceToContext(node, context, deviceEntry);

          // last cache updateColumns need to put "" as time column
          String[] updateColumns = new String[parameter.getMeasurementColumnNames().size() + 1];
          updateColumns[0] = "";
          for (int j = 1; j < updateColumns.length; j++) {
            updateColumns[j] = parameter.getMeasurementColumnNames().get(j - 1);
          }
          TableDeviceSchemaCache.getInstance()
              .initOrInvalidateLastCache(
                  node.getQualifiedObjectName().getDatabaseName(),
                  deviceEntry.getDeviceID(),
                  updateColumns,
                  false);
        } else {
          hitCachesIndexes.add(i);
          lastRowCacheResults.add(lastByResult.get());
          cachedDeviceEntries.add(deviceEntry);
          decreaseDeviceCount(node, context, deviceEntry);
        }
      }
    } else {
      // LAST_VALUES optimize
      lastValuesCacheResults = new ArrayList<>();
      int measurementSize = parameter.getMeasurementColumnNames().size();
      // We don't init time if the last cache will not be updated in process of operator
      boolean needInitTime =
          parameter.getTableAggregators().stream()
              .anyMatch(
                  aggregator ->
                      aggregator.getAccumulator() instanceof LastDescAccumulator
                          && !((LastDescAccumulator) aggregator.getAccumulator())
                              .isMeasurementColumn());
      // When we need last cache of Time column if:
      // 1. query is group by (we need last cache of Time to help judge if there is no data in
      // device)
      // 2. last(time), last(device) or last(attribute) occurs
      boolean needTime = !node.getGroupingKeys().isEmpty() || needInitTime;
      String[] targetColumns;

      if (needTime) {
        targetColumns = new String[measurementSize + 1];
        // put time column in the last for convenience of later processing
        targetColumns[targetColumns.length - 1] = "";
      } else {
        targetColumns = new String[measurementSize];
      }

      for (int j = 0; j < measurementSize; j++) {
        targetColumns[j] = parameter.getMeasurementColumnNames().get(j);
      }

      for (int i = 0; i < node.getDeviceEntries().size(); i++) {
        TimeValuePair[] lastResult =
            TableDeviceSchemaCache.getInstance()
                .getLastEntries(
                    node.getQualifiedObjectName().getDatabaseName(),
                    node.getDeviceEntries().get(i).getDeviceID(),
                    targetColumns);
        boolean allHitCache = true;
        if (lastResult != null) {
          for (TimeValuePair timeValuePair : lastResult) {
            if (timeValuePair == null || timeValuePair.getValue() == null) {
              allHitCache = false;
              break;
            }

            if (updateTimeFilter != null
                && !LastQueryUtil.satisfyFilter(
                    parameter.getSeriesScanOptions().getGlobalTimeFilter(), timeValuePair)) {
              if (isFilterGtOrGe(updateTimeFilter)) {
                // it means there is no data meets Filter
                timeValuePair.setValue(EMPTY_PRIMITIVE_TYPE);
              } else {
                allHitCache = false;
                break;
              }
            }
          }
        } else {
          allHitCache = false;
        }

        DeviceEntry deviceEntry = node.getDeviceEntries().get(i);
        if (!allHitCache) {
          AlignedFullPath alignedPath =
              constructAlignedPath(
                  deviceEntry,
                  parameter.getMeasurementColumnNames(),
                  parameter.getMeasurementSchemas(),
                  parameter.getAllSensors());
          ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
          unCachedDeviceEntries.add(deviceEntry);
          addUncachedDeviceToContext(node, context, deviceEntry);

          TableDeviceSchemaCache.getInstance()
              .initOrInvalidateLastCache(
                  node.getQualifiedObjectName().getDatabaseName(),
                  deviceEntry.getDeviceID(),
                  needInitTime || node.getGroupingKeys().isEmpty()
                      ? targetColumns
                      : Arrays.copyOfRange(targetColumns, 0, targetColumns.length - 1),
                  false);
        } else {
          hitCachesIndexes.add(i);
          lastValuesCacheResults.add(lastResult);
          cachedDeviceEntries.add(deviceEntry);
          decreaseDeviceCount(node, context, deviceEntry);
        }
      }
    }

    parameter.setDeviceEntries(unCachedDeviceEntries);

    // context add TableLastQueryOperator
    LastQueryAggTableScanOperator lastQueryOperator =
        new LastQueryAggTableScanOperator(
            parameter,
            cachedDeviceEntries,
            node.getQualifiedObjectName(),
            hitCachesIndexes,
            lastRowCacheResults,
            lastValuesCacheResults,
            node.getDeviceCountMap(),
            context.getInstanceContext().getDataNodeQueryContext());

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(lastQueryOperator);
    parameter
        .getOperatorContext()
        .setOperatorType(LastQueryAggTableScanOperator.class.getSimpleName());
    return lastQueryOperator;
  }

  private void addUncachedDeviceToContext(
      AggregationTableScanNode node, LocalExecutionPlanContext context, DeviceEntry deviceEntry) {
    boolean deviceInMultiRegion =
        node.getDeviceCountMap() != null && node.getDeviceCountMap().containsKey(deviceEntry);
    if (!deviceInMultiRegion) {
      return;
    }

    context.dataNodeQueryContext.lock(true);
    try {
      context.dataNodeQueryContext.addUnCachedDeviceIfAbsent(
          node.getQualifiedObjectName(), deviceEntry, node.getDeviceCountMap().get(deviceEntry));
    } finally {
      context.dataNodeQueryContext.unLock(true);
    }
  }

  /**
   * Decrease the device count when its last cache was hit. Notice that the count can also be zero
   * after decrease, we need to update last cache if needed.
   */
  private void decreaseDeviceCount(
      AggregationTableScanNode node, LocalExecutionPlanContext context, DeviceEntry deviceEntry) {
    boolean deviceInMultiRegion =
        node.getDeviceCountMap() != null && node.getDeviceCountMap().containsKey(deviceEntry);
    if (!deviceInMultiRegion) {
      return;
    }

    context.dataNodeQueryContext.lock(true);
    try {
      context.dataNodeQueryContext.decreaseDeviceAndMayUpdateLastCache(
          node.getQualifiedObjectName(), deviceEntry, node.getDeviceCountMap().get(deviceEntry));
    } finally {
      context.dataNodeQueryContext.unLock(true);
    }
  }

  private SeriesScanOptions buildSeriesScanOptions(
      LocalExecutionPlanContext context,
      Map<Symbol, ColumnSchema> columnSchemaMap,
      List<String> measurementColumnNames,
      Map<String, Integer> measurementColumnsIndexMap,
      String timeColumnName,
      Optional<Expression> timePredicate,
      long pushDownLimit,
      long pushDownOffset,
      boolean pushLimitToEachDevice,
      Expression pushDownPredicate) {
    SeriesScanOptions.Builder scanOptionsBuilder =
        timePredicate
            .map(expression -> getSeriesScanOptionsBuilder(context, expression))
            .orElseGet(SeriesScanOptions.Builder::new);
    scanOptionsBuilder.withPushDownLimit(pushDownLimit);
    scanOptionsBuilder.withPushDownOffset(pushDownOffset);
    scanOptionsBuilder.withPushLimitToEachDevice(pushLimitToEachDevice);
    scanOptionsBuilder.withAllSensors(new HashSet<>(measurementColumnNames));
    if (pushDownPredicate != null) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate,
              measurementColumnsIndexMap,
              columnSchemaMap,
              timeColumnName,
              context.getZoneId(),
              TimestampPrecisionUtils.currPrecision));
    }
    return scanOptionsBuilder.build();
  }

  @Override
  public Operator visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context) {
    Operator operator = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), ExplainAnalyzeOperator.class.getSimpleName());
    return new ExplainAnalyzeOperator(
        operatorContext, operator, node.getQueryId(), node.isVerbose(), node.getTimeout());
  }

  @Override
  public Operator visitCopyTo(CopyToNode node, LocalExecutionPlanContext context) {
    PlanNode childNode = node.getChild();

    List<Symbol> innerQueryOutputSymbols = node.getInnerQueryOutputSymbols();
    List<Symbol> childOutputSymbols = childNode.getOutputSymbols();
    Map<Symbol, Integer> childOutputSymbolsIndexMap = new HashMap<>(childOutputSymbols.size());
    for (int i = 0; i < childOutputSymbols.size(); i++) {
      childOutputSymbolsIndexMap.put(childOutputSymbols.get(i), i);
    }
    int[] columnIndex2TsBlockColumnIndexList = new int[innerQueryOutputSymbols.size()];
    for (int i = 0; i < innerQueryOutputSymbols.size(); i++) {
      int index = childOutputSymbolsIndexMap.get(innerQueryOutputSymbols.get(i));
      columnIndex2TsBlockColumnIndexList[i] = index;
    }

    Operator operator = childNode.accept(this, context);

    OperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), TableCopyToOperator.class.getSimpleName());
    return new TableCopyToOperator(
        operatorContext,
        operator,
        node.getTargetFilePath(),
        node.getCopyToOptions(),
        node.getInnerQueryDatasetHeader().getColumnHeaders(),
        columnIndex2TsBlockColumnIndexList);
  }

  @Override
  public Operator visitInto(IntoNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), TableIntoOperator.class.getSimpleName());

    PartialPath targetTable = new PartialPath(node.getTable(), false);

    Map<String, TSDataType> tsDataTypeMap = new LinkedHashMap<>();
    Map<String, InputLocation> inputLocationMap = new LinkedHashMap<>();
    List<TSDataType> inputColumnTypes = new ArrayList<>();
    List<TsTableColumnCategory> inputColumnCategories = new ArrayList<>();

    List<ColumnSchema> originColumns = node.getColumns();
    List<Symbol> originInputColumnNames = node.getNeededInputColumnNames();
    int size = originColumns.size();
    List<ColumnSchema> inputColumns = new ArrayList<>(size);

    List<Symbol> childOutputName = node.getChild().getOutputSymbols();
    Map<Symbol, Integer> map = new HashMap<>(childOutputName.size());
    for (int i = 0; i < size; i++) {
      map.put(childOutputName.get(i), i);
      inputColumns.add(null);
    }
    for (int i = 0; i < size; i++) {
      int index = map.get(originInputColumnNames.get(i));
      inputColumns.set(index, originColumns.get(i));
    }
    ColumnSchema timeColumnOfTargetTable = null;
    for (int i = 0; i < inputColumns.size(); i++) {
      String columnName = inputColumns.get(i).getName();
      inputLocationMap.put(columnName, new InputLocation(0, i));

      TsTableColumnCategory columnCategory = inputColumns.get(i).getColumnCategory();
      if (columnCategory == TIME) {
        if (timeColumnOfTargetTable == null) {
          timeColumnOfTargetTable = inputColumns.get(i);
        } else {
          throw new SemanticException("Multiple columns with TIME category found");
        }
        continue;
      }

      TSDataType columnType = InternalTypeManager.getTSDataType(inputColumns.get(i).getType());
      tsDataTypeMap.put(columnName, columnType);
      inputColumnTypes.add(columnType);
      inputColumnCategories.add(columnCategory);
    }
    if (timeColumnOfTargetTable == null) {
      throw new SemanticException("Missing TIME category column");
    }

    long statementSizePerLine =
        OperatorGeneratorUtil.calculateStatementSizePerLine(inputColumnTypes);

    return new TableIntoOperator(
        operatorContext,
        child,
        node.getDatabase(),
        targetTable,
        inputColumnTypes,
        inputColumnCategories,
        inputLocationMap,
        tsDataTypeMap,
        true,
        FragmentInstanceManager.getInstance().getIntoOperationExecutor(),
        statementSizePerLine,
        timeColumnOfTargetTable);
  }

  private boolean[] checkStatisticAndScanOrder(
      AggregationTableScanNode node, String timeColumnName) {
    boolean canUseStatistic = true;
    int ascendingCount = 0, descendingCount = 0;

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = entry.getValue();
      String funcName = aggregation.getResolvedFunction().getSignature().getName();
      Symbol argument = Symbol.from(aggregation.getArguments().get(0));
      Type argumentType = node.getAssignments().get(argument).getType();

      switch (funcName) {
        case COUNT:
        case AVG:
        case SUM:
        case EXTREME:
          break;
        case MAX:
        case MIN:
          if (BlobType.BLOB.equals(argumentType)
              || ObjectType.OBJECT.equals(argumentType)
              || BinaryType.TEXT.equals(argumentType)
              || BooleanType.BOOLEAN.equals(argumentType)) {
            canUseStatistic = false;
          }
          break;
        case FIRST_AGGREGATION:
        case LAST_AGGREGATION:
        case LAST_BY_AGGREGATION:
        case FIRST_BY_AGGREGATION:
          if (FIRST_AGGREGATION.equals(funcName) || FIRST_BY_AGGREGATION.equals(funcName)) {
            ascendingCount++;
          } else {
            descendingCount++;
          }

          // first/last/first_by/last_by aggregation with BLOB or OBJECT type can not use statistics
          if (BlobType.BLOB.equals(argumentType) || ObjectType.OBJECT.equals(argumentType)) {
            canUseStatistic = false;
            break;
          }

          // first and last, the second argument has to be the time column
          if (FIRST_AGGREGATION.equals(funcName) || LAST_AGGREGATION.equals(funcName)) {
            if (!isTimeColumn(aggregation.getArguments().get(1), timeColumnName)) {
              canUseStatistic = false;
              break;
            }
          }

          // first_by and last_by, the second argument has to be the time column
          if (FIRST_BY_AGGREGATION.equals(funcName) || LAST_BY_AGGREGATION.equals(funcName)) {
            if (!isTimeColumn(aggregation.getArguments().get(2), timeColumnName)) {
              canUseStatistic = false;
              break;
            }
          }

          // only last_by(time, x) or last_by(x,time) can use statistic
          if ((LAST_BY_AGGREGATION.equals(funcName) || FIRST_BY_AGGREGATION.equals(funcName))
              && !isTimeColumn(aggregation.getArguments().get(0), timeColumnName)
              && !isTimeColumn(aggregation.getArguments().get(1), timeColumnName)) {
            canUseStatistic = false;
          }
          break;
        default:
          canUseStatistic = false;
      }
    }

    boolean isAscending = node.getScanOrder().isAscending();
    boolean groupByDateBin = node.getProjection() != null && !node.getProjection().isEmpty();
    // only in non-groupByDateBin situation can change the scan order
    if (!groupByDateBin) {
      if (ascendingCount >= descendingCount) {
        node.setScanOrder(Ordering.ASC);
        isAscending = true;
      } else {
        node.setScanOrder(Ordering.DESC);
        isAscending = false;
      }
    }
    return new boolean[] {canUseStatistic, isAscending};
  }

  private OptimizeType canUseLastCacheOptimize(
      List<TableAggregator> aggregators, AggregationTableScanNode node, String timeColumnName) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable() || aggregators.isEmpty()) {
      return OptimizeType.NOOP;
    }

    // has value filter, can not optimize
    if (node.getPushDownPredicate() != null) {
      return OptimizeType.NOOP;
    }

    // has date_bin, can not optimize
    if (!node.getGroupingKeys().isEmpty()
        && node.getProjection() != null
        && !node.getProjection().getMap().isEmpty()) {
      return OptimizeType.NOOP;
    }

    // if the timeColumnName is null, the param of function is just a timestamp column other than
    // the time column
    if (timeColumnName == null || !checkOrderColumnIsTime(node.getAggregations(), timeColumnName)) {
      return OptimizeType.NOOP;
    }

    if (canUseLastRowOptimize(aggregators)) {
      return OptimizeType.LAST_ROW;
    }

    if (canUseLastValuesOptimize(aggregators)) {
      return OptimizeType.LAST_VALUES;
    }

    return OptimizeType.NOOP;
  }

  @Override
  protected SessionInfo getSessionInfo(LocalExecutionPlanContext context) {
    return context.getDriverContext().getFragmentInstanceContext().getSessionInfo();
  }
}
