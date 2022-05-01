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
package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.datatransfer.DataBlockManager;
import org.apache.iotdb.db.mpp.execution.datatransfer.DataBlockService;
import org.apache.iotdb.db.mpp.execution.datatransfer.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.datatransfer.ISourceHandle;
import org.apache.iotdb.db.mpp.execution.driver.DataDriver;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriver;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.ColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.TimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.DevicesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.DevicesSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.LevelTimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.TimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.TimeSeriesSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregateScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SeriesSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OutputColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Used to plan a fragment instance. Currently, we simply change it from PlanNode to executable
 * Operator tree, but in the future, we may split one fragment instance into multiple pipeline to
 * run a fragment instance parallel and take full advantage of multi-cores
 */
public class LocalExecutionPlanner {

  private static final DataBlockManager DATA_BLOCK_MANAGER =
      DataBlockService.getInstance().getDataBlockManager();

  private static final TimeComparator ASC_TIME_COMPARATOR = new AscTimeComparator();

  private static final TimeComparator DESC_TIME_COMPARATOR = new DescTimeComparator();

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public DataDriver plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      Filter timeFilter,
      DataRegion dataRegion) {
    LocalExecutionPlanContext context = new LocalExecutionPlanContext(types, instanceContext);

    Operator root = plan.accept(new Visitor(), context);

    DataDriverContext dataDriverContext =
        new DataDriverContext(
            instanceContext,
            context.getPaths(),
            timeFilter,
            dataRegion,
            context.getSourceOperators());
    instanceContext.setDriverContext(dataDriverContext);
    return new DataDriver(root, context.getSinkHandle(), dataDriverContext);
  }

  public SchemaDriver plan(
      PlanNode plan, FragmentInstanceContext instanceContext, ISchemaRegion schemaRegion) {

    SchemaDriverContext schemaDriverContext =
        new SchemaDriverContext(instanceContext, schemaRegion);
    instanceContext.setDriverContext(schemaDriverContext);

    LocalExecutionPlanContext context = new LocalExecutionPlanContext(instanceContext);

    Operator root = plan.accept(new Visitor(), context);

    return new SchemaDriver(root, context.getSinkHandle(), schemaDriverContext);
  }

  /** This Visitor is responsible for transferring PlanNode Tree to Operator Tree */
  private static class Visitor extends PlanVisitor<Operator, LocalExecutionPlanContext> {

    @Override
    public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
      throw new UnsupportedOperationException("should call the concrete visitXX() method");
    }

    @Override
    public Operator visitSeriesScan(SeriesScanNode node, LocalExecutionPlanContext context) {
      PartialPath seriesPath = node.getSeriesPath();
      boolean ascending = node.getScanOrder() == OrderBy.TIMESTAMP_ASC;
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SeriesScanOperator.class.getSimpleName());

      SeriesScanOperator seriesScanOperator =
          new SeriesScanOperator(
              node.getPlanNodeId(),
              seriesPath,
              node.getAllSensors(),
              seriesPath.getSeriesType(),
              operatorContext,
              node.getTimeFilter(),
              node.getValueFilter(),
              ascending);

      context.addSourceOperator(seriesScanOperator);
      context.addPath(seriesPath);

      return seriesScanOperator;
    }

    @Override
    public Operator visitSchemaScan(SchemaScanNode node, LocalExecutionPlanContext context) {
      if (node instanceof TimeSeriesSchemaScanNode) {
        return visitTimeSeriesSchemaScan((TimeSeriesSchemaScanNode) node, context);
      } else if (node instanceof DevicesSchemaScanNode) {
        return visitDevicesSchemaScan((DevicesSchemaScanNode) node, context);
      } else if (node instanceof DevicesCountNode) {
        return visitDevicesCount((DevicesCountNode) node, context);
      } else if (node instanceof TimeSeriesCountNode) {
        return visitTimeSeriesCount((TimeSeriesCountNode) node, context);
      } else if (node instanceof LevelTimeSeriesCountNode) {
        return visitLevelTimeSeriesCount((LevelTimeSeriesCountNode) node, context);
      }
      return visitPlan(node, context);
    }

    @Override
    public Operator visitTimeSeriesSchemaScan(
        TimeSeriesSchemaScanNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              TimeSeriesSchemaScanOperator.class.getSimpleName());
      return new TimeSeriesSchemaScanOperator(
          node.getPlanNodeId(),
          operatorContext,
          node.getLimit(),
          node.getOffset(),
          node.getPath(),
          node.getKey(),
          node.getValue(),
          node.isContains(),
          node.isOrderByHeat(),
          node.isPrefixPath());
    }

    @Override
    public Operator visitDevicesSchemaScan(
        DevicesSchemaScanNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              DevicesSchemaScanOperator.class.getSimpleName());
      return new DevicesSchemaScanOperator(
          node.getPlanNodeId(),
          operatorContext,
          node.getLimit(),
          node.getOffset(),
          node.getPath(),
          node.isPrefixPath(),
          node.isHasSgCol());
    }

    @Override
    public Operator visitSchemaMerge(
        SeriesSchemaMergeNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(n -> n.accept(this, context))
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SchemaMergeOperator.class.getSimpleName());
      return new SchemaMergeOperator(node.getPlanNodeId(), operatorContext, children);
    }

    @Override
    public Operator visitCountMerge(CountSchemaMergeNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(n -> n.accept(this, context))
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              CountSchemaMergeNode.class.getSimpleName());
      return new CountMergeOperator(node.getPlanNodeId(), operatorContext, children);
    }

    @Override
    public Operator visitDevicesCount(DevicesCountNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              CountSchemaMergeNode.class.getSimpleName());
      return new DevicesCountOperator(
          node.getPlanNodeId(), operatorContext, node.getPath(), node.isPrefixPath());
    }

    @Override
    public Operator visitTimeSeriesCount(
        TimeSeriesCountNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              TimeSeriesCountNode.class.getSimpleName());
      return new TimeSeriesCountOperator(
          node.getPlanNodeId(), operatorContext, node.getPath(), node.isPrefixPath());
    }

    @Override
    public Operator visitLevelTimeSeriesCount(
        LevelTimeSeriesCountNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              LevelTimeSeriesCountNode.class.getSimpleName());
      return new LevelTimeSeriesCountOperator(
          node.getPlanNodeId(),
          operatorContext,
          node.getPath(),
          node.isPrefixPath(),
          node.getLevel());
    }

    @Override
    public Operator visitSeriesAggregate(
        SeriesAggregationScanNode node, LocalExecutionPlanContext context) {
      PartialPath seriesPath = node.getSeriesPath();
      boolean ascending = node.getScanOrder() == OrderBy.TIMESTAMP_ASC;
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SeriesAggregationScanNode.class.getSimpleName());

      SeriesAggregateScanOperator aggregateScanOperator =
          new SeriesAggregateScanOperator(
              node.getPlanNodeId(),
              seriesPath,
              node.getAllSensors(),
              operatorContext,
              node.getAggregateFuncList(),
              node.getTimeFilter(),
              ascending,
              node.getGroupByTimeParameter());

      context.addSourceOperator(aggregateScanOperator);
      context.addPath(seriesPath);

      return aggregateScanOperator;
    }

    @Override
    public Operator visitDeviceView(DeviceViewNode node, LocalExecutionPlanContext context) {
      return super.visitDeviceView(node, context);
    }

    @Override
    public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
      return super.visitFill(node, context);
    }

    @Override
    public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
      return super.visitFilter(node, context);
    }

    @Override
    public Operator visitFilterNull(FilterNullNode node, LocalExecutionPlanContext context) {
      return super.visitFilterNull(node, context);
    }

    @Override
    public Operator visitGroupByLevel(GroupByLevelNode node, LocalExecutionPlanContext context) {
      return super.visitGroupByLevel(node, context);
    }

    @Override
    public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
      Operator child = node.getChild().accept(this, context);
      return new LimitOperator(
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              LimitOperator.class.getSimpleName()),
          node.getLimit(),
          child);
    }

    @Override
    public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
      return super.visitOffset(node, context);
    }

    @Override
    public Operator visitRowBasedSeriesAggregate(
        AggregationNode node, LocalExecutionPlanContext context) {
      return super.visitRowBasedSeriesAggregate(node, context);
    }

    @Override
    public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
      return super.visitSort(node, context);
    }

    @Override
    public Operator visitTimeJoin(TimeJoinNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              TimeJoinOperator.class.getSimpleName());
      TimeComparator timeComparator =
          node.getMergeOrder() == OrderBy.TIMESTAMP_ASC
              ? ASC_TIME_COMPARATOR
              : DESC_TIME_COMPARATOR;
      List<OutputColumn> outputColumns = generateOutputColumns(node);
      List<ColumnMerger> mergers = createColumnMergers(outputColumns, timeComparator);
      List<TSDataType> outputColumnTypes = getOutputColumnTypes(node, context.getTypeProvider());
      return new TimeJoinOperator(
          operatorContext,
          children,
          node.getMergeOrder(),
          outputColumnTypes,
          mergers,
          timeComparator);
    }

    private List<OutputColumn> generateOutputColumns(TimeJoinNode node) {
      return makeLayout(node).values().stream()
          .map(inputLocations -> new OutputColumn(inputLocations, inputLocations.size() > 1))
          .collect(Collectors.toList());
    }

    private List<ColumnMerger> createColumnMergers(
        List<OutputColumn> outputColumns, TimeComparator timeComparator) {
      List<ColumnMerger> mergers = new ArrayList<>(outputColumns.size());
      for (OutputColumn outputColumn : outputColumns) {
        ColumnMerger merger;
        // only has one input column
        if (outputColumn.isSingleInputColumn()) {
          merger = new SingleColumnMerger(outputColumn.getInputLocation(0), timeComparator);
        } else if (!outputColumn.isOverlapped()) {
          // has more than one input columns but time of these input columns is not overlapped
          throw new UnsupportedOperationException(
              "has more than one input columns but time of these input columns is not overlapped is not supported");
        } else {
          // has more than one input columns and time of these input columns is overlapped
          throw new UnsupportedOperationException(
              "has more than one input columns and time of these input columns is overlapped is not supported");
        }
        mergers.add(merger);
      }
      return mergers;
    }

    @Override
    public Operator visitExchange(ExchangeNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SeriesScanOperator.class.getSimpleName());
      FragmentInstanceId localInstanceId = context.instanceContext.getId();
      FragmentInstanceId remoteInstanceId = node.getUpstreamInstanceId();

      ISourceHandle sourceHandle =
          DATA_BLOCK_MANAGER.createSourceHandle(
              localInstanceId.toThrift(),
              node.getPlanNodeId().getId(),
              node.getUpstreamEndpoint(),
              remoteInstanceId.toThrift(),
              context.instanceContext::failed);
      return new ExchangeOperator(operatorContext, sourceHandle, node.getUpstreamPlanNodeId());
    }

    @Override
    public Operator visitFragmentSink(FragmentSinkNode node, LocalExecutionPlanContext context) {
      Operator child = node.getChild().accept(this, context);

      FragmentInstanceId localInstanceId = context.instanceContext.getId();
      FragmentInstanceId targetInstanceId = node.getDownStreamInstanceId();
      ISinkHandle sinkHandle =
          DATA_BLOCK_MANAGER.createSinkHandle(
              localInstanceId.toThrift(),
              node.getDownStreamEndpoint(),
              targetInstanceId.toThrift(),
              node.getDownStreamPlanNodeId().getId(),
              context.instanceContext);
      context.setSinkHandle(sinkHandle);
      return child;
    }

    @Override
    public Operator visitSchemaFetch(SchemaFetchNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SchemaFetchOperator.class.getSimpleName());
      return new SchemaFetchOperator(
          node.getPlanNodeId(),
          operatorContext,
          node.getPatternTree(),
          ((SchemaDriverContext) (context.instanceContext.getDriverContext())).getSchemaRegion());
    }

    private Map<String, List<InputLocation>> makeLayout(PlanNode node) {
      Map<String, List<InputLocation>> outputMappings = new LinkedHashMap<>();
      int tsBlockIndex = 0;
      for (PlanNode childNode : node.getChildren()) {
        int valueColumnIndex = 0;
        for (String columnName : childNode.getOutputColumnNames()) {
          outputMappings
              .computeIfAbsent(columnName, key -> new ArrayList<>())
              .add(new InputLocation(tsBlockIndex, valueColumnIndex));
          valueColumnIndex++;
        }
        tsBlockIndex++;
      }
      return outputMappings;
    }

    private List<TSDataType> getOutputColumnTypes(PlanNode node, TypeProvider typeProvider) {
      return node.getOutputColumnNames().stream()
          .map(typeProvider::getType)
          .collect(Collectors.toList());
    }
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final LocalExecutionPlanner INSTANCE = new LocalExecutionPlanner();
  }

  private static class LocalExecutionPlanContext {
    private final FragmentInstanceContext instanceContext;
    private final List<PartialPath> paths;
    // Used to lock corresponding query resources
    private final List<DataSourceOperator> sourceOperators;
    private ISinkHandle sinkHandle;

    private int nextOperatorId = 0;

    private TypeProvider typeProvider;

    public LocalExecutionPlanContext(
        TypeProvider typeProvider, FragmentInstanceContext instanceContext) {
      this.typeProvider = typeProvider;
      this.instanceContext = instanceContext;
      this.paths = new ArrayList<>();
      this.sourceOperators = new ArrayList<>();
    }

    public LocalExecutionPlanContext(FragmentInstanceContext instanceContext) {
      this.instanceContext = instanceContext;
      this.paths = new ArrayList<>();
      this.sourceOperators = new ArrayList<>();
    }

    private int getNextOperatorId() {
      return nextOperatorId++;
    }

    public List<PartialPath> getPaths() {
      return paths;
    }

    public List<DataSourceOperator> getSourceOperators() {
      return sourceOperators;
    }

    public void addPath(PartialPath path) {
      paths.add(path);
    }

    public void addSourceOperator(DataSourceOperator sourceOperator) {
      sourceOperators.add(sourceOperator);
    }

    public ISinkHandle getSinkHandle() {
      return sinkHandle;
    }

    public void setSinkHandle(ISinkHandle sinkHandle) {
      requireNonNull(sinkHandle, "sinkHandle is null");
      checkArgument(this.sinkHandle == null, "There must be at most one SinkNode");

      this.sinkHandle = sinkHandle;
    }

    public TypeProvider getTypeProvider() {
      return typeProvider;
    }
  }
}
