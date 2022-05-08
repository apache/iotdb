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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
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
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.FilterOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.ColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.MultiColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.NonOverlappedMultiColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.merge.TimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.DevicesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.DevicesSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.LevelTimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.TimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.TimeSeriesSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanOperator;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OutputColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
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
    public Operator visitAlignedSeriesScan(
        AlignedSeriesScanNode node, LocalExecutionPlanContext context) {
      AlignedPath seriesPath = node.getAlignedPath();
      boolean ascending = node.getScanOrder() == OrderBy.TIMESTAMP_ASC;
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              AlignedSeriesScanOperator.class.getSimpleName());

      AlignedSeriesScanOperator seriesScanOperator =
          new AlignedSeriesScanOperator(
              node.getPlanNodeId(),
              seriesPath,
              operatorContext,
              node.getTimeFilter(),
              node.getValueFilter(),
              ascending);

      context.addSourceOperator(seriesScanOperator);
      context.addPath(seriesPath);

      return seriesScanOperator;
    }

    @Override
    public Operator visitSchemaQueryScan(
        SchemaQueryScanNode node, LocalExecutionPlanContext context) {
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
    public Operator visitSchemaQueryMerge(
        SchemaQueryMergeNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(n -> n.accept(this, context))
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SchemaQueryMergeOperator.class.getSimpleName());
      return new SchemaQueryMergeOperator(node.getPlanNodeId(), operatorContext, children);
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

      List<Aggregator> aggregators = new ArrayList<>();
      node.getAggregationDescriptorList()
          .forEach(
              o ->
                  new Aggregator(
                      AccumulatorFactory.createAccumulator(
                          o.getAggregationType(), node.getSeriesPath().getSeriesType(), ascending),
                      o.getStep()));
      SeriesAggregateScanOperator aggregateScanOperator =
          new SeriesAggregateScanOperator(
              node.getPlanNodeId(),
              seriesPath,
              node.getAllSensors(),
              operatorContext,
              aggregators,
              node.getTimeFilter(),
              ascending,
              node.getGroupByTimeParameter());

      context.addSourceOperator(aggregateScanOperator);
      context.addPath(seriesPath);

      return aggregateScanOperator;
    }

    @Override
    public Operator visitDeviceView(DeviceViewNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              DeviceViewNode.class.getSimpleName());
      List<Operator> children =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(Collectors.toList());
      return new DeviceViewOperator(operatorContext, node.getDevices(), children, null, null);
    }

    @Override
    public Operator visitDeviceMerge(DeviceMergeNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              DeviceViewNode.class.getSimpleName());
      List<Operator> children =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(Collectors.toList());
      List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
      TimeSelector selector = null;
      TimeComparator timeComparator = null;
      for (OrderBy orderBy : node.getMergeOrders()) {
        switch (orderBy) {
          case TIMESTAMP_ASC:
            selector = new TimeSelector(node.getChildren().size() << 1, true);
            timeComparator = ASC_TIME_COMPARATOR;
            break;
          case TIMESTAMP_DESC:
            selector = new TimeSelector(node.getChildren().size() << 1, false);
            timeComparator = DESC_TIME_COMPARATOR;
            break;
        }
      }
      return new DeviceMergeOperator(
          operatorContext, node.getDevices(), children, dataTypes, selector, timeComparator);
    }

    @Override
    public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
      return super.visitFill(node, context);
    }

    @Override
    public Operator visitTransform(TransformNode node, LocalExecutionPlanContext context) {
      final OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              TransformNode.class.getSimpleName());
      final Operator inputOperator = generateOnlyChildOperator(node, context);
      final List<TSDataType> inputDataTypes = getOutputColumnTypes(node, context.getTypeProvider());

      try {
        return new TransformOperator(
            operatorContext,
            inputOperator,
            inputDataTypes,
            node.getOutputExpressions(),
            node.isKeepNull(),
            node.getZoneId(),
            context.getTypeProvider());
      } catch (QueryProcessException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
      final OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(), node.getPlanNodeId(), FilterNode.class.getSimpleName());
      final Operator inputOperator = generateOnlyChildOperator(node, context);
      final List<TSDataType> inputDataTypes = getOutputColumnTypes(node, context.getTypeProvider());

      try {
        return new FilterOperator(
            operatorContext,
            inputOperator,
            inputDataTypes,
            node.getPredicate(),
            node.getOutputExpressions(),
            node.isKeepNull(),
            node.getZoneId(),
            context.getTypeProvider());
      } catch (QueryProcessException | IOException e) {
        throw new RuntimeException(e);
      }
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
      Operator child = node.getChild().accept(this, context);
      return new OffsetOperator(
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              OffsetOperator.class.getSimpleName()),
          node.getOffset(),
          child);
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
      // TODO we should also sort the InputLocation for each column if they are not overlapped
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
          merger = new SingleColumnMerger(outputColumn.getSourceLocation(0), timeComparator);
        } else if (outputColumn.isOverlapped()) {
          // has more than one input columns but time of these input columns is overlapped
          merger = new MultiColumnMerger(outputColumn.getSourceLocations());
        } else {
          // has more than one input columns and time of these input columns is not overlapped
          merger =
              new NonOverlappedMultiColumnMerger(outputColumn.getSourceLocations(), timeComparator);
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
    public Operator visitSchemaFetchMerge(
        SchemaFetchMergeNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(n -> n.accept(this, context))
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SchemaFetchMergeOperator.class.getSimpleName());
      return new SchemaFetchMergeOperator(node.getPlanNodeId(), operatorContext, children);
    }

    @Override
    public Operator visitSchemaFetchScan(
        SchemaFetchScanNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SchemaFetchScanOperator.class.getSimpleName());
      return new SchemaFetchScanOperator(
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

    private Operator generateOnlyChildOperator(PlanNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(Collectors.toList());
      Validate.isTrue(children.size() == 1);
      return children.get(0);
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
