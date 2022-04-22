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
package org.apache.iotdb.db.mpp.sql.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.buffer.DataBlockManager;
import org.apache.iotdb.db.mpp.buffer.DataBlockService;
import org.apache.iotdb.db.mpp.buffer.ISinkHandle;
import org.apache.iotdb.db.mpp.buffer.ISourceHandle;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.DataDriver;
import org.apache.iotdb.db.mpp.execution.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.SchemaDriver;
import org.apache.iotdb.db.mpp.execution.SchemaDriverContext;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.operator.process.TimeJoinOperator;
import org.apache.iotdb.db.mpp.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.mpp.operator.schema.DevicesCountOperator;
import org.apache.iotdb.db.mpp.operator.process.merge.ColumnMerger;
import org.apache.iotdb.db.mpp.operator.process.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.operator.schema.DevicesSchemaScanOperator;
import org.apache.iotdb.db.mpp.operator.schema.NodeTimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.operator.schema.SchemaFetchOperator;
import org.apache.iotdb.db.mpp.operator.schema.SchemaMergeOperator;
import org.apache.iotdb.db.mpp.operator.schema.TimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.operator.schema.TimeSeriesSchemaScanOperator;
import org.apache.iotdb.db.mpp.operator.source.DataSourceOperator;
import org.apache.iotdb.db.mpp.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.operator.source.SeriesAggregateScanOperator;
import org.apache.iotdb.db.mpp.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.OutputColumn;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.CountMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.NodeTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaFetchNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.List;
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

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public DataDriver plan(
      PlanNode plan,
      FragmentInstanceContext instanceContext,
      Filter timeFilter,
      DataRegion dataRegion) {
    LocalExecutionPlanContext context = new LocalExecutionPlanContext(instanceContext);

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
    public Operator visitSchemaMerge(SchemaMergeNode node, LocalExecutionPlanContext context) {
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
    public Operator visitCountMerge(CountMergeNode node, LocalExecutionPlanContext context) {
      List<Operator> children =
          node.getChildren().stream()
              .map(n -> n.accept(this, context))
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              CountMergeNode.class.getSimpleName());
      return new CountMergeOperator(node.getPlanNodeId(), operatorContext, children);
    }

    @Override
    public Operator visitDevicesCount(DevicesCountNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              CountMergeNode.class.getSimpleName());
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
    public Operator visitNodeTimeSeriesCount(
        NodeTimeSeriesCountNode node, LocalExecutionPlanContext context) {
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              NodeTimeSeriesCountNode.class.getSimpleName());
      return new NodeTimeSeriesCountOperator(
          node.getPlanNodeId(),
          operatorContext,
          node.getPath(),
          node.isPrefixPath(),
          node.getLevel());
    }

    @Override
    public Operator visitSeriesAggregate(
        SeriesAggregateScanNode node, LocalExecutionPlanContext context) {
      PartialPath seriesPath = node.getSeriesPath();
      boolean ascending = node.getScanOrder() == OrderBy.TIMESTAMP_ASC;
      OperatorContext operatorContext =
          context.instanceContext.addOperatorContext(
              context.getNextOperatorId(),
              node.getPlanNodeId(),
              SeriesAggregateScanNode.class.getSimpleName());

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
    public Operator visitDeviceMerge(DeviceMergeNode node, LocalExecutionPlanContext context) {
      return super.visitDeviceMerge(node, context);
    }

    @Override
    public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
      return super.visitFill(node, context);
    }

    @Override
    public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
      PlanNode child = node.getChild();

      IExpression filterExpression = node.getPredicate();
      List<String> outputSymbols = node.getOutputColumnNames();
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
        AggregateNode node, LocalExecutionPlanContext context) {
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
      List<OutputColumn> outputColumns = node.getOutputColumns();
      List<ColumnMerger> mergers = createColumnMergers(outputColumns);
      return new TimeJoinOperator(
          operatorContext, children, node.getMergeOrder(), node.getOutputColumnTypes(), mergers);
    }

    private List<ColumnMerger> createColumnMergers(List<OutputColumn> outputColumns) {
      List<ColumnMerger> mergers = new ArrayList<>(outputColumns.size());
      for (OutputColumn outputColumn : outputColumns) {
        ColumnMerger merger;
        // only has one input column
        if (outputColumn.isSingleInputColumn()) {
          merger = new SingleColumnMerger(outputColumn.getInputLocation(0));
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
      TEndPoint source = node.getUpstreamEndpoint();

      ISourceHandle sourceHandle =
          DATA_BLOCK_MANAGER.createSourceHandle(
              localInstanceId.toThrift(),
              node.getPlanNodeId().getId(),
              new TEndPoint(
                  source.getIp(),
                  IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort()),
              remoteInstanceId.toThrift());
      return new ExchangeOperator(operatorContext, sourceHandle, node.getUpstreamPlanNodeId());
    }

    @Override
    public Operator visitFragmentSink(FragmentSinkNode node, LocalExecutionPlanContext context) {
      Operator child = node.getChild().accept(this, context);
      TEndPoint target = node.getDownStreamEndpoint();
      FragmentInstanceId localInstanceId = context.instanceContext.getId();
      FragmentInstanceId targetInstanceId = node.getDownStreamInstanceId();
      ISinkHandle sinkHandle =
          DATA_BLOCK_MANAGER.createSinkHandle(
              localInstanceId.toThrift(),
              new TEndPoint(
                  target.getIp(),
                  IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort()),
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
  }
}
