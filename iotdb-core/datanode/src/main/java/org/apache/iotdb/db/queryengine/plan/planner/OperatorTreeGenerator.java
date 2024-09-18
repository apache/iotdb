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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.execution.aggregation.Accumulator;
import org.apache.iotdb.db.queryengine.execution.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.slidingwindow.SlidingWindowAggregatorFactory;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil;
import org.apache.iotdb.db.queryengine.execution.operator.ExplainAnalyzeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ActiveRegionScanMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.AggregationMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.AggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.ColumnInjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.DeviceViewIntoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.FillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.IntoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.LinearFillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.RawDataAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SlidingWindowAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TagAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeTopKOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFillFilter;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter.FixedIntervalFillFilter;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter.MonthIntervalMSFillFilter;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter.MonthIntervalNSFillFilter;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter.MonthIntervalUSFillFilter;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.identity.IdentityFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.identity.IdentityLinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.linear.DoubleLinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.linear.FloatLinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.linear.IntLinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.linear.LongLinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous.BinaryPreviousFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous.BooleanPreviousFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous.DoublePreviousFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous.FloatPreviousFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous.IntPreviousFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.previous.LongPreviousFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.HorizontallyConcatOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.InnerTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.LeftOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MultiColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.NonOverlappedMultiColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.AlignedUpdateLastCacheOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.AlignedUpdateViewPathLastCacheOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryCollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQuerySortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryTransformOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.UpdateViewPathLastCacheOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.CountGroupByLevelMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.CountGroupByLevelScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.NodeManageMemoryMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.NodePathsConvertOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.NodePathsCountOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaCountOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaFetchMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaFetchScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaQueryMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaQueryOrderByHeatOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaQueryScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.SchemaSourceFactory;
import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.execution.operator.sink.ShuffleHelperOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ActiveDeviceRegionScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ActiveTimeSeriesRegionScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ShowQueriesOperator;
import org.apache.iotdb.db.queryengine.execution.operator.window.ConditionWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.CountWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.SessionWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.TimeWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.VariationWindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.WindowParameter;
import org.apache.iotdb.db.queryengine.execution.operator.window.WindowType;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ColumnTransformerVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimatorFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DeviceSchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DevicesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.LogicalViewSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsConvertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.PathsUsingTemplateScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SeriesSchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AI.InferenceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ActiveRegionScanMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryTransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByConditionParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByCountParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupBySessionParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByVariationParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OutputColumn;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.literal.Literal;
import org.apache.iotdb.db.queryengine.statistics.StatisticsManager;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;
import org.apache.iotdb.db.utils.columngenerator.ColumnGenerator;
import org.apache.iotdb.db.utils.columngenerator.ColumnGeneratorType;
import org.apache.iotdb.db.utils.columngenerator.SlidingTimeColumnGenerator;
import org.apache.iotdb.db.utils.columngenerator.parameter.ColumnGeneratorParameter;
import org.apache.iotdb.db.utils.columngenerator.parameter.SlidingTimeColumnGeneratorParameter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeGt;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators.TimeGtEq;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TimeDuration;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.calculateMaxAggregationResultSize;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.calculateMaxAggregationResultSizeForLastQuery;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.getOutputColumnSizePerLine;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.initTimeRangeIterator;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparator;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor.getAggregationTypeByFuncName;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions.updateFilterUsingTTL;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.apache.iotdb.db.utils.TimestampPrecisionUtils.TIMESTAMP_PRECISION;

/** This Visitor is responsible for transferring PlanNode Tree to Operator Tree. */
public class OperatorTreeGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperatorTreeGenerator.class);

  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  private static final DataNodeSchemaCache DATA_NODE_SCHEMA_CACHE =
      DataNodeSchemaCache.getInstance();

  private static final TimeComparator ASC_TIME_COMPARATOR = new AscTimeComparator();

  private static final TimeComparator DESC_TIME_COMPARATOR = new DescTimeComparator();

  private static final IdentityFill IDENTITY_FILL = new IdentityFill();

  private static final IdentityLinearFill IDENTITY_LINEAR_FILL = new IdentityLinearFill();

  private static final Comparator<Binary> ASC_BINARY_COMPARATOR = Comparator.naturalOrder();

  private static final Comparator<Binary> DESC_BINARY_COMPARATOR = Comparator.reverseOrder();

  private static final String UNKNOWN_DATATYPE = "Unknown data type: ";

  @Override
  public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
  }

  @Override
  public Operator visitSeriesScan(SeriesScanNode node, LocalExecutionPlanContext context) {
    NonAlignedFullPath seriesPath =
        (NonAlignedFullPath) IFullPath.convertToIFullPath(node.getSeriesPath());

    SeriesScanOptions.Builder scanOptionsBuilder = getSeriesScanOptionsBuilder(context);
    scanOptionsBuilder.withAllSensors(
        context.getAllSensors(seriesPath.getDeviceId(), seriesPath.getMeasurement()));

    Expression pushDownPredicate = node.getPushDownPredicate();
    boolean predicateCanPushIntoScan = canPushIntoScan(pushDownPredicate);
    if (pushDownPredicate != null && predicateCanPushIntoScan) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate,
              Collections.singletonList(node.getSeriesPath().getMeasurement()),
              context.getTypeProvider().getTemplatedInfo() != null,
              context.getTypeProvider(),
              context.getZoneId()));
    }
    if (pushDownPredicate == null || predicateCanPushIntoScan) {
      scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
      scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SeriesScanOperator.class.getSimpleName());
    operatorContext.recordSpecifiedInfo("SeriesPath", seriesPath.toString());
    SeriesScanOperator seriesScanOperator =
        new SeriesScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            seriesPath,
            node.getScanOrder(),
            scanOptionsBuilder.build());

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(seriesScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(seriesPath);
    context.getDriverContext().setInputDriver(true);

    if (!predicateCanPushIntoScan) {
      checkState(!context.isBuildPlanUseTemplate(), "Push down predicate is not supported yet");
      Operator rootOperator =
          constructFilterOperator(
              pushDownPredicate,
              seriesScanOperator,
              Collections.singletonList(ExpressionFactory.timeSeries(node.getSeriesPath()))
                  .toArray(new Expression[0]),
              Collections.singletonList(node.getSeriesPath().getSeriesType()),
              makeLayout(Collections.singletonList(node)),
              false,
              node.getPlanNodeId(),
              node.getScanOrder(),
              context);
      if (node.getPushDownOffset() > 0) {
        rootOperator =
            new OffsetOperator(
                context
                    .getDriverContext()
                    .addOperatorContext(
                        context.getNextOperatorId(),
                        node.getPlanNodeId(),
                        OffsetOperator.class.getSimpleName()),
                node.getPushDownOffset(),
                rootOperator);
      }
      if (node.getPushDownLimit() > 0) {
        rootOperator =
            new LimitOperator(
                context
                    .getDriverContext()
                    .addOperatorContext(
                        context.getNextOperatorId(),
                        node.getPlanNodeId(),
                        LimitOperator.class.getSimpleName()),
                node.getPushDownLimit(),
                rootOperator);
      }
      return rootOperator;
    }
    return seriesScanOperator;
  }

  @Override
  public Operator visitAlignedSeriesScan(
      AlignedSeriesScanNode node, LocalExecutionPlanContext context) {
    AlignedFullPath seriesPath =
        (AlignedFullPath) IFullPath.convertToIFullPath(node.getAlignedPath());

    SeriesScanOptions.Builder scanOptionsBuilder = getSeriesScanOptionsBuilder(context);
    scanOptionsBuilder.withAllSensors(
        new HashSet<>(
            context.isBuildPlanUseTemplate()
                ? context.getTemplatedInfo().getMeasurementList()
                : seriesPath.getMeasurementList()));

    Expression pushDownPredicate = node.getPushDownPredicate();
    boolean predicateCanPushIntoScan = canPushIntoScan(pushDownPredicate);
    if (pushDownPredicate != null && predicateCanPushIntoScan) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate,
              node.getAlignedPath().getMeasurementList(),
              context.getTypeProvider().getTemplatedInfo() != null,
              context.getTypeProvider(),
              context.getZoneId()));
    }
    if (pushDownPredicate == null || predicateCanPushIntoScan) {
      scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
      scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesScanOperator.class.getSimpleName());

    int maxTsBlockLineNum = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    if (context.getTypeProvider().getTemplatedInfo() != null) {
      maxTsBlockLineNum =
          (int)
              Math.min(
                  context.getTypeProvider().getTemplatedInfo().getLimitValue(), maxTsBlockLineNum);
    }

    AlignedSeriesScanOperator seriesScanOperator =
        new AlignedSeriesScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            seriesPath,
            node.getScanOrder(),
            scanOptionsBuilder.build(),
            node.isQueryAllSensors(),
            context.getTypeProvider().getTemplatedInfo() != null
                ? context.getTypeProvider().getTemplatedInfo().getDataTypes()
                : null,
            maxTsBlockLineNum);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(seriesScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(seriesPath);
    context.getDriverContext().setInputDriver(true);

    if (!predicateCanPushIntoScan) {
      if (context.isBuildPlanUseTemplate()) {
        TemplatedInfo templatedInfo = context.getTemplatedInfo();
        return constructFilterOperator(
            pushDownPredicate,
            seriesScanOperator,
            templatedInfo.getProjectExpressions(),
            templatedInfo.getDataTypes(),
            templatedInfo.getFilterLayoutMap(),
            templatedInfo.isKeepNull(),
            node.getPlanNodeId(),
            templatedInfo.getScanOrder(),
            context);
      }

      AlignedPath alignedPath = node.getAlignedPath();
      List<Expression> expressions = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (int i = 0; i < alignedPath.getMeasurementList().size(); i++) {
        expressions.add(ExpressionFactory.timeSeries(alignedPath.getSubMeasurementPath(i)));
        dataTypes.add(alignedPath.getSubMeasurementDataType(i));
      }

      Operator rootOperator =
          constructFilterOperator(
              pushDownPredicate,
              seriesScanOperator,
              expressions.toArray(new Expression[0]),
              dataTypes,
              makeLayout(Collections.singletonList(node)),
              false,
              node.getPlanNodeId(),
              node.getScanOrder(),
              context);

      if (node.getPushDownOffset() > 0) {
        rootOperator =
            new OffsetOperator(
                context
                    .getDriverContext()
                    .addOperatorContext(
                        context.getNextOperatorId(),
                        node.getPlanNodeId(),
                        OffsetOperator.class.getSimpleName()),
                node.getPushDownOffset(),
                rootOperator);
      }
      if (node.getPushDownLimit() > 0) {
        rootOperator =
            new LimitOperator(
                context
                    .getDriverContext()
                    .addOperatorContext(
                        context.getNextOperatorId(),
                        node.getPlanNodeId(),
                        LimitOperator.class.getSimpleName()),
                node.getPushDownLimit(),
                rootOperator);
      }
      return rootOperator;
    }
    return seriesScanOperator;
  }

  private boolean canPushIntoScan(Expression pushDownPredicate) {
    return pushDownPredicate == null || PredicateUtils.predicateCanPushIntoScan(pushDownPredicate);
  }

  @Override
  public Operator visitProject(ProjectNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ProjectOperator.class.getSimpleName());

    List<String> inputColumnNames;
    List<String> outputColumnNames = node.getOutputColumnNames();
    if (outputColumnNames == null) {
      if (context.getTypeProvider().getTemplatedInfo().isAggregationQuery()) {
        outputColumnNames = context.getTypeProvider().getTemplatedInfo().getDeviceViewOutputNames();
        outputColumnNames = outputColumnNames.subList(1, outputColumnNames.size());
        inputColumnNames = node.getChild().getOutputColumnNames();
      } else {
        outputColumnNames = context.getTypeProvider().getTemplatedInfo().getDeviceViewOutputNames();
        // skip device column
        outputColumnNames = outputColumnNames.subList(1, outputColumnNames.size());
        inputColumnNames = context.getTypeProvider().getTemplatedInfo().getMeasurementList();
      }
    } else {
      inputColumnNames = node.getChild().getOutputColumnNames();
    }

    if (inputColumnNames.equals(outputColumnNames)) {
      // no need to project
      return child;
    }

    List<Integer> remainingColumnIndexList = new ArrayList<>();
    for (String outputColumnName : outputColumnNames) {
      int index = inputColumnNames.indexOf(outputColumnName);
      if (index < 0) {
        throw new IllegalStateException(
            String.format("Cannot find column [%s] in child's output", outputColumnName));
      }
      remainingColumnIndexList.add(index);
    }
    return new ProjectOperator(operatorContext, child, remainingColumnIndexList);
  }

  @Override
  public Operator visitSeriesAggregationScan(
      SeriesAggregationScanNode node, LocalExecutionPlanContext context) {
    NonAlignedFullPath seriesPath =
        (NonAlignedFullPath) IFullPath.convertToIFullPath(node.getSeriesPath());
    boolean ascending = node.getScanOrder() == ASC;
    List<AggregationDescriptor> aggregationDescriptors = node.getAggregationDescriptorList();
    List<Aggregator> aggregators = new ArrayList<>();
    aggregationDescriptors.forEach(
        o ->
            aggregators.add(
                new Aggregator(
                    AccumulatorFactory.createAccumulator(
                        o.getAggregationFuncName(),
                        o.getAggregationType(),
                        Collections.singletonList(node.getSeriesPath().getSeriesType()),
                        o.getInputExpressions(),
                        o.getInputAttributes(),
                        ascending,
                        o.getStep().isInputRaw()),
                    o.getStep())));

    GroupByTimeParameter groupByTimeParameter = node.getGroupByTimeParameter();
    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, true);
    long maxReturnSize =
        AggregationUtil.calculateMaxAggregationResultSize(
            node.getAggregationDescriptorList(), timeRangeIterator, context.getTypeProvider());

    SeriesScanOptions.Builder scanOptionsBuilder = getSeriesScanOptionsBuilder(context);
    scanOptionsBuilder.withAllSensors(
        context.getAllSensors(seriesPath.getDeviceId(), seriesPath.getMeasurement()));

    Expression pushDownPredicate = node.getPushDownPredicate();
    if (pushDownPredicate != null) {
      checkArgument(PredicateUtils.predicateCanPushIntoScan(pushDownPredicate));
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate,
              Collections.singletonList(node.getSeriesPath().getMeasurement()),
              context.getTypeProvider().getTemplatedInfo() != null,
              context.getTypeProvider(),
              context.getZoneId()));
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SeriesAggregationScanOperator.class.getSimpleName());
    boolean canUseStatistics =
        !TSDataType.BLOB.equals(node.getSeriesPath().getSeriesType())
            || (aggregationDescriptors.stream()
                .noneMatch(o -> !judgeCanUseStatistics(o.getAggregationType(), TSDataType.BLOB)));
    SeriesAggregationScanOperator aggregateScanOperator =
        new SeriesAggregationScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            node.getScanOrder(),
            node.isOutputEndTime(),
            scanOptionsBuilder.build(),
            operatorContext,
            aggregators,
            timeRangeIterator,
            node.getGroupByTimeParameter(),
            maxReturnSize,
            canUseStatistics);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(aggregateScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(seriesPath);
    context.getDriverContext().setInputDriver(true);
    return aggregateScanOperator;
  }

  @Override
  public Operator visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, LocalExecutionPlanContext context) {

    if (context.isBuildPlanUseTemplate()) {
      Ordering scanOrder = context.getTemplatedInfo().getScanOrder();
      List<AggregationDescriptor> aggregationDescriptors;
      if (node.getDescriptorType() == 0 || node.getDescriptorType() == 2) {
        aggregationDescriptors = context.getTemplatedInfo().getAscendingDescriptorList();
      } else {
        scanOrder = scanOrder.reverse();
        aggregationDescriptors = context.getTemplatedInfo().getDescendingDescriptorList();
      }

      return constructAlignedSeriesAggregationScanOperator(
          node.getPlanNodeId(),
          node.getAlignedPath(),
          aggregationDescriptors,
          context.getTemplatedInfo().getPushDownPredicate(),
          scanOrder,
          context.getTemplatedInfo().getGroupByTimeParameter(),
          context.getTemplatedInfo().isOutputEndTime(),
          context);
    }

    return constructAlignedSeriesAggregationScanOperator(
        node.getPlanNodeId(),
        node.getAlignedPath(),
        node.getAggregationDescriptorList(),
        node.getPushDownPredicate(),
        node.getScanOrder(),
        node.getGroupByTimeParameter(),
        node.isOutputEndTime(),
        context);
  }

  private Operator constructAlignedSeriesAggregationScanOperator(
      PlanNodeId planNodeId,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Expression pushDownPredicate,
      Ordering scanOrder,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime,
      LocalExecutionPlanContext context) {
    AlignedFullPath seriesPath = (AlignedFullPath) IFullPath.convertToIFullPath(alignedPath);
    boolean ascending = scanOrder == ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    boolean canUseStatistics = true;
    for (AggregationDescriptor descriptor : aggregationDescriptorList) {
      checkArgument(
          descriptor.getInputExpressions().size() == 1,
          "descriptor's input expression size is not 1");

      Expression expression = descriptor.getInputExpressions().get(0);
      if (expression instanceof TimeSeriesOperand) {
        String inputSeries =
            ((TimeSeriesOperand) (descriptor.getInputExpressions().get(0)))
                .getPath()
                .getMeasurement();
        int seriesIndex = alignedPath.getMeasurementList().indexOf(inputSeries);
        TSDataType seriesDataType = alignedPath.getSchemaList().get(seriesIndex).getType();
        if (!judgeCanUseStatistics(descriptor.getAggregationType(), seriesDataType)) {
          canUseStatistics = false;
        }
        aggregators.add(
            new Aggregator(
                AccumulatorFactory.createAccumulator(
                    descriptor.getAggregationFuncName(),
                    descriptor.getAggregationType(),
                    Collections.singletonList(seriesDataType),
                    descriptor.getInputExpressions(),
                    descriptor.getInputAttributes(),
                    ascending,
                    descriptor.getStep().isInputRaw()),
                descriptor.getStep(),
                Collections.singletonList(
                    new InputLocation[] {new InputLocation(0, seriesIndex)})));
      } else if (expression instanceof TimestampOperand) {
        aggregators.add(
            new Aggregator(
                AccumulatorFactory.createAccumulator(
                    descriptor.getAggregationFuncName(),
                    descriptor.getAggregationType(),
                    Collections.singletonList(TSDataType.INT64),
                    descriptor.getInputExpressions(),
                    descriptor.getInputAttributes(),
                    ascending,
                    descriptor.getStep().isInputRaw()),
                descriptor.getStep(),
                Collections.singletonList(new InputLocation[] {new InputLocation(0, -1)})));
      } else {
        throw new IllegalArgumentException(
            "descriptor's input expression must be TimeSeriesOperand/TimestampOperand, current is "
                + expression);
      }
    }

    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, true);
    long maxReturnSize =
        AggregationUtil.calculateMaxAggregationResultSize(
            aggregationDescriptorList, timeRangeIterator, context.getTypeProvider());

    SeriesScanOptions.Builder scanOptionsBuilder = getSeriesScanOptionsBuilder(context);
    scanOptionsBuilder.withAllSensors(new HashSet<>(alignedPath.getMeasurementList()));

    if (pushDownPredicate != null) {
      checkArgument(PredicateUtils.predicateCanPushIntoScan(pushDownPredicate));
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate,
              alignedPath.getMeasurementList(),
              context.getTypeProvider().getTemplatedInfo() != null,
              context.getTypeProvider(),
              context.getZoneId()));
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                planNodeId,
                AlignedSeriesAggregationScanOperator.class.getSimpleName());
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        new AlignedSeriesAggregationScanOperator(
            planNodeId,
            seriesPath,
            scanOrder,
            outputEndTime,
            scanOptionsBuilder.build(),
            operatorContext,
            aggregators,
            timeRangeIterator,
            groupByTimeParameter,
            maxReturnSize,
            canUseStatistics);

    ((DataDriverContext) context.getDriverContext())
        .addSourceOperator(seriesAggregationScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(seriesPath);
    context.getDriverContext().setInputDriver(true);
    return seriesAggregationScanOperator;
  }

  private boolean judgeCanUseStatistics(
      final TAggregationType aggregationType, final TSDataType seriesType) {
    return !TSDataType.BLOB.equals(seriesType)
        || (!TAggregationType.LAST_VALUE.equals(aggregationType)
            && !TAggregationType.FIRST_VALUE.equals(aggregationType));
  }

  private SeriesScanOptions.Builder getSeriesScanOptionsBuilder(LocalExecutionPlanContext context) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();

    Filter globalTimeFilter = context.getGlobalTimeFilter();
    if (globalTimeFilter != null) {
      // time filter may be stateful, so we need to copy it
      scanOptionsBuilder.withGlobalTimeFilter(globalTimeFilter.copy());
    }

    return scanOptionsBuilder;
  }

  @Override
  public Operator visitSchemaQueryOrderByHeat(
      SchemaQueryOrderByHeatNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream().map(n -> n.accept(this, context)).collect(Collectors.toList());

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryOrderByHeatOperator.class.getSimpleName());

    return new SchemaQueryOrderByHeatOperator(operatorContext, children);
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
    } else if (node instanceof NodePathsSchemaScanNode) {
      return visitNodePathsSchemaScan((NodePathsSchemaScanNode) node, context);
    } else if (node instanceof PathsUsingTemplateScanNode) {
      return visitPathsUsingTemplateScan((PathsUsingTemplateScanNode) node, context);
    } else if (node instanceof LogicalViewSchemaScanNode) {
      return visitLogicalViewSchemaScan((LogicalViewSchemaScanNode) node, context);
    }
    return visitPlan(node, context);
  }

  @Override
  public Operator visitTimeSeriesSchemaScan(
      TimeSeriesSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getTimeSeriesSchemaScanSource(
            node.getPath(),
            node.isPrefixPath(),
            node.getLimit(),
            node.getOffset(),
            node.getSchemaFilter(),
            node.getTemplateMap(),
            node.getScope()));
  }

  @Override
  public Operator visitDevicesSchemaScan(
      DevicesSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getDeviceSchemaSource(
            node.getPath(),
            node.isPrefixPath(),
            node.getLimit(),
            node.getOffset(),
            node.isHasSgCol(),
            node.getSchemaFilter(),
            node.getScope()));
  }

  @Override
  public Operator visitSchemaQueryMerge(
      SchemaQueryMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryMergeOperator.class.getSimpleName());
    return new SchemaQueryMergeOperator(operatorContext, children);
  }

  @Override
  public Operator visitCountMerge(CountSchemaMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CountMergeOperator.class.getSimpleName());
    if (node.getChildren().get(0) instanceof LevelTimeSeriesCountNode) {
      return new CountGroupByLevelMergeOperator(operatorContext, children);
    } else {
      return new CountMergeOperator(operatorContext, children);
    }
  }

  @Override
  public Operator visitDevicesCount(DevicesCountNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaCountOperator.class.getSimpleName());
    return new SchemaCountOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getDeviceSchemaSource(
            node.getPath(), node.isPrefixPath(), node.getScope()));
  }

  @Override
  public Operator visitTimeSeriesCount(
      TimeSeriesCountNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaCountOperator.class.getSimpleName());
    return new SchemaCountOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            node.getPath(),
            node.isPrefixPath(),
            node.getSchemaFilter(),
            node.getTemplateMap(),
            node.getScope()));
  }

  @Override
  public Operator visitLevelTimeSeriesCount(
      LevelTimeSeriesCountNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CountGroupByLevelScanOperator.class.getSimpleName());
    return new CountGroupByLevelScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        node.getLevel(),
        SchemaSourceFactory.getTimeSeriesSchemaCountSource(
            node.getPath(),
            node.isPrefixPath(),
            node.getSchemaFilter(),
            node.getTemplateMap(),
            node.getScope()));
  }

  @Override
  public Operator visitNodePathsSchemaScan(
      NodePathsSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getNodeSchemaSource(
            node.getPrefixPath(), node.getLevel(), node.getScope()));
  }

  @Override
  public Operator visitNodeManagementMemoryMerge(
      NodeManagementMemoryMergeNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodeManageMemoryMergeOperator.class.getSimpleName());
    return new NodeManageMemoryMergeOperator(operatorContext, node.getData(), child);
  }

  @Override
  public Operator visitNodePathConvert(
      NodePathsConvertNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodePathsConvertOperator.class.getSimpleName());
    return new NodePathsConvertOperator(operatorContext, child);
  }

  @Override
  public Operator visitNodePathsCount(NodePathsCountNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodePathsCountOperator.class.getSimpleName());
    return new NodePathsCountOperator(operatorContext, child);
  }

  @Override
  public Operator visitSingleDeviceView(
      SingleDeviceViewNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SingleDeviceViewOperator.class.getSimpleName());
    Operator child = node.getChild().accept(this, context);
    List<Integer> deviceColumnIndex =
        context.getTemplatedInfo() != null
            ? context.getTemplatedInfo().getDeviceToMeasurementIndexes()
            : node.getDeviceToMeasurementIndexes();
    List<TSDataType> outputColumnTypes =
        node.isCacheOutputColumnNames()
            ? getOutputColumnTypes(node, context.getTypeProvider())
            : context.getCachedDataTypes();
    if (outputColumnTypes == null || outputColumnTypes.isEmpty()) {
      throw new IllegalStateException("OutputColumTypes should not be null/empty");
    }
    return new SingleDeviceViewOperator(
        operatorContext, node.getDevice().toString(), child, deviceColumnIndex, outputColumnTypes);
  }

  @Override
  public Operator visitDeviceView(DeviceViewNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                DeviceViewOperator.class.getSimpleName());
    List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    List<List<Integer>> deviceColumnIndex = new ArrayList<>(node.getDevices().size());
    if (context.getTemplatedInfo() != null) {
      for (int i = 0; i < node.getDevices().size(); i++) {
        deviceColumnIndex.add(context.getTemplatedInfo().getDeviceToMeasurementIndexes());
      }
    } else {
      deviceColumnIndex =
          node.getDevices().stream()
              .map(deviceName -> node.getDeviceToMeasurementIndexesMap().get(deviceName))
              .collect(Collectors.toList());
    }

    List<TSDataType> outputColumnTypes = getOutputColumnTypes(node, context.getTypeProvider());

    return new DeviceViewOperator(
        operatorContext, node.getDevices(), children, deviceColumnIndex, outputColumnTypes);
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TreeMergeSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    context.setCachedDataTypes(dataTypes);
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    List<SortItem> sortItemList = node.getMergeOrderParameter().getSortItemList();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemList.size());
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemList.size());
    genSortInformation(
        node.getOutputColumnNames(),
        dataTypes,
        sortItemList,
        sortItemIndexList,
        sortItemDataTypeList);
    return new TreeMergeSortOperator(
        operatorContext,
        children,
        dataTypes,
        getComparator(sortItemList, sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitAggregationMergeSort(
      AggregationMergeSortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TreeMergeSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);

    List<SortItem> sortItemList = node.getMergeOrderParameter().getSortItemList();
    if (!sortItemList.get(0).getSortKey().equalsIgnoreCase("Device")) {
      throw new IllegalArgumentException(
          "Only order by device align by device support AggregationMergeSortNode.");
    }

    boolean timeAscending = true;
    for (SortItem sortItem : sortItemList) {
      if (TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(sortItem.getSortKey())
          && (sortItem.getOrdering() == Ordering.DESC)) {
        timeAscending = false;
        break;
      }
    }

    List<Accumulator> accumulators = new ArrayList<>();
    for (Expression expression : node.getSelectExpressions()) {
      if (expression instanceof FunctionExpression) {
        FunctionExpression functionExpression = (FunctionExpression) expression;
        String aggregationName = functionExpression.getFunctionName();
        Accumulator accumulator =
            AccumulatorFactory.createAccumulator(
                aggregationName,
                getAggregationTypeByFuncName(aggregationName),
                Collections.singletonList(
                    context
                        .getTypeProvider()
                        .getTreeModelType(functionExpression.getOutputSymbol())),
                functionExpression.getExpressions(),
                functionExpression.getFunctionAttributes(),
                timeAscending,
                false);
        accumulators.add(accumulator);
      }
    }

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemList.size());
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemList.size());
    genSortInformation(
        node.getOutputColumnNames(),
        dataTypes,
        sortItemList,
        sortItemIndexList,
        sortItemDataTypeList);
    return new AggregationMergeSortOperator(
        operatorContext,
        children,
        dataTypes,
        accumulators,
        node.isHasGroupBy(),
        getComparator(sortItemList, sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TreeTopKOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    context.setCachedDataTypes(dataTypes);
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    List<SortItem> sortItemList = node.getMergeOrderParameter().getSortItemList();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemList.size());
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemList.size());
    genSortInformation(
        node.getOutputColumnNames(),
        dataTypes,
        sortItemList,
        sortItemIndexList,
        sortItemDataTypeList);
    return new TreeTopKOperator(
        operatorContext,
        children,
        dataTypes,
        getComparator(sortItemList, sortItemIndexList, sortItemDataTypeList),
        node.getTopValue(),
        !sortItemList.isEmpty()
            && sortItemList.get(0).getSortKey().equalsIgnoreCase(OrderByKey.TIME)
            && sortItemList.stream()
                .allMatch(
                    i ->
                        (i.getSortKey().equals(OrderByKey.TIME)
                            || i.getSortKey().equals(OrderByKey.DEVICE))));
  }

  private void genSortInformation(
      List<String> outputColumnNames,
      List<TSDataType> dataTypes,
      List<SortItem> sortItemList,
      List<Integer> sortItemIndexList,
      List<TSDataType> sortItemDataTypeList) {
    sortItemList.forEach(
        sortItem -> {
          if (sortItem.getSortKey().equals(OrderByKey.TIME)) {
            sortItemIndexList.add(-1);
            sortItemDataTypeList.add(TSDataType.INT64);
          } else {
            for (int i = 0; i < outputColumnNames.size(); i++) {
              if (sortItem.getSortKey().equalsIgnoreCase(outputColumnNames.get(i))) {
                sortItemIndexList.add(i);
                sortItemDataTypeList.add(dataTypes.get(i));
                break;
              }
              // there is no related column in outputColumnNames
              if (i == outputColumnNames.size() - 1) {
                sortItemIndexList.add(-2);
                sortItemDataTypeList.add(null);
              }
            }
          }
        });
  }

  @Override
  public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    return getFillOperator(node, context, child);
  }

  private ProcessOperator getFillOperator(
      FillNode node, LocalExecutionPlanContext context, Operator child) {
    FillDescriptor descriptor = node.getFillDescriptor();
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumns = inputDataTypes.size();
    FillPolicy fillPolicy = descriptor.getFillPolicy();
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                FillOperator.class.getSimpleName());
    switch (fillPolicy) {
      case VALUE:
        Literal literal = descriptor.getFillValue();
        return new FillOperator(
            operatorContext, getConstantFill(inputColumns, inputDataTypes, literal), child);
      case PREVIOUS:
        return new FillOperator(
            operatorContext,
            getPreviousFill(
                inputColumns,
                inputDataTypes,
                descriptor.getTimeDurationThreshold(),
                context.getZoneId()),
            child);
      case LINEAR:
        return new LinearFillOperator(
            operatorContext, getLinearFill(inputColumns, inputDataTypes), child);
      default:
        throw new IllegalArgumentException("Unknown fill policy: " + fillPolicy);
    }
  }

  private IFill[] getConstantFill(
      int inputColumns, List<TSDataType> inputDataTypes, Literal literal) {
    IFill[] constantFill = new IFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      if (!literal.isDataTypeConsistency(inputDataTypes.get(i))) {
        constantFill[i] = IDENTITY_FILL;
        continue;
      }
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          constantFill[i] = new BooleanConstantFill(literal.getBoolean());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          constantFill[i] = new BinaryConstantFill(literal.getBinary());
          break;
        case INT32:
          constantFill[i] = new IntConstantFill(literal.getInt());
          break;
        case DATE:
          constantFill[i] = new IntConstantFill(literal.getDate());
          break;
        case INT64:
        case TIMESTAMP:
          constantFill[i] = new LongConstantFill(literal.getLong());
          break;
        case FLOAT:
          constantFill[i] = new FloatConstantFill(literal.getFloat());
          break;
        case DOUBLE:
          constantFill[i] = new DoubleConstantFill(literal.getDouble());
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return constantFill;
  }

  private IFill[] getPreviousFill(
      int inputColumns,
      List<TSDataType> inputDataTypes,
      TimeDuration timeDurationThreshold,
      ZoneId zoneId) {
    IFillFilter filter;
    if (timeDurationThreshold == null) {
      filter = IFillFilter.TRUE;
    } else if (!timeDurationThreshold.containsMonth()) {
      filter = new FixedIntervalFillFilter(timeDurationThreshold.nonMonthDuration);
    } else {
      switch (TIMESTAMP_PRECISION) {
        case "ms":
          filter =
              new MonthIntervalMSFillFilter(
                  timeDurationThreshold.monthDuration,
                  timeDurationThreshold.nonMonthDuration,
                  zoneId);
          break;
        case "us":
          filter =
              new MonthIntervalUSFillFilter(
                  timeDurationThreshold.monthDuration,
                  timeDurationThreshold.nonMonthDuration,
                  zoneId);
          break;
        case "ns":
          filter =
              new MonthIntervalNSFillFilter(
                  timeDurationThreshold.monthDuration,
                  timeDurationThreshold.nonMonthDuration,
                  zoneId);
          break;
        default:
          // this case will never reach
          throw new UnsupportedOperationException(
              "not supported time_precision: " + TIMESTAMP_PRECISION);
      }
    }

    IFill[] previousFill = new IFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          previousFill[i] = new BooleanPreviousFill(filter);
          break;
        case TEXT:
        case STRING:
        case BLOB:
          previousFill[i] = new BinaryPreviousFill(filter);
          break;
        case INT32:
        case DATE:
          previousFill[i] = new IntPreviousFill(filter);
          break;
        case INT64:
        case TIMESTAMP:
          previousFill[i] = new LongPreviousFill(filter);
          break;
        case FLOAT:
          previousFill[i] = new FloatPreviousFill(filter);
          break;
        case DOUBLE:
          previousFill[i] = new DoublePreviousFill(filter);
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return previousFill;
  }

  private ILinearFill[] getLinearFill(int inputColumns, List<TSDataType> inputDataTypes) {
    ILinearFill[] linearFill = new ILinearFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      switch (inputDataTypes.get(i)) {
        case INT32:
        case DATE:
          linearFill[i] = new IntLinearFill();
          break;
        case INT64:
        case TIMESTAMP:
          linearFill[i] = new LongLinearFill();
          break;
        case FLOAT:
          linearFill[i] = new FloatLinearFill();
          break;
        case DOUBLE:
          linearFill[i] = new DoubleLinearFill();
          break;
        case BOOLEAN:
        case TEXT:
        case STRING:
        case BLOB:
          linearFill[i] = IDENTITY_LINEAR_FILL;
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return linearFill;
  }

  @Override
  public Operator visitTransform(TransformNode node, LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TransformOperator.class.getSimpleName());
    final Operator inputOperator = generateOnlyChildOperator(node, context);
    final List<TSDataType> inputDataTypes = getInputColumnTypes(node, context.getTypeProvider());
    final Map<String, List<InputLocation>> inputLocations = makeLayout(node);
    final Expression[] projectExpressions = node.getOutputExpressions();
    final Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();

    for (Expression projectExpression : projectExpressions) {
      if (context.isBuildPlanUseTemplate()) {
        ExpressionTypeAnalyzer.analyzeExpression(
            expressionTypes,
            projectExpression,
            new ExpressionTypeAnalyzer.TemplateTypeProvider(
                context.getTemplatedInfo().getSchemaMap()));
      } else {
        ExpressionTypeAnalyzer.analyzeExpression(
            expressionTypes,
            projectExpression,
            new ExpressionTypeAnalyzer.TypeProviderWrapper(
                context.getTypeProvider().getTreeModelTypeMap()));
      }
    }

    boolean hasNonMappableUDF = false;
    for (Expression expression : projectExpressions) {
      if (!expression.isMappable(expressionTypes)) {
        hasNonMappableUDF = true;
        break;
      }
    }

    // Use FilterAndProject Operator when project expressions are all mappable
    if (!hasNonMappableUDF) {
      // init project UDTFContext
      UDTFContext projectContext = new UDTFContext(context.getZoneId());
      projectContext.constructUdfExecutors(projectExpressions);

      List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();
      Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

      // records LeafColumnTransformer of project expressions
      List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

      ColumnTransformerVisitor visitor = new ColumnTransformerVisitor();
      ColumnTransformerVisitor.ColumnTransformerVisitorContext projectColumnTransformerContext =
          new ColumnTransformerVisitor.ColumnTransformerVisitorContext(
              projectContext,
              expressionTypes,
              projectLeafColumnTransformerList,
              inputLocations,
              projectExpressionColumnTransformerMap,
              ImmutableMap.of(),
              ImmutableList.of(),
              inputDataTypes,
              inputLocations.size() - 1,
              null);

      for (Expression expression : projectExpressions) {
        projectOutputTransformerList.add(
            visitor.process(expression, projectColumnTransformerContext));
      }

      return new FilterAndProjectOperator(
          operatorContext,
          inputOperator,
          inputDataTypes,
          ImmutableList.of(),
          null,
          ImmutableList.of(),
          projectLeafColumnTransformerList,
          projectOutputTransformerList,
          false,
          false);
    }

    try {
      return new TransformOperator(
          operatorContext,
          inputOperator,
          inputDataTypes,
          inputLocations,
          node.getOutputExpressions(),
          node.isKeepNull(),
          context.getZoneId(),
          expressionTypes,
          node.getScanOrder() == ASC);
    } catch (QueryProcessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
    if (context.isBuildPlanUseTemplate() && node.isFromWhere()) {
      TemplatedInfo templatedInfo = context.getTemplatedInfo();
      return constructFilterOperator(
          node.getPredicate(),
          generateOnlyChildOperator(node, context),
          templatedInfo.getProjectExpressions(),
          templatedInfo.getDataTypes(),
          templatedInfo.getFilterLayoutMap(),
          templatedInfo.isKeepNull(),
          node.getPlanNodeId(),
          templatedInfo.getScanOrder(),
          context);
    }

    // 1. not use template
    // 2. use template but the FilterNode is not generated by FilterNode
    // the inputDataTypes should be generated by the outputColumns of children
    return constructFilterOperator(
        node.getPredicate(),
        generateOnlyChildOperator(node, context),
        node.getOutputExpressions(),
        node.getChildren().stream()
            .map(PlanNode::getOutputColumnNames)
            .flatMap(List::stream)
            .map(context.getTypeProvider()::getTreeModelType)
            .collect(Collectors.toList()),
        makeLayout(node),
        node.isKeepNull(),
        node.getPlanNodeId(),
        node.getScanOrder(),
        context);
  }

  private Operator constructFilterOperator(
      Expression predicate,
      Operator inputOperator,
      Expression[] projectExpressions,
      List<TSDataType> inputDataTypes,
      Map<String, List<InputLocation>> inputLocations,
      boolean isKeepNull,
      PlanNodeId planNodeId,
      Ordering scanOrder,
      LocalExecutionPlanContext context) {
    final Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();
    if (context.isBuildPlanUseTemplate()) {
      ExpressionTypeAnalyzer.analyzeExpression(
          expressionTypes,
          predicate,
          new ExpressionTypeAnalyzer.TemplateTypeProvider(
              context.getTypeProvider().getTemplatedInfo().getSchemaMap()));
    } else {
      ExpressionTypeAnalyzer.analyzeExpression(
          expressionTypes,
          predicate,
          new ExpressionTypeAnalyzer.TypeProviderWrapper(
              context.getTypeProvider().getTreeModelTypeMap()));
    }

    // check whether predicate contains Non-Mappable UDF
    if (!predicate.isMappable(expressionTypes)) {
      throw new UnsupportedOperationException("Filter can not contain Non-Mappable UDF");
    }

    final List<TSDataType> filterOutputDataTypes = new ArrayList<>(inputDataTypes);

    for (Expression projectExpression : projectExpressions) {
      if (context.isBuildPlanUseTemplate()) {
        ExpressionTypeAnalyzer.analyzeExpression(
            expressionTypes,
            projectExpression,
            new ExpressionTypeAnalyzer.TemplateTypeProvider(
                context.getTypeProvider().getTemplatedInfo().getSchemaMap()));
      } else {
        ExpressionTypeAnalyzer.analyzeExpression(
            expressionTypes,
            projectExpression,
            new ExpressionTypeAnalyzer.TypeProviderWrapper(
                context.getTypeProvider().getTreeModelTypeMap()));
      }
    }

    boolean hasNonMappableUdf = false;
    for (Expression expression : projectExpressions) {
      if (!expression.isMappable(expressionTypes)) {
        hasNonMappableUdf = true;
        break;
      }
    }

    // init UDTFContext
    UDTFContext filterContext = new UDTFContext(context.getZoneId());
    filterContext.constructUdfExecutors(new Expression[] {predicate});

    // records LeafColumnTransformer of filter
    List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records common ColumnTransformer between filter and project expressions
    List<ColumnTransformer> commonTransformerList = new ArrayList<>();

    // records LeafColumnTransformer of project expressions
    List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    ColumnTransformerVisitor visitor = new ColumnTransformerVisitor();

    ColumnTransformerVisitor.ColumnTransformerVisitorContext filterColumnTransformerContext =
        new ColumnTransformerVisitor.ColumnTransformerVisitorContext(
            filterContext,
            expressionTypes,
            filterLeafColumnTransformerList,
            inputLocations,
            filterExpressionColumnTransformerMap,
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0,
            null);

    ColumnTransformer filterOutputTransformer =
        visitor.process(predicate, filterColumnTransformerContext);

    List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();

    Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

    // init project transformer when project expressions are all mappable
    if (!hasNonMappableUdf) {
      // init project UDTFContext
      UDTFContext projectContext = new UDTFContext(context.getZoneId());
      projectContext.constructUdfExecutors(projectExpressions);

      ColumnTransformerVisitor.ColumnTransformerVisitorContext projectColumnTransformerContext =
          new ColumnTransformerVisitor.ColumnTransformerVisitorContext(
              projectContext,
              expressionTypes,
              projectLeafColumnTransformerList,
              inputLocations,
              projectExpressionColumnTransformerMap,
              filterExpressionColumnTransformerMap,
              commonTransformerList,
              filterOutputDataTypes,
              inputLocations.size() - 1,
              null);

      for (Expression expression : projectExpressions) {
        projectOutputTransformerList.add(
            visitor.process(expression, projectColumnTransformerContext));
      }
    }

    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                planNodeId,
                FilterAndProjectOperator.class.getSimpleName());
    Operator filter =
        new FilterAndProjectOperator(
            operatorContext,
            inputOperator,
            filterOutputDataTypes,
            filterLeafColumnTransformerList,
            filterOutputTransformer,
            commonTransformerList,
            projectLeafColumnTransformerList,
            projectOutputTransformerList,
            hasNonMappableUdf,
            true);

    // Project expressions don't contain Non-Mappable UDF, TransformOperator is not needed
    if (!hasNonMappableUdf) {
      return filter;
    }

    // has Non-Mappable UDF, we wrap a TransformOperator for further calculation
    try {
      final OperatorContext transformContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(), planNodeId, TransformOperator.class.getSimpleName());
      return new TransformOperator(
          transformContext,
          filter,
          inputDataTypes,
          inputLocations,
          projectExpressions,
          isKeepNull,
          context.getZoneId(),
          expressionTypes,
          scanOrder == ASC);
    } catch (QueryProcessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Operator visitGroupByLevel(GroupByLevelNode node, LocalExecutionPlanContext context) {
    checkArgument(
        !node.getGroupByLevelDescriptors().isEmpty(),
        "GroupByLevel descriptorList cannot be empty");
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    boolean ascending = node.getScanOrder() == ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    List<CrossSeriesAggregationDescriptor> aggregationDescriptors =
        node.getGroupByLevelDescriptors();
    for (CrossSeriesAggregationDescriptor descriptor : aggregationDescriptors) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      // Use the first set of InputExpression
      List<TSDataType> inputDataTypes =
          IntStream.range(0, descriptor.getExpressionNumOfOneInput())
              .mapToObj(
                  x ->
                      context
                          .getTypeProvider()
                          .getTreeModelType(
                              descriptor.getInputExpressions().get(x).getExpressionString()))
              .collect(Collectors.toList());
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  descriptor.getAggregationFuncName(),
                  descriptor.getAggregationType(),
                  inputDataTypes,
                  descriptor.getInputExpressions(),
                  descriptor.getInputAttributes(),
                  ascending,
                  descriptor.getStep().isInputRaw()),
              descriptor.getStep(),
              inputLocationList));
    }
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AggregationOperator.class.getSimpleName());

    GroupByTimeParameter groupByTimeParameter = node.getGroupByTimeParameter();
    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, false);
    long maxReturnSize =
        calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, context.getTypeProvider());

    return new AggregationOperator(
        operatorContext, aggregators, timeRangeIterator, children, false, maxReturnSize);
  }

  @Override
  public Operator visitGroupByTag(GroupByTagNode node, LocalExecutionPlanContext context) {
    checkArgument(!node.getTagKeys().isEmpty(), "GroupByTag tag keys cannot be empty");
    checkArgument(
        node.getTagValuesToAggregationDescriptors().size() >= 1,
        "GroupByTag aggregation descriptors cannot be empty");

    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);

    boolean ascending = node.getScanOrder() == ASC;
    Map<String, List<InputLocation>> layout = makeLayout(node);
    List<List<String>> groups = new ArrayList<>();
    List<List<Aggregator>> groupedAggregators = new ArrayList<>();
    int aggregatorCount = 0;
    for (Map.Entry<List<String>, List<CrossSeriesAggregationDescriptor>> entry :
        node.getTagValuesToAggregationDescriptors().entrySet()) {
      groups.add(entry.getKey());
      List<Aggregator> aggregators = new ArrayList<>();
      for (CrossSeriesAggregationDescriptor aggregationDescriptor : entry.getValue()) {
        if (aggregationDescriptor == null) {
          aggregators.add(null);
          continue;
        }
        List<InputLocation[]> inputLocations = calcInputLocationList(aggregationDescriptor, layout);
        List<TSDataType> inputDataTypes =
            aggregationDescriptor.getInputExpressions().stream()
                .map(x -> context.getTypeProvider().getTreeModelType(x.getExpressionString()))
                .collect(Collectors.toList());
        aggregators.add(
            new Aggregator(
                AccumulatorFactory.createAccumulator(
                    aggregationDescriptor.getAggregationFuncName(),
                    aggregationDescriptor.getAggregationType(),
                    inputDataTypes,
                    aggregationDescriptor.getInputExpressions(),
                    aggregationDescriptor.getInputAttributes(),
                    ascending,
                    aggregationDescriptor.getStep().isInputRaw()),
                aggregationDescriptor.getStep(),
                inputLocations));
      }
      groupedAggregators.add(aggregators);
      aggregatorCount += aggregators.size();
    }
    GroupByTimeParameter groupByTimeParameter = node.getGroupByTimeParameter();
    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, false);
    List<AggregationDescriptor> aggregationDescriptors =
        node.getTagValuesToAggregationDescriptors().values().stream()
            .flatMap(Collection::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TagAggregationOperator.class.getSimpleName());
    long maxReturnSize =
        calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, context.getTypeProvider());

    return new TagAggregationOperator(
        operatorContext, groups, groupedAggregators, children, maxReturnSize);
  }

  @Override
  public Operator visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, LocalExecutionPlanContext context) {
    checkArgument(
        !node.getAggregationDescriptorList().isEmpty(),
        "Aggregation descriptorList cannot be empty");
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SlidingWindowAggregationOperator.class.getSimpleName());
    Operator child = node.getChild().accept(this, context);
    boolean ascending = node.getScanOrder() == ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    List<AggregationDescriptor> aggregationDescriptors = node.getAggregationDescriptorList();
    for (AggregationDescriptor descriptor : aggregationDescriptors) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      List<TSDataType> dataTypes = new ArrayList<>();
      for (Expression expression : descriptor.getInputExpressions()) {
        if (context.isBuildPlanUseTemplate() && expression instanceof TimeSeriesOperand) {
          dataTypes.add(
              context
                  .getTemplatedInfo()
                  .getSchemaMap()
                  .get(expression.getExpressionString())
                  .getType());
        } else {
          dataTypes.add(
              context.getTypeProvider().getTreeModelType(expression.getExpressionString()));
        }
      }
      aggregators.add(
          SlidingWindowAggregatorFactory.createSlidingWindowAggregator(
              descriptor.getAggregationFuncName(),
              descriptor.getAggregationType(),
              dataTypes,
              descriptor.getInputExpressions(),
              descriptor.getInputAttributes(),
              ascending,
              inputLocationList,
              descriptor.getStep()));
    }

    GroupByTimeParameter groupByTimeParameter = node.getGroupByTimeParameter();
    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, false);
    long maxReturnSize =
        calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, context.getTypeProvider());

    return new SlidingWindowAggregationOperator(
        operatorContext,
        aggregators,
        timeRangeIterator,
        child,
        ascending,
        node.isOutputEndTime(),
        groupByTimeParameter,
        maxReturnSize);
  }

  @Override
  public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LimitOperator.class.getSimpleName());

    return new LimitOperator(operatorContext, node.getLimit(), child);
  }

  @Override
  public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                OffsetOperator.class.getSimpleName());

    return new OffsetOperator(operatorContext, node.getOffset(), child);
  }

  @Override
  public Operator visitRawDataAggregation(
      RawDataAggregationNode node, LocalExecutionPlanContext context) {
    Map<String, List<InputLocation>> layout;
    if (context.isBuildPlanUseTemplate()) {
      layout = context.getTemplatedInfo().getFilterLayoutMap();
    } else {
      layout = makeLayout(node);
    }
    return createRawDataAggregationOperator(node, context, layout);
  }

  private RawDataAggregationOperator createRawDataAggregationOperator(
      RawDataAggregationNode node,
      LocalExecutionPlanContext context,
      Map<String, List<InputLocation>> layout) {
    checkArgument(
        !node.getAggregationDescriptorList().isEmpty(),
        "Aggregation descriptorList cannot be empty");
    Operator child = node.getChild().accept(this, context);
    boolean ascending = node.getScanOrder() == ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    List<AggregationDescriptor> aggregationDescriptors = node.getAggregationDescriptorList();
    for (AggregationDescriptor descriptor : node.getAggregationDescriptorList()) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  descriptor.getAggregationFuncName(),
                  descriptor.getAggregationType(),
                  descriptor.getInputExpressions().stream()
                      .map(x -> context.getTypeProvider().getTreeModelType(x.getExpressionString()))
                      .collect(Collectors.toList()),
                  descriptor.getInputExpressions(),
                  descriptor.getInputAttributes(),
                  ascending,
                  descriptor.getStep().isInputRaw()),
              descriptor.getStep(),
              inputLocationList));
    }

    GroupByTimeParameter groupByTimeParameter = node.getGroupByTimeParameter();
    GroupByParameter groupByParameter = node.getGroupByParameter();

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                RawDataAggregationOperator.class.getSimpleName());

    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(groupByTimeParameter, ascending, true);
    long maxReturnSize =
        calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, context.getTypeProvider());

    // groupByParameter and groupByTimeParameter
    if (groupByParameter != null) {
      WindowType windowType = groupByParameter.getWindowType();

      WindowParameter windowParameter;
      switch (windowType) {
        case VARIATION_WINDOW:
          Expression groupByVariationExpression = node.getGroupByExpression();
          if (groupByVariationExpression == null) {
            throw new IllegalArgumentException("groupByVariationExpression can't be null");
          }
          String controlColumn = groupByVariationExpression.getExpressionString();
          TSDataType controlColumnType = context.getTypeProvider().getTreeModelType(controlColumn);
          windowParameter =
              new VariationWindowParameter(
                  controlColumnType,
                  layout.get(controlColumn).get(0).getValueColumnIndex(),
                  node.isOutputEndTime(),
                  ((GroupByVariationParameter) groupByParameter).isIgnoringNull(),
                  ((GroupByVariationParameter) groupByParameter).getDelta());
          break;
        case CONDITION_WINDOW:
          Expression groupByConditionExpression = node.getGroupByExpression();
          if (groupByConditionExpression == null) {
            throw new IllegalArgumentException("groupByConditionExpression can't be null");
          }
          windowParameter =
              new ConditionWindowParameter(
                  node.isOutputEndTime(),
                  ((GroupByConditionParameter) groupByParameter).isIgnoringNull(),
                  layout
                      .get(groupByConditionExpression.getExpressionString())
                      .get(0)
                      .getValueColumnIndex(),
                  ((GroupByConditionParameter) groupByParameter).getKeepExpression());
          break;
        case SESSION_WINDOW:
          windowParameter =
              new SessionWindowParameter(
                  ((GroupBySessionParameter) groupByParameter).getTimeInterval(),
                  node.isOutputEndTime());
          break;
        case COUNT_WINDOW:
          Expression groupByCountExpression = node.getGroupByExpression();
          if (groupByCountExpression == null) {
            throw new IllegalArgumentException("groupByCountExpression can't be null");
          }
          windowParameter =
              new CountWindowParameter(
                  ((GroupByCountParameter) groupByParameter).getCountNumber(),
                  layout
                      .get(groupByCountExpression.getExpressionString())
                      .get(0)
                      .getValueColumnIndex(),
                  node.isOutputEndTime(),
                  ((GroupByCountParameter) groupByParameter).isIgnoreNull());
          break;
        default:
          throw new IllegalArgumentException("Unsupported window type");
      }
      return new RawDataAggregationOperator(
          operatorContext,
          aggregators,
          timeRangeIterator,
          child,
          ascending,
          maxReturnSize,
          windowParameter);
    }

    WindowParameter windowParameter = new TimeWindowParameter(node.isOutputEndTime());
    return new RawDataAggregationOperator(
        operatorContext,
        aggregators,
        timeRangeIterator,
        child,
        ascending,
        maxReturnSize,
        windowParameter);
  }

  @Override
  public Operator visitAggregation(AggregationNode node, LocalExecutionPlanContext context) {
    checkArgument(
        !node.getAggregationDescriptorList().isEmpty(),
        "Aggregation descriptorList cannot be empty");
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    boolean ascending = node.getScanOrder() == ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    List<AggregationDescriptor> aggregationDescriptors = node.getAggregationDescriptorList();
    for (AggregationDescriptor descriptor : node.getAggregationDescriptorList()) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  descriptor.getAggregationFuncName(),
                  descriptor.getAggregationType(),
                  descriptor.getInputExpressions().stream()
                      .map(x -> context.getTypeProvider().getTreeModelType(x.getExpressionString()))
                      .collect(Collectors.toList()),
                  descriptor.getInputExpressions(),
                  descriptor.getInputAttributes(),
                  ascending,
                  descriptor.getStep().isInputRaw()),
              descriptor.getStep(),
              inputLocationList));
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AggregationOperator.class.getSimpleName());

    ITimeRangeIterator timeRangeIterator =
        initTimeRangeIterator(node.getGroupByTimeParameter(), ascending, true);
    long maxReturnSize =
        calculateMaxAggregationResultSize(
            aggregationDescriptors, timeRangeIterator, context.getTypeProvider());

    return new AggregationOperator(
        operatorContext,
        aggregators,
        timeRangeIterator,
        children,
        node.isOutputEndTime(),
        maxReturnSize);
  }

  private List<InputLocation[]> calcInputLocationList(
      AggregationDescriptor descriptor, Map<String, List<InputLocation>> layout) {
    List<List<String>> inputColumnNames = descriptor.getInputColumnNamesList();
    List<InputLocation[]> inputLocationList = new ArrayList<>();

    for (List<String> inputColumnNamesOfOneInput : inputColumnNames) {
      // it may include double parts
      List<List<InputLocation>> inputLocationParts = new ArrayList<>();
      inputColumnNamesOfOneInput.forEach(o -> inputLocationParts.add(layout.get(o)));
      for (int i = 0; i < inputLocationParts.get(0).size(); i++) {
        if (inputColumnNamesOfOneInput.size() == 1) {
          inputLocationList.add(new InputLocation[] {inputLocationParts.get(0).get(i)});
        } else {
          inputLocationList.add(
              new InputLocation[] {
                inputLocationParts.get(0).get(i), inputLocationParts.get(1).get(i)
              });
        }
      }
    }
    return inputLocationList;
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TreeSortOperator.class.getSimpleName());

    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());

    List<SortItem> sortItemList = node.getOrderByParameter().getSortItemList();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemList.size());
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemList.size());
    genSortInformation(
        node.getOutputColumnNames(),
        dataTypes,
        sortItemList,
        sortItemIndexList,
        sortItemDataTypeList);

    String filePrefix =
        IoTDBDescriptor.getInstance().getConfig().getSortTmpDir()
            + File.separator
            + operatorContext.getDriverContext().getFragmentInstanceContext().getId().getFullId()
            + File.separator
            + operatorContext.getDriverContext().getPipelineId()
            + File.separator;

    context.getDriverContext().setHaveTmpFile(true);
    context.getDriverContext().getFragmentInstanceContext().setMayHaveTmpFile(true);

    Operator child = node.getChild().accept(this, context);

    return new TreeSortOperator(
        operatorContext,
        child,
        dataTypes,
        filePrefix,
        getComparator(sortItemList, sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitInference(InferenceNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                org.apache.iotdb.db.queryengine.execution.operator.process.AI.InferenceOperator
                    .class
                    .getSimpleName());

    ModelInferenceDescriptor modelInferenceDescriptor = node.getModelInferenceDescriptor();
    ModelInformation modelInformation = modelInferenceDescriptor.getModelInformation();
    int[] inputShape = modelInformation.getInputShape();
    int[] outputShape = modelInformation.getOutputShape();
    TSDataType[] inputTypes = modelInferenceDescriptor.getModelInformation().getInputDataType();
    TSDataType[] outputTypes = modelInferenceDescriptor.getModelInformation().getOutputDataType();

    long maxRetainedSize =
        calculateSize(inputShape[0], inputTypes) + TimeColumn.SIZE_IN_BYTES_PER_POSITION;
    long maxReturnSize = calculateSize(outputShape[0], outputTypes);

    return new org.apache.iotdb.db.queryengine.execution.operator.process.AI.InferenceOperator(
        operatorContext,
        child,
        modelInferenceDescriptor,
        FragmentInstanceManager.getInstance().getModelInferenceExecutor(),
        node.getInputColumnNames(),
        node.getChild().getOutputColumnNames(),
        maxRetainedSize,
        maxReturnSize);
  }

  private long calculateSize(long rowNumber, TSDataType[] dataTypes) {
    long size = 0;
    for (int i = 0; i < dataTypes.length; i++) {
      size += getOutputColumnSizePerLine(dataTypes[i]);
    }
    return size * rowNumber;
  }

  @Override
  public Operator visitInto(IntoNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                IntoOperator.class.getSimpleName());

    IntoPathDescriptor intoPathDescriptor = node.getIntoPathDescriptor();

    Map<String, InputLocation> sourceColumnToInputLocationMap =
        constructSourceColumnToInputLocationMap(node);
    Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap =
        new HashMap<>();
    processTargetPathToSourceMap(
        intoPathDescriptor.getTargetPathToSourceMap(),
        targetPathToSourceInputLocationMap,
        sourceColumnToInputLocationMap);

    Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap =
        intoPathDescriptor.getTargetPathToDataTypeMap();
    long statementSizePerLine = calculateStatementSizePerLine(targetPathToDataTypeMap);

    List<Pair<String, PartialPath>> sourceTargetPathPairList =
        intoPathDescriptor.getSourceTargetPathPairList();
    List<String> sourceColumnToViewList = intoPathDescriptor.getSourceColumnToViewList();
    List<Pair<String, PartialPath>> sourceTargetPathPairWithViewList =
        new ArrayList<>(sourceTargetPathPairList);
    for (int i = 0; i < sourceColumnToViewList.size(); i++) {
      String viewPath = sourceColumnToViewList.get(i);
      if (StringUtils.isNotEmpty(viewPath)) {
        sourceTargetPathPairWithViewList.get(i).setLeft(viewPath);
      }
    }

    return new IntoOperator(
        operatorContext,
        child,
        getInputColumnTypes(node, context.getTypeProvider()),
        targetPathToSourceInputLocationMap,
        targetPathToDataTypeMap,
        intoPathDescriptor.getTargetDeviceToAlignedMap(),
        sourceTargetPathPairWithViewList,
        FragmentInstanceManager.getInstance().getIntoOperationExecutor(),
        statementSizePerLine);
  }

  @Override
  public Operator visitDeviceViewInto(DeviceViewIntoNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                DeviceViewIntoOperator.class.getSimpleName());

    DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor =
        node.getDeviceViewIntoPathDescriptor();
    Map<String, InputLocation> sourceColumnToInputLocationMap =
        constructSourceColumnToInputLocationMap(node);

    Map<String, Map<PartialPath, Map<String, InputLocation>>>
        deviceToTargetPathSourceInputLocationMap = new HashMap<>();
    Map<String, Map<PartialPath, Map<String, TSDataType>>> deviceToTargetPathDataTypeMap =
        deviceViewIntoPathDescriptor.getSourceDeviceToTargetPathDataTypeMap();
    Map<String, Map<PartialPath, Map<String, String>>> sourceDeviceToTargetPathMap =
        deviceViewIntoPathDescriptor.getSourceDeviceToTargetPathMap();
    long statementSizePerLine = 0L;
    for (Map.Entry<String, Map<PartialPath, Map<String, String>>> deviceEntry :
        sourceDeviceToTargetPathMap.entrySet()) {
      String sourceDevice = deviceEntry.getKey();
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap =
          new HashMap<>();
      processTargetPathToSourceMap(
          deviceEntry.getValue(),
          targetPathToSourceInputLocationMap,
          sourceColumnToInputLocationMap);
      deviceToTargetPathSourceInputLocationMap.put(
          sourceDevice, targetPathToSourceInputLocationMap);
      statementSizePerLine +=
          calculateStatementSizePerLine(deviceToTargetPathDataTypeMap.get(sourceDevice));
    }

    return new DeviceViewIntoOperator(
        operatorContext,
        child,
        getInputColumnTypes(node, context.getTypeProvider()),
        deviceToTargetPathSourceInputLocationMap,
        deviceToTargetPathDataTypeMap,
        deviceViewIntoPathDescriptor.getTargetDeviceToAlignedMap(),
        deviceViewIntoPathDescriptor.getDeviceToSourceTargetPathPairListMap(),
        sourceColumnToInputLocationMap,
        FragmentInstanceManager.getInstance().getIntoOperationExecutor(),
        statementSizePerLine);
  }

  private Map<String, InputLocation> constructSourceColumnToInputLocationMap(PlanNode node) {
    Map<String, InputLocation> sourceColumnToInputLocationMap = new HashMap<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    for (Map.Entry<String, List<InputLocation>> layoutEntry : layout.entrySet()) {
      sourceColumnToInputLocationMap.put(layoutEntry.getKey(), layoutEntry.getValue().get(0));
    }
    return sourceColumnToInputLocationMap;
  }

  private void processTargetPathToSourceMap(
      Map<PartialPath, Map<String, String>> targetPathToSourceMap,
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<String, InputLocation> sourceColumnToInputLocationMap) {
    for (Map.Entry<PartialPath, Map<String, String>> entry : targetPathToSourceMap.entrySet()) {
      PartialPath targetDevice = entry.getKey();
      Map<String, InputLocation> measurementToInputLocationMap = new HashMap<>();
      for (Map.Entry<String, String> measurementEntry : entry.getValue().entrySet()) {
        String targetMeasurement = measurementEntry.getKey();
        String sourceColumn = measurementEntry.getValue();
        measurementToInputLocationMap.put(
            targetMeasurement, sourceColumnToInputLocationMap.get(sourceColumn));
      }
      targetPathToSourceInputLocationMap.put(targetDevice, measurementToInputLocationMap);
    }
  }

  private long calculateStatementSizePerLine(
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap) {
    long maxStatementSize = Long.BYTES;
    List<TSDataType> dataTypes =
        targetPathToDataTypeMap.values().stream()
            .flatMap(stringTSDataTypeMap -> stringTSDataTypeMap.values().stream())
            .collect(Collectors.toList());
    for (TSDataType dataType : dataTypes) {
      maxStatementSize += getValueSizePerLine(dataType);
    }
    return maxStatementSize;
  }

  private static long getValueSizePerLine(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
      case DATE:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case BOOLEAN:
        return Byte.BYTES;
      case TEXT:
      case BLOB:
      case STRING:
        return StatisticsManager.getInstance().getMaxBinarySizeInBytes();
      default:
        throw new UnsupportedOperationException(UNKNOWN_DATATYPE + tsDataType);
    }
  }

  @Override
  public Operator visitFullOuterTimeJoin(
      FullOuterTimeJoinNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                FullOuterTimeJoinOperator.class.getSimpleName());
    TimeComparator timeComparator =
        node.getMergeOrder() == ASC ? ASC_TIME_COMPARATOR : DESC_TIME_COMPARATOR;
    List<OutputColumn> outputColumns = generateOutputColumnsFromChildren(node);
    List<ColumnMerger> mergers = createColumnMergers(outputColumns, timeComparator);
    List<TSDataType> outputColumnTypes =
        context.getTypeProvider().getTemplatedInfo() != null
            ? getOutputColumnTypesOfTimeJoinNode(node)
            : getOutputColumnTypes(node, context.getTypeProvider());

    return new FullOuterTimeJoinOperator(
        operatorContext,
        children,
        node.getMergeOrder(),
        outputColumnTypes,
        mergers,
        timeComparator);
  }

  @Override
  public Operator visitInnerTimeJoin(InnerTimeJoinNode node, LocalExecutionPlanContext context) {
    node.getTimePartitions().ifPresent(context::setTimePartitions);

    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                InnerTimeJoinOperator.class.getSimpleName());
    TimeComparator timeComparator =
        node.getMergeOrder() == ASC ? ASC_TIME_COMPARATOR : DESC_TIME_COMPARATOR;
    List<TSDataType> outputColumnTypes =
        context.getTypeProvider().getTemplatedInfo() != null
            ? getOutputColumnTypesOfTimeJoinNode(node)
            : getOutputColumnTypes(node, context.getTypeProvider());

    return new InnerTimeJoinOperator(
        operatorContext, children, outputColumnTypes, timeComparator, getOutputColumnMap(node));
  }

  private Map<InputLocation, Integer> getOutputColumnMap(InnerTimeJoinNode innerTimeJoinNode) {
    Map<InputLocation, Integer> result = new HashMap<>();
    if (innerTimeJoinNode.outputColumnNamesIsNull()) {
      int outputIndex = 0;
      for (int i = 0, size = innerTimeJoinNode.getChildren().size(); i < size; i++) {
        PlanNode child = innerTimeJoinNode.getChildren().get(i);
        List<String> childOutputColumns = child.getOutputColumnNames();
        for (int j = 0, childSize = childOutputColumns.size(); j < childSize; j++) {
          result.put(new InputLocation(i, j), outputIndex++);
        }
      }
    } else {
      List<String> outputColumns = innerTimeJoinNode.getOutputColumnNames();
      Map<String, Integer> outputColumnIndexMap = new HashMap<>();
      for (int i = 0; i < outputColumns.size(); i++) {
        outputColumnIndexMap.put(outputColumns.get(i), i);
      }
      for (int i = 0, size = innerTimeJoinNode.getChildren().size(); i < size; i++) {
        PlanNode child = innerTimeJoinNode.getChildren().get(i);
        List<String> childOutputColumns = child.getOutputColumnNames();
        for (int j = 0, childSize = childOutputColumns.size(); j < childSize; j++) {
          result.put(new InputLocation(i, j), outputColumnIndexMap.get(childOutputColumns.get(j)));
        }
      }
    }
    return result;
  }

  @Override
  public Operator visitLeftOuterTimeJoin(
      LeftOuterTimeJoinNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    checkState(children.size() == 2);
    Operator leftChild = children.get(0);
    Operator rightChild = children.get(1);

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LeftOuterTimeJoinOperator.class.getSimpleName());
    TimeComparator timeComparator =
        node.getMergeOrder() == ASC ? ASC_TIME_COMPARATOR : DESC_TIME_COMPARATOR;
    List<TSDataType> outputColumnTypes =
        context.getTypeProvider().getTemplatedInfo() != null
            ? getOutputColumnTypesOfTimeJoinNode(node)
            : getOutputColumnTypes(node, context.getTypeProvider());

    return new LeftOuterTimeJoinOperator(
        operatorContext,
        leftChild,
        node.getLeftChild().getOutputColumnNames().size(),
        rightChild,
        outputColumnTypes,
        timeComparator);
  }

  @Override
  public Operator visitHorizontallyConcat(
      HorizontallyConcatNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                HorizontallyConcatOperator.class.getSimpleName());
    List<TSDataType> outputColumnTypes = getOutputColumnTypes(node, context.getTypeProvider());

    return new HorizontallyConcatOperator(operatorContext, children, outputColumnTypes);
  }

  @Override
  public Operator visitShowQueries(ShowQueriesNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ShowQueriesOperator.class.getSimpleName());

    return new ShowQueriesOperator(
        operatorContext, node.getPlanNodeId(), Coordinator.getInstance());
  }

  private List<OutputColumn> generateOutputColumnsFromChildren(MultiChildProcessNode node) {
    // TODO we should also sort the InputLocation for each column if they are not overlapped
    return makeLayout(node).entrySet().stream()
        .filter(entry -> !TIMESTAMP_EXPRESSION_STRING.equals(entry.getKey()))
        .map(entry -> new OutputColumn(entry.getValue(), entry.getValue().size() > 1))
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
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ExchangeOperator.class.getSimpleName());

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
                context.getInstanceContext()::failed)
            : MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                node.getIndexOfUpstreamSinkHandle(),
                upstreamEndPoint,
                remoteInstanceId.toThrift(),
                context.getInstanceContext()::failed);
    if (!isSameNode) {
      context.addExchangeSumNum(1);
    }
    sourceHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    ExchangeOperator exchangeOperator =
        new ExchangeOperator(operatorContext, sourceHandle, node.getUpstreamPlanNodeId());
    context.addExchangeOperator(exchangeOperator);
    return exchangeOperator;
  }

  @Override
  public Operator visitIdentitySink(IdentitySinkNode node, LocalExecutionPlanContext context) {
    context.addExchangeSumNum(1);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                IdentitySinkOperator.class.getSimpleName());

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
    List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    sinkHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    context.getDriverContext().setSink(sinkHandle);

    return new IdentitySinkOperator(operatorContext, children, downStreamChannelIndex, sinkHandle);
  }

  @Override
  public Operator visitShuffleSink(ShuffleSinkNode node, LocalExecutionPlanContext context) {
    context.addExchangeSumNum(1);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ShuffleHelperOperator.class.getSimpleName());

    // TODO implement pipeline division for shuffle sink
    context.setDegreeOfParallelism(1);
    List<Operator> children = dealWithConsumeAllChildrenPipelineBreaker(node, context);

    checkArgument(
        MPP_DATA_EXCHANGE_MANAGER != null, "MPP_DATA_EXCHANGE_MANAGER should not be null");
    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
    ISinkHandle sinkHandle =
        MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
            node.getDownStreamChannelLocationList(),
            downStreamChannelIndex,
            ShuffleSinkHandle.ShuffleStrategyEnum.SIMPLE_ROUND_ROBIN,
            localInstanceId.toThrift(),
            node.getPlanNodeId().getId(),
            context.getInstanceContext());
    sinkHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    context.getDriverContext().setSink(sinkHandle);

    return new ShuffleHelperOperator(operatorContext, children, downStreamChannelIndex, sinkHandle);
  }

  @Override
  public Operator visitSchemaFetchMerge(
      SchemaFetchMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaFetchMergeOperator.class.getSimpleName());
    return new SchemaFetchMergeOperator(operatorContext, children, node.getStorageGroupList());
  }

  @Override
  public Operator visitSeriesSchemaFetchScan(
      SeriesSchemaFetchScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaFetchScanOperator.class.getSimpleName());
    return SchemaFetchScanOperator.ofSeries(
        node.getPlanNodeId(),
        operatorContext,
        node.getPatternTree(),
        node.getTemplateMap(),
        ((SchemaDriverContext) (context.getDriverContext())).getSchemaRegion(),
        node.isWithTags(),
        node.isWithAttributes(),
        node.isWithTemplate(),
        node.isWithAliasForce());
  }

  @Override
  public Operator visitDeviceSchemaFetchScan(
      DeviceSchemaFetchScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaFetchScanOperator.class.getSimpleName());
    return SchemaFetchScanOperator.ofDevice(
        node.getPlanNodeId(),
        operatorContext,
        node.getPatternTree(),
        node.getAuthorityScope(),
        ((SchemaDriverContext) (context.getDriverContext())).getSchemaRegion());
  }

  @Override
  public Operator visitLastQueryScan(LastQueryScanNode node, LocalExecutionPlanContext context) {
    MeasurementPath seriesPath = node.getSeriesPath();
    TimeValuePair timeValuePair = null;
    context.dataNodeQueryContext.lock();
    try {
      if (!context.dataNodeQueryContext.unCached(seriesPath)) {
        timeValuePair = DATA_NODE_SCHEMA_CACHE.getLastCache(seriesPath);
        if (timeValuePair == null) {
          context.dataNodeQueryContext.addUnCachePath(seriesPath, node.getDataNodeSeriesScanNum());
        }
      }
    } finally {
      context.dataNodeQueryContext.unLock();
    }

    if (timeValuePair == null) { // last value is not cached
      return createUpdateLastCacheOperator(node, context, node.getSeriesPath());
    } else if (timeValuePair.getValue() == null) { // there is no data for this time series
      return null;
    } else if (!LastQueryUtil.satisfyFilter(
        updateFilterUsingTTL(
            context.getGlobalTimeFilter(),
            DataNodeTTLCache.getInstance().getTTL(seriesPath.getDevicePath().getNodes())),
        timeValuePair)) { // cached last value is not satisfied

      if (!isFilterGtOrGe(context.getGlobalTimeFilter())) {
        // time filter is not > or >=, we still need to read from disk
        return createUpdateLastCacheOperator(node, context, node.getSeriesPath());
      } else { // otherwise, we just ignore it and return null
        return null;
      }
    } else { //  cached last value is satisfied, put it into LastCacheScanOperator
      context.addCachedLastValue(timeValuePair, node.outputPathSymbol());
      return null;
    }
  }

  private boolean isFilterGtOrGe(Filter filter) {
    return filter instanceof TimeGt || filter instanceof TimeGtEq;
  }

  private UpdateLastCacheOperator createUpdateLastCacheOperator(
      LastQueryScanNode node, LocalExecutionPlanContext context, MeasurementPath fullPath) {
    SeriesAggregationScanOperator lastQueryScan = createLastQueryScanOperator(node, context);
    if (node.getOutputViewPath() == null) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  UpdateLastCacheOperator.class.getSimpleName());
      return new UpdateLastCacheOperator(
          operatorContext,
          lastQueryScan,
          fullPath,
          node.getSeriesPath().getSeriesType(),
          DATA_NODE_SCHEMA_CACHE,
          context.isNeedUpdateLastCache(),
          context.isNeedUpdateNullEntry());
    } else {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  UpdateViewPathLastCacheOperator.class.getSimpleName());
      return new UpdateViewPathLastCacheOperator(
          operatorContext,
          lastQueryScan,
          fullPath,
          node.getSeriesPath().getSeriesType(),
          DATA_NODE_SCHEMA_CACHE,
          context.isNeedUpdateLastCache(),
          context.isNeedUpdateNullEntry(),
          node.getOutputViewPath());
    }
  }

  private AlignedUpdateLastCacheOperator createAlignedUpdateLastCacheOperator(
      AlignedLastQueryScanNode node, AlignedPath unCachedPath, LocalExecutionPlanContext context) {
    AlignedSeriesAggregationScanOperator lastQueryScan =
        createLastQueryScanOperator(
            node, (AlignedFullPath) IFullPath.convertToIFullPath(unCachedPath), context);

    if (node.getOutputViewPath() == null) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  AlignedUpdateLastCacheOperator.class.getSimpleName());
      return new AlignedUpdateLastCacheOperator(
          operatorContext,
          lastQueryScan,
          unCachedPath,
          DATA_NODE_SCHEMA_CACHE,
          context.isNeedUpdateLastCache(),
          context.isNeedUpdateNullEntry());
    } else {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  AlignedUpdateViewPathLastCacheOperator.class.getSimpleName());
      return new AlignedUpdateViewPathLastCacheOperator(
          operatorContext,
          lastQueryScan,
          unCachedPath,
          DATA_NODE_SCHEMA_CACHE,
          context.isNeedUpdateLastCache(),
          context.isNeedUpdateNullEntry(),
          node.getOutputViewPath());
    }
  }

  private SeriesAggregationScanOperator createLastQueryScanOperator(
      LastQueryScanNode node, LocalExecutionPlanContext context) {
    NonAlignedFullPath seriesPath =
        (NonAlignedFullPath) IFullPath.convertToIFullPath(node.getSeriesPath());
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SeriesAggregationScanOperator.class.getSimpleName());

    // last_time, last_value
    List<Aggregator> aggregators = LastQueryUtil.createAggregators(seriesPath.getSeriesType());
    ITimeRangeIterator timeRangeIterator = initTimeRangeIterator(null, false, false);
    long maxReturnSize = calculateMaxAggregationResultSizeForLastQuery(aggregators);

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(
        context.getAllSensors(seriesPath.getDeviceId(), seriesPath.getMeasurement()));
    scanOptionsBuilder.withGlobalTimeFilter(context.getGlobalTimeFilter());

    SeriesAggregationScanOperator seriesAggregationScanOperator =
        new SeriesAggregationScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            Ordering.DESC,
            scanOptionsBuilder.build(),
            operatorContext,
            aggregators,
            timeRangeIterator,
            null,
            maxReturnSize,
            !TSDataType.BLOB.equals(seriesPath.getSeriesType()));
    ((DataDriverContext) context.getDriverContext())
        .addSourceOperator(seriesAggregationScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(seriesPath);
    return seriesAggregationScanOperator;
  }

  private AlignedSeriesAggregationScanOperator createLastQueryScanOperator(
      AlignedLastQueryScanNode node,
      AlignedFullPath unCachedPath,
      LocalExecutionPlanContext context) {
    // last_time, last_value
    List<Aggregator> aggregators = new ArrayList<>();
    boolean canUseStatistics = true;
    for (int i = 0; i < unCachedPath.getMeasurementList().size(); i++) {
      TSDataType dataType = unCachedPath.getSchemaList().get(i).getType();
      aggregators.addAll(LastQueryUtil.createAggregators(dataType, i));
      if (TSDataType.BLOB.equals(dataType)) {
        canUseStatistics = false;
      }
    }
    ITimeRangeIterator timeRangeIterator = initTimeRangeIterator(null, false, false);
    long maxReturnSize = calculateMaxAggregationResultSizeForLastQuery(aggregators);

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(new HashSet<>(unCachedPath.getMeasurementList()));
    scanOptionsBuilder.withGlobalTimeFilter(context.getGlobalTimeFilter());

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesAggregationScanOperator.class.getSimpleName());
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        new AlignedSeriesAggregationScanOperator(
            node.getPlanNodeId(),
            unCachedPath,
            Ordering.DESC,
            scanOptionsBuilder.build(),
            operatorContext,
            aggregators,
            timeRangeIterator,
            null,
            maxReturnSize,
            canUseStatistics);
    ((DataDriverContext) context.getDriverContext())
        .addSourceOperator(seriesAggregationScanOperator);
    ((DataDriverContext) context.getDriverContext()).addPath(unCachedPath);
    return seriesAggregationScanOperator;
  }

  @Override
  public Operator visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, LocalExecutionPlanContext context) {
    AlignedPath alignedPath = node.getSeriesPath();
    PartialPath devicePath = alignedPath.getDevicePath();
    // get series under aligned entity that has not been cached
    List<Integer> unCachedMeasurementIndexes = new ArrayList<>();
    List<String> measurementList = alignedPath.getMeasurementList();
    for (int i = 0; i < measurementList.size(); i++) {
      MeasurementPath measurementPath = devicePath.concatAsMeasurementPath(measurementList.get(i));
      TimeValuePair timeValuePair = null;
      try {
        context.dataNodeQueryContext.lock();
        if (!context.dataNodeQueryContext.unCached(measurementPath)) {
          timeValuePair = DATA_NODE_SCHEMA_CACHE.getLastCache(measurementPath);
          if (timeValuePair == null) {
            context.dataNodeQueryContext.addUnCachePath(
                measurementPath, new AtomicInteger(node.getDataNodeSeriesScanNum().get()));
          }
        }
      } finally {
        context.dataNodeQueryContext.unLock();
      }

      if (timeValuePair == null) { // last value is not cached
        unCachedMeasurementIndexes.add(i);
      } else if (timeValuePair.getValue() == null) {
        // there is no data for this time series, just ignore
      } else if (!LastQueryUtil.satisfyFilter(
          updateFilterUsingTTL(
              context.getGlobalTimeFilter(),
              DataNodeTTLCache.getInstance().getTTL(devicePath.getNodes())),
          timeValuePair)) { // cached last value is not satisfied

        if (!isFilterGtOrGe(context.getGlobalTimeFilter())) {
          // time filter is not > or >=, we still need to read from disk
          unCachedMeasurementIndexes.add(i);
        }
      } else { //  cached last value is satisfied, put it into LastCacheScanOperator
        if (node.getOutputViewPath() != null) {
          context.addCachedLastValue(timeValuePair, node.getOutputViewPath());
        } else {
          context.addCachedLastValue(timeValuePair, measurementPath.getFullPath());
        }
      }
    }
    if (unCachedMeasurementIndexes.isEmpty()) {
      return null;
    } else {
      AlignedPath unCachedPath = new AlignedPath(alignedPath.getDevicePath());
      for (int i : unCachedMeasurementIndexes) {
        unCachedPath.addMeasurement(measurementList.get(i), alignedPath.getSchemaList().get(i));
      }
      return createAlignedUpdateLastCacheOperator(node, unCachedPath, context);
    }
  }

  @Override
  public Operator visitLastQuery(LastQueryNode node, LocalExecutionPlanContext context) {
    Filter globalTimeFilter = context.getGlobalTimeFilter();
    context.setNeedUpdateLastCache(LastQueryUtil.needUpdateCache(globalTimeFilter));
    context.setNeedUpdateNullEntry(LastQueryUtil.needUpdateNullEntry(globalTimeFilter));

    List<Operator> operatorList =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    List<Pair<TimeValuePair, Binary>> cachedLastValueAndPathList =
        context.getCachedLastValueAndPathList();

    int initSize = cachedLastValueAndPathList != null ? cachedLastValueAndPathList.size() : 0;
    // no need to order by timeseries at first
    if (!node.needOrderByTimeseries()) {
      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(initSize);
      for (int i = 0; i < initSize; i++) {
        TimeValuePair timeValuePair = cachedLastValueAndPathList.get(i).left;
        LastQueryUtil.appendLastValue(
            builder,
            timeValuePair.getTimestamp(),
            cachedLastValueAndPathList.get(i).right,
            timeValuePair.getValue().getStringValue(),
            timeValuePair.getValue().getDataType().name());
      }
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  LastQueryOperator.class.getSimpleName());
      return new LastQueryOperator(operatorContext, operatorList, builder);
    } else {
      // order by timeseries
      Comparator<Binary> comparator =
          node.getTimeseriesOrdering() == ASC ? ASC_BINARY_COMPARATOR : DESC_BINARY_COMPARATOR;
      // sort values from last cache
      if (initSize > 0) {
        cachedLastValueAndPathList.sort(Comparator.comparing(Pair::getRight, comparator));
      }

      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(initSize);
      for (int i = 0; i < initSize; i++) {
        TimeValuePair timeValuePair = cachedLastValueAndPathList.get(i).left;
        LastQueryUtil.appendLastValue(
            builder,
            timeValuePair.getTimestamp(),
            cachedLastValueAndPathList.get(i).right,
            timeValuePair.getValue().getStringValue(),
            timeValuePair.getValue().getDataType().name());
      }

      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  LastQuerySortOperator.class.getSimpleName());
      return new LastQuerySortOperator(operatorContext, builder.build(), operatorList, comparator);
    }
  }

  @Override
  public Operator visitLastQueryMerge(LastQueryMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LastQueryMergeOperator.class.getSimpleName());

    Ordering timeseriesOrdering = node.getTimeseriesOrdering();
    Comparator<Binary> comparator =
        (timeseriesOrdering == null || timeseriesOrdering == ASC)
            ? ASC_BINARY_COMPARATOR
            : DESC_BINARY_COMPARATOR;

    return new LastQueryMergeOperator(operatorContext, children, comparator);
  }

  @Override
  public Operator visitLastQueryCollect(
      LastQueryCollectNode node, LocalExecutionPlanContext context) {
    List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LastQueryCollectOperator.class.getSimpleName());

    return new LastQueryCollectOperator(operatorContext, children);
  }

  @Override
  public Operator visitLastQueryTransform(
      LastQueryTransformNode node, LocalExecutionPlanContext context) {
    Operator operator = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LastQueryCollectOperator.class.getSimpleName());

    return new LastQueryTransformOperator(
        node.getViewPath(), node.getDataType(), operatorContext, operator);
  }

  private Map<String, List<InputLocation>> makeLayout(PlanNode node) {
    return makeLayout(node.getChildren());
  }

  private Map<String, List<InputLocation>> makeLayout(List<PlanNode> children) {
    Map<String, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    for (PlanNode childNode : children) {
      outputMappings
          .computeIfAbsent(TIMESTAMP_EXPRESSION_STRING, key -> new ArrayList<>())
          .add(new InputLocation(tsBlockIndex, -1));
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

  private List<TSDataType> getInputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    if (typeProvider.getTemplatedInfo() == null) {
      return node.getChildren().stream()
          .map(PlanNode::getOutputColumnNames)
          .flatMap(List::stream)
          .map(typeProvider::getTreeModelType)
          .collect(Collectors.toList());
    } else {
      return getInputColumnTypesUseTemplate(node);
    }
  }

  private List<TSDataType> getInputColumnTypesUseTemplate(PlanNode node) {
    // Only templated device + filter situation can invoke this method,
    // the children of FilterNode/TransformNode can be TimeJoinNode, ScanNode, any others?
    List<TSDataType> dataTypes = new ArrayList<>();
    for (PlanNode child : node.getChildren()) {
      if (child instanceof SeriesScanNode) {
        dataTypes.add(((SeriesScanNode) child).getSeriesPath().getSeriesType());
      } else if (child instanceof AlignedSeriesScanNode) {
        AlignedSeriesScanNode alignedSeriesScanNode = (AlignedSeriesScanNode) child;
        alignedSeriesScanNode
            .getAlignedPath()
            .getSchemaList()
            .forEach(c -> dataTypes.add(c.getType()));
      } else {
        dataTypes.addAll(getInputColumnTypesUseTemplate(child));
      }
    }
    return dataTypes;
  }

  private List<TSDataType> getOutputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    return node.getOutputColumnNames().stream()
        .map(typeProvider::getTreeModelType)
        .collect(Collectors.toList());
  }

  private List<TSDataType> getOutputColumnTypesOfTimeJoinNode(PlanNode node) {
    // Only templated device situation can invoke this method,
    // the children of TimeJoinNode can only be ScanNode or TimeJoinNode
    List<TSDataType> dataTypes = new ArrayList<>();
    for (PlanNode child : node.getChildren()) {
      if (child instanceof SeriesScanNode) {
        dataTypes.add(((SeriesScanNode) child).getSeriesPath().getSeriesType());
      } else if (child instanceof AlignedSeriesScanNode) {
        dataTypes.add(((AlignedSeriesScanNode) child).getAlignedPath().getSeriesType());
      } else if (child instanceof FullOuterTimeJoinNode
          || child instanceof InnerTimeJoinNode
          || child instanceof LeftOuterTimeJoinNode) {
        dataTypes.addAll(getOutputColumnTypesOfTimeJoinNode(child));
      } else {
        LOGGER.error(
            "Unexpected PlanNode in getOutputColumnTypesOfTimeJoinNode, type: {}",
            child.getOutputColumnNames());
      }
    }
    return dataTypes;
  }

  private Operator generateOnlyChildOperator(PlanNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    Validate.isTrue(children.size() == 1);
    return children.get(0);
  }

  public Operator visitPathsUsingTemplateScan(
      PathsUsingTemplateScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getPathsUsingTemplateSource(
            node.getPathPatternList(), node.getTemplateId(), node.getScope()));
  }

  public Operator visitLogicalViewSchemaScan(
      LogicalViewSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getLogicalViewSchemaSource(
            node.getPath(),
            node.getLimit(),
            node.getOffset(),
            node.getSchemaFilter(),
            node.getScope()));
  }

  public List<Operator> dealWithConsumeAllChildrenPipelineBreaker(
      PlanNode node, LocalExecutionPlanContext context) {
    // children after pipelining
    List<Operator> parentPipelineChildren = new ArrayList<>();
    if (context.getDegreeOfParallelism() == 1 || node.getChildren().size() == 1) {
      // If dop = 1, we don't create extra pipeline
      for (PlanNode localChild : node.getChildren()) {
        Operator childOperation = localChild.accept(this, context);
        parentPipelineChildren.add(childOperation);
        // if we don't create extra pipeline, the root of the child pipeline should be current root
        List<PipelineMemoryEstimator> childPipelineMemoryEstimators =
            context.getParentPlanNodeIdToMemoryEstimator().get(localChild.getPlanNodeId());
        if (childPipelineMemoryEstimators != null) {
          context.getParentPlanNodeIdToMemoryEstimator().remove(localChild.getPlanNodeId());
          context
              .getParentPlanNodeIdToMemoryEstimator()
              .put(node.getPlanNodeId(), childPipelineMemoryEstimators);
        }
      }
    } else {
      int finalExchangeNum = context.getExchangeSumNum();
      // Keep it since we may change the structure of origin children nodes
      List<PlanNode> afterwardsNodes = new ArrayList<>();
      // 1. Calculate localChildren size
      int localChildrenSize = 0;
      int firstChildIndex = -1;
      for (int i = 0; i < node.getChildren().size(); i++) {
        if (!(node.getChildren().get(i) instanceof ExchangeNode)) {
          localChildrenSize++;
          firstChildIndex = firstChildIndex == -1 ? i : firstChildIndex;
          // deal with exchangeNode at head
        } else if (firstChildIndex == -1) {
          Operator childOperation = node.getChildren().get(i).accept(this, context);
          finalExchangeNum += 1;
          parentPipelineChildren.add(childOperation);
          afterwardsNodes.add(node.getChildren().get(i));
        }
      }
      if (firstChildIndex == -1) {
        context.setExchangeSumNum(finalExchangeNum);
        return parentPipelineChildren;
      }
      // If dop > localChildrenSize + 1, we can allocate extra dop to child node
      // Extra dop = dop - localChildrenSize, since dop = 1 means serial but not 0
      int dopForChild = Math.max(1, context.getDegreeOfParallelism() - localChildrenSize);
      // If dop > localChildrenSize, we create one new pipeline for each child
      if (context.getDegreeOfParallelism() > localChildrenSize) {
        for (int i = firstChildIndex; i < node.getChildren().size(); i++) {
          PlanNode childNode = node.getChildren().get(i);
          if (childNode instanceof ExchangeNode) {
            Operator childOperation = childNode.accept(this, context);
            finalExchangeNum += 1;
            parentPipelineChildren.add(childOperation);
          } else {
            LocalExecutionPlanContext subContext = context.createSubContext();
            subContext.setDegreeOfParallelism(dopForChild);

            int originPipeNum = context.getPipelineNumber();
            Operator sourceOperator =
                createNewPipelineForChildNode(context, subContext, childNode, node.getPlanNodeId());
            parentPipelineChildren.add(sourceOperator);
            dopForChild =
                Math.max(1, dopForChild - (subContext.getPipelineNumber() - 1 - originPipeNum));
            finalExchangeNum += subContext.getExchangeSumNum() - context.getExchangeSumNum() + 1;
          }
        }
      } else {
        // If dop <= localChildrenSize, we have to divide every childNumInEachPipeline localChildren
        // to different pipeline
        int[] childNumInEachPipeline =
            getChildNumInEachPipeline(
                node.getChildren(), localChildrenSize, context.getDegreeOfParallelism());
        int childGroupNum = Math.min(context.getDegreeOfParallelism(), localChildrenSize);
        int startIndex;
        int endIndex = firstChildIndex;
        for (int i = 0; i < childGroupNum; i++) {
          startIndex = endIndex;
          endIndex += childNumInEachPipeline[i];
          // Only if dop >= size(children) + 1, split all children to new pipeline
          // Otherwise, the first group will belong to the parent pipeline
          if (i == 0) {
            for (int j = startIndex; j < endIndex; j++) {
              context.setDegreeOfParallelism(1);
              Operator childOperation = node.getChildren().get(j).accept(this, context);
              parentPipelineChildren.add(childOperation);
              afterwardsNodes.add(node.getChildren().get(j));
            }
            continue;
          }
          LocalExecutionPlanContext subContext = context.createSubContext();
          subContext.setDegreeOfParallelism(1);
          // Create partial parent operator for children
          PlanNode partialParentNode;
          if (endIndex - startIndex == 1) {
            partialParentNode = node.getChildren().get(startIndex);
          } else {
            // PartialParentNode is equals to parentNode except children
            partialParentNode = node.createSubNode(i, startIndex, endIndex);
          }

          Operator sourceOperator =
              createNewPipelineForChildNode(
                  context, subContext, partialParentNode, node.getPlanNodeId());
          parentPipelineChildren.add(sourceOperator);
          afterwardsNodes.add(partialParentNode);
          finalExchangeNum += subContext.getExchangeSumNum() - context.getExchangeSumNum() + 1;
        }

        if (node instanceof MultiChildProcessNode) {
          ((MultiChildProcessNode) node).setChildren(afterwardsNodes);
        } else if (node instanceof TwoChildProcessNode) {
          checkState(afterwardsNodes.size() == 2);
          ((TwoChildProcessNode) node).setLeftChild(afterwardsNodes.get(0));
          ((TwoChildProcessNode) node).setRightChild(afterwardsNodes.get(1));
        } else {
          throw new IllegalArgumentException("Unknown node type: " + node.getClass().getName());
        }
      }
      context.setExchangeSumNum(finalExchangeNum);
    }
    return parentPipelineChildren;
  }

  /**
   * Now, we allocate children to each pipeline as average as possible. For example, 5 children with
   * 3 dop, the children group will be [1, 2, 2]. After we can estimate the workload of each
   * operator, maybe we can allocate based on workload rather than child number.
   *
   * <p>If child is ExchangeNode, it won't affect the children number of current group.
   *
   * <p>This method can only be invoked when dop <= localChildrenSize.
   */
  public int[] getChildNumInEachPipeline(
      List<PlanNode> allChildren, int localChildrenSize, int dop) {
    int maxPipelineNum = Math.min(localChildrenSize, dop);
    int[] childNumInEachPipeline = new int[maxPipelineNum];
    int avgChildNum = Math.max(1, localChildrenSize / dop);
    // allocate remaining child to group from splitIndex
    int splitIndex = maxPipelineNum - localChildrenSize % dop;
    int childIndex = 0;
    // Skip ExchangeNode at head
    while (childIndex < allChildren.size() && allChildren.get(childIndex) instanceof ExchangeNode) {
      childIndex++;
    }
    int pipelineIndex = 0;
    while (pipelineIndex < maxPipelineNum) {
      int childNum = pipelineIndex < splitIndex ? avgChildNum : avgChildNum + 1;
      int originChildIndex = childIndex;
      while (childNum >= 0 && childIndex < allChildren.size()) {
        if (!(allChildren.get(childIndex) instanceof ExchangeNode)) {
          childNum--;
          // Try to keep the first of a pipeline is not a ExchangeNode
          if (childNum == -1) {
            childIndex--;
          }
        }
        childIndex++;
      }
      childNumInEachPipeline[pipelineIndex++] = childIndex - originChildIndex;
    }
    return childNumInEachPipeline;
  }

  private Operator createNewPipelineForChildNode(
      LocalExecutionPlanContext context,
      LocalExecutionPlanContext subContext,
      PlanNode childNode,
      PlanNodeId parentNodeId) {
    Operator childOperation = childNode.accept(this, subContext);
    ISinkChannel localSinkChannel =
        MPP_DATA_EXCHANGE_MANAGER.createLocalSinkChannelForPipeline(
            // Attention, there is no parent node, use first child node instead
            subContext.getDriverContext(), childNode.getPlanNodeId().getId());
    subContext.setISink(localSinkChannel);
    subContext.addPipelineDriverFactory(childOperation, subContext.getDriverContext(), 0);
    subContext.constructPipelineMemoryEstimator(childOperation, parentNodeId, childNode, -1);

    ExchangeOperator sourceOperator =
        new ExchangeOperator(
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(), null, ExchangeOperator.class.getSimpleName()),
            MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandleForPipeline(
                ((LocalSinkChannel) localSinkChannel).getSharedTsBlockQueue(),
                context.getDriverContext()),
            childNode.getPlanNodeId(),
            childOperation.calculateMaxReturnSize());

    context.addExchangeOperator(sourceOperator);
    return sourceOperator;
  }

  public List<Operator> dealWithConsumeChildrenOneByOneNode(
      PlanNode node, LocalExecutionPlanContext context) {
    checkArgument(PipelineMemoryEstimatorFactory.isConsumeChildrenOneByOneNode(node));
    List<Operator> parentPipelineChildren = new ArrayList<>();
    int originExchangeNum = context.getExchangeSumNum();
    int finalExchangeNum = context.getExchangeSumNum();

    // 1. divide every child to pipeline using the max dop
    if (context.getDegreeOfParallelism() == 1 || node.getChildren().size() == 1) {
      // If dop = 1, we don't create extra pipeline
      for (PlanNode childSource : node.getChildren()) {
        Operator childOperation = childSource.accept(this, context);
        finalExchangeNum = Math.max(finalExchangeNum, context.getExchangeSumNum());
        context.setExchangeSumNum(originExchangeNum);
        parentPipelineChildren.add(childOperation);
        // if node.getChildren().size() == 1 and we don't create extra pipeline, the root of the
        // child pipeline should be current root
        // for example, we have IdentitySinkNode -> DeviceViewNode -> [ScanNode, ScanNode, ScanNode]
        // the parent of the pipeline of ScanNode should be IdentitySinkNode in the map, otherwise
        // we will lose the information of these pipelines
        List<PipelineMemoryEstimator> childPipelineMemoryEstimators =
            context.getParentPlanNodeIdToMemoryEstimator().get(childSource.getPlanNodeId());
        if (childPipelineMemoryEstimators != null) {
          context.getParentPlanNodeIdToMemoryEstimator().remove(childSource.getPlanNodeId());
          context
              .getParentPlanNodeIdToMemoryEstimator()
              .put(node.getPlanNodeId(), childPipelineMemoryEstimators);
        }
      }
    } else {
      List<Integer> childPipelineNums = new ArrayList<>();
      List<Integer> childExchangeNums = new ArrayList<>();
      int sumOfChildPipelines = 0;
      int sumOfChildExchangeNums = 0;
      int dependencyChildNode = 0;
      int dependencyPipeId = 0;
      for (PlanNode childNode : node.getChildren()) {
        if (childNode instanceof ExchangeNode) {
          Operator childOperation = childNode.accept(this, context);
          finalExchangeNum = Math.max(finalExchangeNum, context.getExchangeSumNum());
          context.setExchangeSumNum(originExchangeNum);
          parentPipelineChildren.add(childOperation);
        } else {
          LocalExecutionPlanContext subContext = context.createSubContext();
          // Only context.getDegreeOfParallelism() - 1 can be allocated to child
          int dopForChild = context.getDegreeOfParallelism() - 1;
          subContext.setDegreeOfParallelism(dopForChild);
          int originPipeNum = context.getPipelineNumber();
          Operator childOperation = childNode.accept(this, subContext);
          ISinkChannel localSinkChannel =
              MPP_DATA_EXCHANGE_MANAGER.createLocalSinkChannelForPipeline(
                  // Attention, there is no parent node, use first child node instead
                  context.getDriverContext(), childNode.getPlanNodeId().getId());
          subContext.setISink(localSinkChannel);
          subContext.addPipelineDriverFactory(childOperation, subContext.getDriverContext(), 0);

          // OneByOneChild may be divided into more than dop pipelines, but the number of running
          // actually is dop
          int curChildPipelineNum =
              Math.min(dopForChild, subContext.getPipelineNumber() - originPipeNum);

          childPipelineNums.add(curChildPipelineNum);
          sumOfChildPipelines += curChildPipelineNum;
          // If sumOfChildPipelines > dopForChild, we have to wait until some pipelines finish
          if (sumOfChildPipelines > dopForChild) {
            // Update dependencyPipeId, after which finishes we can submit curChildPipeline
            while (sumOfChildPipelines > dopForChild) {
              sumOfChildPipelines -= childPipelineNums.get(dependencyChildNode);
              // The dependency pipeline must be a parent pipeline rather than a child pipeline
              dependencyPipeId = context.getPipelineNumber() - sumOfChildPipelines - 1;
              sumOfChildExchangeNums -= childExchangeNums.get(dependencyChildNode);
              dependencyChildNode++;
            }
          }
          // Add dependency for all pipelines under current node
          if (dependencyChildNode != 0) {
            for (int i = originPipeNum; i < subContext.getPipelineNumber(); i++) {
              context.getPipelineDriverFactories().get(i).setDependencyPipeline(dependencyPipeId);
            }
          }

          subContext.constructPipelineMemoryEstimator(
              childOperation,
              node.getPlanNodeId(),
              childNode,
              dependencyChildNode == 0 ? -1 : dependencyPipeId);

          ExchangeOperator sourceOperator =
              new ExchangeOperator(
                  context
                      .getDriverContext()
                      .addOperatorContext(
                          context.getNextOperatorId(),
                          null,
                          ExchangeOperator.class.getSimpleName()),
                  MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandleForPipeline(
                      ((LocalSinkChannel) localSinkChannel).getSharedTsBlockQueue(),
                      context.getDriverContext()),
                  childNode.getPlanNodeId(),
                  childOperation.calculateMaxReturnSize());
          context.getCurrentPipelineDriverFactory().setDownstreamOperator(sourceOperator);
          parentPipelineChildren.add(sourceOperator);
          context.addExchangeOperator(sourceOperator);
          int childExchangeNum = subContext.getExchangeSumNum() - context.getExchangeSumNum() + 1;
          sumOfChildExchangeNums += childExchangeNum;
          childExchangeNums.add(childExchangeNum);
          finalExchangeNum =
              Math.max(finalExchangeNum, context.getExchangeSumNum() + sumOfChildExchangeNums);
        }
      }
    }
    context.setExchangeSumNum(finalExchangeNum);
    return parentPipelineChildren;
  }

  @Override
  public Operator visitColumnInject(ColumnInjectNode node, LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ColumnInjectNode.class.getSimpleName());

    Operator childOperator = node.getChild().accept(this, context);
    ColumnGeneratorParameter parameter = node.getColumnGeneratorParameter();
    ColumnGenerator columnGenerator = genColumnGeneratorAccordingToParameter(parameter);
    long maxExtraColumnSize = 0;
    for (TSDataType dataType : node.getGeneratedColumnTypes()) {
      maxExtraColumnSize += getOutputColumnSizePerLine(dataType);
    }
    maxExtraColumnSize *= TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

    return new ColumnInjectOperator(
        operatorContext, childOperator, columnGenerator, node.getTargetIndex(), maxExtraColumnSize);
  }

  private ColumnGenerator genColumnGeneratorAccordingToParameter(
      ColumnGeneratorParameter columnGeneratorParameter) {
    ColumnGeneratorType type = columnGeneratorParameter.getGeneratorType();
    if (type == ColumnGeneratorType.SLIDING_TIME) {
      SlidingTimeColumnGeneratorParameter slidingTimeColumnGeneratorParameter =
          (SlidingTimeColumnGeneratorParameter) columnGeneratorParameter;
      return new SlidingTimeColumnGenerator(
          initTimeRangeIterator(
              slidingTimeColumnGeneratorParameter.getGroupByTimeParameter(),
              slidingTimeColumnGeneratorParameter.isAscending(),
              false));
    } else {
      throw new UnsupportedOperationException("Unsupported column generator type: " + type);
    }
  }

  @Override
  public Operator visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context) {
    Operator operator = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ExplainAnalyzeOperator.class.getSimpleName());
    return new ExplainAnalyzeOperator(
        operatorContext, operator, node.getQueryId(), node.isVerbose(), node.getTimeout());
  }

  private List<TSDataType> generateRepeatTsDataType(int size, TSDataType type) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      dataTypes.add(type);
    }
    return dataTypes;
  }

  @Override
  public Operator visitRegionMerge(
      ActiveRegionScanMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> operatorList =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ActiveRegionScanMergeOperator.class.getName());

    List<TSDataType> dataTypes;
    if (node.isOutputCount()) {
      dataTypes = new ArrayList<>(Collections.singleton(TSDataType.INT64));
    } else {
      dataTypes =
          generateRepeatTsDataType(
              node.getChildren().get(0).getOutputColumnNames().size(), TSDataType.TEXT);
    }
    return new ActiveRegionScanMergeOperator(
        operatorContext,
        operatorList,
        dataTypes,
        node.isOutputCount(),
        node.isNeedMerge(),
        node.getEstimatedSize());
  }

  @Override
  public Operator visitDeviceRegionScan(
      DeviceRegionScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ActiveDeviceRegionScanOperator.class.getSimpleName());
    Filter filter = context.getGlobalTimeFilter();

    Map<IDeviceID, DeviceContext> deviceIDToContext = new HashMap<>();
    Map<IDeviceID, Long> ttlCache = new HashMap<>();
    for (Map.Entry<PartialPath, DeviceContext> entry :
        node.getDevicePathToContextMap().entrySet()) {
      PartialPath devicePath = entry.getKey();
      IDeviceID deviceID = devicePath.getIDeviceID();
      deviceIDToContext.put(deviceID, entry.getValue());
      ttlCache.put(deviceID, DataNodeTTLCache.getInstance().getTTL(deviceID));
    }
    ActiveDeviceRegionScanOperator regionScanOperator =
        new ActiveDeviceRegionScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            deviceIDToContext,
            filter,
            ttlCache,
            node.isOutputCount());

    DataDriverContext dataDriverContext = (DataDriverContext) context.getDriverContext();
    dataDriverContext.addSourceOperator(regionScanOperator);
    dataDriverContext.setDeviceIDToContext(deviceIDToContext);
    dataDriverContext.setQueryDataSourceType(QueryDataSourceType.DEVICE_REGION_SCAN);

    return regionScanOperator;
  }

  @Override
  public Operator visitTimeSeriesRegionScan(
      TimeseriesRegionScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ActiveTimeSeriesRegionScanOperator.class.getSimpleName());
    Filter filter = context.getGlobalTimeFilter();
    DataDriverContext dataDriverContext = (DataDriverContext) context.getDriverContext();

    Map<IDeviceID, Map<String, TimeseriesContext>> timeseriesToSchemaInfo = new HashMap<>();
    Map<IDeviceID, Long> ttlCache = new HashMap<>();
    for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesContext>>> entryMap :
        node.getDeviceToTimeseriesSchemaInfo().entrySet()) {
      PartialPath devicePath = entryMap.getKey();
      IDeviceID deviceID = devicePath.getIDeviceID();
      Map<String, TimeseriesContext> timeseriesSchemaInfoMap =
          getTimeseriesSchemaInfoMap(entryMap, dataDriverContext);
      timeseriesToSchemaInfo.put(deviceID, timeseriesSchemaInfoMap);
      ttlCache.put(deviceID, DataNodeTTLCache.getInstance().getTTL(deviceID));
    }
    ActiveTimeSeriesRegionScanOperator regionScanOperator =
        new ActiveTimeSeriesRegionScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            timeseriesToSchemaInfo,
            filter,
            ttlCache,
            node.isOutputCount());

    dataDriverContext.addSourceOperator(regionScanOperator);
    dataDriverContext.setQueryDataSourceType(QueryDataSourceType.TIME_SERIES_REGION_SCAN);

    return regionScanOperator;
  }

  private static Map<String, TimeseriesContext> getTimeseriesSchemaInfoMap(
      Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesContext>>> entryMap,
      DataDriverContext context) {
    Map<String, TimeseriesContext> timeseriesSchemaInfoMap = new HashMap<>();
    for (Map.Entry<PartialPath, List<TimeseriesContext>> entry : entryMap.getValue().entrySet()) {
      PartialPath path = entry.getKey();
      if (path instanceof MeasurementPath) {
        timeseriesSchemaInfoMap.put(path.getMeasurement(), entry.getValue().get(0));
        context.addPath(
            new NonAlignedFullPath(
                path.getIDeviceID(),
                new MeasurementSchema(
                    path.getMeasurement(),
                    TSDataType.valueOf(entry.getValue().get(0).getDataType()))));
      } else if (path instanceof AlignedPath) {
        AlignedPath alignedPath = (AlignedPath) path;
        List<String> measurementList = alignedPath.getMeasurementList();
        if (measurementList.size() != entry.getValue().size()) {
          throw new IllegalArgumentException(
              "The size of measurementList and timeseriesSchemaInfoList should be equal in aligned path.");
        }
        int size = measurementList.size();
        List<IMeasurementSchema> schemaList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          timeseriesSchemaInfoMap.put(measurementList.get(i), entry.getValue().get(i));
          schemaList.add(
              new MeasurementSchema(
                  measurementList.get(i),
                  TSDataType.valueOf(entry.getValue().get(i).getDataType())));
        }
        context.addPath(new AlignedFullPath(path.getIDeviceID(), measurementList, schemaList));
      }
    }
    return timeseriesSchemaInfoMap;
  }
}
