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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TableDateBinTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TableSingleTimeWindowIterator;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.EmptyDataOperator;
import org.apache.iotdb.db.queryengine.execution.operator.ExplainAnalyzeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.AssignUniqueIdOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.CollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.EnforceSingleRowOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.PatternRecognitionOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.PreviousFillWithGroupOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableFillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableLinearFillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableLinearFillWithGroupOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableStreamSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableTopKOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.TableFunctionLeafOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.function.TableFunctionOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWGroupWMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWGroupWoMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWoGroupWMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWoGroupWoMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.InnerTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.SimpleNestedLoopCrossJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.TableLeftOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparatorFactory;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.LogicalIndexNavigation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PatternVariableRecognizer;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValueAccessor;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.Computation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression.PatternExpressionComputation;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.IrRowPatternToProgramRewriter;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.Matcher;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.Program;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.TableWindowOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunctionFactory;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate.AggregationWindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate.WindowAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AsofMergeSortInnerJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AsofMergeSortLeftJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DefaultAggTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DeviceIteratorScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.LastQueryAggTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MarkDistinctOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MergeSortFullOuterJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MergeSortInnerJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MergeSortLeftJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MergeSortSemiJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeAlignedDeviceViewAggregationScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeAlignedDeviceViewScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TreeToTableViewAdaptorOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastByDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.HashAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.StreamingAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.StreamingHashAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToBlobLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToBooleanLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToDateLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToDoubleLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToFloatLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToInt32LiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToInt64LiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToStringLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToTimestampLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ClassifierValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.LogicalIndexPointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.MatchNumberValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ScalarValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.idcolumn.FourOrHigherLevelDBExtractor;
import org.apache.tsfile.file.metadata.idcolumn.ThreeLevelDBExtractor;
import org.apache.tsfile.file.metadata.idcolumn.TwoLevelDBExtractor;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.getAggregationTypeByFuncName;
import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer.CLASSIFIER;
import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer.MATCH_NUMBER;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaContentSupplierFactory.getSupplier;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory.createAccumulator;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory.createBuiltinAccumulator;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory.createGroupedAccumulator;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.DEFAULT_GROUP_NUMBER;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.IDENTITY_FILL;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.UNKNOWN_DATATYPE;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.getLinearFill;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.getPreviousFill;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions.updateFilterUsingTTL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.isTimeColumn;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch.ONE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.SkipToPosition.LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.utils.constant.SqlConstant.AVG;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;
import static org.apache.iotdb.db.utils.constant.SqlConstant.EXTREME;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_BY_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_BY_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.MAX;
import static org.apache.iotdb.db.utils.constant.SqlConstant.MIN;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUM;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

/** This Visitor is responsible for transferring Table PlanNode Tree to Table Operator Tree. */
public class TableOperatorGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext> {

  private final Metadata metadata;

  public TableOperatorGenerator(Metadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
  }

  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

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
    sinkHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    context.getDriverContext().setSink(sinkHandle);

    if (node.getChildren().size() == 1) {
      Operator child = node.getChildren().get(0).accept(this, context);
      List<Operator> children = new ArrayList<>(1);
      children.add(child);
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
      for (TsTableColumnSchema columnSchema : tsTable.getColumnList()) {
        if (columnSchema.getColumnCategory() == FIELD) {
          node.getAssignments()
              .put(
                  new Symbol(columnSchema.getColumnName()),
                  new ColumnSchema(
                      columnSchema.getColumnName(),
                      TypeFactory.getType(columnSchema.getDataType()),
                      false,
                      columnSchema.getColumnCategory()));
          containsFieldColumn = true;
        }
      }
    }
    // For non-aligned series, scan cannot be performed when no field columns
    // can be obtained, so an empty result set is returned.
    if (!containsFieldColumn || node.getDeviceEntries().isEmpty()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  EmptyDataOperator.class.getSimpleName());
      return new EmptyDataOperator(operatorContext);
    }
    String treePrefixPath = DataNodeTreeViewSchemaUtils.getPrefixPath(tsTable);
    IDeviceID.TreeDeviceIdColumnValueExtractor extractor =
        TableOperatorGenerator.createTreeDeviceIdColumnValueExtractor(treePrefixPath);
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
    List<String> measurementColumnNames = commonParameter.measurementColumnNames;
    List<ColumnSchema> fullColumnSchemas = commonParameter.columnSchemas;
    List<Symbol> symbolInputs = commonParameter.symbolInputs;
    int[] columnsIndexArray = commonParameter.columnsIndexArray;

    boolean isSingleColumn = measurementSchemas.size() == 1;

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(context.getNextOperatorId(), node.getPlanNodeId(), className);

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

          @Override
          public boolean keepOffsetAndLimitOperatorAfterDeviceIterator() {
            calculateSeriesScanOptionsList();
            return !removeUpperOffsetAndLimitOperator && !node.isPushLimitToEachDevice();
          }

          @Override
          public void generateCurrentDeviceOperatorTree(DeviceEntry deviceEntry) {
            calculateSeriesScanOptionsList();
            operator = constructTreeToTableViewAdaptorOperator(deviceEntry);
            if (isSingleColumn) {
              return;
            }
            if (!cannotPushDownConjuncts.isEmpty()
                || node.getAssignments().size() != node.getOutputSymbols().size()) {
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
            Map<String, List<Expression>> pushDownConjunctsForEachMeasurement = new HashMap<>();
            if (node.getPushDownPredicate() != null) {
              List<Expression> conjuncts = IrUtils.extractConjuncts(node.getPushDownPredicate());
              for (Expression conjunct : conjuncts) {
                Set<Symbol> symbols = SymbolsExtractor.extractUnique(conjunct);
                boolean containsMultiDataSource = symbols.size() > 1;
                if (containsMultiDataSource) {
                  cannotPushDownConjuncts.add(conjunct);
                  continue;
                }
                String symbolName = symbols.iterator().next().getName();
                pushDownConjunctsForEachMeasurement
                    .computeIfAbsent(symbolName, k -> new ArrayList<>())
                    .add(conjunct);
              }
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
            for (int i = 0; i < measurementSchemas.size(); i++) {
              IMeasurementSchema measurementSchema = measurementSchemas.get(i);
              List<Expression> pushDownPredicatesForCurrentMeasurement =
                  pushDownConjunctsForEachMeasurement.get(measurementSchema.getMeasurementName());
              Expression pushDownPredicateForCurrentMeasurement =
                  isSingleColumn
                      ? node.getPushDownPredicate()
                      : (pushDownPredicatesForCurrentMeasurement == null
                          ? null
                          : IrUtils.combineConjuncts(pushDownPredicatesForCurrentMeasurement));
              SeriesScanOptions.Builder builder =
                  node.getTimePredicate()
                      .map(expression -> getSeriesScanOptionsBuilder(context, expression))
                      .orElseGet(SeriesScanOptions.Builder::new);
              builder
                  .withIsTableViewForTreeModel(true)
                  .withAllSensors(new HashSet<>(measurementColumnNames));
              if (pushDownPredicateForCurrentMeasurement != null) {
                builder.withPushDownFilter(
                    convertPredicateToFilter(
                        pushDownPredicateForCurrentMeasurement,
                        Collections.singletonMap(measurementSchema.getMeasurementName(), 0),
                        commonParameter.columnSchemaMap,
                        commonParameter.timeColumnName));
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
                    TableOperatorGenerator.this.constructFilterAndProjectOperator(
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

  public static IDeviceID.TreeDeviceIdColumnValueExtractor createTreeDeviceIdColumnValueExtractor(
      String treeDBName) {
    try {
      PartialPath db = new PartialPath(treeDBName);
      int dbLevel = db.getNodes().length;
      if (dbLevel == 2) {
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

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(context.getNextOperatorId(), node.getPlanNodeId(), className);

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
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  EmptyDataOperator.class.getSimpleName());
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

  public static Map<Symbol, List<InputLocation>> makeLayout(final List<PlanNode> children) {
    final Map<Symbol, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    for (final PlanNode childNode : children) {
      int valueColumnIndex = 0;
      for (final Symbol columnName : childNode.getOutputSymbols()) {
        outputMappings
            .computeIfAbsent(columnName, key -> new ArrayList<>())
            .add(new InputLocation(tsBlockIndex, valueColumnIndex));
        valueColumnIndex++;
      }
      tsBlockIndex++;
    }
    return outputMappings;
  }

  private ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols) {
    if (outputSymbols == null) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    int channel = 0;
    for (Symbol symbol : outputSymbols) {
      outputMappings.put(symbol, channel);
      channel++;
    }
    return outputMappings.buildOrThrow();
  }

  private SeriesScanOptions.Builder getSeriesScanOptionsBuilder(
      LocalExecutionPlanContext context, @NotNull Expression timePredicate) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();

    Filter timeFilter = timePredicate.accept(new ConvertPredicateToTimeFilterVisitor(), null);
    context.getDriverContext().getFragmentInstanceContext().setTimeFilterForTableModel(timeFilter);
    // time filter may be stateful, so we need to copy it
    scanOptionsBuilder.withGlobalTimeFilter(timeFilter.copy());

    return scanOptionsBuilder;
  }

  @Override
  public Operator visitInformationSchemaTableScan(
      final InformationSchemaTableScanNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                InformationSchemaTableScanOperator.class.getSimpleName());

    final List<TSDataType> dataTypes =
        node.getOutputSymbols().stream()
            .map(symbol -> getTSDataType(context.getTypeProvider().getTableModelType(symbol)))
            .collect(Collectors.toList());

    return new InformationSchemaTableScanOperator(
        operatorContext,
        node.getPlanNodeId(),
        getSupplier(
            node.getQualifiedObjectName().getObjectName(),
            dataTypes,
            context
                .getDriverContext()
                .getFragmentInstanceContext()
                .getSessionInfo()
                .getUserName()));
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
    TypeProvider typeProvider = context.getTypeProvider();
    Optional<Expression> predicate = Optional.of(node.getPredicate());
    Operator inputOperator = node.getChild().accept(this, context);
    List<TSDataType> inputDataTypes = getInputColumnTypes(node, typeProvider);
    Map<Symbol, List<InputLocation>> inputLocations = makeLayout(node.getChildren());

    return constructFilterAndProjectOperator(
        predicate,
        inputOperator,
        node.getOutputSymbols().stream().map(Symbol::toSymbolReference).toArray(Expression[]::new),
        inputDataTypes,
        inputLocations,
        node.getPlanNodeId(),
        context);
  }

  private Operator constructFilterAndProjectOperator(
      Optional<Expression> predicate,
      Operator inputOperator,
      Expression[] projectExpressions,
      List<TSDataType> inputDataTypes,
      Map<Symbol, List<InputLocation>> inputLocations,
      PlanNodeId planNodeId,
      LocalExecutionPlanContext context) {

    final List<TSDataType> filterOutputDataTypes = new ArrayList<>(inputDataTypes);

    // records LeafColumnTransformer of filter
    List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    ColumnTransformerBuilder visitor = new ColumnTransformerBuilder();

    ColumnTransformer filterOutputTransformer =
        predicate
            .map(
                p -> {
                  ColumnTransformerBuilder.Context filterColumnTransformerContext =
                      new ColumnTransformerBuilder.Context(
                          context.getDriverContext().getFragmentInstanceContext().getSessionInfo(),
                          filterLeafColumnTransformerList,
                          inputLocations,
                          filterExpressionColumnTransformerMap,
                          ImmutableMap.of(),
                          ImmutableList.of(),
                          ImmutableList.of(),
                          0,
                          context.getTypeProvider(),
                          metadata);

                  return visitor.process(p, filterColumnTransformerContext);
                })
            .orElse(null);

    // records LeafColumnTransformer of project expressions
    List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

    List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();

    Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

    // records common ColumnTransformer between filter and project expressions
    List<ColumnTransformer> commonTransformerList = new ArrayList<>();

    ColumnTransformerBuilder.Context projectColumnTransformerContext =
        new ColumnTransformerBuilder.Context(
            context.getDriverContext().getFragmentInstanceContext().getSessionInfo(),
            projectLeafColumnTransformerList,
            inputLocations,
            projectExpressionColumnTransformerMap,
            filterExpressionColumnTransformerMap,
            commonTransformerList,
            filterOutputDataTypes,
            inputLocations.size(),
            context.getTypeProvider(),
            metadata);

    for (Expression expression : projectExpressions) {
      projectOutputTransformerList.add(
          visitor.process(expression, projectColumnTransformerContext));
    }

    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                planNodeId,
                FilterAndProjectOperator.class.getSimpleName());

    // Project expressions don't contain Non-Mappable UDF, TransformOperator is not needed
    return new FilterAndProjectOperator(
        operatorContext,
        inputOperator,
        filterOutputDataTypes,
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        commonTransformerList,
        projectLeafColumnTransformerList,
        projectOutputTransformerList,
        false,
        predicate.isPresent());
  }

  @Override
  public Operator visitProject(ProjectNode node, LocalExecutionPlanContext context) {
    TypeProvider typeProvider = context.getTypeProvider();
    Optional<Expression> predicate;
    Operator inputOperator;
    List<TSDataType> inputDataTypes;
    Map<Symbol, List<InputLocation>> inputLocations;
    if (node.getChild() instanceof FilterNode) {
      FilterNode filterNode = (FilterNode) node.getChild();
      predicate = Optional.of(filterNode.getPredicate());
      inputOperator = filterNode.getChild().accept(this, context);
      inputDataTypes = getInputColumnTypes(filterNode, typeProvider);
      inputLocations = makeLayout(filterNode.getChildren());
    } else {
      predicate = Optional.empty();
      inputOperator = node.getChild().accept(this, context);
      inputDataTypes = getInputColumnTypes(node, typeProvider);
      inputLocations = makeLayout(node.getChildren());
    }

    return constructFilterAndProjectOperator(
        predicate,
        inputOperator,
        node.getAssignments().getMap().values().toArray(new Expression[0]),
        inputDataTypes,
        inputLocations,
        node.getPlanNodeId(),
        context);
  }

  private List<TSDataType> getInputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    // ignore "time" column
    return node.getChildren().stream()
        .map(PlanNode::getOutputSymbols)
        .flatMap(List::stream)
        .map(s -> getTSDataType(typeProvider.getTableModelType(s)))
        .collect(Collectors.toList());
  }

  @Override
  public Operator visitGapFill(GapFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int timeColumnIndex = getColumnIndex(node.getGapFillColumn(), node.getChild());
    if (node.getGapFillGroupingKeys().isEmpty()) { // without group keys
      if (node.getMonthDuration() == 0) { // without month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWoGroupWoMoOperator.class.getSimpleName());
        return new GapFillWoGroupWoMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            inputDataTypes,
            node.getNonMonthDuration());
      } else { // with month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWoGroupWMoOperator.class.getSimpleName());
        return new GapFillWoGroupWMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            inputDataTypes,
            node.getMonthDuration(),
            context.getZoneId());
      }

    } else { // with group keys
      Set<Integer> groupingKeysIndexSet = new HashSet<>();
      Comparator<SortKey> groupKeyComparator =
          genFillGroupKeyComparator(
              node.getGapFillGroupingKeys(), node, inputDataTypes, groupingKeysIndexSet);
      if (node.getMonthDuration() == 0) { // without month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWGroupWoMoOperator.class.getSimpleName());
        return new GapFillWGroupWoMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            groupKeyComparator,
            inputDataTypes,
            groupingKeysIndexSet,
            node.getNonMonthDuration());
      } else { // with month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWGroupWMoOperator.class.getSimpleName());
        return new GapFillWGroupWMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            groupKeyComparator,
            inputDataTypes,
            groupingKeysIndexSet,
            node.getMonthDuration(),
            context.getZoneId());
      }
    }
  }

  @Override
  public Operator visitPreviousFill(PreviousFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);

    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    int helperColumnIndex = -1;
    if (node.getHelperColumn().isPresent()) {
      helperColumnIndex = getColumnIndex(node.getHelperColumn().get(), node.getChild());
    }
    IFill[] fillArray =
        getPreviousFill(
            inputColumnCount,
            inputDataTypes,
            node.getTimeBound().orElse(null),
            context.getZoneId());

    if (node.getGroupingKeys().isPresent()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  PreviousFillWithGroupOperator.class.getSimpleName());
      return new PreviousFillWithGroupOperator(
          operatorContext,
          fillArray,
          child,
          helperColumnIndex,
          genFillGroupKeyComparator(
              node.getGroupingKeys().get(), node, inputDataTypes, new HashSet<>()),
          inputDataTypes);
    } else {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableFillOperator.class.getSimpleName());
      return new TableFillOperator(operatorContext, fillArray, child, helperColumnIndex);
    }
  }

  // used by fill and gapfill
  private Comparator<SortKey> genFillGroupKeyComparator(
      List<Symbol> groupingKeys,
      SingleChildProcessNode node,
      List<TSDataType> inputDataTypes,
      Set<Integer> groupKeysIndex) {
    int groupKeysCount = groupingKeys.size();
    List<SortOrder> sortOrderList = new ArrayList<>(groupKeysCount);
    List<Integer> groupItemIndexList = new ArrayList<>(groupKeysCount);
    List<TSDataType> groupItemDataTypeList = new ArrayList<>(groupKeysCount);
    Map<Symbol, Integer> columnIndex =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());
    for (Symbol symbol : groupingKeys) {
      // sort order for fill_group should always be ASC_NULLS_LAST, it should be same as
      // QueryPlanner.fillGroup
      sortOrderList.add(ASC_NULLS_LAST);
      int index = columnIndex.get(symbol);
      groupItemIndexList.add(index);
      groupItemDataTypeList.add(inputDataTypes.get(index));
    }
    groupKeysIndex.addAll(groupItemIndexList);
    return getComparatorForTable(sortOrderList, groupItemIndexList, groupItemDataTypeList);
  }

  // index starts from 0
  private int getColumnIndex(Symbol symbol, PlanNode node) {
    String name = symbol.getName();
    int channel = 0;
    for (Symbol columnName : node.getOutputSymbols()) {
      if (columnName.getName().equals(name)) {
        return channel;
      }
      channel++;
    }
    throw new IllegalStateException(
        String.format("Found no column %s in %s", symbol, node.getOutputSymbols()));
  }

  @Override
  public Operator visitLinearFill(LinearFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);

    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    int helperColumnIndex = getColumnIndex(node.getHelperColumn(), node.getChild());
    ILinearFill[] fillArray = getLinearFill(inputColumnCount, inputDataTypes);

    if (node.getGroupingKeys().isPresent()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableLinearFillWithGroupOperator.class.getSimpleName());
      return new TableLinearFillWithGroupOperator(
          operatorContext,
          fillArray,
          child,
          helperColumnIndex,
          genFillGroupKeyComparator(
              node.getGroupingKeys().get(), node, inputDataTypes, new HashSet<>()),
          inputDataTypes);
    } else {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableLinearFillOperator.class.getSimpleName());
      return new TableLinearFillOperator(operatorContext, fillArray, child, helperColumnIndex);
    }
  }

  @Override
  public Operator visitValueFill(ValueFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableFillOperator.class.getSimpleName());
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    Literal filledValue = node.getFilledValue();
    return new TableFillOperator(
        operatorContext,
        getValueFill(inputColumnCount, inputDataTypes, filledValue, context),
        child,
        -1);
  }

  private IFill[] getValueFill(
      int inputColumnCount,
      List<TSDataType> inputDataTypes,
      Literal filledValue,
      LocalExecutionPlanContext context) {
    IFill[] constantFill = new IFill[inputColumnCount];
    for (int i = 0; i < inputColumnCount; i++) {
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          Boolean bool = filledValue.accept(new CastToBooleanLiteralVisitor(), null);
          if (bool == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new BooleanConstantFill(bool);
          }
          break;
        case TEXT:
        case STRING:
          Binary binary =
              filledValue.accept(new CastToStringLiteralVisitor(TSFileConfig.STRING_CHARSET), null);
          if (binary == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new BinaryConstantFill(binary);
          }
          break;
        case BLOB:
          Binary blob = filledValue.accept(new CastToBlobLiteralVisitor(), null);
          if (blob == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new BinaryConstantFill(blob);
          }
          break;
        case INT32:
          Integer intValue = filledValue.accept(new CastToInt32LiteralVisitor(), null);
          if (intValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new IntConstantFill(intValue);
          }
          break;
        case DATE:
          Integer dateValue = filledValue.accept(new CastToDateLiteralVisitor(), null);
          if (dateValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new IntConstantFill(dateValue);
          }
          break;
        case INT64:
          Long longValue = filledValue.accept(new CastToInt64LiteralVisitor(), null);
          if (longValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new LongConstantFill(longValue);
          }
          break;
        case TIMESTAMP:
          Long timestampValue =
              filledValue.accept(new CastToTimestampLiteralVisitor(context.getZoneId()), null);
          if (timestampValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new LongConstantFill(timestampValue);
          }
          break;
        case FLOAT:
          Float floatValue = filledValue.accept(new CastToFloatLiteralVisitor(), null);
          if (floatValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new FloatConstantFill(floatValue);
          }
          break;
        case DOUBLE:
          Double doubleValue = filledValue.accept(new CastToDoubleLiteralVisitor(), null);
          if (doubleValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new DoubleConstantFill(doubleValue);
          }
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return constantFill;
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

    return new LimitOperator(operatorContext, node.getCount(), child);
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

    return new OffsetOperator(operatorContext, node.getCount(), child);
  }

  @Override
  public Operator visitOutput(OutputNode node, LocalExecutionPlanContext context) {
    return node.getChild().accept(this, context);
  }

  @Override
  public Operator visitCollect(CollectNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CollectOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    return new CollectOperator(operatorContext, children);
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableMergeSortOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());

    return new TableMergeSortOperator(
        operatorContext,
        children,
        dataTypes,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());

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

    return new TableSortOperator(
        operatorContext,
        child,
        dataTypes,
        filePrefix,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableTopKOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());
    return new TableTopKOperator(
        operatorContext,
        children,
        dataTypes,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList),
        (int) node.getCount(),
        node.isChildrenDataInOrder());
  }

  private List<TSDataType> getOutputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    return node.getOutputSymbols().stream()
        .map(s -> getTSDataType(typeProvider.getTableModelType(s)))
        .collect(Collectors.toList());
  }

  private void genSortInformation(
      List<Symbol> outputSymbols,
      OrderingScheme orderingScheme,
      List<Integer> sortItemIndexList,
      List<TSDataType> sortItemDataTypeList,
      TypeProvider typeProvider) {
    Map<Symbol, Integer> columnIndex = new HashMap<>();
    int index = 0;
    for (Symbol symbol : outputSymbols) {
      columnIndex.put(symbol, index++);
    }
    orderingScheme
        .getOrderBy()
        .forEach(
            sortItem -> {
              Integer i = columnIndex.get(sortItem);
              if (i == null) {
                throw new IllegalStateException(
                    String.format(
                        "Sort Item %s is not included in children's output columns", sortItem));
              }
              sortItemIndexList.add(i);
              sortItemDataTypeList.add(getTSDataType(typeProvider.getTableModelType(sortItem)));
            });
  }

  @Override
  public Operator visitStreamSort(StreamSortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableStreamSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());

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

    return new TableStreamSortOperator(
        operatorContext,
        child,
        dataTypes,
        filePrefix,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList),
        getComparatorForTable(
            node.getOrderingScheme()
                .getOrderingList()
                .subList(0, node.getStreamCompareKeyEndIndex() + 1),
            sortItemIndexList.subList(0, node.getStreamCompareKeyEndIndex() + 1),
            sortItemDataTypeList.subList(0, node.getStreamCompareKeyEndIndex() + 1)),
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber());
  }

  @Override
  public Operator visitGroup(GroupNode node, LocalExecutionPlanContext context) {
    StreamSortNode streamSortNode =
        new StreamSortNode(
            node.getPlanNodeId(),
            node.getChild(),
            node.getOrderingScheme(),
            false,
            false,
            node.getPartitionKeyCount() - 1);
    return visitStreamSort(streamSortNode, context);
  }

  @Override
  public Operator visitJoin(JoinNode node, LocalExecutionPlanContext context) {
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());

    Operator leftChild = node.getLeftChild().accept(this, context);
    Operator rightChild = node.getRightChild().accept(this, context);

    ImmutableMap<Symbol, Integer> leftColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getLeftChild().getOutputSymbols());
    int[] leftOutputSymbolIdx = new int[node.getLeftOutputSymbols().size()];
    for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
      Integer index = leftColumnNamesMap.get(node.getLeftOutputSymbols().get(i));
      if (index == null) {
        throw new IllegalStateException(
            "Left child of JoinNode doesn't contain LeftOutputSymbol "
                + node.getLeftOutputSymbols().get(i));
      }
      leftOutputSymbolIdx[i] = index;
    }

    ImmutableMap<Symbol, Integer> rightColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getRightChild().getOutputSymbols());
    int[] rightOutputSymbolIdx = new int[node.getRightOutputSymbols().size()];
    for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
      Integer index = rightColumnNamesMap.get(node.getRightOutputSymbols().get(i));
      if (index == null) {
        throw new IllegalStateException(
            "Right child of JoinNode doesn't contain RightOutputSymbol "
                + node.getLeftOutputSymbols().get(i));
      }
      rightOutputSymbolIdx[i] = index;
    }

    // cross join does not need time column
    if (node.isCrossJoin()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  SimpleNestedLoopCrossJoinOperator.class.getSimpleName());
      return new SimpleNestedLoopCrossJoinOperator(
          operatorContext,
          leftChild,
          rightChild,
          leftOutputSymbolIdx,
          rightOutputSymbolIdx,
          dataTypes);
    }

    semanticCheckForJoin(node);

    JoinNode.AsofJoinClause asofJoinClause = node.getAsofCriteria().orElse(null);
    int equiSize = node.getCriteria().size();
    int size = equiSize + (asofJoinClause == null ? 0 : 1);
    int[] leftJoinKeyPositions = new int[size];
    for (int i = 0; i < equiSize; i++) {
      Integer leftJoinKeyPosition = leftColumnNamesMap.get(node.getCriteria().get(i).getLeft());
      if (leftJoinKeyPosition == null) {
        throw new IllegalStateException("Left child of JoinNode doesn't contain left join key.");
      }
      leftJoinKeyPositions[i] = leftJoinKeyPosition;
    }

    List<Type> joinKeyTypes = new ArrayList<>(size);
    int[] rightJoinKeyPositions = new int[size];
    for (int i = 0; i < equiSize; i++) {
      Integer rightJoinKeyPosition = rightColumnNamesMap.get(node.getCriteria().get(i).getRight());
      if (rightJoinKeyPosition == null) {
        throw new IllegalStateException("Right child of JoinNode doesn't contain right join key.");
      }
      rightJoinKeyPositions[i] = rightJoinKeyPosition;

      Type leftJoinKeyType =
          context.getTypeProvider().getTableModelType(node.getCriteria().get(i).getLeft());
      checkIfJoinKeyTypeMatches(
          leftJoinKeyType,
          context.getTypeProvider().getTableModelType(node.getCriteria().get(i).getRight()));
      joinKeyTypes.add(leftJoinKeyType);
    }

    if (asofJoinClause != null) {
      Integer leftAsofJoinKeyPosition = leftColumnNamesMap.get(asofJoinClause.getLeft());
      if (leftAsofJoinKeyPosition == null) {
        throw new IllegalStateException(
            "Left child of JoinNode doesn't contain left ASOF main join key.");
      }
      leftJoinKeyPositions[equiSize] = leftAsofJoinKeyPosition;
      Integer rightAsofJoinKeyPosition = rightColumnNamesMap.get(asofJoinClause.getRight());
      if (rightAsofJoinKeyPosition == null) {
        throw new IllegalStateException(
            "Right child of JoinNode doesn't contain right ASOF main join key.");
      }
      rightJoinKeyPositions[equiSize] = rightAsofJoinKeyPosition;

      if (context.getTypeProvider().getTableModelType(asofJoinClause.getLeft()) != TIMESTAMP) {
        throw new IllegalStateException("Type of left ASOF Join key is not TIMESTAMP");
      }
      if (context.getTypeProvider().getTableModelType(asofJoinClause.getRight()) != TIMESTAMP) {
        throw new IllegalStateException("Type of right ASOF Join key is not TIMESTAMP");
      }

      ComparisonExpression.Operator asofOperator = asofJoinClause.getOperator();

      if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.INNER) {
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    AsofMergeSortInnerJoinOperator.class.getSimpleName());
        return new AsofMergeSortInnerJoinOperator(
            operatorContext,
            leftChild,
            leftJoinKeyPositions,
            leftOutputSymbolIdx,
            rightChild,
            rightJoinKeyPositions,
            rightOutputSymbolIdx,
            JoinKeyComparatorFactory.getAsofComparators(
                joinKeyTypes,
                asofOperator == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL
                    || asofOperator == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                !asofJoinClause.isOperatorContainsGreater()),
            dataTypes);
      } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.LEFT) {
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    AsofMergeSortLeftJoinOperator.class.getSimpleName());
        return new AsofMergeSortLeftJoinOperator(
            operatorContext,
            leftChild,
            leftJoinKeyPositions,
            leftOutputSymbolIdx,
            rightChild,
            rightJoinKeyPositions,
            rightOutputSymbolIdx,
            JoinKeyComparatorFactory.getAsofComparators(
                joinKeyTypes,
                asofOperator == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL
                    || asofOperator == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                !asofJoinClause.isOperatorContainsGreater()),
            dataTypes);
      } else {
        throw new IllegalStateException("Unsupported ASOF join type: " + node.getJoinType());
      }
    }

    if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.INNER) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  MergeSortInnerJoinOperator.class.getSimpleName());
      return new MergeSortInnerJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPositions,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPositions,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparators(joinKeyTypes, true),
          dataTypes);
    } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.FULL) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  MergeSortFullOuterJoinOperator.class.getSimpleName());
      return new MergeSortFullOuterJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPositions,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPositions,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparators(joinKeyTypes, true),
          dataTypes,
          joinKeyTypes.stream().map(this::buildUpdateLastRowFunction).collect(Collectors.toList()));
    } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.LEFT) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  MergeSortLeftJoinOperator.class.getSimpleName());
      return new MergeSortLeftJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPositions,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPositions,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparators(joinKeyTypes, true),
          dataTypes);
    }

    throw new IllegalStateException("Unsupported join type: " + node.getJoinType());
  }

  private void semanticCheckForJoin(JoinNode node) {
    try {
      checkArgument(
          !node.getFilter().isPresent() || node.getFilter().get().equals(TRUE_LITERAL),
          String.format(
              "Filter is not supported in %s. Filter is %s.",
              node.getJoinType(), node.getFilter().map(Expression::toString).orElse("null")));
      checkArgument(
          !node.getCriteria().isEmpty() || node.getAsofCriteria().isPresent(),
          String.format("%s must have join keys.", node.getJoinType()));
    } catch (IllegalArgumentException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  private BiFunction<Column, Integer, Column> buildUpdateLastRowFunction(Type joinKeyType) {
    switch (joinKeyType.getTypeEnum()) {
      case INT32:
      case DATE:
        return (inputColumn, rowIndex) ->
            new IntColumn(1, Optional.empty(), new int[] {inputColumn.getInt(rowIndex)});
      case INT64:
      case TIMESTAMP:
        return (inputColumn, rowIndex) ->
            new LongColumn(1, Optional.empty(), new long[] {inputColumn.getLong(rowIndex)});
      case FLOAT:
        return (inputColumn, rowIndex) ->
            new FloatColumn(1, Optional.empty(), new float[] {inputColumn.getFloat(rowIndex)});
      case DOUBLE:
        return (inputColumn, rowIndex) ->
            new DoubleColumn(1, Optional.empty(), new double[] {inputColumn.getDouble(rowIndex)});
      case BOOLEAN:
        return (inputColumn, rowIndex) ->
            new BooleanColumn(
                1, Optional.empty(), new boolean[] {inputColumn.getBoolean(rowIndex)});
      case STRING:
      case TEXT:
      case BLOB:
        return (inputColumn, rowIndex) ->
            new BinaryColumn(1, Optional.empty(), new Binary[] {inputColumn.getBinary(rowIndex)});
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + joinKeyType);
    }
  }

  @Override
  public Operator visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context) {
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());

    Operator leftChild = node.getLeftChild().accept(this, context);
    Operator rightChild = node.getRightChild().accept(this, context);

    ImmutableMap<Symbol, Integer> sourceColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getSource().getOutputSymbols());
    List<Symbol> sourceOutputSymbols = node.getSource().getOutputSymbols();
    int[] sourceOutputSymbolIdx = new int[node.getSource().getOutputSymbols().size()];
    for (int i = 0; i < sourceOutputSymbolIdx.length; i++) {
      Integer index = sourceColumnNamesMap.get(sourceOutputSymbols.get(i));
      checkNotNull(index, "Source of SemiJoinNode doesn't contain sourceOutputSymbol.");
      sourceOutputSymbolIdx[i] = index;
    }

    ImmutableMap<Symbol, Integer> filteringSourceColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getRightChild().getOutputSymbols());

    Integer sourceJoinKeyPosition = sourceColumnNamesMap.get(node.getSourceJoinSymbol());
    checkNotNull(sourceJoinKeyPosition, "Source of SemiJoinNode doesn't contain sourceJoinSymbol.");

    Integer filteringSourceJoinKeyPosition =
        filteringSourceColumnNamesMap.get(node.getFilteringSourceJoinSymbol());
    checkNotNull(
        filteringSourceJoinKeyPosition,
        "FilteringSource of SemiJoinNode doesn't contain filteringSourceJoinSymbol.");

    Type sourceJoinKeyType =
        context.getTypeProvider().getTableModelType(node.getSourceJoinSymbol());

    checkIfJoinKeyTypeMatches(
        sourceJoinKeyType,
        context.getTypeProvider().getTableModelType(node.getFilteringSourceJoinSymbol()));

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                MergeSortSemiJoinOperator.class.getSimpleName());
    return new MergeSortSemiJoinOperator(
        operatorContext,
        leftChild,
        sourceJoinKeyPosition,
        sourceOutputSymbolIdx,
        rightChild,
        filteringSourceJoinKeyPosition,
        JoinKeyComparatorFactory.getComparator(sourceJoinKeyType, true),
        dataTypes);
  }

  private void checkIfJoinKeyTypeMatches(Type leftJoinKeyType, Type rightJoinKeyType) {
    if (leftJoinKeyType != rightJoinKeyType) {
      throw new SemanticException(
          "Join key type mismatch. Left join key type: "
              + leftJoinKeyType
              + ", right join key type: "
              + rightJoinKeyType);
    }
  }

  @Override
  public Operator visitEnforceSingleRow(
      EnforceSingleRowNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                EnforceSingleRowOperator.class.getSimpleName());

    return new EnforceSingleRowOperator(operatorContext, child);
  }

  @Override
  public Operator visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                EnforceSingleRowOperator.class.getSimpleName());

    return new AssignUniqueIdOperator(operatorContext, child);
  }

  @Override
  public Operator visitCountMerge(
      final CountSchemaMergeNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CountMergeOperator.class.getSimpleName());
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
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
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
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    SchemaQueryScanOperator.class.getSimpleName()),
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
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaCountOperator.class.getSimpleName()),
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
                                context
                                    .getDriverContext()
                                    .getFragmentInstanceContext()
                                    .getSessionInfo(),
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
  public Operator visitAggregation(AggregationNode node, LocalExecutionPlanContext context) {

    Operator child = node.getChild().accept(this, context);

    if (node.getGroupingKeys().isEmpty()) {
      return planGlobalAggregation(node, child, context.getTypeProvider(), context);
    }

    return planGroupByAggregation(node, child, context.getTypeProvider(), context);
  }

  private Operator planGlobalAggregation(
      AggregationNode node,
      Operator child,
      TypeProvider typeProvider,
      LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AggregationOperator.class.getSimpleName());
    Map<Symbol, AggregationNode.Aggregation> aggregationMap = node.getAggregations();
    ImmutableList.Builder<TableAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    node.getOutputSymbols()
        .forEach(
            symbol ->
                aggregatorBuilder.add(
                    buildAggregator(
                        childLayout,
                        symbol,
                        aggregationMap.get(symbol),
                        node.getStep(),
                        typeProvider,
                        true,
                        null)));
    return new AggregationOperator(operatorContext, child, aggregatorBuilder.build());
  }

  // timeColumnName will only be set for AggTableScan.
  private TableAggregator buildAggregator(
      Map<Symbol, Integer> childLayout,
      Symbol symbol,
      AggregationNode.Aggregation aggregation,
      AggregationNode.Step step,
      TypeProvider typeProvider,
      boolean scanAscending,
      String timeColumnName) {
    List<Integer> argumentChannels = new ArrayList<>();
    for (Expression argument : aggregation.getArguments()) {
      Symbol argumentSymbol = Symbol.from(argument);
      argumentChannels.add(childLayout.get(argumentSymbol));
    }

    String functionName = aggregation.getResolvedFunction().getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        aggregation.getResolvedFunction().getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());
    TableAccumulator accumulator =
        createAccumulator(
            functionName,
            getAggregationTypeByFuncName(functionName),
            originalArgumentTypes,
            aggregation.getArguments(),
            Collections.emptyMap(),
            scanAscending,
            timeColumnName,
            aggregation.isDistinct());

    OptionalInt maskChannel = OptionalInt.empty();
    if (aggregation.hasMask()) {
      maskChannel = OptionalInt.of(childLayout.get(aggregation.getMask().get()));
    }

    return new TableAggregator(
        accumulator,
        step,
        getTSDataType(typeProvider.getTableModelType(symbol)),
        argumentChannels,
        maskChannel);
  }

  private Operator planGroupByAggregation(
      AggregationNode node,
      Operator child,
      TypeProvider typeProvider,
      LocalExecutionPlanContext context) {
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    List<Integer> groupByChannels = getChannelsForSymbols(node.getGroupingKeys(), childLayout);
    List<Type> groupByTypes =
        node.getGroupingKeys().stream()
            .map(typeProvider::getTableModelType)
            .collect(toImmutableList());

    if (node.isStreamable()) {
      if (groupByTypes.size() == node.getPreGroupedSymbols().size()) {
        ImmutableList.Builder<TableAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
        node.getAggregations()
            .forEach(
                (k, v) ->
                    aggregatorBuilder.add(
                        buildAggregator(
                            childLayout, k, v, node.getStep(), typeProvider, true, null)));

        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    StreamingAggregationOperator.class.getSimpleName());
        return new StreamingAggregationOperator(
            operatorContext,
            child,
            groupByTypes,
            groupByChannels,
            genGroupKeyComparator(groupByTypes, groupByChannels),
            aggregatorBuilder.build(),
            Long.MAX_VALUE,
            false,
            Long.MAX_VALUE);
      }

      ImmutableList.Builder<GroupedAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
      node.getAggregations()
          .forEach(
              (k, v) ->
                  aggregatorBuilder.add(
                      buildGroupByAggregator(childLayout, k, v, node.getStep(), typeProvider)));

      Set<Symbol> preGroupedKeys = ImmutableSet.copyOf(node.getPreGroupedSymbols());
      List<Symbol> groupingKeys = node.getGroupingKeys();
      ImmutableList.Builder<Type> preGroupedTypesBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> preGroupedChannelsBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> preGroupedIndexInResultBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Type> unPreGroupedTypesBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> unPreGroupedChannelsBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> unPreGroupedIndexInResultBuilder =
          new ImmutableList.Builder<>();
      for (int i = 0; i < groupByTypes.size(); i++) {
        if (preGroupedKeys.contains(groupingKeys.get(i))) {
          preGroupedTypesBuilder.add(groupByTypes.get(i));
          preGroupedChannelsBuilder.add(groupByChannels.get(i));
          preGroupedIndexInResultBuilder.add(i);
        } else {
          unPreGroupedTypesBuilder.add(groupByTypes.get(i));
          unPreGroupedChannelsBuilder.add(groupByChannels.get(i));
          unPreGroupedIndexInResultBuilder.add(i);
        }
      }

      List<Integer> preGroupedChannels = preGroupedChannelsBuilder.build();
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  StreamingHashAggregationOperator.class.getSimpleName());
      return new StreamingHashAggregationOperator(
          operatorContext,
          child,
          preGroupedChannels,
          preGroupedIndexInResultBuilder.build(),
          unPreGroupedTypesBuilder.build(),
          unPreGroupedChannelsBuilder.build(),
          unPreGroupedIndexInResultBuilder.build(),
          genGroupKeyComparator(preGroupedTypesBuilder.build(), preGroupedChannels),
          aggregatorBuilder.build(),
          node.getStep(),
          DEFAULT_GROUP_NUMBER,
          Long.MAX_VALUE,
          false,
          Long.MAX_VALUE);
    }

    ImmutableList.Builder<GroupedAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
    node.getAggregations()
        .forEach(
            (k, v) ->
                aggregatorBuilder.add(
                    buildGroupByAggregator(childLayout, k, v, node.getStep(), typeProvider)));
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                HashAggregationOperator.class.getSimpleName());

    return new HashAggregationOperator(
        operatorContext,
        child,
        groupByTypes,
        groupByChannels,
        aggregatorBuilder.build(),
        node.getStep(),
        DEFAULT_GROUP_NUMBER,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }

  private Comparator<SortKey> genGroupKeyComparator(
      List<Type> groupTypes, List<Integer> groupByChannels) {
    return getComparatorForTable(
        // SortOrder is not sensitive here, the comparator is just used to judge equality.
        groupTypes.stream().map(k -> ASC_NULLS_LAST).collect(Collectors.toList()),
        groupByChannels,
        groupTypes.stream().map(InternalTypeManager::getTSDataType).collect(Collectors.toList()));
  }

  private static List<Integer> getChannelsForSymbols(
      List<Symbol> symbols, Map<Symbol, Integer> layout) {
    ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    for (Symbol symbol : symbols) {
      builder.add(layout.get(symbol));
    }
    return builder.build();
  }

  private GroupedAggregator buildGroupByAggregator(
      Map<Symbol, Integer> childLayout,
      Symbol symbol,
      AggregationNode.Aggregation aggregation,
      AggregationNode.Step step,
      TypeProvider typeProvider) {
    List<Integer> argumentChannels = new ArrayList<>();
    for (Expression argument : aggregation.getArguments()) {
      Symbol argumentSymbol = Symbol.from(argument);
      argumentChannels.add(childLayout.get(argumentSymbol));
    }

    String functionName = aggregation.getResolvedFunction().getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        aggregation.getResolvedFunction().getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());
    GroupedAccumulator accumulator =
        createGroupedAccumulator(
            functionName,
            getAggregationTypeByFuncName(functionName),
            originalArgumentTypes,
            Collections.emptyList(),
            Collections.emptyMap(),
            true,
            aggregation.isDistinct());

    OptionalInt maskChannel = OptionalInt.empty();
    if (aggregation.hasMask()) {
      maskChannel = OptionalInt.of(childLayout.get(aggregation.getMask().get()));
    }

    return new GroupedAggregator(
        accumulator,
        step,
        getTSDataType(typeProvider.getTableModelType(symbol)),
        argumentChannels,
        maskChannel);
  }

  @Override
  public Operator visitAggregationTreeDeviceViewScan(
      AggregationTreeDeviceViewScanNode node, LocalExecutionPlanContext context) {
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
        AggregationTreeDeviceViewScanNode.class.getSimpleName());
    return treeAlignedDeviceViewAggregationScanOperator;
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
              timeColumnName));
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
        context
            .getDriverContext()
            .addOperatorContext(context.getNextOperatorId(), node.getPlanNodeId(), className);
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
        timeColumnName);
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

    if (canUseLastCacheOptimize(
        parameter.getTableAggregators(), node, parameter.getTimeColumnName())) {
      return constructLastQueryAggTableScanOperator(node, parameter, context);
    } else {
      DefaultAggTableScanOperator aggTableScanOperator = new DefaultAggTableScanOperator(parameter);

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
      LocalExecutionPlanContext context) {
    List<Integer> hitCachesIndexes = new ArrayList<>();
    List<Pair<OptionalLong, TsPrimitiveType[]>> hitCachedResults = new ArrayList<>();
    List<DeviceEntry> cachedDeviceEntries = new ArrayList<>();
    List<DeviceEntry> unCachedDeviceEntries = new ArrayList<>();
    long tableTTL =
        DataNodeTTLCache.getInstance()
            .getTTLForTable(
                node.getQualifiedObjectName().getDatabaseName(),
                node.getQualifiedObjectName().getObjectName());
    Filter updateTimeFilter =
        updateFilterUsingTTL(parameter.getSeriesScanOptions().getGlobalTimeFilter(), tableTTL);
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

      if (!allHitCache) {
        DeviceEntry deviceEntry = node.getDeviceEntries().get(i);
        AlignedFullPath alignedPath =
            constructAlignedPath(
                deviceEntry,
                parameter.getMeasurementColumnNames(),
                parameter.getMeasurementSchemas(),
                parameter.getAllSensors());
        ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
        unCachedDeviceEntries.add(deviceEntry);

        // last cache updateColumns need put "" as time column
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
        hitCachedResults.add(lastByResult.get());
        cachedDeviceEntries.add(node.getDeviceEntries().get(i));
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
            hitCachedResults);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(lastQueryOperator);
    parameter
        .getOperatorContext()
        .setOperatorType(LastQueryAggTableScanOperator.class.getSimpleName());
    return lastQueryOperator;
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
              pushDownPredicate, measurementColumnsIndexMap, columnSchemaMap, timeColumnName));
    }
    return scanOptionsBuilder.build();
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

  @Override
  public Operator visitTableFunctionProcessor(
      TableFunctionProcessorNode node, LocalExecutionPlanContext context) {
    TableFunction tableFunction = metadata.getTableFunction(node.getName());
    TableFunctionProcessorProvider processorProvider =
        tableFunction.getProcessorProvider(node.getTableFunctionHandle());
    if (node.getChildren().isEmpty()) {
      List<TSDataType> outputDataTypes =
          node.getOutputSymbols().stream()
              .map(context.getTypeProvider()::getTableModelType)
              .map(InternalTypeManager::getTSDataType)
              .collect(Collectors.toList());
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableFunctionLeafOperator.class.getSimpleName());
      return new TableFunctionLeafOperator(operatorContext, processorProvider, outputDataTypes);
    } else {
      Operator operator = node.getChild().accept(this, context);
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableFunctionOperator.class.getSimpleName());

      List<TSDataType> inputDataTypes =
          node.getChild().getOutputSymbols().stream()
              .map(context.getTypeProvider()::getTableModelType)
              .map(InternalTypeManager::getTSDataType)
              .collect(Collectors.toList());

      List<TSDataType> outputDataTypes =
          node.getOutputSymbols().stream()
              .map(context.getTypeProvider()::getTableModelType)
              .map(InternalTypeManager::getTSDataType)
              .collect(Collectors.toList());

      int properChannelCount = node.getProperOutputs().size();
      Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification =
          node.getPassThroughSpecification();

      Map<Symbol, Integer> childLayout =
          makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());
      List<Integer> requiredChannels =
          getChannelsForSymbols(node.getRequiredSymbols(), childLayout);
      List<Integer> passThroughChannels =
          passThroughSpecification
              .map(
                  passThrough ->
                      getChannelsForSymbols(
                          passThrough.getColumns().stream()
                              .map(TableFunctionNode.PassThroughColumn::getSymbol)
                              .collect(Collectors.toList()),
                          childLayout))
              .orElse(Collections.emptyList());
      List<Integer> partitionChannels;
      if (node.getDataOrganizationSpecification().isPresent()) {
        partitionChannels =
            getChannelsForSymbols(
                node.getDataOrganizationSpecification().get().getPartitionBy(), childLayout);
      } else {
        partitionChannels = Collections.emptyList();
      }
      return new TableFunctionOperator(
          operatorContext,
          processorProvider,
          operator,
          inputDataTypes,
          outputDataTypes,
          properChannelCount,
          requiredChannels,
          passThroughChannels,
          passThroughSpecification
              .map(TableFunctionNode.PassThroughSpecification::isDeclaredAsPassThrough)
              .orElse(false),
          partitionChannels,
          node.isRequireRecordSnapshot());
    }
  }

  @Override
  public Operator visitPatternRecognition(
      PatternRecognitionNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                PatternRecognitionOperator.class.getSimpleName());

    Operator child = node.getChild().accept(this, context);

    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    List<Symbol> partitionBySymbols = node.getPartitionBy();
    List<Integer> partitionChannels =
        ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, childLayout));

    List<Integer> sortChannels = ImmutableList.of();
    List<SortOrder> sortOrder = ImmutableList.of();

    if (node.getOrderingScheme().isPresent()) {
      OrderingScheme orderingScheme = node.getOrderingScheme().get();
      sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), childLayout);
      sortOrder = orderingScheme.getOrderingList();
    }

    // The output order for pattern recognition operation is defined as follows:
    // - for ONE ROW PER MATCH: partition by symbols, then measures,
    // - for ALL ROWS PER MATCH: partition by symbols, order by symbols, measures, remaining input
    // symbols.

    // all output column types of the input table
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());

    // input channels to be passed directly to output, excluding MEASURES columns
    ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
    // output dataTypes, used to construct the output TsBlock, including MEASURES columns
    ImmutableList.Builder<TSDataType> outputDataTypes = ImmutableList.builder();

    if (node.getRowsPerMatch() == ONE) {
      // ONE ROW PER MATCH: partition columns, MEASURES

      // add all partition columns
      outputChannels.addAll(partitionChannels);
      for (int i = 0; i < partitionBySymbols.size(); i++) {
        Symbol symbol = partitionBySymbols.get(i);
        // obtain the absolute index of the symbol in the base table through `childLayout`
        outputDataTypes.add(inputDataTypes.get(childLayout.get(symbol)));
      }
    } else {
      // ALL ROWS PER MATCH: all input columns, MEASURES

      outputChannels.addAll(
          IntStream.range(0, inputDataTypes.size()).boxed().collect(toImmutableList()));
      outputDataTypes.addAll(inputDataTypes);
    }

    // add MEASURES columns
    for (Map.Entry<Symbol, Measure> measure : node.getMeasures().entrySet()) {
      outputDataTypes.add(getTSDataType(measure.getValue().getType()));
    }

    // prepare structures specific to PatternRecognitionNode
    // 1. establish a two-way mapping of IrLabels to `int`
    List<IrLabel> primaryLabels = ImmutableList.copyOf(node.getVariableDefinitions().keySet());
    ImmutableList.Builder<String> labelNamesBuilder = ImmutableList.builder();
    ImmutableMap.Builder<IrLabel, Integer> mappingBuilder = ImmutableMap.builder();
    for (int i = 0; i < primaryLabels.size(); i++) {
      IrLabel label = primaryLabels.get(i);
      labelNamesBuilder.add(label.getName());
      mappingBuilder.put(label, i);
    }
    Map<IrLabel, Integer> mapping = mappingBuilder.buildOrThrow();
    List<String> labelNames = labelNamesBuilder.build();

    // 2. rewrite pattern to program
    Program program = IrRowPatternToProgramRewriter.rewrite(node.getPattern(), mapping);

    // 3. DEFINE: prepare patternVariableComputation (PatternVariableRecognizer is to be
    // instantiated once per partition)
    ImmutableList.Builder<PatternVariableRecognizer.PatternVariableComputation> evaluationsBuilder =
        ImmutableList.builder();

    for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry :
        node.getVariableDefinitions().entrySet()) {
      String variableName = entry.getKey().getName();
      ExpressionAndValuePointers expressionAndValuePointers = entry.getValue();

      // convert the `ValuePointer` in the `Assignment` to `PhysicalValueAccessor`
      List<PhysicalValueAccessor> valueAccessors = new ArrayList<>();
      for (ExpressionAndValuePointers.Assignment assignment :
          expressionAndValuePointers.getAssignments()) {
        ValuePointer pointer = assignment.getValuePointer();
        if (pointer instanceof MatchNumberValuePointer) {
          valueAccessors.add(
              new PhysicalValuePointer(MATCH_NUMBER, INT64, LogicalIndexNavigation.NO_OP));
        } else if (pointer instanceof ClassifierValuePointer) {
          ClassifierValuePointer classifierPointer = (ClassifierValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  CLASSIFIER,
                  STRING,
                  classifierPointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
        } else if (pointer instanceof ScalarValuePointer) {
          ScalarValuePointer scalarPointer = (ScalarValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  getOnlyElement(
                      getChannelsForSymbols(
                          ImmutableList.of(scalarPointer.getInputSymbol()), childLayout)),
                  context.getTypeProvider().getTableModelType(scalarPointer.getInputSymbol()),
                  scalarPointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
        }
      }

      // transform the symbolic expression tree in the logical planning stage into a parametric
      // expression tree
      Computation computation =
          Computation.ComputationParser.parse(expressionAndValuePointers.getExpression());

      // construct a `PatternVariableComputation` object, where valueAccessors is a parameter list
      // and computation is a parametric expression tree, encapsulating the computation logic
      PatternVariableRecognizer.PatternVariableComputation patternVariableComputation =
          new PatternVariableRecognizer.PatternVariableComputation(
              valueAccessors, computation, labelNames);

      evaluationsBuilder.add(patternVariableComputation);
    }

    // 4. MEASURES: prepare measures computations
    ImmutableList.Builder<PatternExpressionComputation> measureComputationsBuilder =
        ImmutableList.builder();

    for (Measure measure : node.getMeasures().values()) {
      ExpressionAndValuePointers expressionAndValuePointers =
          measure.getExpressionAndValuePointers();

      // convert the `ValuePointer` in the `Assignment` to `PhysicalValueAccessor`
      List<PhysicalValueAccessor> valueAccessors = new ArrayList<>();
      for (ExpressionAndValuePointers.Assignment assignment :
          expressionAndValuePointers.getAssignments()) {
        ValuePointer pointer = assignment.getValuePointer();
        if (pointer instanceof MatchNumberValuePointer) {
          valueAccessors.add(
              new PhysicalValuePointer(MATCH_NUMBER, INT64, LogicalIndexNavigation.NO_OP));
        } else if (pointer instanceof ClassifierValuePointer) {
          ClassifierValuePointer classifierPointer = (ClassifierValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  CLASSIFIER,
                  STRING,
                  classifierPointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
        } else if (pointer instanceof ScalarValuePointer) {
          ScalarValuePointer scalarPointer = (ScalarValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  getOnlyElement(
                      getChannelsForSymbols(
                          ImmutableList.of(scalarPointer.getInputSymbol()), childLayout)),
                  context.getTypeProvider().getTableModelType(scalarPointer.getInputSymbol()),
                  scalarPointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
        }
      }

      // transform the symbolic expression tree in the logical planning stage into a parametric
      // expression tree
      Computation computation =
          Computation.ComputationParser.parse(expressionAndValuePointers.getExpression());

      // construct a `PatternExpressionComputation` object, where valueAccessors is a parameter
      // list
      // and computation is a parametric expression tree, encapsulating the computation logic
      PatternExpressionComputation measureComputation =
          new PatternExpressionComputation(valueAccessors, computation);

      measureComputationsBuilder.add(measureComputation);
    }

    // 5. prepare SKIP TO navigation
    Optional<LogicalIndexNavigation> skipToNavigation = Optional.empty();
    if (!node.getSkipToLabels().isEmpty()) {
      boolean last = node.getSkipToPosition().equals(LAST);
      skipToNavigation =
          Optional.of(
              new LogicalIndexPointer(node.getSkipToLabels(), last, false, 0, 0)
                  .toLogicalIndexNavigation(mapping));
    }

    return new PatternRecognitionOperator(
        operatorContext,
        child,
        inputDataTypes,
        outputDataTypes.build(),
        outputChannels.build(),
        partitionChannels,
        sortChannels,
        node.getRowsPerMatch(),
        node.getSkipToPosition(),
        skipToNavigation,
        new Matcher(program),
        evaluationsBuilder.build(),
        measureComputationsBuilder.build(),
        labelNames);
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

          // first/last/first_by/last_by aggregation with BLOB type can not use statistics
          if (BlobType.BLOB.equals(argumentType)) {
            canUseStatistic = false;
            break;
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

  private boolean canUseLastCacheOptimize(
      List<TableAggregator> aggregators, AggregationTableScanNode node, String timeColumnName) {
    if (!CommonDescriptor.getInstance().getConfig().isLastCacheEnable() || aggregators.isEmpty()) {
      return false;
    }

    // has value filter, can not optimize
    if (node.getPushDownPredicate() != null) {
      return false;
    }

    // has date_bin, can not optimize
    if (!node.getGroupingKeys().isEmpty()
        && node.getProjection() != null
        && !node.getProjection().getMap().isEmpty()) {
      return false;
    }

    for (TableAggregator aggregator : aggregators) {
      if (aggregator.getAccumulator() instanceof LastDescAccumulator) {
        if (!((LastDescAccumulator) aggregator.getAccumulator()).isTimeColumn()) {
          return false;
        }
      } else if (aggregator.getAccumulator() instanceof LastByDescAccumulator) {
        if (!((LastByDescAccumulator) aggregator.getAccumulator()).yIsTimeColumn()) {
          return false;
        }
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  public Operator visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ExplainAnalyzeOperator.class.getSimpleName());

    TypeProvider typeProvider = context.getTypeProvider();
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    return new MarkDistinctOperator(
        operatorContext,
        child,
        node.getChild().getOutputSymbols().stream()
            .map(typeProvider::getTableModelType)
            .collect(Collectors.toList()),
        node.getDistinctSymbols().stream().map(childLayout::get).collect(Collectors.toList()),
        Optional.empty());
  }

  @Override
  public Operator visitWindowFunction(WindowNode node, LocalExecutionPlanContext context) {
    TypeProvider typeProvider = context.getTypeProvider();
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableWindowOperator.class.getSimpleName());

    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    // Partition channel
    List<Symbol> partitionBySymbols = node.getSpecification().getPartitionBy();
    List<Integer> partitionChannels =
        ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, childLayout));

    // Sort channel
    List<Integer> sortChannels = ImmutableList.of();
    List<SortOrder> sortOrder = ImmutableList.of();
    if (node.getSpecification().getOrderingScheme().isPresent()) {
      OrderingScheme orderingScheme = node.getSpecification().getOrderingScheme().get();
      sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), childLayout);
      sortOrder = orderingScheme.getOrderingList();
    }

    // Output channel
    ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
    List<TSDataType> outputDataTypes = new ArrayList<>();
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
      outputDataTypes.add(inputDataTypes.get(i));
    }

    // Window functions
    List<FrameInfo> frameInfoList = new ArrayList<>();
    List<WindowFunction> windowFunctions = new ArrayList<>();
    List<Symbol> windowFunctionOutputSymbols = new ArrayList<>();
    List<TSDataType> windowFunctionOutputDataTypes = new ArrayList<>();
    for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
      // Create FrameInfo
      WindowNode.Frame frame = entry.getValue().getFrame();

      Optional<Integer> frameStartChannel = Optional.empty();
      if (frame.getStartValue().isPresent()) {
        frameStartChannel = Optional.ofNullable(childLayout.get(frame.getStartValue().get()));
      }
      Optional<Integer> frameEndChannel = Optional.empty();
      if (frame.getEndValue().isPresent()) {
        frameEndChannel = Optional.ofNullable(childLayout.get(frame.getEndValue().get()));
      }

      Optional<Integer> sortKeyChannel = Optional.empty();
      Optional<SortOrder> ordering = Optional.empty();
      if (node.getSpecification().getOrderingScheme().isPresent()) {
        sortKeyChannel = Optional.of(sortChannels.get(0));
        if (sortOrder.get(0).isNullsFirst()) {
          if (sortOrder.get(0).isAscending()) {
            ordering = Optional.of(ASC_NULLS_FIRST);
          } else {
            ordering = Optional.of(DESC_NULLS_FIRST);
          }
        } else {
          if (sortOrder.get(0).isAscending()) {
            ordering = Optional.of(ASC_NULLS_LAST);
          } else {
            ordering = Optional.of(DESC_NULLS_LAST);
          }
        }
      }
      FrameInfo frameInfo =
          new FrameInfo(
              frame.getType(),
              frame.getStartType(),
              frameStartChannel,
              frame.getEndType(),
              frameEndChannel,
              sortKeyChannel,
              ordering);
      frameInfoList.add(frameInfo);

      // Arguments
      WindowNode.Function function = entry.getValue();
      ResolvedFunction resolvedFunction = function.getResolvedFunction();
      List<Integer> argumentChannels = new ArrayList<>();
      for (Expression argument : function.getArguments()) {
        Symbol argumentSymbol = Symbol.from(argument);
        argumentChannels.add(childLayout.get(argumentSymbol));
      }

      // Return value
      Type returnType = resolvedFunction.getSignature().getReturnType();
      windowFunctionOutputDataTypes.add(getTSDataType(returnType));

      // Window function
      Symbol symbol = entry.getKey();
      WindowFunction windowFunction;
      FunctionKind functionKind = resolvedFunction.getFunctionKind();
      if (functionKind == FunctionKind.AGGREGATE) {
        WindowAggregator tableWindowAggregator =
            buildWindowAggregator(symbol, function, typeProvider, argumentChannels);
        windowFunction = new AggregationWindowFunction(tableWindowAggregator);
      } else if (functionKind == FunctionKind.WINDOW) {
        String functionName = function.getResolvedFunction().getSignature().getName();
        windowFunction =
            WindowFunctionFactory.createBuiltinWindowFunction(
                functionName, argumentChannels, function.isIgnoreNulls());
      } else {
        throw new UnsupportedOperationException("Unsupported function kind: " + functionKind);
      }

      windowFunctions.add(windowFunction);
      windowFunctionOutputSymbols.add(symbol);
    }

    // Compute layout
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    for (Symbol symbol : node.getChild().getOutputSymbols()) {
      outputMappings.put(symbol, childLayout.get(symbol));
    }
    int channel = inputDataTypes.size();

    for (Symbol symbol : windowFunctionOutputSymbols) {
      outputMappings.put(symbol, channel);
      channel++;
    }

    outputDataTypes.addAll(windowFunctionOutputDataTypes);
    return new TableWindowOperator(
        operatorContext,
        child,
        inputDataTypes,
        outputDataTypes,
        outputChannels.build(),
        windowFunctions,
        frameInfoList,
        partitionChannels,
        sortChannels);
  }

  private WindowAggregator buildWindowAggregator(
      Symbol symbol,
      WindowNode.Function function,
      TypeProvider typeProvider,
      List<Integer> argumentChannels) {
    // Create accumulator first
    String functionName = function.getResolvedFunction().getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        function.getResolvedFunction().getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());
    TableAccumulator accumulator =
        createBuiltinAccumulator(
            getAggregationTypeByFuncName(functionName),
            originalArgumentTypes,
            function.getArguments(),
            Collections.emptyMap(),
            true);

    // Create aggregator by accumulator
    return new WindowAggregator(
        accumulator, getTSDataType(typeProvider.getTableModelType(symbol)), argumentChannels);
  }
}
