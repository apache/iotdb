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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllRows;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Columns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Extract;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupingElement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupingSets;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.MeasureDefinition;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NaturalJoin;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternRecognitionRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SetOperation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SkipTo;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SubsetDefinition;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionInvocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionTableArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.VariableDefinition;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Window;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowDefinition;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowFrame;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowSpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.commons.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.commons.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.commons.queryengine.utils.cte.CteDataStore;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.M4TableFunction;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext.ExplainType;
import org.apache.iotdb.db.queryengine.execution.warnings.IoTDBWarning;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction.ArgumentAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction.ArgumentsAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction.TableArgumentAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction.TableFunctionInvocationAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ReadTsFileTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TranslationMap;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractQueryDeviceWithCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractTraverseDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AsofJoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CopyTo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropSubscription;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreateDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.CompatibleResolver;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.argument.TableArgument;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.TableParameterSpecification;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isTimestampType;
import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.ForecastTableFunction.TIMECOL_PARAMETER_NAME;
import static org.apache.iotdb.commons.queryengine.plan.relational.metadata.MetadataUtil.createQualifiedObjectName;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression.getQualifiedName;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join.Type.FULL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join.Type.INNER;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join.Type.LEFT;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join.Type.RIGHT;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternRecognitionRelation.RowsPerMatch.ONE;
import static org.apache.iotdb.commons.schema.table.TsTable.TABLE_ALLOWED_PROPERTIES;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.DATE_BIN;
import static org.apache.iotdb.db.queryengine.execution.warnings.StandardWarningCode.REDUNDANT_ORDER_BY;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.CanonicalizationAware.canonicalizationAwareKey;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.asQualifiedName;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope.BasisType.TABLE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.IrExpressionInterpreter.evaluateConstantExpression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil.preOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.NodeUtils.getSortItemsFromOrderBy;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

public class StatementAnalyzer {

  private final StatementAnalyzerFactory statementAnalyzerFactory;

  private final Analysis analysis;

  private boolean hasFillInParentScope = false;
  private boolean hasDiffFunctionInParentScope = false;
  private final MPPQueryContext queryContext;

  private final AccessControl accessControl;

  private final WarningCollector warningCollector;

  private final SessionInfo sessionContext;

  private final Metadata metadata;

  private final CorrelationSupport correlationSupport;

  private final TypeManager typeManager;

  public StatementAnalyzer(
      StatementAnalyzerFactory statementAnalyzerFactory,
      Analysis analysis,
      MPPQueryContext queryContext,
      AccessControl accessControl,
      WarningCollector warningCollector,
      SessionInfo sessionContext,
      Metadata metadata,
      CorrelationSupport correlationSupport,
      TypeManager typeManager) {
    this.statementAnalyzerFactory = statementAnalyzerFactory;
    this.analysis = analysis;
    this.queryContext = queryContext;
    this.accessControl = accessControl;
    this.warningCollector = warningCollector;
    this.sessionContext = sessionContext;
    this.metadata = metadata;
    this.correlationSupport = correlationSupport;
    this.typeManager = typeManager;
  }

  public Scope analyze(Node node) {
    return analyze(node, Optional.empty(), true);
  }

  public Scope analyze(Node node, Scope outerQueryScope) {
    return analyze(node, Optional.of(outerQueryScope), false);
  }

  private Scope analyze(Node node, Optional<Scope> outerQueryScope, boolean isTopLevel) {
    return new Visitor(outerQueryScope, warningCollector, Optional.empty(), isTopLevel)
        .process(node, Optional.empty());
  }

  public Scope analyzeForUpdate(
      Relation relation, Optional<Scope> outerQueryScope, UpdateKind updateKind) {
    return new Visitor(outerQueryScope, warningCollector, Optional.of(updateKind), true)
        .process(relation, Optional.empty());
  }

  private enum UpdateKind {
    DELETE,
    UPDATE,
    MERGE,
  }

  private static final class SelectAnalysis {
    private final List<Expression> outputExpressions;
    private final SelectAliasLookup aliases;
    private final Map<NodeRef<SingleColumn>, Expression> singleColumnExpressions;

    private SelectAnalysis(
        List<Expression> outputExpressions,
        SelectAliasLookup aliases,
        Map<NodeRef<SingleColumn>, Expression> singleColumnExpressions) {
      this.outputExpressions =
          ImmutableList.copyOf(
              requireNonNull(
                  outputExpressions,
                  DataNodeQueryMessages.EXCEPTION_OUTPUTEXPRESSIONS_IS_NULL_7BD39A8F));
      this.aliases =
          requireNonNull(aliases, DataNodeQueryMessages.EXCEPTION_ALIASES_IS_NULL_6D98FA6C);
      this.singleColumnExpressions =
          ImmutableMap.copyOf(
              requireNonNull(
                  singleColumnExpressions,
                  DataNodeQueryMessages.EXCEPTION_SINGLECOLUMNEXPRESSIONS_IS_NULL_EFB296FD));
    }

    private List<Expression> getOutputExpressions() {
      return outputExpressions;
    }

    private SelectAliasLookup getAliases() {
      return aliases;
    }

    private Optional<Expression> getSingleColumnExpression(SingleColumn column) {
      return Optional.ofNullable(singleColumnExpressions.get(NodeRef.of(column)));
    }
  }

  private static final class SelectAlias {
    private final String canonicalName;
    private final int position;
    private final Expression rewrittenExpression;
    private final boolean containsWindowFunction;

    private SelectAlias(String canonicalName, int position, Expression rewrittenExpression) {
      this.canonicalName =
          requireNonNull(
              canonicalName, DataNodeQueryMessages.EXCEPTION_CANONICALNAME_IS_NULL_DDFE6DB1);
      this.position = position;
      this.rewrittenExpression =
          requireNonNull(
              rewrittenExpression,
              DataNodeQueryMessages.EXCEPTION_REWRITTENEXPRESSION_IS_NULL_6EE23088);
      this.containsWindowFunction =
          !extractWindowFunctions(ImmutableList.of(rewrittenExpression)).isEmpty();
    }

    private String getCanonicalName() {
      return canonicalName;
    }

    private int getPosition() {
      return position;
    }

    private Expression getRewrittenExpression() {
      return rewrittenExpression;
    }

    private boolean containsWindowFunction() {
      return containsWindowFunction;
    }
  }

  private interface SelectAliasResolver {
    boolean isEmpty();

    Optional<SelectAlias> resolve(Identifier identifier);
  }

  private static final class SelectAliasLookup implements SelectAliasResolver {
    private final Map<String, List<SelectAlias>> aliasesByCanonicalName;
    private final int size;

    private SelectAliasLookup(Map<String, List<SelectAlias>> aliasesByCanonicalName) {
      requireNonNull(
          aliasesByCanonicalName,
          DataNodeQueryMessages.EXCEPTION_ALIASESBYCANONICALNAME_IS_NULL_635C7ED3);

      ImmutableMap.Builder<String, List<SelectAlias>> aliasesBuilder = ImmutableMap.builder();
      int aliasCount = 0;
      for (Map.Entry<String, List<SelectAlias>> entry : aliasesByCanonicalName.entrySet()) {
        List<SelectAlias> aliases = ImmutableList.copyOf(entry.getValue());
        aliasesBuilder.put(entry.getKey(), aliases);
        aliasCount += aliases.size();
      }
      this.aliasesByCanonicalName = aliasesBuilder.build();
      this.size = aliasCount;
    }

    private static Builder builder() {
      return new Builder();
    }

    @Override
    public boolean isEmpty() {
      return size == 0;
    }

    @Override
    public Optional<SelectAlias> resolve(Identifier identifier) {
      return resolveFrom(aliasesByCanonicalName, identifier);
    }

    private static Optional<SelectAlias> resolveFrom(
        Map<String, List<SelectAlias>> aliasesByCanonicalName, Identifier identifier) {
      List<SelectAlias> matches = aliasesByCanonicalName.get(canonicalSelectAliasName(identifier));
      if (matches == null || matches.isEmpty()) {
        return Optional.empty();
      }
      if (matches.size() > 1) {
        throw new SemanticException(
            String.format(
                "Column alias '%s' is ambiguous at positions %s",
                identifier.getValue(),
                matches.stream()
                    .map(alias -> Integer.toString(alias.getPosition()))
                    .collect(Collectors.joining(", "))));
      }
      return Optional.of(matches.get(0));
    }

    private static final class Builder implements SelectAliasResolver {
      private final Map<String, List<SelectAlias>> aliasesByCanonicalName = new HashMap<>();
      private int size;

      private void add(SelectAlias alias) {
        requireNonNull(alias, DataNodeQueryMessages.EXCEPTION_ALIAS_IS_NULL_862D23B1);
        aliasesByCanonicalName
            .computeIfAbsent(alias.getCanonicalName(), ignored -> new ArrayList<>())
            .add(alias);
        size++;
      }

      private SelectAliasLookup build() {
        return new SelectAliasLookup(aliasesByCanonicalName);
      }

      @Override
      public boolean isEmpty() {
        return size == 0;
      }

      @Override
      public Optional<SelectAlias> resolve(Identifier identifier) {
        return resolveFrom(aliasesByCanonicalName, identifier);
      }
    }
  }

  private static String canonicalSelectAliasName(Identifier identifier) {
    return identifier.getCanonicalValue().toUpperCase(ENGLISH);
  }

  private static boolean resolvesToInputColumn(Scope scope, Identifier identifier) {
    return scope
        .tryResolveField(identifier, QualifiedName.of(identifier.getValue()))
        .filter(ResolvedField::isLocal)
        .isPresent();
  }

  private static Optional<SelectAlias> resolveSelectAlias(
      Identifier identifier, SelectAliasResolver aliases) {
    return aliases.resolve(identifier);
  }

  private static Expression rewriteLateralColumnAliases(
      Expression expression, Scope scope, SelectAliasResolver visibleAliases) {
    if (visibleAliases.isEmpty()) {
      return expression;
    }
    return rewriteLateralColumnAliasesWithReferences(expression, scope, visibleAliases)
        .getExpression();
  }

  private static LateralColumnAliasRewrite rewriteLateralColumnAliasesWithReferences(
      Expression expression, Scope scope, SelectAliasResolver visibleAliases) {
    if (visibleAliases.isEmpty()) {
      return new LateralColumnAliasRewrite(expression, ImmutableMap.of());
    }
    LateralColumnAliasRewriter rewriter = new LateralColumnAliasRewriter(scope, visibleAliases);
    return new LateralColumnAliasRewrite(
        ExpressionTreeRewriter.rewriteWith(rewriter, expression), rewriter.getReferences());
  }

  private static final class LateralColumnAliasRewrite {
    private final Expression expression;
    private final Map<NodeRef<Expression>, Expression> references;

    private LateralColumnAliasRewrite(
        Expression expression, Map<NodeRef<Expression>, Expression> references) {
      this.expression =
          requireNonNull(expression, DataNodeQueryMessages.EXCEPTION_EXPRESSION_IS_NULL_16C079B5);
      this.references =
          ImmutableMap.copyOf(
              requireNonNull(
                  references, DataNodeQueryMessages.EXCEPTION_REFERENCES_IS_NULL_25A7E3AF));
    }

    private Expression getExpression() {
      return expression;
    }

    private Map<NodeRef<Expression>, Expression> getReferences() {
      return references;
    }
  }

  private static final class LateralColumnAliasRewriter extends ExpressionRewriter<Void> {
    private final Scope scope;
    private final SelectAliasResolver visibleAliases;
    private final Map<NodeRef<Expression>, Expression> references = new LinkedHashMap<>();

    private LateralColumnAliasRewriter(Scope scope, SelectAliasResolver visibleAliases) {
      this.scope = requireNonNull(scope, DataNodeQueryMessages.EXCEPTION_SCOPE_IS_NULL_4F364BA2);
      this.visibleAliases =
          requireNonNull(
              visibleAliases, DataNodeQueryMessages.EXCEPTION_VISIBLEALIASES_IS_NULL_630B27F1);
    }

    private Map<NodeRef<Expression>, Expression> getReferences() {
      return ImmutableMap.copyOf(references);
    }

    @Override
    public Expression rewriteIdentifier(
        Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      if (resolvesToInputColumn(scope, node)) {
        return node;
      }

      Optional<SelectAlias> selectAlias = resolveSelectAlias(node, visibleAliases);
      if (!selectAlias.isPresent()) {
        return node;
      }

      if (selectAlias.get().containsWindowFunction()) {
        throw new SemanticException(
            String.format(
                "Lateral column alias '%s' containing window function is not supported",
                node.getValue()));
      }
      Expression rewrittenExpression = selectAlias.get().getRewrittenExpression();
      references.put(NodeRef.of(rewrittenExpression), rewrittenExpression);
      return rewrittenExpression;
    }

    @Override
    public Expression rewriteDereferenceExpression(
        DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      return node;
    }

    @Override
    public Expression rewriteSubqueryExpression(
        SubqueryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      return node;
    }

    @Override
    public Expression rewriteCast(
        Cast node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      Expression expression = treeRewriter.rewrite(node.getExpression(), context);
      if (expression == node.getExpression()) {
        return node;
      }
      return new Cast(expression, node.getType(), node.isSafe(), node.isTypeOnly());
    }

    @Override
    public Expression rewriteFunctionCall(
        FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      List<Expression> arguments =
          node.getArguments().stream()
              .map(argument -> treeRewriter.rewrite(argument, context))
              .collect(toImmutableList());

      Optional<Window> window = rewriteWindow(node.getWindow(), context, treeRewriter);
      if (ExpressionTreeRewriter.sameElements(node.getArguments(), arguments)
          && ExpressionTreeRewriter.sameElements(node.getWindow(), window)) {
        return node;
      }

      if (node.getLocation().isPresent()) {
        return new FunctionCall(
            node.getLocation().get(),
            node.getName(),
            window,
            node.getNullTreatment(),
            node.isDistinct(),
            node.getProcessingMode(),
            arguments);
      }

      if (node.getWindow().isPresent() || node.getNullTreatment().isPresent()) {
        throw new SemanticException(
            String.format(
                "Lateral column alias in window function arguments is not supported: %s", node));
      }
      return new FunctionCall(
          node.getName(), node.isDistinct(), node.getProcessingMode(), arguments);
    }

    private Optional<Window> rewriteWindow(
        Optional<Window> window, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      if (!window.isPresent()) {
        return Optional.empty();
      }
      if (!(window.get() instanceof WindowSpecification)) {
        return window;
      }

      WindowSpecification specification = (WindowSpecification) window.get();
      List<Expression> partitionBy =
          specification.getPartitionBy().stream()
              .map(expression -> treeRewriter.rewrite(expression, context))
              .collect(toImmutableList());
      Optional<OrderBy> orderBy =
          specification.getOrderBy().map(value -> rewriteOrderBy(value, context, treeRewriter));
      Optional<WindowFrame> frame =
          specification.getFrame().map(value -> rewriteWindowFrame(value, context, treeRewriter));

      if (ExpressionTreeRewriter.sameElements(specification.getPartitionBy(), partitionBy)
          && ExpressionTreeRewriter.sameElements(specification.getOrderBy(), orderBy)
          && ExpressionTreeRewriter.sameElements(specification.getFrame(), frame)) {
        return window;
      }

      return Optional.of(
          new WindowSpecification(
              specification
                  .getLocation()
                  .orElseThrow(
                      () -> new IllegalStateException("WindowSpecification location is missing")),
              specification.getExistingWindowName(),
              partitionBy,
              orderBy,
              frame));
    }

    private OrderBy rewriteOrderBy(
        OrderBy node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      List<SortItem> sortItems =
          node.getSortItems().stream()
              .map(sortItem -> rewriteSortItem(sortItem, context, treeRewriter))
              .collect(toImmutableList());
      if (ExpressionTreeRewriter.sameElements(node.getSortItems(), sortItems)) {
        return node;
      }
      return node.getLocation()
          .map(location -> new OrderBy(location, sortItems))
          .orElseGet(() -> new OrderBy(sortItems));
    }

    private SortItem rewriteSortItem(
        SortItem node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      Expression sortKey = treeRewriter.rewrite(node.getSortKey(), context);
      if (sortKey == node.getSortKey()) {
        return node;
      }
      return node.getLocation()
          .map(
              location ->
                  new SortItem(location, sortKey, node.getOrdering(), node.getNullOrdering()))
          .orElseGet(() -> new SortItem(sortKey, node.getOrdering(), node.getNullOrdering()));
    }

    private WindowFrame rewriteWindowFrame(
        WindowFrame node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      FrameBound start = rewriteFrameBound(node.getStart(), context, treeRewriter);
      Optional<FrameBound> end =
          node.getEnd().map(value -> rewriteFrameBound(value, context, treeRewriter));
      if (start == node.getStart() && ExpressionTreeRewriter.sameElements(node.getEnd(), end)) {
        return node;
      }
      return new WindowFrame(
          node.getLocation()
              .orElseThrow(() -> new IllegalStateException("WindowFrame location is missing")),
          node.getType(),
          start,
          end);
    }

    private FrameBound rewriteFrameBound(
        FrameBound node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      Optional<Expression> value =
          node.getValue().map(expression -> treeRewriter.rewrite(expression, context));
      if (ExpressionTreeRewriter.sameElements(node.getValue(), value)) {
        return node;
      }
      return new FrameBound(node.getLocation().orElse(null), node.getType(), value.orElse(null));
    }

    @Override
    public Expression rewriteQuantifiedComparison(
        QuantifiedComparisonExpression node,
        Void context,
        ExpressionTreeRewriter<Void> treeRewriter) {
      Expression value = treeRewriter.rewrite(node.getValue(), context);
      if (value != node.getValue()) {
        return new QuantifiedComparisonExpression(
            node.getOperator(), node.getQuantifier(), value, node.getSubquery());
      }
      return node;
    }
  }

  /**
   * Visitor context represents local query scope (if exists). The invariant is that the local query
   * scopes hierarchy should always have outer query scope (if provided) as ancestor.
   */
  private final class Visitor implements AstVisitor<Scope, Optional<Scope>> {

    private final boolean isTopLevel;
    private final Optional<Scope> outerQueryScope;
    private final WarningCollector warningCollector;
    private final Optional<UpdateKind> updateKind;

    private Visitor(
        final Optional<Scope> outerQueryScope,
        final WarningCollector warningCollector,
        final Optional<UpdateKind> updateKind,
        final boolean isTopLevel) {
      this.outerQueryScope =
          requireNonNull(outerQueryScope, QueryMessages.EXCEPTION_OUTERQUERYSCOPE_IS_NULL_D5305685);
      this.warningCollector =
          requireNonNull(
              warningCollector, QueryMessages.EXCEPTION_WARNINGCOLLECTOR_IS_NULL_7A524A68);
      this.updateKind =
          requireNonNull(updateKind, QueryMessages.EXCEPTION_UPDATEKIND_IS_NULL_6CA83291);
      this.isTopLevel = isTopLevel;
    }

    @Override
    public Scope process(Node node, final Optional<Scope> scope) {
      final Scope returnScope = AstVisitor.super.process(node, scope);
      if (node instanceof PipeEnriched) {
        node = ((PipeEnriched) node).getInnerStatement();
      }
      if (node instanceof CreateOrUpdateDevice
          || node instanceof FetchDevice
          || node instanceof ShowDevice
          || node instanceof CountDevice
          || node instanceof Update
          || node instanceof DeleteDevice) {
        return returnScope;
      }
      checkState(
          returnScope.getOuterQueryParent().equals(outerQueryScope),
          QueryMessages
              .EXCEPTION_RESULT_SCOPE_SHOULD_HAVE_OUTER_QUERY_SCOPE_EQUAL_WITH_PARAMETER_OUTER_QUERY_SCOP_BE9CDB22);
      scope.ifPresent(
          value ->
              checkState(
                  hasScopeAsLocalParent(returnScope, value),
                  QueryMessages
                      .EXCEPTION_RETURN_SCOPE_SHOULD_HAVE_CONTEXT_SCOPE_AS_ONE_OF_ITS_ANCESTORS_BBBFFE3E));
      return returnScope;
    }

    private Scope process(Node node, Scope scope) {
      return process(node, Optional.of(scope));
    }

    @Override
    public Scope visitNode(Node node, Optional<Scope> context) {
      throw new IllegalStateException(
          DataNodeQueryMessages.UNSUPPORTED_NODE_TYPE + node.getClass().getName());
    }

    @Override
    public Scope visitCreateDB(CreateDB node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.CREATE_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitAlterDB(AlterDB node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.ALTER_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitDropDB(DropDB node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.DROP_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitShowDB(ShowDB node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.SHOW_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitShowCreateDatabase(ShowCreateDatabase node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitCreateTable(final CreateTable node, final Optional<Scope> context) {
      validateProperties(node.getProperties(), context);
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitCreateView(final CreateView node, final Optional<Scope> context) {
      validateProperties(node.getProperties(), context);
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDropTable(final DropTable node, final Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowTables(ShowTables node, Optional<Scope> context) {
      throw new SemanticException(DataNodeQueryMessages.SHOW_TABLES_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitRenameTable(RenameTable node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDescribeTable(DescribeTable node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.DESCRIBE_TABLE_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitSetProperties(final SetProperties node, final Optional<Scope> context) {
      validateProperties(node.getProperties(), context);
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitRenameColumn(RenameColumn node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDropColumn(DropColumn node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitAddColumn(AddColumn node, Optional<Scope> context) {
      throw new SemanticException(DataNodeQueryMessages.ADD_COLUMN_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitCreateIndex(CreateIndex node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.CREATE_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitDropIndex(DropIndex node, Optional<Scope> context) {
      throw new SemanticException(DataNodeQueryMessages.DROP_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitShowIndex(ShowIndex node, Optional<Scope> context) {
      throw new SemanticException(DataNodeQueryMessages.SHOW_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitUpdate(final Update node, final Optional<Scope> context) {
      queryContext.setQueryType(QueryType.WRITE);
      node.parseTable(sessionContext);
      accessControl.checkCanInsertIntoTable(
          sessionContext.getUserName(),
          new QualifiedObjectName(node.getDatabase(), node.getTableName()),
          queryContext);
      final TranslationMap translationMap = analyzeTraverseDevice(node, context, true);
      final TsTable table =
          DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTableName());
      DataNodeTreeViewSchemaUtils.checkTableInWrite(node.getDatabase(), table);
      if (!node.parseWhere(
          null,
          table,
          table.getColumnList().stream()
              .filter(
                  columnSchema ->
                      columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE))
              .map(TsTableColumnSchema::getColumnName)
              .collect(Collectors.toList()),
          queryContext)) {
        analysis.setFinishQueryAfterAnalyze();
        return null;
      }

      // If node.location is absent, this is a pipe-transferred update, namely the assignments are
      // already parsed at the sender
      if (node.getLocation().isPresent()) {
        final Set<SymbolReference> attributeNames = new HashSet<>();
        node.setAssignments(
            node.getAssignments().stream()
                .map(
                    assignment -> {
                      final Expression parsedColumn =
                          analyzeAndRewriteExpression(
                                  translationMap, translationMap.getScope(), assignment.getName())
                              .getRight();
                      if (!(parsedColumn instanceof SymbolReference)
                          || table
                                  .getColumnSchema(((SymbolReference) parsedColumn).getName())
                                  .getColumnCategory()
                              != TsTableColumnCategory.ATTRIBUTE) {
                        throw new SemanticException(
                            DataNodeQueryMessages.UPDATE_CAN_ONLY_SPECIFY_ATTRIBUTE_COLUMNS);
                      }
                      if (attributeNames.contains(parsedColumn)) {
                        throw new SemanticException(
                            DataNodeQueryMessages
                                .UPDATE_ATTRIBUTE_SHALL_SPECIFY_A_ATTRIBUTE_ONLY_ONCE);
                      }
                      attributeNames.add((SymbolReference) parsedColumn);

                      final Pair<Type, Expression> expressionPair;
                      try {
                        expressionPair =
                            analyzeAndRewriteExpression(
                                translationMap, translationMap.getScope(), assignment.getValue());
                      } catch (final Exception e) {
                        if (e.getMessage().contains(DataNodeQueryMessages.CANNOT_BE_RESOLVED)) {
                          throw new SemanticException(
                              new IoTDBException(
                                  e.getCause()
                                      .getMessage()
                                      .replace(
                                          DataNodeQueryMessages.CANNOT_BE_RESOLVED,
                                          DataNodeQueryMessages.IS_NOT_AN_ATTRIBUTE_OR_TAG_COLUMN),
                                  TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
                        }
                        throw e;
                      }
                      if (!expressionPair.getLeft().equals(StringType.STRING)
                          && !expressionPair.getLeft().equals(BinaryType.TEXT)
                          && !expressionPair.getLeft().equals(UnknownType.UNKNOWN)) {
                        throw new SemanticException(
                            DataNodeQueryMessages
                                .UPDATE_S_ATTRIBUTE_VALUE_MUST_BE_STRING_TEXT_OR_NULL);
                      }
                      return new UpdateAssignment(parsedColumn, expressionPair.getRight());
                    })
                .collect(Collectors.toList()));
      }
      return null;
    }

    @Override
    public Scope visitDeleteDevice(final DeleteDevice node, final Optional<Scope> context) {
      // Actually write, but will return the result
      queryContext.setQueryType(QueryType.READ);
      node.parseTable(sessionContext);
      accessControl.checkCanDeleteFromTable(
          sessionContext.getUserName(),
          new QualifiedObjectName(node.getDatabase(), node.getTableName()),
          queryContext);
      final TsTable table =
          DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTableName());
      DataNodeTreeViewSchemaUtils.checkTableInWrite(node.getDatabase(), table);
      node.parseModEntries(table, node.getDatabase(), queryContext);
      analyzeTraverseDevice(node, context, node.getWhere().isPresent());
      node.parseWhere(
          null,
          table,
          table.getColumnList().stream()
              .filter(
                  columnSchema ->
                      columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE))
              .map(TsTableColumnSchema::getColumnName)
              .collect(Collectors.toList()),
          queryContext);
      return null;
    }

    @Override
    public Scope visitDropFunction(DropFunction node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.DROP_FUNCTION_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitShowFunctions(ShowFunctions node, Optional<Scope> context) {
      throw new SemanticException(
          DataNodeQueryMessages.SHOW_FUNCTION_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    @Override
    public Scope visitUse(Use node, Optional<Scope> scope) {
      throw new SemanticException(DataNodeQueryMessages.USE_STATEMENT_IS_NOT_SUPPORTED_YET);
    }

    private boolean containsAnyFieldColumn(
        Set<String> insertColumns, Map<String, ColumnSchema> columnSchemaMap) {
      for (String column : insertColumns) {
        if (columnSchemaMap.containsKey(column)
            && columnSchemaMap.get(column).getColumnCategory() == TsTableColumnCategory.FIELD) {
          return true;
        }
      }
      return false;
    }

    // Do not consider type coercion at the moment
    private boolean typesMatchForInsert(List<Type> tableTypes, List<Type> queryTypes) {
      if (tableTypes.size() != queryTypes.size()) {
        return false;
      }

      for (int i = 0; i < tableTypes.size(); i++) {
        if (!tableTypes.get(i).equals(queryTypes.get(i))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public Scope visitInsert(Insert insert, Optional<Scope> scope) {
      queryContext.setQueryType(QueryType.READ_WRITE);
      // analyze the query that creates the data
      Scope queryScope = analyze(insert.getQuery(), Optional.empty(), false);

      // verify the insert table exists
      QualifiedObjectName targetTable =
          createQualifiedObjectName(sessionContext, insert.getTarget());
      if (!metadata.tableExists(targetTable)) {
        CommonMetadataUtils.throwTableNotExistsException(
            targetTable.getDatabaseName(), targetTable.getObjectName());
      }
      // verify access privileges
      accessControl.checkCanInsertIntoTable(
          sessionContext.getUserName(), targetTable, queryContext);

      // verify the insert destination columns match the query
      Optional<TableSchema> tableSchema = metadata.getTableSchema(sessionContext, targetTable);
      if (!tableSchema.isPresent()) {
        CommonMetadataUtils.throwTableNotExistsException(
            targetTable.getDatabaseName(), targetTable.getObjectName());
      }
      List<ColumnSchema> columns =
          tableSchema.get().getColumns().stream()
              .filter(column -> !column.isHidden())
              .collect(toImmutableList());
      analysis.registerTable(insert.getTable(), tableSchema, targetTable);

      LinkedHashSet<String> tableColumns = new LinkedHashSet<>();
      String actualTimeColumnName = null;
      for (ColumnSchema column : columns) {
        tableColumns.add(column.getName().toLowerCase(ENGLISH));

        if (column.getColumnCategory() == TsTableColumnCategory.TIME) {
          if (actualTimeColumnName != null) {
            throw new SemanticException(
                DataNodeQueryMessages.MULTIPLE_COLUMNS_FOUND_WITH_TIME_CATEGORY_IN_TABLE_SCHEMA);
          }
          actualTimeColumnName = column.getName();
        }
      }
      if (actualTimeColumnName == null) {
        throw new SemanticException(
            DataNodeQueryMessages.TARGET_TABLE_SCHEMA_MISSES_A_TIME_CATEGORY_COLUMN);
      }

      LinkedHashSet<String> insertColumns;
      if (insert.getColumns().isPresent()) {
        insertColumns =
            insert.getColumns().get().stream()
                .map(Identifier::getValue)
                .map(column -> column.toLowerCase(ENGLISH))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> columnNames = new HashSet<>();
        for (String insertColumn : insertColumns) {
          if (!tableColumns.contains(insertColumn)) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.INSERT_COLUMN_NAME_DOES_NOT_EXIST_IN_TARGET_TABLE_S,
                    insertColumn));
          }
          if (!columnNames.add(insertColumn)) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.INSERT_COLUMN_NAME_IS_SPECIFIED_MORE_THAN_ONCE_S,
                    insertColumn));
          }
        }
      } else {
        insertColumns = tableColumns;
      }

      // insert columns should contain time
      if (!insertColumns.contains(actualTimeColumnName)) {
        throw new SemanticException(DataNodeQueryMessages.TIME_COLUMN_CAN_NOT_BE_NULL);
      }
      // insert columns should contain at least one field column
      Map<String, ColumnSchema> columnSchemaMap = tableSchema.get().getColumnSchemaMap();
      if (!containsAnyFieldColumn(insertColumns, columnSchemaMap)) {
        throw new SemanticException(DataNodeQueryMessages.NO_FIELD_COLUMN_PRESENT);
      }

      // set Insert in analysis
      analysis.setInsert(
          new Analysis.Insert(
              insert.getTable(),
              insertColumns.stream().map(columnSchemaMap::get).collect(toImmutableList())));

      List<Type> tableTypes =
          insertColumns.stream()
              .map(insertColumn -> tableSchema.get().getColumn(insertColumn).getType())
              .collect(toImmutableList());
      List<Type> queryTypes =
          queryScope.getRelationType().getVisibleFields().stream()
              .map(Field::getType)
              .collect(toImmutableList());
      if (!typesMatchForInsert(tableTypes, queryTypes)) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.INSERT_QUERY_HAS_MISMATCHED_COLUMN_TYPES_TABLE_S_QUERY_S,
                Joiner.on(QueryMessages.EXCEPTION_COMMA_50AD1C01).join(tableTypes),
                Joiner.on(QueryMessages.EXCEPTION_COMMA_50AD1C01).join(queryTypes)));
      }

      return createAndAssignScope(
          insert, scope, Field.newUnqualified(Insert.ROWS, Insert.ROWS_TYPE, Insert.ROWS_CATEGORY));
    }

    @Override
    public Scope visitInsertRow(InsertRow node, Optional<Scope> context) {
      return visitInsert(node, context);
    }

    public Scope visitInsertTablet(InsertTablet insert, Optional<Scope> scope) {
      return visitInsert(insert, scope);
    }

    @Override
    public Scope visitInsertRows(InsertRows node, Optional<Scope> context) {
      return visitInsert(node, context);
    }

    private Scope visitInsert(WrappedInsertStatement insert, Optional<Scope> scope) {
      final Scope ret = Scope.create();

      final MPPQueryContext context = insert.getContext();
      InsertBaseStatement innerInsert = insert.getInnerTreeStatement();
      innerInsert.toLowerCaseForDevicePath();

      innerInsert =
          AnalyzeUtils.analyzeInsert(
              context,
              innerInsert,
              () -> SchemaValidator.validate(metadata, insert, context, accessControl),
              metadata::getOrCreateDataPartition,
              AnalyzeUtils::computeTableDataPartitionParams,
              analysis,
              false);

      insert.setInnerTreeStatement(innerInsert);
      analysis.setScope(insert, ret);

      return ret;
    }

    @Override
    public Scope visitDelete(Delete node, Optional<Scope> scope) {
      final Scope ret = Scope.create();
      accessControl.checkCanDeleteFromTable(
          sessionContext.getUserName(),
          new QualifiedObjectName(
              AnalyzeUtils.getDatabaseName(node, queryContext),
              node.getTable().getName().getSuffix()),
          queryContext);
      AnalyzeUtils.analyzeDelete(node, queryContext);

      analysis.setScope(node, ret);
      return ret;
    }

    @Override
    public Scope visitPipeEnriched(PipeEnriched node, Optional<Scope> scope) {
      // The LoadTsFile statement is a special case, it needs isGeneratedByPipe information
      // in the analyzer to execute the tsfile-tablet conversion in some cases.
      if (node.getInnerStatement() instanceof LoadTsFile) {
        ((LoadTsFile) node.getInnerStatement()).markIsGeneratedByPipe();
      }

      final Scope ret = node.getInnerStatement().accept(this, scope);
      createAndAssignScope(node, scope);
      analysis.setScope(node, ret);
      return ret;
    }

    @Override
    public Scope visitLoadTsFile(final LoadTsFile node, final Optional<Scope> scope) {
      queryContext.setQueryType(QueryType.OTHER);

      try (final LoadTsFileAnalyzer loadTsFileAnalyzer =
          new LoadTsFileAnalyzer(node, node.isGeneratedByPipe(), queryContext)) {
        loadTsFileAnalyzer.analyzeFileByFile(analysis);
      } catch (final Exception e) {
        final String exceptionMessage =
            String.format(
                "Failed to execute load tsfile statement %s. Detail: %s",
                node, e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        analysis.setFinishQueryAfterAnalyze(true);
        analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
      }

      return createAndAssignScope(node, scope);
    }

    @Override
    public Scope visitExplain(Explain node, Optional<Scope> context) {
      queryContext.setExplainType(ExplainType.EXPLAIN);
      queryContext.setExplainOutputFormat(node.getOutputFormat());
      analysis.setFinishQueryAfterAnalyze();
      return visitQuery((Query) node.getStatement(), context);
    }

    @Override
    public Scope visitCopyTo(CopyTo node, Optional<Scope> context) {
      accessControl.checkUserGlobalSysPrivilege(queryContext);
      Scope innerQueryScope = visitQuery((Query) node.getQueryStatement(), context);
      analysis.setScope(node, innerQueryScope);
      return innerQueryScope;
    }

    @Override
    public Scope visitExplainAnalyze(ExplainAnalyze node, Optional<Scope> context) {
      queryContext.setExplainType(ExplainType.EXPLAIN_ANALYZE);
      queryContext.setVerbose(node.isVerbose());
      queryContext.setExplainOutputFormat(node.getOutputFormat());
      return visitQuery((Query) node.getStatement(), context);
    }

    @Override
    public Scope visitQuery(Query node, Optional<Scope> context) {
      boolean originalHasFillInParentScope = hasFillInParentScope;
      boolean originalHasDiffFunctionInParentScope = hasDiffFunctionInParentScope;
      analysis.setQuery(true);
      Scope withScope = analyzeWith(node, context);
      hasFillInParentScope = node.getFill().isPresent() || hasFillInParentScope;
      hasDiffFunctionInParentScope =
          containsDiffFunctionInQuery(node) || hasDiffFunctionInParentScope;
      Scope queryBodyScope = process(node.getQueryBody(), withScope);

      if (node.getFill().isPresent()) {
        analyzeFill(node.getFill().get(), queryBodyScope);
      }

      List<Expression> orderByExpressions = emptyList();
      if (node.getOrderBy().isPresent()) {
        orderByExpressions =
            analyzeOrderBy(
                node,
                getSortItemsFromOrderBy(node.getOrderBy()),
                queryBodyScope,
                SelectAliasLookup.builder().build());

        if ((queryBodyScope.getOuterQueryParent().isPresent() || !isTopLevel)
            && !node.getLimit().isPresent()
            && !node.getOffset().isPresent()
            && !hasFillInParentScope
            && !hasDiffFunctionInParentScope) {
          // not the root scope and ORDER BY is ineffective
          analysis.markRedundantOrderBy(node.getOrderBy().get());
          warningCollector.add(
              new IoTDBWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
        }
      }
      analysis.setOrderByExpressions(node, orderByExpressions);

      if (node.getOffset().isPresent()) {
        analyzeOffset(node.getOffset().get(), queryBodyScope);
      }

      if (node.getLimit().isPresent()) {
        boolean requiresOrderBy = analyzeLimit(node.getLimit().get(), queryBodyScope);
        if (requiresOrderBy && !node.getOrderBy().isPresent()) {
          throw new SemanticException(
              DataNodeQueryMessages.FETCH_FIRST_WITH_TIES_CLAUSE_REQUIRES_ORDER_BY);
        }
      }

      // Input fields == Output fields
      analysis.setSelectExpressions(
          node,
          descriptorToFields(queryBodyScope).stream()
              .map(expression -> new Analysis.SelectExpression(expression, Optional.empty()))
              .collect(toImmutableList()));

      Scope queryScope =
          Scope.builder()
              .withParent(withScope)
              .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
              .withTables(queryBodyScope.getTables())
              .build();

      analysis.setScope(node, queryScope);
      hasFillInParentScope = originalHasFillInParentScope;
      hasDiffFunctionInParentScope = originalHasDiffFunctionInParentScope;
      return queryScope;
    }

    private List<Expression> descriptorToFields(Scope scope) {
      ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (int fieldIndex = 0;
          fieldIndex < scope.getRelationType().getAllFieldCount();
          fieldIndex++) {
        FieldReference expression = new FieldReference(fieldIndex);
        builder.add(expression);
        analyzeExpression(expression, scope);
      }
      return builder.build();
    }

    private Scope analyzeWith(Query node, Optional<Scope> scope) {
      if (!node.getWith().isPresent()) {
        return createScope(scope);
      }

      // analyze WITH clause
      With with = node.getWith().get();
      analysis.setWith(with);
      Scope.Builder withScopeBuilder = scopeBuilder(scope);

      for (WithQuery withQuery : with.getQueries()) {
        String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
        if (withScopeBuilder.containsNamedQuery(name)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.WITH_QUERY_NAME_S_SPECIFIED_MORE_THAN_ONCE, name));
        }

        boolean isRecursive = false;
        if (with.isRecursive()) {
          throw new SemanticException(DataNodeQueryMessages.RECURSIVE_CTE_IS_NOT_SUPPORTED_YET);
        }

        if (!isRecursive) {
          Query query = withQuery.getQuery();
          Scope queryScope = analyze(query, withScopeBuilder.build());

          // check if all or none of the columns are explicitly alias
          if (withQuery.getColumnNames().isPresent()) {
            validateColumnAliases(
                withQuery.getColumnNames().get(),
                analysis.getOutputDescriptor(query).getVisibleFieldCount());
          }

          withScopeBuilder.withNamedQuery(name, withQuery);
          queryContext.addSubQueryTables(withQuery.getQuery(), queryScope.getTables());
        }
      }
      Scope withScope = withScopeBuilder.build();
      analysis.setScope(with, withScope);
      return withScope;
    }

    private boolean tryProcessRecursiveQuery(
        WithQuery withQuery, String name, Scope.Builder withScopeBuilder) {
      if (!withQuery.getColumnNames().isPresent()) {
        throw new SemanticException(
            DataNodeQueryMessages.MISSING_COLUMN_ALIASES_IN_RECURSIVE_WITH_QUERY);
      }
      preOrder(withQuery.getQuery())
          .filter(child -> child instanceof With && ((With) child).isRecursive())
          .findFirst()
          .ifPresent(
              child -> {
                throw new SemanticException(DataNodeQueryMessages.NESTED_RECURSIVE_WITH_QUERY);
              });
      // if RECURSIVE is specified, all queries in the WITH list are considered potentially
      // recursive
      // try resolve WITH query as expandable query
      // a) validate shape of the query and location of recursive reference
      if (!(withQuery.getQuery().getQueryBody() instanceof Union)) {
        return false;
      }
      Union union = (Union) withQuery.getQuery().getQueryBody();
      if (union.getRelations().size() != 2) {
        return false;
      }
      Relation anchor = union.getRelations().get(0);
      Relation step = union.getRelations().get(1);
      List<Node> anchorReferences = findReferences(anchor, withQuery.getName());
      if (!anchorReferences.isEmpty()) {
        throw new SemanticException(
            DataNodeQueryMessages.WITH_TABLE_NAME_IS_REFERENCED_IN_THE_BASE_RELATION_OF_RECURSION);
      }
      // a WITH query is linearly recursive if it has a single recursive reference
      List<Node> stepReferences = findReferences(step, withQuery.getName());
      if (stepReferences.size() > 1) {
        throw new SemanticException(
            DataNodeQueryMessages.MULTIPLE_RECURSIVE_REFERENCES_IN_THE_STEP_RELATION_OF_RECURSION);
      }
      if (stepReferences.size() != 1) {
        return false;
      }
      // search for QuerySpecification in parenthesized subquery
      Relation specification = step;
      while (specification instanceof TableSubquery) {
        Query query = ((TableSubquery) specification).getQuery();
        query
            .getLimit()
            .ifPresent(
                limit -> {
                  throw new SemanticException(
                      DataNodeQueryMessages
                          .FETCH_FIRST_LIMIT_CLAUSE_IN_THE_STEP_RELATION_OF_RECURSION);
                });
        specification = query.getQueryBody();
      }
      if (!(specification instanceof QuerySpecification)
          || !((QuerySpecification) specification).getFrom().isPresent()) {
        throw new SemanticException(
            DataNodeQueryMessages
                .RECURSIVE_REFERENCE_OUTSIDE_OF_FROM_CLAUSE_OF_THE_STEP_RELATION_OF_RECURSION);
      }
      Relation from = ((QuerySpecification) specification).getFrom().get();
      List<Node> fromReferences = findReferences(from, withQuery.getName());
      if (fromReferences.isEmpty()) {
        throw new SemanticException(
            DataNodeQueryMessages
                .RECURSIVE_REFERENCE_OUTSIDE_OF_FROM_CLAUSE_OF_THE_STEP_RELATION_OF_RECURSION);
      }

      // b) validate top-level shape of recursive query
      withQuery
          .getQuery()
          .getWith()
          .ifPresent(
              innerWith -> {
                throw new SemanticException(
                    DataNodeQueryMessages
                        .IMMEDIATE_WITH_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED);
              });
      withQuery
          .getQuery()
          .getFill()
          .ifPresent(
              orderBy -> {
                throw new SemanticException(
                    DataNodeQueryMessages
                        .IMMEDIATE_FILL_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED);
              });
      withQuery
          .getQuery()
          .getOrderBy()
          .ifPresent(
              orderBy -> {
                throw new SemanticException(
                    DataNodeQueryMessages
                        .IMMEDIATE_ORDER_BY_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED);
              });
      withQuery
          .getQuery()
          .getOffset()
          .ifPresent(
              offset -> {
                throw new SemanticException(
                    DataNodeQueryMessages
                        .IMMEDIATE_OFFSET_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED);
              });
      withQuery
          .getQuery()
          .getLimit()
          .ifPresent(
              limit -> {
                throw new SemanticException(
                    DataNodeQueryMessages
                        .IMMEDIATE_FETCH_FIRST_LIMIT_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORT);
              });

      // c) validate recursion step has no illegal clauses
      validateFromClauseOfRecursiveTerm(from, withQuery.getName());

      // shape validation complete - process query as expandable query
      Scope parentScope = withScopeBuilder.build();
      // process expandable query -- anchor
      Scope anchorScope = process(anchor, parentScope);
      // set aliases in anchor scope as defined for WITH query. Recursion step will refer to anchor
      // fields by aliases.
      Scope aliasedAnchorScope =
          setAliases(anchorScope, withQuery.getName(), withQuery.getColumnNames().get());
      // record expandable query base scope for recursion step analysis
      Node recursiveReference = fromReferences.get(0);
      analysis.setExpandableBaseScope(recursiveReference, aliasedAnchorScope);
      // process expandable query -- recursion step
      Scope stepScope = process(step, parentScope);

      // verify anchor and step have matching descriptors
      RelationType anchorType = aliasedAnchorScope.getRelationType().withOnlyVisibleFields();
      RelationType stepType = stepScope.getRelationType().withOnlyVisibleFields();
      if (anchorType.getVisibleFieldCount() != stepType.getVisibleFieldCount()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .BASE_AND_STEP_RELATIONS_OF_RECURSION_HAVE_DIFFERENT_NUMBER_OF_FIELDS_S_S,
                anchorType.getVisibleFieldCount(),
                stepType.getVisibleFieldCount()));
      }

      List<Type> anchorFieldTypes =
          anchorType.getVisibleFields().stream().map(Field::getType).collect(toImmutableList());
      List<Type> stepFieldTypes =
          stepType.getVisibleFields().stream().map(Field::getType).collect(toImmutableList());

      for (int i = 0; i < anchorFieldTypes.size(); i++) {
        if (stepFieldTypes.get(i) != anchorFieldTypes.get(i)) {
          // TODO for more precise error location, pass the mismatching select expression instead of
          // `step`
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .RECURSION_STEP_RELATION_OUTPUT_TYPE_S_IS_NOT_COERCIBLE_TO_RECURSION_BASE_RELATION_OUTPUT,
                  stepFieldTypes.get(i),
                  anchorFieldTypes.get(i),
                  i + 1));
        }
      }

      if (!anchorFieldTypes.equals(stepFieldTypes)) {
        analysis.addRelationCoercion(step, anchorFieldTypes.toArray(new Type[0]));
      }

      analysis.setScope(withQuery.getQuery(), aliasedAnchorScope);
      analysis.registerExpandableQuery(withQuery.getQuery(), recursiveReference);
      withScopeBuilder.withNamedQuery(name, withQuery);
      return true;
    }

    @Override
    public Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope) {
      StatementAnalyzer analyzer =
          statementAnalyzerFactory.createStatementAnalyzer(
              analysis, queryContext, sessionContext, warningCollector, CorrelationSupport.ALLOWED);
      analyzer.hasFillInParentScope = hasFillInParentScope;
      analyzer.hasDiffFunctionInParentScope = hasDiffFunctionInParentScope;
      Scope queryScope =
          analyzer.analyze(
              node.getQuery(),
              scope.orElseThrow(
                  () -> new NoSuchElementException(DataNodeQueryMessages.NO_VALUE_PRESENT)));
      return createAndAssignScope(node, scope, queryScope.getRelationType());
    }

    @Override
    public Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope) {
      boolean originalHasFillInParentScope = hasFillInParentScope;
      boolean originalHasDiffFunctionInParentScope = hasDiffFunctionInParentScope;
      // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
      // to pass down to analyzeFrom
      hasFillInParentScope = node.getFill().isPresent() || hasFillInParentScope;
      hasDiffFunctionInParentScope =
          containsDiffFunctionInQuerySpecification(node) || hasDiffFunctionInParentScope;

      Scope sourceScope = analyzeFrom(node, scope);
      analyzeWindowDefinitions(node, sourceScope);
      resolveFunctionCallAndMeasureWindows(node);

      node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

      SelectAnalysis selectAnalysis = analyzeSelect(node, sourceScope);
      List<Expression> outputExpressions = selectAnalysis.getOutputExpressions();
      Analysis.GroupingSetAnalysis groupByAnalysis =
          analyzeGroupBy(node, sourceScope, outputExpressions, selectAnalysis.getAliases());
      analyzeHaving(node, sourceScope);

      Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope, selectAnalysis);

      node.getFill()
          .ifPresent(
              fill -> {
                Scope fillScope = computeAndAssignFillScope(fill, sourceScope, outputScope);
                analyzeFill(fill, fillScope);
              });

      List<Expression> orderByExpressions = emptyList();
      Optional<Scope> orderByScope = Optional.empty();
      if (node.getOrderBy().isPresent()) {
        OrderBy orderBy = node.getOrderBy().get();
        orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

        orderByExpressions =
            analyzeOrderBy(
                node, orderBy.getSortItems(), orderByScope.get(), selectAnalysis.getAliases());

        if ((sourceScope.getOuterQueryParent().isPresent() || !isTopLevel)
            && !node.getLimit().isPresent()
            && !node.getOffset().isPresent()
            && !hasFillInParentScope
            && !hasDiffFunctionInParentScope) {
          // not the root scope and ORDER BY is ineffective
          analysis.markRedundantOrderBy(orderBy);
          warningCollector.add(
              new IoTDBWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
        }
      }
      analysis.setOrderByExpressions(node, orderByExpressions);

      if (node.getOffset().isPresent()) {
        analyzeOffset(node.getOffset().get(), outputScope);
      }

      if (node.getLimit().isPresent()) {
        boolean requiresOrderBy = analyzeLimit(node.getLimit().get(), outputScope);
        if (requiresOrderBy && !node.getOrderBy().isPresent()) {
          throw new SemanticException(
              DataNodeQueryMessages.FETCH_FIRST_WITH_TIES_CLAUSE_REQUIRES_ORDER_BY);
        }
      }

      List<Expression> sourceExpressions = new ArrayList<>();
      analysis.getSelectExpressions(node).stream()
          .map(Analysis.SelectExpression::getExpression)
          .forEach(sourceExpressions::add);
      node.getHaving().ifPresent(sourceExpressions::add);

      for (WindowDefinition windowDefinition : node.getWindows()) {
        WindowSpecification window = windowDefinition.getWindow();
        sourceExpressions.addAll(window.getPartitionBy());
        getSortItemsFromOrderBy(window.getOrderBy()).stream()
            .map(SortItem::getSortKey)
            .forEach(sourceExpressions::add);
        if (window.getFrame().isPresent()) {
          WindowFrame frame = window.getFrame().get();
          frame.getStart().getValue().ifPresent(sourceExpressions::add);
          frame.getEnd().flatMap(FrameBound::getValue).ifPresent(sourceExpressions::add);
        }
      }

      analyzeAggregations(
          node, sourceScope, orderByScope, groupByAnalysis, sourceExpressions, orderByExpressions);
      analyzeWindowFunctionsAndMeasures(node, outputExpressions, orderByExpressions);

      if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
        ImmutableList.Builder<Expression> aggregates =
            ImmutableList.<Expression>builder()
                .addAll(groupByAnalysis.getOriginalExpressions())
                .addAll(extractAggregateFunctions(orderByExpressions));

        analysis.setOrderByAggregates(node.getOrderBy().get(), aggregates.build());
      }

      if (node.getOrderBy().isPresent() && node.getSelect().isDistinct()) {
        verifySelectDistinct(
            node,
            orderByExpressions,
            outputExpressions,
            sourceScope,
            orderByScope.orElseThrow(
                () -> new NoSuchElementException(DataNodeQueryMessages.NO_VALUE_PRESENT)));
      }

      hasFillInParentScope = originalHasFillInParentScope;
      hasDiffFunctionInParentScope = originalHasDiffFunctionInParentScope;
      return outputScope;
    }

    private void analyzeWindowFunctionsAndMeasures(
        QuerySpecification node,
        List<Expression> outputExpressions,
        List<Expression> orderByExpressions) {
      analysis.setWindowFunctions(node, analyzeWindowFunctions(node, outputExpressions));
      if (node.getOrderBy().isPresent()) {
        OrderBy orderBy = node.getOrderBy().get();
        analysis.setOrderByWindowFunctions(
            orderBy, analyzeWindowFunctions(node, orderByExpressions));
      }
    }

    private List<FunctionCall> analyzeWindowFunctions(
        QuerySpecification node, List<Expression> expressions) {
      List<FunctionCall> windowFunctions = extractWindowFunctions(expressions);

      for (FunctionCall windowFunction : windowFunctions) {
        List<Expression> nestedWindowExpressions =
            extractWindowExpressions(windowFunction.getArguments());
        if (!nestedWindowExpressions.isEmpty()) {
          throw new SemanticException(
              DataNodeQueryMessages
                  .CANNOT_NEST_WINDOW_FUNCTIONS_OR_ROW_PATTERN_MEASURES_INSIDE_WINDOW_FUNCTION_ARGUMENTS);
        }

        if (windowFunction.isDistinct()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.DISTINCT_IN_WINDOW_FUNCTION_PARAMETERS_NOT_YET_SUPPORTED_S,
                  windowFunction));
        }

        Analysis.ResolvedWindow window = analysis.getWindow(windowFunction);
        String name = windowFunction.getName().toString().toLowerCase(ENGLISH);
        if (name.equals("lag") || name.equals("lead")) {
          if (!window.getOrderBy().isPresent()) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.S_FUNCTION_REQUIRES_AN_ORDER_BY_WINDOW_CLAUSE,
                    windowFunction.getName()));
          }
          if (window.getFrame().isPresent()) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.CANNOT_SPECIFY_WINDOW_FRAME_FOR_S_FUNCTION,
                    windowFunction.getName()));
          }
        }
      }

      return windowFunctions;
    }

    // cover case: (query1) UNION (query2) order by ...
    private boolean containsDiffFunctionInQuery(Query node) {
      return getSortItemsFromOrderBy(node.getOrderBy()).stream()
          .map(SortItem::getSortKey)
          .anyMatch(expression -> containsDiffFunction(expression));
    }

    private boolean containsDiffFunctionInQuerySpecification(QuerySpecification node) {
      for (SelectItem selectItem : node.getSelect().getSelectItems()) {
        if (selectItem instanceof AllColumns) {
          Optional<Expression> target = ((AllColumns) selectItem).getTarget();
          if (target.isPresent() && containsDiffFunction(target.get())) {
            return true;
          }
        } else if (selectItem instanceof SingleColumn
            && containsDiffFunction(((SingleColumn) selectItem).getExpression())) {
          return true;
        }
      }

      if (node.getWhere().isPresent() && containsDiffFunction(node.getWhere().get())) {
        return true;
      }
      if (node.getHaving().isPresent() && containsDiffFunction(node.getHaving().get())) {
        return true;
      }
      return getSortItemsFromOrderBy(node.getOrderBy()).stream()
          .map(SortItem::getSortKey)
          .anyMatch(expression -> containsDiffFunction(expression));
    }

    private void resolveFunctionCallAndMeasureWindows(QuerySpecification querySpecification) {
      ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

      // SELECT expressions and ORDER BY expressions can contain window functions
      for (SelectItem item : querySpecification.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          ((AllColumns) item).getTarget().ifPresent(expressions::add);
        } else if (item instanceof SingleColumn) {
          expressions.add(((SingleColumn) item).getExpression());
        }
      }
      for (SortItem sortItem : getSortItemsFromOrderBy(querySpecification.getOrderBy())) {
        expressions.add(sortItem.getSortKey());
      }

      for (FunctionCall windowFunction : extractWindowFunctions(expressions.build())) {
        if (analysis.getWindow(windowFunction) != null) {
          continue;
        }
        Analysis.ResolvedWindow resolvedWindow =
            resolveWindowSpecification(querySpecification, windowFunction.getWindow().get());
        analysis.setWindow(windowFunction, resolvedWindow);
      }
    }

    private void resolveFunctionCallAndMeasureWindows(
        QuerySpecification querySpecification, Expression expression) {
      for (FunctionCall windowFunction : extractWindowFunctions(ImmutableList.of(expression))) {
        if (analysis.getWindow(windowFunction) != null) {
          continue;
        }
        Analysis.ResolvedWindow resolvedWindow =
            resolveWindowSpecification(querySpecification, windowFunction.getWindow().get());
        analysis.setWindow(windowFunction, resolvedWindow);
      }
    }

    private void analyzeWindowDefinitions(QuerySpecification node, Scope scope) {
      for (WindowDefinition windowDefinition : node.getWindows()) {
        CanonicalizationAware<Identifier> canonicalName =
            canonicalizationAwareKey(windowDefinition.getName());

        if (analysis.getWindowDefinition(node, canonicalName) != null) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.WINDOW_NAME_S_SPECIFIED_MORE_THAN_ONCE,
                  windowDefinition.getName()));
        }

        Analysis.ResolvedWindow resolvedWindow =
            resolveWindowSpecification(node, windowDefinition.getWindow());

        // Analyze window after it is resolved, because resolving might provide necessary
        // information, e.g. ORDER BY necessary for frame analysis.
        // Analyze only newly introduced window properties. Properties of the referenced window have
        // been already analyzed.
        analyzeWindow(node, resolvedWindow, scope, windowDefinition.getWindow());

        analysis.addWindowDefinition(node, canonicalName, resolvedWindow);
      }
    }

    private void analyzeWindow(
        QuerySpecification querySpecification,
        Analysis.ResolvedWindow window,
        Scope scope,
        Node originalNode) {
      ExpressionAnalysis expressionAnalysis =
          ExpressionAnalyzer.analyzeWindow(
              metadata,
              sessionContext,
              queryContext,
              statementAnalyzerFactory,
              accessControl,
              scope,
              analysis,
              WarningCollector.NOOP,
              correlationSupport,
              window,
              originalNode);
      analysis.recordSubqueries(querySpecification, expressionAnalysis);
    }

    private Analysis.ResolvedWindow resolveWindowSpecification(
        QuerySpecification querySpecification, Window window) {
      if (window instanceof WindowReference) {
        WindowReference windowReference = (WindowReference) window;
        CanonicalizationAware<Identifier> canonicalName =
            canonicalizationAwareKey(windowReference.getName());
        Analysis.ResolvedWindow referencedWindow =
            analysis.getWindowDefinition(querySpecification, canonicalName);
        if (referencedWindow == null) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.CANNOT_RESOLVE_WINDOW_NAME_S, windowReference.getName()));
        }

        return new Analysis.ResolvedWindow(
            referencedWindow.getPartitionBy(),
            referencedWindow.getOrderBy(),
            referencedWindow.getFrame(),
            !referencedWindow.getPartitionBy().isEmpty(),
            referencedWindow.getOrderBy().isPresent(),
            referencedWindow.getFrame().isPresent());
      }

      WindowSpecification windowSpecification = (WindowSpecification) window;

      if (windowSpecification.getExistingWindowName().isPresent()) {
        Identifier referencedName = windowSpecification.getExistingWindowName().get();
        CanonicalizationAware<Identifier> canonicalName = canonicalizationAwareKey(referencedName);
        Analysis.ResolvedWindow referencedWindow =
            analysis.getWindowDefinition(querySpecification, canonicalName);
        if (referencedWindow == null) {
          throw new SemanticException(
              String.format(DataNodeQueryMessages.CANNOT_RESOLVE_WINDOW_NAME_S, referencedName));
        }

        // analyze dependencies between this window specification and referenced window
        // specification
        if (!windowSpecification.getPartitionBy().isEmpty()) {
          throw new SemanticException(
              DataNodeQueryMessages
                  .WINDOW_SPECIFICATION_WITH_NAMED_WINDOW_REFERENCE_CANNOT_SPECIFY_PARTITION_BY);
        }
        if (windowSpecification.getOrderBy().isPresent()
            && referencedWindow.getOrderBy().isPresent()) {
          throw new SemanticException(
              DataNodeQueryMessages
                  .CANNOT_SPECIFY_ORDER_BY_IF_REFERENCED_NAMED_WINDOW_SPECIFIES_ORDER_BY);
        }
        if (referencedWindow.getFrame().isPresent()) {
          throw new SemanticException(
              DataNodeQueryMessages.CANNOT_REFERENCE_NAMED_WINDOW_CONTAINING_FRAME_SPECIFICATION);
        }

        // resolve window
        Optional<OrderBy> orderBy = windowSpecification.getOrderBy();
        boolean orderByInherited = false;
        if (!orderBy.isPresent() && referencedWindow.getOrderBy().isPresent()) {
          orderBy = referencedWindow.getOrderBy();
          orderByInherited = true;
        }

        List<Expression> partitionBy = windowSpecification.getPartitionBy();
        boolean partitionByInherited = false;
        if (!referencedWindow.getPartitionBy().isEmpty()) {
          partitionBy = referencedWindow.getPartitionBy();
          partitionByInherited = true;
        }

        Optional<WindowFrame> windowFrame = windowSpecification.getFrame();
        boolean frameInherited = false;
        if (!windowFrame.isPresent() && referencedWindow.getFrame().isPresent()) {
          windowFrame = referencedWindow.getFrame();
          frameInherited = true;
        }

        return new Analysis.ResolvedWindow(
            partitionBy,
            orderBy,
            windowFrame,
            partitionByInherited,
            orderByInherited,
            frameInherited);
      }

      return new Analysis.ResolvedWindow(
          windowSpecification.getPartitionBy(),
          windowSpecification.getOrderBy(),
          windowSpecification.getFrame(),
          false,
          false,
          false);
    }

    private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope) {
      if (node.getFrom().isPresent()) {
        return process(node.getFrom().get(), scope);
      }

      Scope result = createScope(scope);
      analysis.setImplicitFromScope(node, result);
      return result;
    }

    private void analyzeWhere(Node node, Scope scope, Expression predicate) {
      verifyNoAggregateWindowOrGroupingFunctions(predicate, "WHERE clause");

      // contains Columns, expand them and concat them
      if (containsColumns(predicate)) {
        ExpandColumnsVisitor visitor = new ExpandColumnsVisitor(null);
        List<Expression> expandedExpressions = visitor.process(predicate, scope);
        if (expandedExpressions.isEmpty()) {
          throw new IllegalStateException(
              DataNodeQueryMessages.THERE_IS_AT_LEAST_ONE_RESULT_OF_EXPANDED);
        }
        if (expandedExpressions.size() >= 2) {
          predicate = new LogicalExpression(LogicalExpression.Operator.AND, expandedExpressions);
        } else {
          predicate = expandedExpressions.get(0);
        }
      }

      ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
      analysis.recordSubqueries(node, expressionAnalysis);

      Type predicateType = expressionAnalysis.getType(predicate);
      if (!predicateType.equals(BOOLEAN)) {
        //        if (!predicateType.equals(UNKNOWN)) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.WHERE_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S,
                predicateType));
        //        }
        // coerce null to boolean
        //        analysis.addCoercion(predicate, BOOLEAN, false);
      }

      analysis.setWhere(node, predicate);
    }

    private SelectAnalysis analyzeSelect(QuerySpecification node, Scope scope) {
      ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();
      ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder =
          ImmutableList.builder();
      SelectAliasLookup.Builder selectAliasBuilder = SelectAliasLookup.builder();
      ImmutableMap.Builder<NodeRef<SingleColumn>, Expression> singleColumnExpressionBuilder =
          ImmutableMap.builder();

      int outputPosition = 1;
      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          outputPosition +=
              analyzeSelectAllColumns(
                  (AllColumns) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
        } else if (item instanceof SingleColumn) {
          SingleColumn singleColumn = (SingleColumn) item;
          Expression selectExpression = singleColumn.getExpression();
          if (containsColumns(selectExpression)) {
            ExpandColumnsVisitor visitor =
                new ExpandColumnsVisitor(singleColumn.getAlias().orElse(null));
            List<Expression> expandedExpressions = visitor.process(selectExpression, scope);
            if (expandedExpressions.isEmpty()) {
              throw new IllegalStateException(
                  DataNodeQueryMessages.THERE_IS_AT_LEAST_ONE_RESULT_OF_EXPANDED);
            }
            singleColumn.setExpandedExpressions(expandedExpressions);
            singleColumn.setAccordingColumnName(visitor.getAccordingColumnNames());
            for (Expression expression : expandedExpressions) {
              analyzeSelectSingleColumn(
                  expression, node, scope, outputExpressionBuilder, selectExpressionBuilder);
              outputPosition++;
            }
          } else {
            LateralColumnAliasRewrite lateralColumnAliasRewrite =
                rewriteLateralColumnAliasesWithReferences(
                    selectExpression, scope, selectAliasBuilder);
            Expression rewrittenExpression = lateralColumnAliasRewrite.getExpression();
            resolveFunctionCallAndMeasureWindows(node, rewrittenExpression);
            analyzeSelectSingleColumn(
                rewrittenExpression,
                node,
                scope,
                outputExpressionBuilder,
                selectExpressionBuilder,
                lateralColumnAliasRewrite.getReferences());
            singleColumnExpressionBuilder.put(NodeRef.of(singleColumn), rewrittenExpression);
            if (singleColumn.getAlias().isPresent()) {
              Identifier alias = singleColumn.getAlias().get();
              SelectAlias selectAlias =
                  new SelectAlias(
                      canonicalSelectAliasName(alias), outputPosition, rewrittenExpression);
              selectAliasBuilder.add(selectAlias);
            }
            outputPosition++;
          }
        } else {
          throw new IllegalArgumentException(
              String.format(
                  DataNodeQueryMessages.QUERY_EXCEPTION_UNSUPPORTED_SELECTITEM_TYPE_S_4B0B155A,
                  item.getClass().getName()));
        }
      }
      analysis.setSelectExpressions(node, selectExpressionBuilder.build());

      if (node.getSelect().isDistinct()) {
        analysis.setContainsSelectDistinct();
      }

      return new SelectAnalysis(
          outputExpressionBuilder.build(),
          selectAliasBuilder.build(),
          singleColumnExpressionBuilder.build());
    }

    /**
     * Check if there is Columns function in expression, and verify they are same if there are multi
     * Column functions.
     *
     * @param expression input expression
     * @return if there is Columns function in expression
     * @throws SemanticException if there are multi Columns functions but different
     */
    private boolean containsColumns(Expression expression) {
      return containsColumnsHelper(expression) != null;
    }

    private Node containsColumnsHelper(Node node) {
      if (node instanceof Columns) {
        return node;
      }

      Node target = null;
      for (Node child : node.getChildren()) {
        Node childResult = containsColumnsHelper(child);

        if (childResult == null) {
          continue;
        }

        // initialize target
        if (target == null) {
          target = childResult;
          continue;
        }

        if (!childResult.equals(target)) {
          throw new SemanticException(
              DataNodeQueryMessages
                  .MULTIPLE_DIFFERENT_COLUMNS_IN_THE_SAME_EXPRESSION_ARE_NOT_SUPPORTED);
        }
      }
      return target;
    }

    private class ExpandColumnsVisitor implements AstVisitor<List<Expression>, Scope> {
      private final Identifier alias;
      // Record Columns expanded result in process, not always equals with final result
      private List<Expression> expandedExpressions;
      // Records the actual output column name of each Expression, used to compute output Scope.
      private List<String> accordingColumnNames;

      private ExpandColumnsVisitor(Identifier alias) {
        this.alias = alias;
      }

      public List<String> getAccordingColumnNames() {
        return accordingColumnNames;
      }

      public List<Expression> visitNode(Node node, Scope scope) {
        throw new UnsupportedOperationException(
            DataNodeQueryMessages
                .QUERY_EXCEPTION_THIS_VISITOR_ONLY_SUPPORTED_PROCESS_OF_EXPRESSION_7CEB79CB);
      }

      public List<Expression> visitExpression(Expression node, Scope scope) {
        if (node.getChildren().isEmpty()) {
          return Collections.singletonList(node);
        }
        throw new UnsupportedOperationException(
            DataNodeQueryMessages.UNSUPPORTED_EXPRESSION_2 + node);
      }

      @Override
      public List<Expression> visitColumns(Columns node, Scope context) {
        // avoid redundant process
        if (expandedExpressions != null) {
          return expandedExpressions;
        }

        List<Field> requestedFields = (List<Field>) context.getRelationType().getVisibleFields();
        List<Field> fields = filterInaccessibleFields(requestedFields);
        if (fields.isEmpty()) {
          if (!requestedFields.isEmpty()) {
            throw new SemanticException(DataNodeQueryMessages.RELATION_NOT_FOUND_OR_NOT_ALLOWED);
          }
          throw new SemanticException(
              DataNodeQueryMessages.COLUMNS_NOT_ALLOWED_FOR_RELATION_THAT_HAS_NO);
        }

        ImmutableList.Builder<Expression> matchedColumns = ImmutableList.builder();
        ImmutableList.Builder<String> outputColumnNames = ImmutableList.builder();
        if (node.isColumnsAsterisk()) {
          for (Field field : fields) {
            String columnName = field.getName().orElse(null);
            if (columnName == null) {
              throw new SemanticException(DataNodeQueryMessages.UNKNOWN_COLUMNNAME + field);
            }
            matchedColumns.add(new Identifier(columnName));
            outputColumnNames.add(alias == null ? columnName : alias.getValue());
          }
        } else {
          Pattern pattern;
          try {
            pattern = Pattern.compile(node.getPattern());
          } catch (PatternSyntaxException e) {
            throw new SemanticException(
                String.format(DataNodeQueryMessages.INVALID_REGEX, node.getPattern()));
          }
          Matcher matcher = pattern.matcher("");

          for (Field field : fields) {
            String columnName = field.getName().orElse(null);
            if (columnName == null) {
              throw new SemanticException(DataNodeQueryMessages.UNKNOWN_COLUMNNAME + field);
            }
            matcher.reset(columnName);
            if (matcher.matches()) {
              matchedColumns.add(new Identifier(columnName));

              // process alias
              if (alias != null) {
                try {
                  outputColumnNames.add(matcher.replaceAll(alias.getValue()));
                } catch (Exception e) {
                  throw new SemanticException(e.getMessage());
                }
              } else {
                outputColumnNames.add(columnName);
              }
            }
          }
        }
        List<Expression> result = matchedColumns.build();
        if (result.isEmpty()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.NO_MATCHING_COLUMNS_FOUND_THAT_MATCH_REGEX_S,
                  node.getPattern()));
        }
        expandedExpressions = result;
        accordingColumnNames = outputColumnNames.build();

        return result;
      }

      @Override
      public List<Expression> visitArithmeticBinary(
          ArithmeticBinaryExpression node, Scope context) {
        List<Expression> leftResult = process(node.getLeft(), context);
        List<Expression> rightResult = process(node.getRight(), context);

        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int leftSize = leftResult.size();
        int rightSize = rightResult.size();
        int maxSize = Math.max(leftSize, rightSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger leftIndex = (leftSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger rightIndex = (rightSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new ArithmeticBinaryExpression(
                  node.getOperator(),
                  leftResult.get(leftIndex.get()),
                  rightResult.get(rightIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitArithmeticUnary(ArithmeticUnaryExpression node, Scope context) {
        List<Expression> childResult = process(node.getValue(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(new ArithmeticUnaryExpression(node.getSign(), expression));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitBetweenPredicate(BetweenPredicate node, Scope context) {
        List<Expression> valueResult = process(node.getValue(), context);
        List<Expression> minResult = process(node.getMin(), context);
        List<Expression> maxResult = process(node.getMax(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int valueResultSize = valueResult.size();
        int minResultSize = minResult.size();
        int maxResultSize = maxResult.size();
        int maxSize = Math.max(valueResultSize, Math.max(minResultSize, maxResultSize));

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger valueIndex = (valueResultSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger minIndex = (minResultSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger maxIndex = (maxResultSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new BetweenPredicate(
                  valueResult.get(valueIndex.get()),
                  minResult.get(minIndex.get()),
                  maxResult.get(maxIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitCast(Cast node, Scope context) {
        List<Expression> childResult = process(node.getExpression(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(new Cast(expression, node.getType()));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitCoalesceExpression(CoalesceExpression node, Scope context) {
        ImmutableList.Builder<List<Expression>> childrenResultListBuilder =
            new ImmutableList.Builder<>();
        node.getOperands()
            .forEach(operand -> childrenResultListBuilder.add(process(operand, context)));
        List<List<Expression>> childrenResultList = childrenResultListBuilder.build();
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int maxSize = childrenResultList.stream().mapToInt(List::size).max().orElse(0);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger[] childrenIndexes = new AtomicInteger[childrenResultList.size()];
        for (int i = 0; i < childrenIndexes.length; i++) {
          childrenIndexes[i] =
              (childrenResultList.get(i).size() == maxSize) ? baseIndex : new AtomicInteger(0);
        }
        for (int i = 0; i < maxSize; i++) {
          ImmutableList.Builder<Expression> operandListBuilder = new ImmutableList.Builder<>();
          for (int j = 0; j < childrenIndexes.length; j++) {
            int operandIndexInResult = childrenIndexes[j].get();
            operandListBuilder.add(childrenResultList.get(j).get(operandIndexInResult));
          }
          resultBuilder.add(new CoalesceExpression(operandListBuilder.build()));
          baseIndex.getAndIncrement();
        }
        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitComparisonExpression(ComparisonExpression node, Scope context) {
        List<Expression> leftResult = process(node.getLeft(), context);
        List<Expression> rightResult = process(node.getRight(), context);

        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int leftSize = leftResult.size();
        int rightSize = rightResult.size();
        int maxSize = Math.max(leftSize, rightSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger leftIndex = (leftSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger rightIndex = (rightSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new ComparisonExpression(
                  node.getOperator(),
                  leftResult.get(leftIndex.get()),
                  rightResult.get(rightIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitDereferenceExpression(
          DereferenceExpression node, Scope context) {
        process(node.getBase(), context);
        if (expandedExpressions == null) {
          return Collections.singletonList(node);
        }
        throw new SemanticException(
            DataNodeQueryMessages.COLUMNS_ARE_NOT_SUPPORTED_IN_DEREFERENCEEXPRESSION);
      }

      @Override
      public List<Expression> visitExists(ExistsPredicate node, Scope context) {
        // We don't need to process Query here
        return Collections.singletonList(node);
      }

      @Override
      public List<Expression> visitFunctionCall(FunctionCall node, Scope context) {
        ImmutableList.Builder<List<Expression>> childrenResultListBuilder =
            new ImmutableList.Builder<>();
        node.getArguments()
            .forEach(operand -> childrenResultListBuilder.add(process(operand, context)));
        List<List<Expression>> childrenResultList = childrenResultListBuilder.build();
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int maxSize = childrenResultList.stream().mapToInt(List::size).max().orElse(0);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger[] childrenIndexes = new AtomicInteger[childrenResultList.size()];
        for (int i = 0; i < childrenIndexes.length; i++) {
          childrenIndexes[i] =
              (childrenResultList.get(i).size() == maxSize) ? baseIndex : new AtomicInteger(0);
        }
        for (int i = 0; i < maxSize; i++) {
          List<Expression> operandListBuilder = new ArrayList<>(childrenIndexes.length);
          for (int j = 0; j < childrenIndexes.length; j++) {
            int operandIndexInResult = childrenIndexes[j].get();
            operandListBuilder.add(childrenResultList.get(j).get(operandIndexInResult));
          }
          resultBuilder.add(new FunctionCall(node.getName(), operandListBuilder));
          baseIndex.getAndIncrement();
        }
        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitIdentifier(Identifier node, Scope context) {
        return Collections.singletonList(node);
      }

      @Override
      public List<Expression> visitIfExpression(IfExpression node, Scope context) {
        List<Expression> firstResult = process(node.getCondition(), context);
        List<Expression> secondResult = process(node.getTrueValue(), context);
        List<Expression> thirdResult =
            node.getFalseValue().isPresent() ? process(node.getFalseValue().get(), context) : null;
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int firstSize = firstResult.size();
        int secondSize = secondResult.size();
        int thirdSize = thirdResult == null ? 0 : thirdResult.size();
        int maxSize = Math.max(thirdSize, Math.max(firstSize, secondSize));

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger firstIndex = (firstSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger secondIndex = (secondSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger thirdIndex = (thirdSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new IfExpression(
                  firstResult.get(firstIndex.get()),
                  secondResult.get(secondIndex.get()),
                  thirdResult == null ? null : thirdResult.get(thirdIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitInListExpression(InListExpression node, Scope context) {
        ImmutableList.Builder<List<Expression>> childrenResultListBuilder =
            new ImmutableList.Builder<>();
        node.getValues()
            .forEach(operand -> childrenResultListBuilder.add(process(operand, context)));
        List<List<Expression>> childrenResultList = childrenResultListBuilder.build();
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int maxSize = childrenResultList.stream().mapToInt(List::size).max().orElse(0);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger[] childrenIndexes = new AtomicInteger[childrenResultList.size()];
        for (int i = 0; i < childrenIndexes.length; i++) {
          childrenIndexes[i] =
              (childrenResultList.get(i).size() == maxSize) ? baseIndex : new AtomicInteger(0);
        }
        for (int i = 0; i < maxSize; i++) {
          ImmutableList.Builder<Expression> operandListBuilder = new ImmutableList.Builder<>();
          for (int j = 0; j < childrenIndexes.length; j++) {
            int operandIndexInResult = childrenIndexes[j].get();
            operandListBuilder.add(childrenResultList.get(j).get(operandIndexInResult));
          }
          resultBuilder.add(new InListExpression(operandListBuilder.build()));
          baseIndex.getAndIncrement();
        }
        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitInPredicate(InPredicate node, Scope context) {
        List<Expression> leftResult = process(node.getValue(), context);
        List<Expression> rightResult = process(node.getValueList(), context);

        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int leftSize = leftResult.size();
        int rightSize = rightResult.size();
        int maxSize = Math.max(leftSize, rightSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger leftIndex = (leftSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger rightIndex = (rightSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new InPredicate(leftResult.get(leftIndex.get()), rightResult.get(rightIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitIsNotNullPredicate(IsNotNullPredicate node, Scope context) {
        List<Expression> childResult = process(node.getValue(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(new IsNotNullPredicate(expression));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitIsNullPredicate(IsNullPredicate node, Scope context) {
        List<Expression> childResult = process(node.getValue(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(new IsNullPredicate(expression));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitLikePredicate(LikePredicate node, Scope context) {
        List<Expression> firstResult = process(node.getValue(), context);
        List<Expression> secondResult = process(node.getPattern(), context);
        List<Expression> thirdResult =
            node.getEscape().isPresent() ? process(node.getEscape().get(), context) : null;
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int firstSize = firstResult.size();
        int secondSize = secondResult.size();
        int thirdSize = thirdResult == null ? 0 : thirdResult.size();
        int maxSize = Math.max(thirdSize, Math.max(firstSize, secondSize));

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger firstIndex = (firstSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger secondIndex = (secondSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger thirdIndex = (thirdSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new LikePredicate(
                  firstResult.get(firstIndex.get()),
                  secondResult.get(secondIndex.get()),
                  thirdResult == null ? null : thirdResult.get(thirdIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitLiteral(Literal node, Scope context) {
        return Collections.singletonList(node);
      }

      @Override
      public List<Expression> visitLogicalExpression(LogicalExpression node, Scope context) {
        ImmutableList.Builder<List<Expression>> childrenResultListBuilder =
            new ImmutableList.Builder<>();
        node.getTerms()
            .forEach(operand -> childrenResultListBuilder.add(process(operand, context)));
        List<List<Expression>> childrenResultList = childrenResultListBuilder.build();
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int maxSize = childrenResultList.stream().mapToInt(List::size).max().orElse(0);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger[] childrenIndexes = new AtomicInteger[childrenResultList.size()];
        for (int i = 0; i < childrenIndexes.length; i++) {
          childrenIndexes[i] =
              (childrenResultList.get(i).size() == maxSize) ? baseIndex : new AtomicInteger(0);
        }
        for (int i = 0; i < maxSize; i++) {
          ImmutableList.Builder<Expression> operandListBuilder = new ImmutableList.Builder<>();
          for (int j = 0; j < childrenIndexes.length; j++) {
            int operandIndexInResult = childrenIndexes[j].get();
            operandListBuilder.add(childrenResultList.get(j).get(operandIndexInResult));
          }
          resultBuilder.add(new LogicalExpression(node.getOperator(), operandListBuilder.build()));
          baseIndex.getAndIncrement();
        }
        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitNotExpression(NotExpression node, Scope context) {
        List<Expression> childResult = process(node.getValue(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(new NotExpression(expression));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitNullIfExpression(NullIfExpression node, Scope context) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.S_ARE_NOT_SUPPORTED_NOW, node.getClass().getSimpleName()));
      }

      @Override
      public List<Expression> visitQuantifiedComparisonExpression(
          QuantifiedComparisonExpression node, Scope context) {
        List<Expression> childResult = process(node.getValue(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(
              new QuantifiedComparisonExpression(
                  node.getOperator(), node.getQuantifier(), expression, node.getSubquery()));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitRow(Row node, Scope context) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.S_ARE_NOT_SUPPORTED_NOW, node.getClass().getSimpleName()));
      }

      @Override
      public List<Expression> visitExtract(Extract node, Scope context) {
        List<Expression> childResult = process(node.getExpression(), context);
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        for (Expression expression : childResult) {
          resultBuilder.add(new Extract(expression, node.getField()));
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitSearchedCaseExpression(
          SearchedCaseExpression node, Scope context) {
        ImmutableList.Builder<List<Expression>> firstChildResultListBuilder =
            new ImmutableList.Builder<>();
        node.getWhenClauses()
            .forEach(when -> firstChildResultListBuilder.add(process(when, context)));
        List<List<Expression>> firstChildResultList = firstChildResultListBuilder.build();
        List<Expression> secondResult =
            node.getDefaultValue().isPresent()
                ? process(node.getDefaultValue().get(), context)
                : null;

        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int secondSize = secondResult == null ? 0 : secondResult.size();
        int maxSize = firstChildResultList.stream().mapToInt(List::size).max().orElse(0);
        maxSize = Math.max(maxSize, secondSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger[] childrenIndexes = new AtomicInteger[firstChildResultList.size()];
        for (int i = 0; i < childrenIndexes.length; i++) {
          childrenIndexes[i] =
              (firstChildResultList.get(i).size() == maxSize) ? baseIndex : new AtomicInteger(0);
        }
        AtomicInteger secondIndex = (secondSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          ImmutableList.Builder<WhenClause> operandListBuilder = new ImmutableList.Builder<>();
          for (int j = 0; j < childrenIndexes.length; j++) {
            int operandIndexInResult = childrenIndexes[j].get();
            operandListBuilder.add(
                (WhenClause) firstChildResultList.get(j).get(operandIndexInResult));
          }

          resultBuilder.add(
              new SearchedCaseExpression(
                  operandListBuilder.build(),
                  (secondResult == null ? null : secondResult.get(secondIndex.get()))));
          baseIndex.getAndIncrement();
        }
        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitSimpleCaseExpression(SimpleCaseExpression node, Scope context) {
        List<Expression> firstResult = process(node.getOperand(), context);
        ImmutableList.Builder<List<Expression>> whenResultListBuilder =
            new ImmutableList.Builder<>();
        node.getWhenClauses().forEach(when -> whenResultListBuilder.add(process(when, context)));
        List<List<Expression>> whenResultList = whenResultListBuilder.build();
        List<Expression> secondResult =
            node.getDefaultValue().isPresent()
                ? process(node.getDefaultValue().get(), context)
                : null;

        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int firstSize = firstResult.size();
        int secondSize = secondResult == null ? 0 : secondResult.size();
        int maxSize = whenResultList.stream().mapToInt(List::size).max().orElse(0);
        maxSize = Math.max(Math.max(firstSize, maxSize), secondSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger[] childrenIndexes = new AtomicInteger[whenResultList.size()];
        AtomicInteger firstIndex = (firstSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < childrenIndexes.length; i++) {
          childrenIndexes[i] =
              (whenResultList.get(i).size() == maxSize) ? baseIndex : new AtomicInteger(0);
        }
        AtomicInteger secondIndex = (secondSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          ImmutableList.Builder<WhenClause> operandListBuilder = new ImmutableList.Builder<>();
          for (int j = 0; j < childrenIndexes.length; j++) {
            int operandIndexInResult = childrenIndexes[j].get();
            operandListBuilder.add((WhenClause) whenResultList.get(j).get(operandIndexInResult));
          }

          resultBuilder.add(
              new SimpleCaseExpression(
                  firstResult.get(firstIndex.get()),
                  operandListBuilder.build(),
                  (secondResult == null ? null : secondResult.get(secondIndex.get()))));
          baseIndex.getAndIncrement();
        }
        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitSubqueryExpression(SubqueryExpression node, Scope context) {
        // We don't need to process Query here
        return Collections.singletonList(node);
      }

      @Override
      public List<Expression> visitTrim(Trim node, Scope context) {
        List<Expression> firstResult = process(node.getTrimSource(), context);
        List<Expression> secondResult =
            node.getTrimCharacter().isPresent()
                ? process(node.getTrimCharacter().get(), context)
                : null;
        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int firstSize = firstResult.size();
        int secondSize = secondResult == null ? 0 : secondResult.size();
        int maxSize = Math.max(secondSize, firstSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger firstIndex = (firstSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger secondIndex = (secondSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new Trim(
                  node.getSpecification(),
                  firstResult.get(firstIndex.get()),
                  secondResult == null ? null : secondResult.get(secondIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }

      @Override
      public List<Expression> visitWhenClause(WhenClause node, Scope context) {
        List<Expression> leftResult = process(node.getOperand(), context);
        List<Expression> rightResult = process(node.getResult(), context);

        if (expandedExpressions == null) {
          // no Columns need to be expanded
          return Collections.singletonList(node);
        }

        ImmutableList.Builder<Expression> resultBuilder = new ImmutableList.Builder<>();
        int leftSize = leftResult.size();
        int rightSize = rightResult.size();
        int maxSize = Math.max(leftSize, rightSize);

        AtomicInteger baseIndex = new AtomicInteger(0);
        // if child is expanded, index of it reference the baseIndex, else the index of it always be
        // 0
        AtomicInteger leftIndex = (leftSize == maxSize) ? baseIndex : new AtomicInteger(0);
        AtomicInteger rightIndex = (rightSize == maxSize) ? baseIndex : new AtomicInteger(0);
        for (int i = 0; i < maxSize; i++) {
          resultBuilder.add(
              new WhenClause(leftResult.get(leftIndex.get()), rightResult.get(rightIndex.get())));
          baseIndex.getAndIncrement();
        }

        return resultBuilder.build();
      }
    }

    private int analyzeSelectAllColumns(
        AllColumns allColumns,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder) {
      // expand * and expression.*
      if (allColumns.getTarget().isPresent()) {
        // analyze AllColumns with target expression (expression.*)
        Expression expression = allColumns.getTarget().get();

        QualifiedName prefix = asQualifiedName(expression);
        if (prefix != null) {
          // analyze prefix as an 'asterisked identifier chain'
          Scope.AsteriskedIdentifierChainBasis identifierChainBasis =
              scope
                  .resolveAsteriskedIdentifierChainBasis(prefix, allColumns)
                  .orElseThrow(
                      () ->
                          new SemanticException(
                              String.format(
                                  DataNodeQueryMessages.UNABLE_TO_RESOLVE_REFERENCE_S, prefix)));
          if (identifierChainBasis.getBasisType() == TABLE) {
            RelationType relationType =
                identifierChainBasis
                    .getRelationType()
                    .orElseThrow(
                        () -> new NoSuchElementException(DataNodeQueryMessages.NO_VALUE_PRESENT));
            List<Field> requestedFields =
                relationType.resolveVisibleFieldsWithRelationPrefix(Optional.of(prefix));
            List<Field> fields = filterInaccessibleFields(requestedFields);
            if (fields.isEmpty()) {
              if (!requestedFields.isEmpty()) {
                throw new SemanticException(
                    DataNodeQueryMessages.RELATION_NOT_FOUND_OR_NOT_ALLOWED);
              }
              throw new SemanticException(
                  DataNodeQueryMessages.SELECT_NOT_ALLOWED_FROM_RELATION_THAT_HAS_NO);
            }
            boolean local =
                scope.isLocalScope(
                    identifierChainBasis
                        .getScope()
                        .orElseThrow(
                            () ->
                                new NoSuchElementException(
                                    DataNodeQueryMessages.NO_VALUE_PRESENT)));
            return analyzeAllColumnsFromTable(
                fields,
                allColumns,
                node,
                local ? scope : identifierChainBasis.getScope().get(),
                outputExpressionBuilder,
                selectExpressionBuilder,
                relationType,
                local);
          }
        }
        // identifierChainBasis.get().getBasisType == FIELD or target expression isn't a
        // QualifiedName
        throw new SemanticException(
            DataNodeQueryMessages
                .IDENTIFIERCHAINBASIS_GET_GETBASISTYPE_EQ_EQ_FIELD_OR_TARGET_EXPRESSION_ISN_T_A);
        //        analyzeAllFieldsFromRowTypeExpression(expression, allColumns, node, scope,
        // outputExpressionBuilder,
        //            selectExpressionBuilder);
      } else {
        // analyze AllColumns without target expression ('*')
        if (!allColumns.getAliases().isEmpty()) {
          throw new SemanticException(DataNodeQueryMessages.COLUMN_ALIASES_NOT_SUPPORTED);
        }

        List<Field> requestedFields = (List<Field>) scope.getRelationType().getVisibleFields();
        List<Field> fields = filterInaccessibleFields(requestedFields);
        if (fields.isEmpty()) {
          if (!node.getFrom().isPresent()) {
            throw new SemanticException(
                DataNodeQueryMessages.SELECT_NOT_ALLOWED_IN_QUERIES_WITHOUT_FROM_CLAUSE);
          }
          if (!requestedFields.isEmpty()) {
            throw new SemanticException(DataNodeQueryMessages.RELATION_NOT_FOUND_OR_NOT_ALLOWED);
          }
          throw new SemanticException(
              DataNodeQueryMessages.SELECT_NOT_ALLOWED_FROM_RELATION_THAT_HAS_NO);
        }

        return analyzeAllColumnsFromTable(
            fields,
            allColumns,
            node,
            scope,
            outputExpressionBuilder,
            selectExpressionBuilder,
            scope.getRelationType(),
            true);
      }
    }

    private List<Field> filterInaccessibleFields(List<Field> fields) {

      ImmutableSet.Builder<Field> accessibleFields = ImmutableSet.builder();

      // collect fields by table
      ListMultimap<QualifiedObjectName, Field> tableFieldsMap = ArrayListMultimap.create();
      fields.forEach(
          field -> {
            Optional<QualifiedObjectName> originTable = field.getOriginTable();
            if (originTable.isPresent()) {
              tableFieldsMap.put(originTable.get(), field);
            } else {
              // keep anonymous fields accessible
              accessibleFields.add(field);
            }
          });

      // TODO Auth control
      tableFieldsMap
          .asMap()
          .forEach(
              (table, tableFields) -> {
                //              Set<String> accessibleColumns = accessControl.filterColumns(
                //                      session.toSecurityContext(),
                //                      table.getCatalogName(),
                //                      ImmutableMap.of(
                //                          table.asSchemaTableName(),
                //                          tableFields.stream()
                //                              .map(field -> field.getOriginColumnName().get())
                //                              .collect(toImmutableSet())))
                //                  .getOrDefault(table.asSchemaTableName(), ImmutableSet.of());
                accessibleFields.addAll(
                    tableFields.stream()
                        // .filter(field ->
                        // accessibleColumns.contains(field.getOriginColumnName().get()))
                        .collect(toImmutableList()));
              });

      return fields.stream().filter(accessibleFields.build()::contains).collect(toImmutableList());
    }

    private int analyzeAllColumnsFromTable(
        List<Field> fields,
        AllColumns allColumns,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder,
        RelationType relationType,
        boolean local) {
      if (!allColumns.getAliases().isEmpty()) {
        validateColumnAliasesCount(allColumns.getAliases(), fields.size());
      }

      ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();

      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        Expression fieldExpression;
        if (local) {
          fieldExpression = new FieldReference(relationType.indexOf(field));
        } else {
          if (!field.getName().isPresent()) {
            throw new SemanticException(
                DataNodeQueryMessages
                    .SELECT_STAR_FROM_OUTER_SCOPE_TABLE_NOT_SUPPORTED_WITH_ANONYMOUS_COLUMNS);
          }
          checkState(
              field.getRelationAlias().isPresent(),
              QueryMessages.EXCEPTION_MISSING_RELATION_ALIAS_437EDF9C);
          fieldExpression =
              new DereferenceExpression(
                  DereferenceExpression.from(field.getRelationAlias().get()),
                  new Identifier(field.getName().get()));
        }
        analyzeExpression(fieldExpression, scope);
        outputExpressionBuilder.add(fieldExpression);
        selectExpressionBuilder.add(
            new Analysis.SelectExpression(fieldExpression, Optional.empty()));

        Optional<String> alias = field.getName();
        if (!allColumns.getAliases().isEmpty()) {
          alias = Optional.of(allColumns.getAliases().get(i).getValue());
        }

        Field newField =
            new Field(
                field.getRelationAlias(),
                alias,
                field.getType(),
                field.getColumnCategory(),
                false,
                field.getOriginTable(),
                field.getOriginColumnName(),
                !allColumns.getAliases().isEmpty() || field.isAliased());
        itemOutputFieldBuilder.add(newField);
        analysis.addSourceColumns(newField, analysis.getSourceColumns(field));

        Type type = field.getType();
        if (node.getSelect().isDistinct() && !type.isComparable()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S,
                  type));
        }
      }
      analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
      return fields.size();
    }

    //    private void analyzeAllFieldsFromRowTypeExpression(
    //        Expression expression,
    //        AllColumns allColumns,
    //        QuerySpecification node,
    //        Scope scope,
    //        ImmutableList.Builder<Expression> outputExpressionBuilder,
    //        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder) {
    //      ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();
    //
    //      ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
    //      Type type = expressionAnalysis.getType(expression);
    //      if (!(type instanceof RowType)) {
    //        throw semanticException(TYPE_MISMATCH, node.getSelect(), "expected expression of type
    // Row");
    //      }
    //      int referencedFieldsCount = ((RowType) type).getFields().size();
    //      if (!allColumns.getAliases().isEmpty()) {
    //        validateColumnAliasesCount(allColumns.getAliases(), referencedFieldsCount);
    //      }
    //      analysis.recordSubqueries(node, expressionAnalysis);
    //
    //      ImmutableList.Builder<Expression> unfoldedExpressionsBuilder = ImmutableList.builder();
    //      for (int i = 0; i < referencedFieldsCount; i++) {
    //        Expression outputExpression = new SubscriptExpression(expression, new LongLiteral("" +
    // (i + 1)));
    //        outputExpressionBuilder.add(outputExpression);
    //        analyzeExpression(outputExpression, scope);
    //        unfoldedExpressionsBuilder.add(outputExpression);
    //
    //        Type outputExpressionType = type.getTypeParameters().get(i);
    //        if (node.getSelect().isDistinct() && !outputExpressionType.isComparable()) {
    //          throw semanticException(TYPE_MISMATCH, node.getSelect(),
    //              "DISTINCT can only be applied to comparable types (actual: %s)",
    // type.getTypeParameters().get(i));
    //        }
    //
    //        Optional<String> name = ((RowType) type).getFields().get(i).getName();
    //        if (!allColumns.getAliases().isEmpty()) {
    //          name = Optional.of(allColumns.getAliases().get(i).getValue());
    //        }
    //        itemOutputFieldBuilder.add(Field.newUnqualified(name, outputExpressionType));
    //      }
    //      selectExpressionBuilder.add(new SelectExpression(expression,
    // Optional.of(unfoldedExpressionsBuilder.build())));
    //      analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
    //    }

    private void analyzeSelectSingleColumn(
        Expression expression,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder) {
      analyzeSelectSingleColumn(
          expression,
          node,
          scope,
          outputExpressionBuilder,
          selectExpressionBuilder,
          ImmutableMap.of());
    }

    private void analyzeSelectSingleColumn(
        Expression expression,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder,
        Map<NodeRef<Expression>, Expression> lateralColumnAliasReferences) {
      ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
      analysis.recordSubqueries(node, expressionAnalysis);
      outputExpressionBuilder.add(expression);
      selectExpressionBuilder.add(
          new Analysis.SelectExpression(
              expression, Optional.empty(), lateralColumnAliasReferences));

      Type type = expressionAnalysis.getType(expression);
      if (node.getSelect().isDistinct() && !type.isComparable()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S_S,
                type,
                expression));
      }
    }

    private Analysis.GroupingSetAnalysis analyzeGroupBy(
        QuerySpecification node,
        Scope scope,
        List<Expression> outputExpressions,
        SelectAliasLookup selectAliases) {
      if (node.getGroupBy().isPresent()) {
        ImmutableList.Builder<List<Set<FieldId>>> cubes = ImmutableList.builder();
        ImmutableList.Builder<List<Set<FieldId>>> rollups = ImmutableList.builder();
        ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
        ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
        ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();
        FunctionCall gapFillColumn = null;
        ImmutableList.Builder<Expression> gapFillGroupingExpressions = ImmutableList.builder();

        GroupBy groupBy = node.getGroupBy().get();
        List<Expression> allGroupByExpressions = ImmutableList.of();
        List<GroupingElement> groupingElements;

        if (groupBy.isAll()) {
          allGroupByExpressions = inferAllGroupByExpressions(outputExpressions);
          groupingElements = ImmutableList.of(new SimpleGroupBy(allGroupByExpressions));
        } else {
          checkGroupingSetsCount(groupBy);
          groupingElements = groupBy.getGroupingElements();
        }

        for (GroupingElement groupingElement : groupingElements) {
          if (groupingElement instanceof SimpleGroupBy) {
            for (Expression column : groupingElement.getExpressions()) {
              // simple GROUP BY expressions allow ordinals or arbitrary expressions
              if (!groupBy.isAll() && column instanceof LongLiteral) {
                long ordinal = ((LongLiteral) column).getParsedValue();
                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                  throw new SemanticException(
                      String.format(
                          DataNodeQueryMessages.GROUP_BY_POSITION_S_IS_NOT_IN_SELECT_LIST,
                          ordinal));
                }

                column = outputExpressions.get(toIntExact(ordinal - 1));
                verifyNoAggregateWindowOrGroupingFunctions(column, "GROUP BY clause");
              } else {
                column = resolveGroupBySelectAlias(column, scope, outputExpressions, selectAliases);
                verifyNoAggregateWindowOrGroupingFunctions(column, "GROUP BY clause");
                analyzeExpression(column, scope);
              }

              ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(column));
              if (field != null) {
                sets.add(ImmutableList.of(ImmutableSet.of(field.getFieldId())));
              } else {
                analysis.recordSubqueries(node, analyzeExpression(column, scope));
                complexExpressions.add(column);
              }

              if (isDateBinGapFill(column)) {
                if (gapFillColumn != null) {
                  throw new SemanticException(
                      DataNodeQueryMessages.MULTIPLE_DATE_BIN_GAPFILL_CALLS_NOT_ALLOWED);
                }
                gapFillColumn = (FunctionCall) column;
              } else {
                gapFillGroupingExpressions.add(column);
              }

              groupingExpressions.add(column);
            }
          } else if (groupingElement instanceof GroupingSets) {
            GroupingSets element = (GroupingSets) groupingElement;
            Map<NodeRef<Expression>, Expression> resolvedGroupingColumns = new HashMap<>();
            for (Expression column : groupingElement.getExpressions()) {
              Expression originalColumn = column;
              column = resolveGroupBySelectAlias(column, scope, outputExpressions, selectAliases);
              verifyNoAggregateWindowOrGroupingFunctions(column, "GROUP BY clause");
              analyzeExpression(column, scope);
              if (!analysis.getColumnReferences().contains(NodeRef.of(column))) {
                throw new SemanticException(
                    String.format(
                        DataNodeQueryMessages.GROUP_BY_EXPRESSION_MUST_BE_A_COLUMN_REFERENCE_S,
                        column));
              }

              resolvedGroupingColumns.put(NodeRef.of(originalColumn), column);
              groupingExpressions.add(column);
            }

            List<Set<FieldId>> groupingSets =
                element.getSets().stream()
                    .map(
                        set ->
                            set.stream()
                                .map(
                                    expression ->
                                        requireNonNull(
                                            resolvedGroupingColumns.get(NodeRef.of(expression)),
                                            "resolved grouping expression is null"))
                                .map(NodeRef::of)
                                .map(analysis.getColumnReferenceFields()::get)
                                .map(ResolvedField::getFieldId)
                                .collect(toImmutableSet()))
                    .collect(toImmutableList());

            switch (element.getType()) {
              case CUBE:
                cubes.add(groupingSets);
                break;
              case ROLLUP:
                rollups.add(groupingSets);
                break;
              case EXPLICIT:
                sets.add(groupingSets);
                break;
            }
          }
        }

        List<Expression> expressions = groupingExpressions.build();
        for (Expression expression : expressions) {
          Type type = analysis.getType(expression);
          if (!type.isComparable()) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages
                        .S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_GROUP_BY,
                    type));
          }
        }

        Analysis.GroupingSetAnalysis groupingSets =
            new Analysis.GroupingSetAnalysis(
                expressions,
                cubes.build(),
                rollups.build(),
                sets.build(),
                complexExpressions.build());
        analysis.setGroupingSets(node, groupingSets);
        if (gapFillColumn != null) {
          analysis.setGapFill(node, gapFillColumn);
          analysis.setGapFillGroupingKeys(node, gapFillGroupingExpressions.build());
        }
        return groupingSets;
      }

      Analysis.GroupingSetAnalysis result =
          new Analysis.GroupingSetAnalysis(
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of());

      if (hasAggregates(node) || node.getHaving().isPresent()) {
        analysis.setGroupingSets(node, result);
      }

      return result;
    }

    private Expression resolveGroupBySelectAlias(
        Expression expression,
        Scope scope,
        List<Expression> outputExpressions,
        SelectAliasLookup selectAliases) {
      if (!(expression instanceof Identifier)) {
        return expression;
      }

      Identifier identifier = (Identifier) expression;
      if (resolvesToInputColumn(scope, identifier)) {
        return expression;
      }

      return resolveSelectAlias(identifier, selectAliases)
          .map(SelectAlias::getRewrittenExpression)
          .orElse(expression);
    }

    private List<Expression> inferAllGroupByExpressions(List<Expression> outputExpressions) {
      ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();
      for (Expression expression : outputExpressions) {
        if (extractAggregateFunctions(ImmutableList.of(expression)).isEmpty()
            && extractWindowFunctions(ImmutableList.of(expression)).isEmpty()) {
          groupingExpressions.add(expression);
        }
      }
      return groupingExpressions.build();
    }

    private boolean isDateBinGapFill(Expression column) {
      return column instanceof FunctionCall
          && DATE_BIN
              .getFunctionName()
              .equalsIgnoreCase(((FunctionCall) column).getName().getSuffix())
          && ((FunctionCall) column).getArguments().size() == 5;
    }

    private boolean hasAggregates(QuerySpecification node) {
      List<Node> toExtract =
          ImmutableList.<Node>builder()
              .addAll(node.getSelect().getSelectItems())
              .addAll(getSortItemsFromOrderBy(node.getOrderBy()))
              .build();

      List<FunctionCall> aggregates = extractAggregateFunctions(toExtract);

      return !aggregates.isEmpty();
    }

    private void checkGroupingSetsCount(GroupBy node) {
      // If groupBy is distinct then crossProduct will be overestimated if there are duplicate
      // grouping sets.
      int crossProduct = 1;
      for (GroupingElement element : node.getGroupingElements()) {
        try {
          int product = 0;
          if (element instanceof SimpleGroupBy) {
            product = 1;
          } else if (element instanceof GroupingSets) {
            GroupingSets groupingSets = (GroupingSets) element;
            switch (groupingSets.getType()) {
              case CUBE:
                int exponent = ((GroupingSets) element).getSets().size();
                if (exponent > 30) {
                  throw new ArithmeticException();
                }
                product = 1 << exponent;
                break;
              case ROLLUP:
                product = groupingSets.getSets().size() + 1;
                break;
              case EXPLICIT:
                product = groupingSets.getSets().size();
                break;
            }
          } else {
            throw new UnsupportedOperationException(
                String.format(
                    DataNodeQueryMessages
                        .QUERY_EXCEPTION_UNSUPPORTED_GROUPING_ELEMENT_TYPE_S_B3C526E6,
                    element.getClass().getName()));
          }
          crossProduct = Math.multiplyExact(crossProduct, product);
        } catch (ArithmeticException e) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.GROUP_BY_HAS_MORE_THAN_S_GROUPING_SETS, Integer.MAX_VALUE));
        }
        //        if (crossProduct > getMaxGroupingSets(session)) {
        //          throw semanticException(TOO_MANY_GROUPING_SETS, node,
        //              "GROUP BY has %s grouping sets but can contain at most %s", crossProduct,
        // getMaxGroupingSets(session));
        //        }
      }
    }

    private void analyzeHaving(QuerySpecification node, Scope scope) {
      if (node.getHaving().isPresent()) {
        Expression predicate = node.getHaving().get();

        ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
        analysis.recordSubqueries(node, expressionAnalysis);

        Type predicateType = expressionAnalysis.getType(predicate);
        if (!predicateType.equals(BOOLEAN)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.HAVING_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S,
                  predicateType));
        }

        analysis.setHaving(node, predicate);
      }
    }

    private Scope computeAndAssignOutputScope(
        QuerySpecification node,
        Optional<Scope> scope,
        Scope sourceScope,
        SelectAnalysis selectAnalysis) {
      ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          AllColumns allColumns = (AllColumns) item;
          List<Field> fields = analysis.getSelectAllResultFields(allColumns);
          checkNotNull(
              fields,
              QueryMessages.EXCEPTION_OUTPUT_FIELDS_IS_NULL_FOR_SELECT_ITEM_ARG_56AFEC51,
              item);
          for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);

            Optional<String> name;
            if (!allColumns.getAliases().isEmpty()) {
              name = Optional.of(allColumns.getAliases().get(i).getCanonicalValue());
            } else {
              name = field.getName();
            }

            Field newField =
                Field.newUnqualified(
                    name,
                    field.getType(),
                    field.getColumnCategory(),
                    field.getOriginTable(),
                    field.getOriginColumnName(),
                    false);
            analysis.addSourceColumns(newField, analysis.getSourceColumns(field));
            outputFields.add(newField);
          }
        } else if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          Expression expression =
              selectAnalysis.getSingleColumnExpression(column).orElse(column.getExpression());
          Expression outputNameExpression = column.getExpression();

          // process Columns
          List<Expression> expandedExpressions = column.getExpandedExpressions();
          if (expandedExpressions != null) {
            for (int i = 0; i < expandedExpressions.size(); i++) {
              expression = expandedExpressions.get(i);

              // Different from process of normal SingleColumn, alias has been processed when
              // expanded Columns in analyzeSelect, so we needn't process alias here.
              Optional<String> field = Optional.empty();
              Optional<QualifiedObjectName> originTable = Optional.empty();
              // Put accordingColumnName into originColumn to rename expr in OutputNode if necessary
              Optional<String> originColumn = Optional.of(column.getAccordingColumnNames().get(i));
              QualifiedName name = null;

              if (expression instanceof Identifier) {
                name = QualifiedName.of(((Identifier) expression).getValue());
              }

              if (name != null) {
                Field matchingField = null;
                try {
                  matchingField = analysis.getResolvedField(expression).getField();
                } catch (IllegalArgumentException e) {
                  List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                  if (!matchingFields.isEmpty()) {
                    matchingField = matchingFields.get(0);
                  }
                }
                if (matchingField != null) {
                  originTable = matchingField.getOriginTable();
                }

                // expression is Identifier, the name of field is original column name
                field = originColumn;
              }

              boolean aliased = column.getAlias().isPresent();
              Field newField =
                  Field.newUnqualified(
                      aliased ? originColumn : field,
                      analysis.getType(expression),
                      TsTableColumnCategory.FIELD,
                      originTable,
                      originColumn,
                      aliased);
              // outputExpressions to look up the type
              if (originTable.isPresent()) {
                analysis.addSourceColumns(
                    newField,
                    ImmutableSet.of(
                        new Analysis.SourceColumn(
                            originTable.get(),
                            originColumn.orElseThrow(
                                () ->
                                    new NoSuchElementException(
                                        DataNodeQueryMessages.NO_VALUE_PRESENT)))));
              } else {
                analysis.addSourceColumns(
                    newField, analysis.getExpressionSourceColumns(expression));
              }
              outputFields.add(newField);
            }
            continue;
          }

          Optional<Identifier> field = column.getAlias();

          Optional<QualifiedObjectName> originTable = Optional.empty();
          Optional<String> originColumn = Optional.empty();
          QualifiedName name = null;
          QualifiedName outputName = null;

          if (expression instanceof Identifier) {
            name = QualifiedName.of(((Identifier) expression).getValue());
          } else if (expression instanceof DereferenceExpression) {
            name = getQualifiedName((DereferenceExpression) expression);
          }

          if (outputNameExpression instanceof Identifier) {
            outputName = QualifiedName.of(((Identifier) outputNameExpression).getValue());
          } else if (outputNameExpression instanceof DereferenceExpression) {
            outputName = getQualifiedName((DereferenceExpression) outputNameExpression);
          }

          if (name != null) {
            Field matchingField = null;
            try {
              matchingField = analysis.getResolvedField(expression).getField();
            } catch (IllegalArgumentException e) {
              List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
              if (!matchingFields.isEmpty()) {
                matchingField = matchingFields.get(0);
              }
            }
            if (matchingField != null) {
              originTable = matchingField.getOriginTable();
              originColumn = matchingField.getOriginColumnName();
            }
          }

          if (!field.isPresent() && (outputName != null)) {
            field = Optional.of(getLast(outputName.getOriginalParts()));
          }

          Field newField =
              Field.newUnqualified(
                  field.map(Identifier::getValue),
                  analysis.getType(expression),
                  TsTableColumnCategory.FIELD,
                  originTable,
                  originColumn,
                  column.getAlias().isPresent()); // TODO don't use analysis as a side-channel. Use
          // outputExpressions to look up the type
          if (originTable.isPresent()) {
            analysis.addSourceColumns(
                newField,
                ImmutableSet.of(
                    new Analysis.SourceColumn(
                        originTable.get(),
                        originColumn.orElseThrow(
                            () ->
                                new NoSuchElementException(
                                    DataNodeQueryMessages.NO_VALUE_PRESENT)))));
          } else {
            analysis.addSourceColumns(newField, analysis.getExpressionSourceColumns(expression));
          }
          outputFields.add(newField);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  DataNodeQueryMessages.QUERY_EXCEPTION_UNSUPPORTED_SELECTITEM_TYPE_S_4B0B155A,
                  item.getClass().getName()));
        }
      }

      return createAndAssignScope(node, scope, outputFields.build());
    }

    private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope) {
      // ORDER BY should "see" both output and FROM fields during initial analysis and
      // non-aggregation query planning
      Scope orderByScope =
          Scope.builder()
              .withParent(sourceScope)
              .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
              .build();
      analysis.setScope(node, orderByScope);
      return orderByScope;
    }

    private Scope computeAndAssignFillScope(Fill node, Scope sourceScope, Scope outputScope) {
      // Fill should "see" both output and FROM fields during initial analysis and
      // non-aggregation query planning
      Scope fillScope =
          Scope.builder()
              .withParent(sourceScope)
              .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
              .build();
      analysis.setScope(node, fillScope);
      return fillScope;
    }

    @Override
    public Scope visitSubqueryExpression(SubqueryExpression node, Optional<Scope> context) {
      return process(node.getQuery(), context);
    }

    @Override
    public Scope visitSetOperation(SetOperation node, Optional<Scope> scope) {
      checkState(node.getRelations().size() >= 2);

      List<RelationType> childrenTypes =
          node.getRelations().stream()
              .map(relation -> process(relation, scope).getRelationType().withOnlyVisibleFields())
              .collect(toImmutableList());

      String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
      Type[] outputFieldTypes =
          childrenTypes.get(0).getVisibleFields().stream().map(Field::getType).toArray(Type[]::new);
      for (RelationType relationType : childrenTypes) {
        int outputFieldSize = outputFieldTypes.length;
        int descFieldSize = relationType.getVisibleFields().size();
        if (outputFieldSize != descFieldSize) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.S_QUERY_HAS_DIFFERENT_NUMBER_OF_FIELDS_D_D,
                  setOperationName,
                  outputFieldSize,
                  descFieldSize));
        }
        for (int i = 0; i < descFieldSize; i++) {
          Type descFieldType = relationType.getFieldByIndex(i).getType();
          Optional<Type> commonSuperType =
              CompatibleResolver.getCommonSuperType(outputFieldTypes[i], descFieldType);
          if (!commonSuperType.isPresent()) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.COLUMN_D_IN_S_QUERY_HAS_INCOMPATIBLE_TYPES_S_S,
                    i + 1,
                    setOperationName,
                    outputFieldTypes[i].getDisplayName(),
                    descFieldType.getDisplayName()));
          }
          outputFieldTypes[i] = commonSuperType.get();
        }
      }

      if (node instanceof Intersect
          || node instanceof Except
          || node instanceof Union && node.isDistinct()) {
        for (Type type : outputFieldTypes) {
          if (!type.isComparable()) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages
                        .TYPE_S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_SS,
                    type,
                    setOperationName,
                    node instanceof Union
                        ? DataNodeQueryMessages.DISTINCT
                        : QueryMessages.EMPTY_MESSAGE));
          }
        }
      }

      Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
      RelationType firstDescriptor = childrenTypes.get(0);
      for (int i = 0; i < outputFieldTypes.length; i++) {
        Field oldField = firstDescriptor.getFieldByIndex(i);
        outputDescriptorFields[i] =
            new Field(
                oldField.getRelationAlias(),
                oldField.getName(),
                outputFieldTypes[i],
                oldField.getColumnCategory(),
                oldField.isHidden(),
                oldField.getOriginTable(),
                oldField.getOriginColumnName(),
                oldField.isAliased());

        int index = i; // Variable used in Lambda should be final
        analysis.addSourceColumns(
            outputDescriptorFields[index],
            childrenTypes.stream()
                .map(relationType -> relationType.getFieldByIndex(index))
                .flatMap(field -> analysis.getSourceColumns(field).stream())
                .collect(toImmutableSet()));
      }

      for (int i = 0; i < node.getRelations().size(); i++) {
        Relation relation = node.getRelations().get(i);
        RelationType relationType = childrenTypes.get(i);
        for (int j = 0; j < relationType.getVisibleFields().size(); j++) {
          Type outputFieldType = outputFieldTypes[j];
          Type descFieldType = relationType.getFieldByIndex(j).getType();
          if (!outputFieldType.equals(descFieldType)) {
            analysis.addRelationCoercion(relation, outputFieldTypes);
            break;
          }
        }
      }
      return createAndAssignScope(node, scope, outputDescriptorFields);
    }

    @Override
    public Scope visitTable(Table table, Optional<Scope> scope) {
      if (!table.getName().getPrefix().isPresent()) {
        scope.ifPresent(s -> s.addTable(table));
        // is this a reference to a WITH query?
        Optional<WithQuery> withQuery =
            createScope(scope).getNamedQuery(table.getName().getSuffix());
        if (withQuery.isPresent()) {
          analysis.setRelationName(table, table.getName());
          return createScopeForCommonTableExpression(table, scope, withQuery.get());
        }
        // is this a recursive reference in expandable WITH query? If so, there's base scope
        // recorded.
        Optional<Scope> expandableBaseScope = analysis.getExpandableBaseScope(table);
        if (expandableBaseScope.isPresent()) {
          Scope baseScope = expandableBaseScope.get();
          // adjust local and outer parent scopes accordingly to the local context of the recursive
          // reference
          Scope resultScope =
              scopeBuilder(scope)
                  .withRelationType(baseScope.getRelationId(), baseScope.getRelationType())
                  .build();
          analysis.setScope(table, resultScope);
          analysis.setRelationName(table, table.getName());
          return resultScope;
        }
      }
      QualifiedObjectName name = createQualifiedObjectName(sessionContext, table.getName());

      // access control
      accessControl.checkCanSelectFromTable(sessionContext.getUserName(), name, queryContext);

      analysis.setRelationName(
          table, QualifiedName.of(name.getDatabaseName(), name.getObjectName()));

      // check if table schema is found in CTE data stores
      CteDataStore dataStore = queryContext.getCteDataStore(table);
      Optional<TableSchema> tableSchema = Optional.empty();
      if (dataStore != null) {
        tableSchema = Optional.of(dataStore.getTableSchema());
        List<Integer> columnIndex2TsBlockColumnIndexList =
            dataStore.getColumnIndex2TsBlockColumnIndexList();
        if (columnIndex2TsBlockColumnIndexList != null
            && !columnIndex2TsBlockColumnIndexList.isEmpty()) {
          // Check if the list is completely sequential (0, 1, 2, ...)
          boolean isSequential = true;
          for (int i = 0; i < columnIndex2TsBlockColumnIndexList.size(); i++) {
            if (columnIndex2TsBlockColumnIndexList.get(i) != i) {
              isSequential = false;
              break;
            }
          }

          // Generate new TableSchema with reordered columns only if not sequential
          if (!isSequential) {
            tableSchema =
                reorderTableSchemaColumns(tableSchema.get(), columnIndex2TsBlockColumnIndexList);
          }
        }
      }
      // If table schema is not found, check if it is in metadata
      if (!tableSchema.isPresent()) {
        tableSchema = metadata.getTableSchema(sessionContext, name);
      }

      // This can only be a table
      if (!tableSchema.isPresent()) {
        CommonMetadataUtils.throwTableNotExistsException(
            name.getDatabaseName(), name.getObjectName());
      }
      analysis.addEmptyColumnReferencesForTable(accessControl, sessionContext.getIdentity(), name);

      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      fields.addAll(analyzeTableOutputFields(table, name, tableSchema.get()));

      List<Field> outputFields = fields.build();

      RelationType relationType = new RelationType(outputFields);
      analysis.registerTable(table, tableSchema, name);

      return createAndAssignScope(table, scope, relationType);
    }

    private Optional<TableSchema> reorderTableSchemaColumns(
        TableSchema tableSchema, List<Integer> columnIndex2TsBlockColumnIndexList) {
      List<ColumnSchema> columnSchemas = tableSchema.getColumns();
      final List<ColumnSchema> columnSchemaList =
          columnIndex2TsBlockColumnIndexList.stream()
              .map(columnSchemas::get)
              .collect(Collectors.toList());

      return Optional.of(new TableSchema(tableSchema.getTableName(), columnSchemaList));
    }

    private Scope createScopeForCommonTableExpression(
        Table table, Optional<Scope> scope, WithQuery withQuery) {
      Query query = withQuery.getQuery();
      query.setMaterialized(withQuery.isMaterialized());
      analysis.registerNamedQuery(table, query);

      // re-alias the fields with the name assigned to the query in the WITH declaration
      RelationType queryDescriptor = analysis.getOutputDescriptor(query);

      List<Field> fields;
      Optional<List<Identifier>> columnNames = withQuery.getColumnNames();
      if (columnNames.isPresent()) {
        // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
        checkState(
            columnNames.get().size() == queryDescriptor.getVisibleFieldCount(),
            QueryMessages.EXCEPTION_MISMATCHED_ALIASES_A6D3F8CE);
        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        Iterator<Identifier> aliases = columnNames.get().iterator();
        for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
          Field inputField = queryDescriptor.getFieldByIndex(i);
          if (!inputField.isHidden()) {
            Field field =
                Field.newQualified(
                    QualifiedName.of(table.getName().getSuffix()),
                    Optional.of(aliases.next().getValue()),
                    inputField.getType(),
                    inputField.getColumnCategory(),
                    false,
                    inputField.getOriginTable(),
                    inputField.getOriginColumnName(),
                    inputField.isAliased());
            fieldBuilder.add(field);
            analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
          }
        }
        fields = fieldBuilder.build();
      } else {
        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
          Field inputField = queryDescriptor.getFieldByIndex(i);
          if (!inputField.isHidden()) {
            Field field =
                Field.newQualified(
                    QualifiedName.of(table.getName().getSuffix()),
                    inputField.getName(),
                    inputField.getType(),
                    inputField.getColumnCategory(),
                    false,
                    inputField.getOriginTable(),
                    inputField.getOriginColumnName(),
                    inputField.isAliased());
            fieldBuilder.add(field);
            analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
          }
        }
        fields = fieldBuilder.build();
      }

      return createAndAssignScope(table, scope, fields);
    }

    private List<Field> analyzeTableOutputFields(
        final Table table, final QualifiedObjectName tableName, final TableSchema tableSchema) {
      // TODO: discover columns lazily based on where they are needed (to support connectors that
      // can't enumerate all tables)
      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      for (ColumnSchema column : tableSchema.getColumns()) {
        Field field =
            Field.newQualified(
                table.getName(),
                Optional.of(column.getName()),
                column.getType(),
                column.getColumnCategory(),
                column.isHidden(),
                Optional.of(tableName),
                Optional.of(column.getName()),
                false);
        fields.add(field);
        analysis.addSourceColumns(
            field, ImmutableSet.of(new Analysis.SourceColumn(tableName, column.getName())));
      }
      return fields.build();
    }

    //    private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, RelationType
    // relationType,
    //                                        Scope accessControlScope) {
    //      for (int index = 0; index < relationType.getAllFieldCount(); index++) {
    //        Field field = relationType.getFieldByIndex(index);
    //        if (field.getName().isPresent()) {
    //          Optional<ViewExpression> mask =
    //              accessControl.getColumnMask(session.toSecurityContext(), name,
    // field.getName().get(), field.getType());
    //
    //          if (mask.isPresent() && checkCanSelectFromColumn(name,
    // field.getName().orElseThrow())) {
    //            analyzeColumnMask(session.getIdentity().getUser(), table, name, field,
    // accessControlScope, mask.get());
    //          }
    //        }
    //      }
    //
    //      accessControl.getRowFilters(session.toSecurityContext(), name)
    //          .forEach(
    //              filter -> analyzeRowFilter(session.getIdentity().getUser(), table, name,
    // accessControlScope, filter));
    //    }

    public Scope visitPatternRecognitionRelation(
        PatternRecognitionRelation relation, Optional<Scope> scope) {
      Scope inputScope = process(relation.getInput(), scope);

      // check that input table column names are not ambiguous
      // Note: This check is not compliant with SQL identifier semantics. Quoted identifiers should
      // have different comparison rules than unquoted identifiers.
      // However, field names do not contain the information about quotation, and so every
      // comparison is case-insensitive. For example, if there are fields named
      // 'a' and 'A' (quoted), they should be considered non-ambiguous. However, their names will be
      // compared case-insensitive and will cause failure as ambiguous.
      Set<String> inputNames = new HashSet<>();
      for (Field field : inputScope.getRelationType().getAllFields()) {
        field
            .getName()
            .ifPresent(
                name -> {
                  if (!inputNames.add(name.toUpperCase(ENGLISH))) {
                    throw new SemanticException(
                        String.format(
                            DataNodeQueryMessages.AMBIGUOUS_COLUMN_S_IN_ROW_PATTERN_INPUT_RELATION,
                            name));
                  }
                });
      }

      // analyze PARTITION BY
      for (Expression expression : relation.getPartitionBy()) {
        // The PARTITION BY clause is a list of columns of the row pattern input table.
        validateAndGetInputField(expression, inputScope);
        Type type = analyzeExpression(expression, inputScope).getType(expression);
        if (!type.isComparable()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_PARTITION_BY,
                  type));
        }
      }

      // analyze ORDER BY
      for (SortItem sortItem : getSortItemsFromOrderBy(relation.getOrderBy())) {
        // The ORDER BY clause is a list of columns of the row pattern input table.
        Expression expression = sortItem.getSortKey();
        validateAndGetInputField(expression, inputScope);
        Type type = analyzeExpression(expression, inputScope).getType(sortItem.getSortKey());
        if (!type.isOrderable()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY,
                  type));
        }
      }

      // analyze pattern recognition clauses
      PatternRecognitionAnalysis patternRecognitionAnalysis =
          PatternRecognitionAnalyzer.analyze(
              relation.getSubsets(),
              relation.getVariableDefinitions(),
              relation.getMeasures(),
              relation.getPattern(),
              relation.getAfterMatchSkipTo());

      relation
          .getAfterMatchSkipTo()
          .flatMap(SkipTo::getIdentifier)
          .ifPresent(label -> analysis.addResolvedLabel(label, label.getCanonicalValue()));

      for (SubsetDefinition subset : relation.getSubsets()) {
        analysis.addResolvedLabel(subset.getName(), subset.getName().getCanonicalValue());
        analysis.addSubsetLabels(
            subset,
            subset.getIdentifiers().stream()
                .map(Identifier::getCanonicalValue)
                .collect(Collectors.toSet()));
      }

      analysis.setUndefinedLabels(
          relation.getPattern(), patternRecognitionAnalysis.getUndefinedLabels());
      analysis.setRanges(patternRecognitionAnalysis.getRanges());

      PatternRecognitionAnalyzer.validatePatternExclusions(
          relation.getRowsPerMatch(), relation.getPattern());

      // Notes on potential name ambiguity between pattern labels and other identifiers:
      // Labels are allowed in expressions of MEASURES and DEFINE clauses. In those expressions,
      // qualifying column names with table name is not allowed.
      // Theoretically, user might define pattern label "T" where input table name was "T". Then a
      // dereference "T.column" would refer to:
      // - input table's column, if it was in PARTITION BY or ORDER BY clause,
      // - subset of rows matched with label "T", if it was in MEASURES or DEFINE clause.
      // There could be a check to catch such non-intuitive situation and produce a warning.
      // Similarly, it is possible to define pattern label with the same name as some input column.
      // However, this causes no ambiguity, as labels can only
      // appear as column name's prefix, and column names in pattern recognition context cannot be
      // dereferenced.

      // analyze expressions in MEASURES and DEFINE (with set of all labels passed as context)
      for (VariableDefinition variableDefinition : relation.getVariableDefinitions()) {
        Expression expression = variableDefinition.getExpression();
        ExpressionAnalysis expressionAnalysis =
            analyzePatternRecognitionExpression(
                expression, inputScope, patternRecognitionAnalysis.getAllLabels());
        analysis.recordSubqueries(relation, expressionAnalysis);
        analysis.addResolvedLabel(
            variableDefinition.getName(), variableDefinition.getName().getCanonicalValue());
        Type type = expressionAnalysis.getType(expression);
        if (!type.equals(BOOLEAN)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.EXPRESSION_DEFINING_A_LABEL_MUST_BE_BOOLEAN_ACTUAL_TYPE_S,
                  type));
        }
      }
      ImmutableMap.Builder<NodeRef<Node>, Type> measureTypesBuilder = ImmutableMap.builder();
      for (MeasureDefinition measureDefinition : relation.getMeasures()) {
        Expression expression = measureDefinition.getExpression();
        ExpressionAnalysis expressionAnalysis =
            analyzePatternRecognitionExpression(
                expression, inputScope, patternRecognitionAnalysis.getAllLabels());
        analysis.recordSubqueries(relation, expressionAnalysis);
        analysis.addResolvedLabel(
            measureDefinition.getName(), measureDefinition.getName().getCanonicalValue());
        measureTypesBuilder.put(NodeRef.of(expression), expressionAnalysis.getType(expression));
      }
      Map<NodeRef<Node>, Type> measureTypes = measureTypesBuilder.buildOrThrow();

      // create output scope
      // ONE ROW PER MATCH: PARTITION BY columns, then MEASURES columns in order of declaration
      // ALL ROWS PER MATCH: PARTITION BY columns, ORDER BY columns, MEASURES columns, then any
      // remaining input table columns in order of declaration
      // Note: row pattern input table name should not be exposed on output
      PatternRecognitionRelation.RowsPerMatch rowsPerMatch = relation.getRowsPerMatch().orElse(ONE);
      boolean oneRowPerMatch = rowsPerMatch == ONE;

      ImmutableSet.Builder<Field> inputFieldsOnOutputBuilder = ImmutableSet.builder();
      ImmutableList.Builder<Field> outputFieldsBuilder = ImmutableList.builder();

      for (Expression expression : relation.getPartitionBy()) {
        Field inputField = validateAndGetInputField(expression, inputScope);
        outputFieldsBuilder.add(unqualifiedVisible(inputField));
        inputFieldsOnOutputBuilder.add(inputField);
      }

      if (!oneRowPerMatch) {
        for (SortItem sortItem : getSortItemsFromOrderBy(relation.getOrderBy())) {
          Field inputField = validateAndGetInputField(sortItem.getSortKey(), inputScope);
          outputFieldsBuilder.add(unqualifiedVisible(inputField));
          inputFieldsOnOutputBuilder.add(
              inputField); // might have duplicates (ORDER BY a ASC, a DESC)
        }
      }

      for (MeasureDefinition measureDefinition : relation.getMeasures()) {
        outputFieldsBuilder.add(
            Field.newUnqualified(
                measureDefinition.getName().getValue(),
                measureTypes.get(NodeRef.of(measureDefinition.getExpression())),
                TsTableColumnCategory.FIELD));
      }

      if (!oneRowPerMatch) {
        Set<Field> inputFieldsOnOutput = inputFieldsOnOutputBuilder.build();
        for (Field inputField : inputScope.getRelationType().getAllFields()) {
          if (!inputFieldsOnOutput.contains(inputField)) {
            outputFieldsBuilder.add(unqualified(inputField));
          }
        }
      }

      // pattern recognition output must have at least 1 column
      List<Field> outputFields = outputFieldsBuilder.build();
      if (outputFields.isEmpty()) {
        throw new SemanticException(
            DataNodeQueryMessages.PATTERN_RECOGNITION_OUTPUT_TABLE_HAS_NO_COLUMNS);
      }

      return createAndAssignScope(relation, scope, outputFields);
    }

    private Field unqualifiedVisible(Field field) {
      return new Field(
          Optional.empty(),
          field.getName(),
          field.getType(),
          field.getColumnCategory(),
          false,
          field.getOriginTable(),
          field.getOriginColumnName(),
          field.isAliased());
    }

    private Field unqualified(Field field) {
      return new Field(
          Optional.empty(),
          field.getName(),
          field.getType(),
          field.getColumnCategory(),
          field.isHidden(),
          field.getOriginTable(),
          field.getOriginColumnName(),
          field.isAliased());
    }

    private ExpressionAnalysis analyzePatternRecognitionExpression(
        Expression expression, Scope scope, Set<String> labels) {

      return ExpressionAnalyzer.analyzePatternRecognitionExpression(
          metadata,
          queryContext,
          sessionContext,
          statementAnalyzerFactory,
          accessControl,
          scope,
          analysis,
          expression,
          warningCollector,
          labels);
    }

    @Override
    public Scope visitValues(Values node, Optional<Scope> scope) {
      checkState(!node.getRows().isEmpty());

      List<Type> rowTypes =
          node.getRows().stream()
              .map(row -> analyzeExpression(row, createScope(scope)).getType(row))
              .map(
                  type -> {
                    if (type instanceof RowType) {
                      return type;
                    }
                    return RowType.anonymousRow(type);
                  })
              .collect(toImmutableList());

      int fieldCount = rowTypes.get(0).getTypeParameters().size();
      Type commonSuperType = rowTypes.get(0);
      for (Type rowType : rowTypes) {
        // check field count consistency for rows
        if (rowType.getTypeParameters().size() != fieldCount) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.VALUES_ROWS_HAVE_MISMATCHED_SIZES_S_VS_S,
                  fieldCount,
                  rowType.getTypeParameters().size()));
        }

        // determine common super type of the rows
        //        commonSuperType = typeCoercion.getCommonSuperType(rowType, commonSuperType)
        //            .orElseThrow(() -> semanticException(TYPE_MISMATCH,
        //                node,
        //                "Values rows have mismatched types: %s vs %s",
        //                rowTypes.get(0),
        //                rowType));
      }

      // add coercions
      int rowIndex = 0;
      for (Expression row : node.getRows()) {
        Type actualType = analysis.getType(row);
        if (row instanceof Row) {
          // coerce Row by fields to preserve Row structure and enable optimizations based on this
          // structure, e.g.pruning, predicate extraction
          // TODO coerce the whole Row and add an Optimizer rule that converts CAST(ROW(...) AS
          // ...)into ROW (CAST(...),CAST(...), ...).
          //  The rule would also handle Row-type expressions that were specified as CAST(ROW).It
          // should support multiple casts over a ROW.
          for (int i = 0; i < actualType.getTypeParameters().size(); i++) {
            //            Expression item = ((Row) row).getItems().get(i);
            Type actualItemType = actualType.getTypeParameters().get(i);
            Type expectedItemType = commonSuperType.getTypeParameters().get(i);
            if (!actualItemType.equals(expectedItemType)) {
              throw new SemanticException(
                  String.format(
                      DataNodeQueryMessages
                          .TYPE_OF_ROW_D_COLUMN_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S,
                      rowIndex,
                      i,
                      expectedItemType,
                      actualItemType));
              //              analysis.addCoercion(item, expectedItemType,
              //                  typeCoercion.isTypeOnlyCoercion(actualItemType,
              // expectedItemType));
            }
          }
        } else if (actualType instanceof RowType) {
          // coerce row-type expression as a whole
          //          if (!actualType.equals(commonSuperType)) {
          //            analysis.addCoercion(row, commonSuperType,
          //                typeCoercion.isTypeOnlyCoercion(actualType, commonSuperType));
          //          }

          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.TYPE_OF_ROW_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S,
                  rowIndex,
                  commonSuperType,
                  actualType));
        } else {
          // coerce field. it will be wrapped in Row by Planner
          Type superType = getOnlyElement(commonSuperType.getTypeParameters());
          if (!actualType.equals(superType)) {
            //            analysis.addCoercion(row, superType,
            // typeCoercion.isTypeOnlyCoercion(actualType,
            //                superType));
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.TYPE_OF_ROW_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S,
                    rowIndex,
                    superType,
                    actualType));
          }
        }
        rowIndex++;
      }

      List<Field> fields =
          commonSuperType.getTypeParameters().stream()
              .map(
                  valueType ->
                      Field.newUnqualified(
                          Optional.empty(), valueType, TsTableColumnCategory.FIELD))
              .collect(toImmutableList());

      return createAndAssignScope(node, scope, fields);
    }

    @Override
    public Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope) {
      analysis.setRelationName(relation, QualifiedName.of(ImmutableList.of(relation.getAlias())));
      analysis.addAliased(relation.getRelation());
      Scope relationScope = process(relation.getRelation(), scope);
      RelationType relationType = relationScope.getRelationType();

      // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the
      // node object
      if (relation.getColumnNames() != null) {
        int totalColumns = relationType.getVisibleFieldCount();
        if (totalColumns != relation.getColumnNames().size()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_S_HAS_S_COLUMNS_AVAILABLE,
                  relation.getColumnNames().size(),
                  relation.getAlias(),
                  totalColumns));
        }
      }

      List<String> aliases = null;
      Collection<Field> inputFields = relationType.getAllFields();
      if (relation.getColumnNames() != null) {
        aliases =
            relation.getColumnNames().stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());
        // hidden fields are not exposed when there are column aliases
        inputFields = relationType.getVisibleFields();
      }

      RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

      checkArgument(
          inputFields.size() == descriptor.getAllFieldCount(),
          QueryMessages.EXCEPTION_EXPECTED_ARG_FIELDS_COMMA_GOT_ARG_498063F2,
          descriptor.getAllFieldCount(),
          inputFields.size());

      Streams.forEachPair(
          descriptor.getAllFields().stream(),
          inputFields.stream(),
          (newField, field) ->
              analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));

      return createAndAssignScope(relation, scope, descriptor);
    }

    @Override
    public Scope visitJoin(Join node, Optional<Scope> scope) {
      JoinCriteria criteria = node.getCriteria().orElse(null);

      joinConditionCheck(criteria);

      // Remember original tables before processing left
      List<Identifier> originalTables = new ArrayList<>();
      scope.ifPresent(s -> originalTables.addAll(s.getTables()));

      Scope left = process(node.getLeft(), scope);
      // Restore tables for right processing
      scope.ifPresent(s -> s.setTables(originalTables));
      Scope right = process(node.getRight(), scope);

      // Add back tables added during left processing to preserve them in the scope
      if (left != null) {
        List<Identifier> leftAddedTables =
            left.getTables().stream()
                .filter(table -> !originalTables.contains(table))
                .collect(Collectors.toList());
        scope.ifPresent(s -> s.getTables().addAll(leftAddedTables));
      }

      if (criteria instanceof JoinUsing) {
        return analyzeJoinUsing(node, ((JoinUsing) criteria).getColumns(), scope, left, right);
      }

      Scope output =
          createAndAssignScope(
              node, scope, left.getRelationType().joinWith(right.getRelationType()));

      if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
        return output;
      }
      if (criteria instanceof JoinOn) {
        boolean isAsofJoin = criteria instanceof AsofJoinOn;

        Expression expression = ((JoinOn) criteria).getExpression();
        if (expression != null) {
          verifyNoAggregateWindowOrGroupingFunctions(expression, "JOIN clause");

          // Need to register coercions in case when join criteria requires coercion (e.g. join on
          // char(1) = char(2))
          // Correlations are only currently support in the join criteria for INNER joins
          ExpressionAnalysis expressionAnalysis =
              analyzeExpression(
                  expression,
                  output,
                  node.getType() == INNER && !isAsofJoin
                      ? CorrelationSupport.ALLOWED
                      : CorrelationSupport.DISALLOWED);
          Type clauseType = expressionAnalysis.getType(expression);
          if (!clauseType.equals(BOOLEAN)) {
            //          if (!clauseType.equals(UNKNOWN)) {
            //            throw semanticException(
            //                TYPE_MISMATCH,
            //                expression,
            //                "JOIN ON clause must evaluate to a boolean: actual type %s",
            //                clauseType);
            //          }
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.JOIN_ON_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S,
                    clauseType));
            // coerce expression to boolean
            //          analysis.addCoercion(expression, BOOLEAN, false);
          }

          if (!isAsofJoin) {
            analysis.recordSubqueries(node, expressionAnalysis);
          }
        }

        if (isAsofJoin) {
          // The asofExpression must be ComparisonExpression, it has been checked in AstBuilder
          ComparisonExpression asofExpression =
              (ComparisonExpression) ((AsofJoinOn) criteria).getAsofExpression();

          verifyNoAggregateWindowOrGroupingFunctions(asofExpression, "JOIN clause");

          // ASOF Join does not support Correlation
          ExpressionAnalysis expressionAnalysis =
              analyzeExpression(asofExpression, output, CorrelationSupport.DISALLOWED);
          Type clauseType = expressionAnalysis.getType(asofExpression);
          if (!clauseType.equals(BOOLEAN)) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages
                        .ASOF_MAIN_JOIN_EXPRESSION_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S,
                    clauseType));
          }

          clauseType = expressionAnalysis.getType(asofExpression.getLeft());
          if (!clauseType.equals(TimestampType.TIMESTAMP)) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages
                        .LEFT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S,
                    clauseType));
          }

          clauseType = expressionAnalysis.getType(asofExpression.getRight());
          if (!clauseType.equals(TimestampType.TIMESTAMP)) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages
                        .RIGHT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S,
                    clauseType));
          }
        }
        analysis.setJoinCriteria(node, expression);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                DataNodeQueryMessages.QUERY_EXCEPTION_UNSUPPORTED_JOIN_CRITERIA_S_311289C1,
                criteria.getClass().getName()));
      }

      return output;
    }

    private void joinConditionCheck(JoinCriteria criteria) {
      if (criteria instanceof NaturalJoin) {
        throw new SemanticException(DataNodeQueryMessages.NATURAL_JOIN_NOT_SUPPORTED);
      }
    }

    private Scope analyzeJoinUsing(
        Join node, List<Identifier> columns, Optional<Scope> scope, Scope left, Scope right) {
      List<Field> joinFields = new ArrayList<>();

      List<Integer> leftJoinFields = new ArrayList<>();
      List<Integer> rightJoinFields = new ArrayList<>();

      Set<Identifier> seen = new HashSet<>();
      for (Identifier column : columns) {
        if (!seen.add(column)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.COLUMN_S_APPEARS_MULTIPLE_TIMES_IN_USING_CLAUSE,
                  column.getValue()));
        }

        ResolvedField leftField =
            left.tryResolveField(column)
                .orElseThrow(
                    () ->
                        new SemanticException(
                            String.format(
                                DataNodeQueryMessages.COLUMN_S_IS_MISSING_FROM_LEFT_SIDE_OF_JOIN,
                                column.getValue())));
        ResolvedField rightField =
            right
                .tryResolveField(column)
                .orElseThrow(
                    () ->
                        new SemanticException(
                            String.format(
                                DataNodeQueryMessages.COLUMN_S_IS_MISSING_FROM_RIGHT_SIDE_OF_JOIN,
                                column.getValue())));

        // ensure a comparison operator exists for the given types (applying coercions if necessary)
        //        try {
        //          metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(
        //              leftField.getType(), rightField.getType()));
        //        } catch (OperatorNotFoundException e) {
        //          throw semanticException(TYPE_MISMATCH, column, e, "%s", e.getMessage());
        //        }
        if (leftField.getType() != rightField.getType()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .COLUMN_TYPES_OF_LEFT_AND_RIGHT_SIDE_ARE_DIFFERENT_LEFT_IS_S_RIGHT_IS_S,
                  leftField.getType(),
                  rightField.getType()));
        }

        analysis.addTypes(ImmutableMap.of(NodeRef.of(column), leftField.getType()));

        joinFields.add(
            Field.newUnqualified(
                column.getValue(), leftField.getType(), leftField.getColumnCategory()));

        leftJoinFields.add(leftField.getRelationFieldIndex());
        rightJoinFields.add(rightField.getRelationFieldIndex());

        recordColumnAccess(leftField.getField());
        recordColumnAccess(rightField.getField());
      }

      ImmutableList.Builder<Field> outputs = ImmutableList.builder();
      outputs.addAll(joinFields);

      ImmutableList.Builder<Integer> leftFields = ImmutableList.builder();
      for (int i = 0; i < left.getRelationType().getAllFieldCount(); i++) {
        if (!leftJoinFields.contains(i)) {
          outputs.add(left.getRelationType().getFieldByIndex(i));
          leftFields.add(i);
        }
      }

      ImmutableList.Builder<Integer> rightFields = ImmutableList.builder();
      for (int i = 0; i < right.getRelationType().getAllFieldCount(); i++) {
        if (!rightJoinFields.contains(i)) {
          outputs.add(right.getRelationType().getFieldByIndex(i));
          rightFields.add(i);
        }
      }

      analysis.setJoinUsing(
          node,
          new Analysis.JoinUsingAnalysis(
              leftJoinFields, rightJoinFields, leftFields.build(), rightFields.build()));

      return createAndAssignScope(node, scope, new RelationType(outputs.build()));
    }

    private void recordColumnAccess(Field field) {
      if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
        analysis.addTableColumnReferences(
            accessControl,
            sessionContext.getIdentity(),
            ImmutableMultimap.of(field.getOriginTable().get(), field.getOriginColumnName().get()));
      }
    }

    private void analyzeFill(Fill node, Scope scope) {
      Analysis.FillAnalysis fillAnalysis;
      if (node.getFillMethod() == FillPolicy.PREVIOUS) {
        FieldReference timeColumn = null;
        List<FieldReference> groupingKeys = null;
        if (node.getTimeBound().isPresent() || node.getFillGroupingElements().isPresent()) {
          timeColumn = getHelperColumn(node, scope, FillPolicy.PREVIOUS);
          ExpressionAnalyzer.analyzeExpression(
              metadata,
              queryContext,
              sessionContext,
              statementAnalyzerFactory,
              accessControl,
              scope,
              analysis,
              timeColumn,
              WarningCollector.NOOP,
              correlationSupport);

          groupingKeys = analyzeFillGroup(node, scope, FillPolicy.PREVIOUS);
        }
        fillAnalysis =
            new Analysis.PreviousFillAnalysis(
                node.getTimeBound().orElse(null), timeColumn, groupingKeys);
      } else if (node.getFillMethod() == FillPolicy.NEXT) {
        FieldReference timeColumn = null;
        List<FieldReference> groupingKeys = null;
        if (node.getTimeBound().isPresent() || node.getFillGroupingElements().isPresent()) {
          timeColumn = getHelperColumn(node, scope, FillPolicy.NEXT);
          ExpressionAnalyzer.analyzeExpression(
              metadata,
              queryContext,
              sessionContext,
              statementAnalyzerFactory,
              accessControl,
              scope,
              analysis,
              timeColumn,
              WarningCollector.NOOP,
              correlationSupport);

          groupingKeys = analyzeFillGroup(node, scope, FillPolicy.NEXT);
        }
        fillAnalysis =
            new Analysis.NextFillAnalysis(
                node.getTimeBound().orElse(null), timeColumn, groupingKeys);
      } else if (node.getFillMethod() == FillPolicy.CONSTANT) {
        Literal literal = node.getFillValue().get();
        ExpressionAnalyzer.analyzeExpression(
            metadata,
            queryContext,
            sessionContext,
            statementAnalyzerFactory,
            accessControl,
            scope,
            analysis,
            literal,
            WarningCollector.NOOP,
            correlationSupport);
        fillAnalysis = new Analysis.ValueFillAnalysis(literal);
      } else if (node.getFillMethod() == FillPolicy.LINEAR) {
        FieldReference helperColumn = getHelperColumn(node, scope, FillPolicy.LINEAR);
        ExpressionAnalyzer.analyzeExpression(
            metadata,
            queryContext,
            sessionContext,
            statementAnalyzerFactory,
            accessControl,
            scope,
            analysis,
            helperColumn,
            WarningCollector.NOOP,
            correlationSupport);
        List<FieldReference> groupingKeys = analyzeFillGroup(node, scope, FillPolicy.LINEAR);
        fillAnalysis = new Analysis.LinearFillAnalysis(helperColumn, groupingKeys);
      } else {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.UNKNOWN_FILL_METHOD + node.getFillMethod());
      }

      analysis.setFill(node, fillAnalysis);
    }

    private FieldReference getHelperColumn(Fill node, Scope scope, FillPolicy fillMethod) {
      FieldReference helperColumn;
      if (node.getTimeColumnIndex().isPresent()) {
        helperColumn =
            getFieldReferenceForTimeColumn(node.getTimeColumnIndex().get(), scope, fillMethod);
      } else {
        // if user doesn't specify the index of helper column, we use first column whose data type
        // is TIMESTAMP instead.
        int index = -1;
        for (Field field : scope.getRelationType().getVisibleFields()) {
          if (isTimestampType(field.getType())) {
            index = scope.getRelationType().indexOf(field);
            break;
          }
        }
        if (index == -1) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .CANNOT_INFER_TIME_COLUMN_FOR_S_FILL_THERE_EXISTS_NO_COLUMN_WHOSE_TYPE_IS_TIMESTAMP,
                  fillMethod.name()));
        }
        helperColumn = new FieldReference(index);
      }
      return helperColumn;
    }

    private List<FieldReference> analyzeFillGroup(Fill node, Scope scope, FillPolicy fillMethod) {
      if (node.getFillGroupingElements().isPresent()) {
        ImmutableList.Builder<FieldReference> groupingFieldsBuilder = ImmutableList.builder();
        for (LongLiteral index : node.getFillGroupingElements().get()) {
          FieldReference element = getFieldReferenceForFillGroup(index, scope, fillMethod);
          groupingFieldsBuilder.add(element);
          ExpressionAnalyzer.analyzeExpression(
              metadata,
              queryContext,
              sessionContext,
              statementAnalyzerFactory,
              accessControl,
              scope,
              analysis,
              element,
              WarningCollector.NOOP,
              correlationSupport);
        }
        return groupingFieldsBuilder.build();
      } else {
        return null;
      }
    }

    private FieldReference getFieldReferenceForTimeColumn(
        LongLiteral index, Scope scope, FillPolicy fillMethod) {
      long ordinal = index.getParsedValue();
      if (ordinal < 1 || ordinal > scope.getRelationType().getVisibleFieldCount()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.S_FILL_TIME_COLUMN_POSITION_S_IS_NOT_IN_SELECT_LIST,
                fillMethod.name(),
                ordinal));
      } else if (!isTimestampType(
          scope.getRelationType().getFieldByIndex((int) ordinal - 1).getType())) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .TYPE_OF_TIME_COLUMN_FOR_S_FILL_SHOULD_ONLY_BE_TIMESTAMP_BUT_TYPE_OF_THE_COLUMN_YOU,
                fillMethod.name(),
                scope.getRelationType().getFieldByIndex((int) ordinal - 1).getType()));
      } else {
        return new FieldReference(toIntExact(ordinal - 1));
      }
    }

    private FieldReference getFieldReferenceForFillGroup(
        LongLiteral index, Scope scope, FillPolicy fillMethod) {
      long ordinal = index.getParsedValue();
      if (ordinal < 1 || ordinal > scope.getRelationType().getVisibleFieldCount()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.S_FILL_FILL_GROUP_POSITION_S_IS_NOT_IN_SELECT_LIST,
                fillMethod.name(),
                ordinal));
      } else if (!scope
          .getRelationType()
          .getFieldByIndex((int) ordinal - 1)
          .getType()
          .isOrderable()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_FILL_GROUP_S,
                scope.getRelationType().getFieldByIndex((int) ordinal - 1).getType(),
                index));
      } else {
        return new FieldReference(toIntExact(ordinal - 1));
      }
    }

    private List<Expression> analyzeOrderBy(
        Node node, List<SortItem> sortItems, Scope orderByScope, SelectAliasLookup selectAliases) {
      ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

      for (SortItem item : sortItems) {
        Expression expression = item.getSortKey();

        if (expression instanceof LongLiteral) {
          // this is an ordinal in the output tuple

          long ordinal = ((LongLiteral) expression).getParsedValue();
          if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.ORDER_BY_POSITION_S_IS_NOT_IN_SELECT_LIST, ordinal));
          }

          expression = new FieldReference(toIntExact(ordinal - 1));
        } else {
          Optional<SelectAlias> selectAlias = resolveOrderBySelectAlias(expression, selectAliases);
          if (selectAlias.isPresent()) {
            expression = new FieldReference(selectAlias.get().getPosition() - 1);
          }
        }

        ExpressionAnalysis expressionAnalysis =
            ExpressionAnalyzer.analyzeExpression(
                metadata,
                queryContext,
                sessionContext,
                statementAnalyzerFactory,
                accessControl,
                orderByScope,
                analysis,
                expression,
                WarningCollector.NOOP,
                correlationSupport);
        analysis.recordSubqueries(node, expressionAnalysis);

        Type type = analysis.getType(expression);
        if (!type.isOrderable()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY_S,
                  type,
                  expression));
        }

        orderByFieldsBuilder.add(expression);
      }

      return orderByFieldsBuilder.build();
    }

    private Optional<SelectAlias> resolveOrderBySelectAlias(
        Expression expression, SelectAliasLookup selectAliases) {
      if (!(expression instanceof Identifier)) {
        return Optional.empty();
      }

      return resolveSelectAlias((Identifier) expression, selectAliases);
    }

    private void analyzeOffset(Offset node, Scope scope) {
      long rowCount;
      if (node.getRowCount() instanceof LongLiteral) {
        rowCount = ((LongLiteral) node.getRowCount()).getParsedValue();
      } else {
        checkState(
            node.getRowCount() instanceof Parameter,
            QueryMessages.EXCEPTION_UNEXPECTED_OFFSET_ROWCOUNT_COLON_977E305C
                + node.getRowCount().getClass().getSimpleName());
        OptionalLong providedValue =
            analyzeParameterAsRowCount((Parameter) node.getRowCount(), scope, "OFFSET");
        rowCount = providedValue.orElse(0);
      }
      if (rowCount < 0) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.OFFSET_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S,
                rowCount));
      }
      analysis.setOffset(node, rowCount);
    }

    /**
     * @return true if the Query / QuerySpecification containing the analyzed Limit or FetchFirst,
     *     must contain orderBy (i.e., for FetchFirst with ties).
     */
    private boolean analyzeLimit(Node node, Scope scope) {
      //      checkState(
      //          node instanceof FetchFirst || node instanceof Limit,
      //          "Invalid limit node type. Expected: FetchFirst or Limit. Actual: %s",
      // node.getClass().getName());
      checkState(
          node instanceof Limit,
          QueryMessages
              .EXCEPTION_INVALID_LIMIT_NODE_TYPE_DOT_EXPECTED_COLON_LIMIT_DOT_ACTUAL_COLON_ARG_5BCD1C76,
          node.getClass().getName());
      //      if (node instanceof FetchFirst) {
      //        return analyzeLimit((FetchFirst) node, scope);
      //      }
      return analyzeLimit((Limit) node, scope);
    }

    //    private boolean analyzeLimit(FetchFirst node, Scope scope) {
    //      long rowCount = 1;
    //      if (node.getRowCount().isPresent()) {
    //        Expression count = node.getRowCount().get();
    //        if (count instanceof LongLiteral) {
    //          rowCount = ((LongLiteral) count).getParsedValue();
    //        } else {
    //          checkState(count instanceof Parameter,
    //              "unexpected FETCH FIRST rowCount: " + count.getClass().getSimpleName());
    //          OptionalLong providedValue = analyzeParameterAsRowCount((Parameter) count, scope,
    // "FETCH FIRST");
    //          if (providedValue.isPresent()) {
    //            rowCount = providedValue.getAsLong();
    //          }
    //        }
    //      }
    //      if (rowCount <= 0) {
    //        throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node,
    //            "FETCH FIRST row count must be positive (actual value: %s)", rowCount);
    //      }
    //      analysis.setLimit(node, rowCount);
    //
    //      return node.isWithTies();
    //    }

    private boolean analyzeLimit(Limit node, Scope scope) {
      OptionalLong rowCount;
      if (node.getRowCount() instanceof AllRows) {
        rowCount = OptionalLong.empty();
      } else if (node.getRowCount() instanceof LongLiteral) {
        rowCount = OptionalLong.of(((LongLiteral) node.getRowCount()).getParsedValue());
      } else {
        checkState(
            node.getRowCount() instanceof Parameter,
            QueryMessages.EXCEPTION_UNEXPECTED_LIMIT_ROWCOUNT_COLON_CFF154F0
                + node.getRowCount().getClass().getSimpleName());
        rowCount = analyzeParameterAsRowCount((Parameter) node.getRowCount(), scope, "LIMIT");
      }
      rowCount.ifPresent(
          count -> {
            if (count < 0) {
              throw new SemanticException(
                  String.format(
                      DataNodeQueryMessages
                          .LIMIT_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S,
                      count));
            }
          });

      analysis.setLimit(node, rowCount);

      return false;
    }

    private OptionalLong analyzeParameterAsRowCount(
        Parameter parameter, Scope scope, String context) {
      // validate parameter index
      analyzeExpression(parameter, scope);
      Expression providedValue = analysis.getParameters().get(NodeRef.of(parameter));
      Object value;
      try {
        value =
            evaluateConstantExpression(
                providedValue, new PlannerContext(metadata, typeManager), sessionContext);

      } catch (VerifyException e) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.NON_CONSTANT_PARAMETER_VALUE_FOR_S_S,
                context,
                providedValue));
      }
      if (value == null) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.PARAMETER_VALUE_PROVIDED_FOR_S_IS_NULL_S,
                context,
                providedValue));
      }
      return OptionalLong.of((long) value);
    }

    private void analyzeAggregations(
        QuerySpecification node,
        Scope sourceScope,
        Optional<Scope> orderByScope,
        Analysis.GroupingSetAnalysis groupByAnalysis,
        List<Expression> outputExpressions,
        List<Expression> orderByExpressions) {
      checkState(
          orderByExpressions.isEmpty() || orderByScope.isPresent(),
          QueryMessages
              .EXCEPTION_NON_MINUS_EMPTY_ORDERBYEXPRESSIONS_LIST_WITHOUT_ORDERBYSCOPE_PROVIDED_240DDD64);

      List<FunctionCall> aggregates =
          extractAggregateFunctions(Iterables.concat(outputExpressions, orderByExpressions));
      analysis.setAggregates(node, aggregates);

      if (analysis.isAggregation(node)) {
        // ensure SELECT, ORDER BY and HAVING are constant with respect to group
        // e.g, these are all valid expressions:
        //     SELECT f(a) GROUP BY a
        //     SELECT f(a + 1) GROUP BY a + 1
        //     SELECT a + sum(b) GROUP BY a
        List<Expression> distinctGroupingColumns =
            ImmutableSet.copyOf(groupByAnalysis.getOriginalExpressions()).asList();

        verifySourceAggregations(distinctGroupingColumns, sourceScope, outputExpressions, analysis);
        if (!orderByExpressions.isEmpty()) {
          verifyOrderByAggregations(
              distinctGroupingColumns,
              sourceScope,
              orderByScope.orElseThrow(
                  () -> new NoSuchElementException(DataNodeQueryMessages.NO_VALUE_PRESENT)),
              orderByExpressions,
              analysis);
        }
      }
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      return ExpressionAnalyzer.analyzeExpression(
          metadata,
          queryContext,
          sessionContext,
          statementAnalyzerFactory,
          accessControl,
          scope,
          analysis,
          expression,
          warningCollector,
          correlationSupport);
    }

    private ExpressionAnalysis analyzeExpression(
        Expression expression, Scope scope, CorrelationSupport correlationSupport) {
      return ExpressionAnalyzer.analyzeExpression(
          metadata,
          queryContext,
          sessionContext,
          statementAnalyzerFactory,
          accessControl,
          scope,
          analysis,
          expression,
          warningCollector,
          correlationSupport);
    }

    private List<Node> findReferences(Node node, Identifier name) {
      Stream<Node> allReferences = preOrder(node).filter(isTableWithName(name));

      // TODO: recursive references could be supported in subquery before the point of shadowing.
      // currently, the recursive query name is considered shadowed in the whole subquery if the
      // subquery defines a common table with the same name
      Set<Node> shadowedReferences =
          preOrder(node)
              .filter(isQueryWithNameShadowed(name))
              .flatMap(query -> preOrder(query).filter(isTableWithName(name)))
              .collect(toImmutableSet());

      return allReferences
          .filter(reference -> !shadowedReferences.contains(reference))
          .collect(toImmutableList());
    }

    private Predicate<Node> isTableWithName(Identifier name) {
      return node -> {
        if (!(node instanceof Table)) {
          return false;
        }
        Table table = (Table) node;
        QualifiedName tableName = table.getName();
        return !tableName.getPrefix().isPresent()
            && tableName.hasSuffix(QualifiedName.of(name.getValue()));
      };
    }

    private Predicate<Node> isQueryWithNameShadowed(Identifier name) {
      return node -> {
        if (!(node instanceof Query)) {
          return false;
        }
        Query query = (Query) node;
        if (!query.getWith().isPresent()) {
          return false;
        }
        return query.getWith().get().getQueries().stream()
            .map(WithQuery::getName)
            .map(Identifier::getValue)
            .anyMatch(withQueryName -> withQueryName.equalsIgnoreCase(name.getValue()));
      };
    }

    private void validateFromClauseOfRecursiveTerm(Relation from, Identifier name) {
      preOrder(from)
          .filter(Join.class::isInstance)
          .forEach(
              node -> {
                Join join = (Join) node;
                Join.Type type = join.getType();
                if (type == LEFT || type == RIGHT || type == FULL) {
                  List<Node> leftRecursiveReferences = findReferences(join.getLeft(), name);
                  List<Node> rightRecursiveReferences = findReferences(join.getRight(), name);
                  if (!leftRecursiveReferences.isEmpty() && (type == RIGHT || type == FULL)) {
                    throw new SemanticException(
                        String.format(
                            DataNodeQueryMessages.RECURSIVE_REFERENCE_IN_LEFT_SOURCE_OF_S_JOIN,
                            type));
                  }
                  if (!rightRecursiveReferences.isEmpty() && (type == LEFT || type == FULL)) {
                    throw new SemanticException(
                        String.format(
                            DataNodeQueryMessages.RECURSIVE_REFERENCE_IN_RIGHT_SOURCE_OF_S_JOIN,
                            type));
                  }
                }
              });

      preOrder(from)
          .filter(node -> node instanceof Intersect && !((Intersect) node).isDistinct())
          .forEach(
              node -> {
                Intersect intersect = (Intersect) node;
                intersect.getRelations().stream()
                    .flatMap(relation -> findReferences(relation, name).stream())
                    .findFirst()
                    .ifPresent(
                        reference -> {
                          throw new SemanticException(
                              DataNodeQueryMessages.RECURSIVE_REFERENCE_IN_INTERSECT_ALL);
                        });
              });

      preOrder(from)
          .filter(Except.class::isInstance)
          .forEach(
              node -> {
                Except except = (Except) node;
                List<Node> rightRecursiveReferences = findReferences(except.getRight(), name);
                if (!rightRecursiveReferences.isEmpty()) {
                  throw new SemanticException(
                      String.format(
                          DataNodeQueryMessages.RECURSIVE_REFERENCE_IN_RIGHT_RELATION_OF_EXCEPT_S,
                          except.isDistinct()
                              ? DataNodeQueryMessages.DISTINCT_2
                              : DataNodeQueryMessages.ALL));
                }
                if (!except.isDistinct()) {
                  List<Node> leftRecursiveReferences = findReferences(except.getLeft(), name);
                  if (!leftRecursiveReferences.isEmpty()) {
                    throw new SemanticException(
                        DataNodeQueryMessages.RECURSIVE_REFERENCE_IN_LEFT_RELATION_OF_EXCEPT_ALL);
                  }
                }
              });
    }

    private Scope setAliases(Scope scope, Identifier tableName, List<Identifier> columnNames) {
      RelationType oldDescriptor = scope.getRelationType();
      validateColumnAliases(columnNames, oldDescriptor.getVisibleFieldCount());
      RelationType newDescriptor =
          oldDescriptor.withAlias(
              tableName.getValue(),
              columnNames.stream().map(Identifier::getValue).collect(toImmutableList()));

      Streams.forEachPair(
          oldDescriptor.getAllFields().stream(),
          newDescriptor.getAllFields().stream(),
          (newField, field) ->
              analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));
      return scope.withRelationType(newDescriptor);
    }

    private void verifySelectDistinct(
        QuerySpecification node,
        List<Expression> orderByExpressions,
        List<Expression> outputExpressions,
        Scope sourceScope,
        Scope orderByScope) {
      Set<CanonicalizationAware<Identifier>> aliases = getAliases(node.getSelect());

      Set<ScopeAware<Expression>> expressions =
          outputExpressions.stream()
              .map(e -> ScopeAware.scopeAwareKey(e, analysis, sourceScope))
              .collect(Collectors.toSet());

      for (Expression expression : orderByExpressions) {
        if (expression instanceof FieldReference) {
          continue;
        }

        // In a query such as
        //    SELECT a FROM t ORDER BY a
        // the "a" in the SELECT clause is bound to the FROM scope, while the "a" in ORDER BY clause
        // is bound
        // to the "a" from the SELECT clause, so we can't compare by field id / relation id.
        if (expression instanceof Identifier
            && aliases.contains(canonicalizationAwareKey(expression))) {
          continue;
        }

        if (!expressions.contains(ScopeAware.scopeAwareKey(expression, analysis, orderByScope))) {
          throw new SemanticException(
              DataNodeQueryMessages
                  .FOR_SELECT_DISTINCT_ORDER_BY_EXPRESSIONS_MUST_APPEAR_IN_SELECT_LIST);
        }
      }

      //      for (Expression expression : orderByExpressions) {
      //        if (!isDeterministic(expression, this::getResolvedFunction)) {
      //          throw new SemanticException("Non deterministic ORDER BY expression is not
      // supported with SELECT DISTINCT");
      //        }
      //      }
    }

    private Set<CanonicalizationAware<Identifier>> getAliases(Select node) {
      ImmutableSet.Builder<CanonicalizationAware<Identifier>> aliases = ImmutableSet.builder();
      for (SelectItem item : node.getSelectItems()) {
        if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          Optional<Identifier> alias = column.getAlias();
          if (alias.isPresent()) {
            aliases.add(canonicalizationAwareKey(alias.get()));
          } else if (column.getExpression() instanceof Identifier) {
            Identifier identifier = (Identifier) column.getExpression();
            aliases.add(canonicalizationAwareKey(identifier));
          } else if (column.getExpression() instanceof DereferenceExpression) {
            DereferenceExpression dereferenceExpression =
                (DereferenceExpression) column.getExpression();
            aliases.add(
                canonicalizationAwareKey(
                    dereferenceExpression
                        .getField()
                        .orElseThrow(
                            () ->
                                new NoSuchElementException(
                                    DataNodeQueryMessages.NO_VALUE_PRESENT))));
          }
        } else if (item instanceof AllColumns) {
          AllColumns allColumns = (AllColumns) item;
          List<Field> fields = analysis.getSelectAllResultFields(allColumns);
          checkNotNull(
              fields,
              QueryMessages.EXCEPTION_OUTPUT_FIELDS_IS_NULL_FOR_SELECT_ITEM_ARG_56AFEC51,
              item);
          for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);

            if (!allColumns.getAliases().isEmpty()) {
              aliases.add(canonicalizationAwareKey(allColumns.getAliases().get(i)));
            } else if (field.getName().isPresent()) {
              aliases.add(canonicalizationAwareKey(new Identifier(field.getName().get())));
            }
          }
        }
      }

      return aliases.build();
    }

    private void validateProperties(final List<Property> properties, final Optional<Scope> scope) {
      final Set<String> propertyNames = new HashSet<>();
      for (final Property property : properties) {
        final String key = property.getName().getValue().toLowerCase(Locale.ENGLISH);
        if (!TABLE_ALLOWED_PROPERTIES.contains(key)) {
          throw new SemanticException(
              DataNodeQueryMessages.TABLE_PROPERTY_2
                  + key
                  + DataNodeQueryMessages.IS_CURRENTLY_NOT_ALLOWED_2);
        }
        if (!propertyNames.add(key)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.DUPLICATE_PROPERTY_S, property.getName().getValue()));
        }
        if (!property.isSetToDefault()) {
          final Expression value = property.getNonDefaultValue();
          if (!(value instanceof LongLiteral)) {
            throw new SemanticException(
                DataNodeQueryMessages.TTL_VALUE_MUST_BE_A_INF_OR_A_LONGLITERAL_BUT_NOW_IS
                    + value.toString());
          }
        }
      }
      for (final Property property : properties) {
        process(property, scope);
      }
    }

    private void validateColumns(Statement node, RelationType descriptor) {
      // verify that all column names are specified and unique
      // TODO: collect errors and return them all at once
      Set<String> names = new HashSet<>();
      for (Field field : descriptor.getVisibleFields()) {
        String fieldName =
            field
                .getName()
                .orElseThrow(
                    () ->
                        new SemanticException(
                            String.format(
                                DataNodeQueryMessages.COLUMN_NAME_NOT_SPECIFIED_AT_POSITION_S,
                                descriptor.indexOf(field) + 1)));
        if (!names.add(fieldName)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.COLUMN_NAME_S_SPECIFIED_MORE_THAN_ONCE, fieldName));
        }
      }
    }

    private void validateColumnAliases(List<Identifier> columnAliases, int sourceColumnSize) {
      validateColumnAliasesCount(columnAliases, sourceColumnSize);
      Set<String> names = new HashSet<>();
      for (Identifier identifier : columnAliases) {
        if (names.contains(identifier.getValue().toLowerCase(ENGLISH))) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.COLUMN_NAME_S_SPECIFIED_MORE_THAN_ONCE,
                  identifier.getValue()));
        }
        names.add(identifier.getValue().toLowerCase(ENGLISH));
      }
    }

    private void validateColumnAliasesCount(List<Identifier> columnAliases, int sourceColumnSize) {
      if (columnAliases.size() != sourceColumnSize) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_RELATION_HAS_S_COLUMNS,
                columnAliases.size(),
                sourceColumnSize));
      }
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope) {
      return createAndAssignScope(node, parentScope, emptyList());
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields) {
      return createAndAssignScope(node, parentScope, new RelationType(fields));
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields) {
      return createAndAssignScope(node, parentScope, new RelationType(fields));
    }

    private Scope createAndAssignScope(
        Node node, Optional<Scope> parentScope, RelationType relationType) {
      Scope.Builder scopeBuilder =
          scopeBuilder(parentScope).withRelationType(RelationId.of(node), relationType);
      parentScope.ifPresent(scope -> scopeBuilder.withTables(scope.getTables()));
      Scope scope = scopeBuilder.build();

      analysis.setScope(node, scope);
      return scope;
    }

    private Scope createScope(Optional<Scope> parentScope) {
      return scopeBuilder(parentScope).build();
    }

    private Scope.Builder scopeBuilder(Optional<Scope> parentScope) {
      Scope.Builder scopeBuilder = Scope.builder();

      if (parentScope.isPresent()) {
        // parent scope represents local query scope hierarchy. Local query scope
        // hierarchy should have outer query scope as ancestor already.
        scopeBuilder.withParent(parentScope.get());
      } else {
        outerQueryScope.ifPresent(scopeBuilder::withOuterQueryParent);
      }

      return scopeBuilder;
    }

    @Override
    public Scope visitCreateOrUpdateDevice(
        final CreateOrUpdateDevice node, final Optional<Scope> context) {
      queryContext.setQueryType(QueryType.OTHER);
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(queryContext, SchemaLockType.VALIDATE_VS_DELETION_TABLE);
      // Check if the table exists
      DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTable());
      return null;
    }

    @Override
    public Scope visitFetchDevice(final FetchDevice node, final Optional<Scope> context) {
      return null;
    }

    @Override
    public Scope visitShowDevice(final ShowDevice node, final Optional<Scope> context) {
      analyzeQueryDevice(node, context);
      // TODO: use real scope when parameter in offset and limit is supported
      if (Objects.nonNull(node.getOffset())) {
        analyzeOffset(node.getOffset(), null);
      }
      if (Objects.nonNull(node.getLimit())) {
        analyzeLimit(node.getLimit(), null);
      }
      return null;
    }

    @Override
    public Scope visitCountDevice(final CountDevice node, final Optional<Scope> context) {
      analyzeQueryDevice(node, context);
      return null;
    }

    private void analyzeQueryDevice(
        final AbstractQueryDeviceWithCache node, final Optional<Scope> context) {
      node.parseTable(sessionContext);
      accessControl.checkCanSelectFromTable(
          sessionContext.getUserName(),
          new QualifiedObjectName(node.getDatabase(), node.getTableName()),
          queryContext);
      analyzeTraverseDevice(node, context, node.getWhere().isPresent());

      final TsTable table =
          DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTableName());
      if (!node.parseRawExpression(
          table,
          table.getColumnList().stream()
              .filter(
                  columnSchema ->
                      columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE))
              .map(TsTableColumnSchema::getColumnName)
              .collect(Collectors.toList()),
          queryContext)) {
        // Cache hit
        // Currently we disallow "Or" filter for precise get, thus if it hit cache
        // it'll be only one device
        // TODO: Ensure the disjointness of expressions and allow Or filter
        analysis.setFinishQueryAfterAnalyze();
      }
    }

    // NOTICE: We construct transition map here because currently we set the used fields in
    // the statement. Other queries shall not do this and shall do it in logical plan phase.
    private TranslationMap analyzeTraverseDevice(
        final AbstractTraverseDevice node,
        final Optional<Scope> context,
        final boolean shallCreateTranslationMap) {
      final String database = node.getDatabase();
      final String tableName = node.getTableName();

      if (Objects.isNull(database)) {
        throw new SemanticException(DataNodeQueryMessages.THE_DATABASE_MUST_BE_SET);
      }

      if (!metadata.tableExists(new QualifiedObjectName(database, tableName))) {
        CommonMetadataUtils.throwTableNotExistsException(database, tableName);
      }
      node.setColumnHeaderList();

      TranslationMap translationMap = null;
      if (shallCreateTranslationMap) {
        final QualifiedObjectName name = new QualifiedObjectName(database, tableName);
        final Optional<TableSchema> tableSchema = metadata.getTableSchema(sessionContext, name);
        // This can only be a table
        if (!tableSchema.isPresent()) {
          CommonMetadataUtils.throwTableNotExistsException(database, tableName);
        }

        final TableSchema originalSchema = tableSchema.get();
        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        // We only leave attribute & tag here because the others are not allowed in device related
        // SQLs
        fields.addAll(
            analyzeTableOutputFields(
                node.getTable(),
                name,
                new TableSchema(
                    originalSchema.getTableName(),
                    originalSchema.getColumns().stream()
                        .filter(
                            columnSchema ->
                                columnSchema
                                        .getColumnCategory()
                                        .equals(TsTableColumnCategory.ATTRIBUTE)
                                    || columnSchema
                                        .getColumnCategory()
                                        .equals(TsTableColumnCategory.TAG))
                        .collect(Collectors.toList()))));
        final List<Field> fieldList = fields.build();
        final Scope scope = createAndAssignScope(node, context, fieldList);
        translationMap =
            new TranslationMap(
                Optional.empty(),
                scope,
                analysis,
                fieldList.stream()
                    .map(field -> Symbol.of(field.getName().orElse(null)))
                    .collect(Collectors.toList()),
                new PlannerContext(metadata, null));

        if (node.getWhere().isPresent()) {
          try {
            analyzeWhere(node, translationMap.getScope(), node.getWhere().get());
          } catch (final Throwable e) {
            if (e instanceof SemanticException
                && e.getMessage().contains(DataNodeQueryMessages.CANNOT_BE_RESOLVED)) {
              throw new SemanticException(
                  new IoTDBException(
                      e.getCause()
                          .getMessage()
                          .replace(
                              DataNodeQueryMessages.CANNOT_BE_RESOLVED,
                              DataNodeQueryMessages.IS_NOT_AN_ATTRIBUTE_OR_TAG_COLUMN),
                      TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
            }
            throw e;
          }
          node.setWhere(translationMap.rewrite(analysis.getWhere(node)));
        }
      }

      return translationMap;
    }

    private Pair<Type, Expression> analyzeAndRewriteExpression(
        final TranslationMap translationMap, final Scope scope, final Expression expression) {
      final Type type = analyzeExpression(expression, scope).getType(expression);
      scope.getRelationType().getAllFields();
      return new Pair<>(type, translationMap.rewrite(expression));
    }

    @Override
    public Scope visitCreatePipe(CreatePipe node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitAlterPipe(AlterPipe node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDropPipe(DropPipe node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitStartPipe(StartPipe node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitStopPipe(StopPipe node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowPipes(ShowPipes node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowCreatePipe(ShowCreatePipe node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitCreatePipePlugin(CreatePipePlugin node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDropPipePlugin(DropPipePlugin node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowPipePlugins(ShowPipePlugins node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitCreateTopic(CreateTopic node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitAlterTopic(AlterTopic node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDropTopic(DropTopic node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowTopics(ShowTopics node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowCreateTopic(ShowCreateTopic node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitShowSubscriptions(ShowSubscriptions node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitDropSubscription(DropSubscription node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    public Scope visitTableFunctionInvocation(TableFunctionInvocation node, Optional<Scope> scope) {
      String functionName = node.getName().toString();
      TableFunction function = metadata.getTableFunction(functionName);

      Node errorLocation = node;
      if (!node.getArguments().isEmpty()) {
        errorLocation = node.getArguments().get(0);
      }

      ArgumentsAnalysis argumentsAnalysis =
          analyzeArguments(
              function.getArgumentsSpecifications(),
              node.getArguments(),
              scope,
              errorLocation,
              functionName);

      TableFunctionAnalysis functionAnalysis;
      try {
        if (function instanceof ReadTsFileTableFunction) {
          ((ReadTsFileTableFunction) function).setMPPQueryContext(queryContext);
        }
        functionAnalysis = function.analyze(argumentsAnalysis.getPassedArguments());
      } catch (UDFException e) {
        throw new SemanticException(e.getMessage());
      }

      // At most one table argument can be passed to a table function now
      if (argumentsAnalysis.getTableArgumentAnalyses().size() > 1) {
        throw new SemanticException(DataNodeQueryMessages.AT_MOST_ONE_TABLE_ARGUMENT_CAN_BE_PASSED);
      }

      // validate the required input columns
      // <TableArgumentName, TableColumnIndexes>
      Map<String, List<Integer>> requiredColumns = functionAnalysis.getRequiredColumns();
      Map<String, TableArgumentAnalysis> tableArgumentsByName =
          argumentsAnalysis.getTableArgumentAnalyses().stream()
              .collect(toImmutableMap(TableArgumentAnalysis::getArgumentName, Function.identity()));
      Set<String> tableArgumentNameSet = ImmutableSet.copyOf(tableArgumentsByName.keySet());
      requiredColumns.forEach(
          (name, columns) -> {
            if (!tableArgumentNameSet.contains(name)) {
              throw new SemanticException(
                  String.format(
                      DataNodeQueryMessages
                          .TABLE_FUNCTION_S_SPECIFIES_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S_WHICH_CANNOT_BE_FOUND,
                      node.getName(),
                      name));
            }
            // make sure the required columns are not empty and positive
            if (columns.isEmpty()) {
              throw new SemanticException(
                  String.format(
                      DataNodeQueryMessages
                          .TABLE_FUNCTION_S_SPECIFIES_EMPTY_LIST_OF_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S,
                      node.getName(),
                      name));
            }
            if (columns.stream().anyMatch(column -> column < 0)) {
              throw new SemanticException(
                  String.format(
                      DataNodeQueryMessages
                          .TABLE_FUNCTION_S_SPECIFIES_NEGATIVE_INDEX_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S,
                      node.getName(),
                      name));
            }
            // the scope is recorded, because table arguments are already analyzed
            Scope inputScope = analysis.getScope(tableArgumentsByName.get(name).getRelation());
            columns.stream()
                .filter(column -> column >= inputScope.getRelationType().getVisibleFieldCount())
                .findFirst()
                .ifPresent(
                    column -> {
                      throw new SemanticException(
                          String.format(
                              DataNodeQueryMessages
                                  .INDEX_S_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S_IS_OUT_OF_BOUNDS_FOR_TABLE_WITH_S,
                              column,
                              name,
                              inputScope.getRelationType().getAllFieldCount()));
                    });
            // record the required columns for access control
            columns.stream()
                .map(inputScope.getRelationType()::getFieldByIndex)
                .forEach(this::recordColumnAccess);
          });
      // check that all required inputs are specified
      Set<String> requiredInputs = ImmutableSet.copyOf(requiredColumns.keySet());
      tableArgumentNameSet.stream()
          .filter(input -> !requiredInputs.contains(input))
          .findFirst()
          .ifPresent(
              input -> {
                throw new SemanticException(
                    String.format(
                        DataNodeQueryMessages
                            .TABLE_FUNCTION_S_DOES_NOT_SPECIFY_REQUIRED_INPUT_COLUMNS_FROM_TABLE_ARGUMENT_S,
                        node.getName(),
                        input));
              });

      // The result relation type of a table function consists of:
      // 1. columns created by the table function, called the proper columns.
      // 2. passed columns from input tables:
      // - for tables with the "pass through columns" option, these are all columns of the table,
      // - for tables without the "pass through columns" option, these are the partitioning columns
      // of the table, if any.
      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      Optional<DescribedSchema> properSchema = functionAnalysis.getProperColumnSchema();
      properSchema.ifPresent(
          i ->
              i.getFields().stream()
                  .map(
                      f -> {
                        Type type =
                            UDFDataTypeTransformer.transformUDFDataTypeToReadType(f.getType());
                        if (type == ObjectType.OBJECT) {
                          throw new SemanticException(
                              DataNodeQueryMessages.OBJECT_TYPE_IS_NOT_SUPPORTED_AS_RETURN_TYPE);
                        }
                        return Field.newUnqualified(f.getName(), type, TsTableColumnCategory.FIELD);
                      })
                  .forEach(fields::add));

      // next, columns derived from table arguments, in order of argument declarations
      List<String> tableArgumentNames =
          function.getArgumentsSpecifications().stream()
              .filter(TableParameterSpecification.class::isInstance)
              .map(ParameterSpecification::getName)
              .collect(toImmutableList());

      // table arguments in order of argument declarations
      ImmutableList.Builder<TableArgumentAnalysis> orderedTableArguments = ImmutableList.builder();

      for (String name : tableArgumentNames) {
        TableArgumentAnalysis argument = tableArgumentsByName.get(name);
        // analyze arguments will make sure that all table arguments are present
        checkArgument(
            argument != null,
            QueryMessages.EXCEPTION_MISSING_TABLE_ARGUMENT_COLON_ARG_4F1E9F4E,
            name);
        orderedTableArguments.add(argument);
        Scope argumentScope = analysis.getScope(argument.getRelation());
        if (argument.isPassThroughColumns()) {
          argumentScope.getRelationType().getAllFields().forEach(fields::add);
        } else if (!TableBuiltinTableFunction.M4.getFunctionName().equalsIgnoreCase(functionName)
            && argument.getPartitionBy().isPresent()) {
          argument.getPartitionBy().get().stream()
              .map(expression -> validateAndGetInputField(expression, argumentScope))
              .forEach(fields::add);
        }
      }

      analysis.setTableFunctionAnalysis(
          node,
          new TableFunctionInvocationAnalysis(
              node.getName().toString(),
              argumentsAnalysis.getPassedArguments(),
              functionAnalysis.getTableFunctionHandle(),
              orderedTableArguments.build(),
              requiredColumns,
              properSchema.map(describedSchema -> describedSchema.getFields().size()).orElse(0),
              functionAnalysis.isRequireRecordSnapshot()));

      return createAndAssignScope(node, scope, fields.build());
    }

    private String castNameAsSpecification(Set<String> specifiedNames, String passedName) {
      if (specifiedNames.contains(passedName)) {
        return passedName;
      }
      for (String name : specifiedNames) {
        if (name.equalsIgnoreCase(passedName)) {
          return name;
        }
      }
      return null;
    }

    private ArgumentsAnalysis analyzeArguments(
        List<ParameterSpecification> parameterSpecifications,
        List<TableFunctionArgument> arguments,
        Optional<Scope> scope,
        Node errorLocation,
        String functionName) {
      if (parameterSpecifications.size() < arguments.size()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages
                    .TOO_MANY_ARGUMENTS_EXPECTED_AT_MOST_S_ARGUMENTS_GOT_S_ARGUMENTS,
                parameterSpecifications.size(),
                arguments.size()));
      }

      if (parameterSpecifications.isEmpty()) {
        return new ArgumentsAnalysis(ImmutableMap.of(), ImmutableList.of());
      }

      boolean argumentsPassedByName =
          !arguments.isEmpty()
              && arguments.stream().allMatch(argument -> argument.getName().isPresent());
      boolean argumentsPassedByPosition =
          arguments.stream().noneMatch(argument -> argument.getName().isPresent());
      if (!argumentsPassedByName && !argumentsPassedByPosition) {
        throw new SemanticException(
            DataNodeQueryMessages
                .ALL_ARGUMENTS_MUST_BE_PASSED_BY_NAME_OR_ALL_MUST_BE_PASSED_POSITIONALLY);
      }

      ImmutableMap.Builder<String, Argument> passedArguments = ImmutableMap.builder();
      ImmutableList.Builder<TableArgumentAnalysis> tableArgumentAnalyses = ImmutableList.builder();
      if (argumentsPassedByName) {
        Map<String, ParameterSpecification> argumentSpecificationsByName = new HashMap<>();
        for (ParameterSpecification parameterSpecification : parameterSpecifications) {
          if (argumentSpecificationsByName.put(
                  parameterSpecification.getName(), parameterSpecification)
              != null) {
            // this should never happen, because the argument names are validated at function
            // registration time
            throw new IllegalStateException(
                String.format(
                    DataNodeQueryMessages
                        .QUERY_EXCEPTION_DUPLICATE_ARGUMENT_SPECIFICATION_FOR_NAME_S_F804F3DC,
                    parameterSpecification.getName()));
          }
        }

        // append order by time asc for built-in forecast tvf if user doesn't specify order by
        // clause
        tryUpdateOrderByForForecastByName(functionName, arguments, argumentSpecificationsByName);

        Set<String> uniqueArgumentNames = new HashSet<>();
        Set<String> specifiedArgumentNames =
            ImmutableSet.copyOf(argumentSpecificationsByName.keySet());
        for (TableFunctionArgument argument : arguments) {
          // it has been checked that all arguments have different names
          String argumentName =
              castNameAsSpecification(
                  specifiedArgumentNames, argument.getName().get().getCanonicalValue());
          if (argumentName == null) {
            throw new SemanticException(
                String.format(
                    DataNodeQueryMessages.UNEXPECTED_ARGUMENT_NAME_S,
                    argument.getName().get().getValue()));
          }
          if (!uniqueArgumentNames.add(argumentName)) {
            throw new SemanticException(
                String.format(DataNodeQueryMessages.DUPLICATE_ARGUMENT_NAME, argumentName));
          }
          ParameterSpecification parameterSpecification =
              argumentSpecificationsByName.remove(argumentName);
          ArgumentAnalysis argumentAnalysis =
              analyzeArgument(parameterSpecification, argument, scope);
          passedArguments.put(argumentName, argumentAnalysis.getArgument());
          argumentAnalysis.getTableArgumentAnalysis().ifPresent(tableArgumentAnalyses::add);
        }
        // apply defaults for not specified arguments
        for (Map.Entry<String, ParameterSpecification> entry :
            argumentSpecificationsByName.entrySet()) {
          ParameterSpecification parameterSpecification = entry.getValue();
          passedArguments.put(
              parameterSpecification.getName(),
              analyzeDefault(parameterSpecification, errorLocation));
        }
      } else {
        // append order by time asc for built-in forecast tvf if user doesn't specify order by
        // clause
        tryUpdateOrderByForForecastByPosition(functionName, arguments, parameterSpecifications);
        for (int i = 0; i < arguments.size(); i++) {
          TableFunctionArgument argument = arguments.get(i);
          ParameterSpecification parameterSpecification = parameterSpecifications.get(i);
          ArgumentAnalysis argumentAnalysis =
              analyzeArgument(parameterSpecification, argument, scope);
          passedArguments.put(parameterSpecification.getName(), argumentAnalysis.getArgument());
          argumentAnalysis.getTableArgumentAnalysis().ifPresent(tableArgumentAnalyses::add);
        }
        // apply defaults for not specified arguments
        for (int i = arguments.size(); i < parameterSpecifications.size(); i++) {
          ParameterSpecification parameterSpecification = parameterSpecifications.get(i);
          passedArguments.put(
              parameterSpecification.getName(),
              analyzeDefault(parameterSpecification, errorLocation));
        }
      }
      tryAppendM4ModeArgument(functionName, arguments, parameterSpecifications, passedArguments);
      return new ArgumentsAnalysis(passedArguments.buildOrThrow(), tableArgumentAnalyses.build());
    }

    private void tryAppendM4ModeArgument(
        String functionName,
        List<TableFunctionArgument> arguments,
        List<ParameterSpecification> parameterSpecifications,
        ImmutableMap.Builder<String, Argument> passedArguments) {
      if (!TableBuiltinTableFunction.M4.getFunctionName().equalsIgnoreCase(functionName)) {
        return;
      }

      TableFunctionArgument sizeArgument =
          findTableFunctionArgument(
              arguments, parameterSpecifications, M4TableFunction.SIZE_PARAMETER_NAME);
      if (!(sizeArgument.getValue() instanceof Expression)) {
        throw new SemanticException(
            String.format(
                QueryMessages.EXCEPTION_INVALID_ARGUMENT_ARG_EXPECTED_SCALAR_ARGUMENT_4AD4E32F,
                M4TableFunction.SIZE_PARAMETER_NAME));
      }

      boolean isTimeWindow = sizeArgument.getValue() instanceof TimeDurationLiteral;
      Optional<TableFunctionArgument> slideArgument =
          findOptionalTableFunctionArgument(
              arguments, parameterSpecifications, M4TableFunction.SLIDE_PARAMETER_NAME);
      if (slideArgument.isPresent()) {
        if (!(slideArgument.get().getValue() instanceof Expression)) {
          throw new SemanticException(
              String.format(
                  QueryMessages.EXCEPTION_INVALID_ARGUMENT_ARG_EXPECTED_SCALAR_ARGUMENT_4AD4E32F,
                  M4TableFunction.SLIDE_PARAMETER_NAME));
        }
        boolean isTimeSlide = slideArgument.get().getValue() instanceof TimeDurationLiteral;
        if (isTimeWindow != isTimeSlide) {
          throw new SemanticException(
              QueryMessages
                  .EXCEPTION_THE_SLIDE_ARGUMENT_MUST_HAVE_THE_SAME_WINDOW_MODE_AS_THE_SIZE_ARGUMENT_419C9844);
        }
      }
      if (!isTimeWindow
          && containsTableFunctionArgument(
              arguments, parameterSpecifications, M4TableFunction.ORIGIN_PARAMETER_NAME)) {
        throw new SemanticException(
            QueryMessages
                .EXCEPTION_THE_ORIGIN_ARGUMENT_IS_ONLY_SUPPORTED_IN_TIME_WINDOW_MODE_B3F358B9);
      }
      validateM4OrderBySortOrder(arguments, parameterSpecifications);
      passedArguments.put(
          M4TableFunction.WINDOW_MODE_PARAMETER_NAME,
          new ScalarArgument(org.apache.iotdb.udf.api.type.Type.BOOLEAN, isTimeWindow));
    }

    private void validateM4OrderBySortOrder(
        List<TableFunctionArgument> arguments,
        List<ParameterSpecification> parameterSpecifications) {
      Optional<TableFunctionArgument> dataArgument =
          findOptionalTableFunctionArgument(
              arguments, parameterSpecifications, M4TableFunction.DATA_PARAMETER_NAME);
      if (!dataArgument.isPresent()
          || !(dataArgument.get().getValue() instanceof TableFunctionTableArgument)) {
        return;
      }

      Optional<OrderBy> orderBy =
          ((TableFunctionTableArgument) dataArgument.get().getValue()).getOrderBy();
      if (!orderBy.isPresent()) {
        return;
      }

      for (SortItem sortItem : orderBy.get().getSortItems()) {
        if (sortItem.getOrdering() != SortItem.Ordering.ASCENDING) {
          throw new SemanticException(
              QueryMessages
                  .EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_SORT_THE_TIME_COLUMN_IN_ASCENDING_ORDER_C3FD0F6A);
        }
      }
    }

    private boolean containsTableFunctionArgument(
        List<TableFunctionArgument> arguments,
        List<ParameterSpecification> parameterSpecifications,
        String argumentName) {
      return findOptionalTableFunctionArgument(arguments, parameterSpecifications, argumentName)
          .isPresent();
    }

    private Optional<TableFunctionArgument> findOptionalTableFunctionArgument(
        List<TableFunctionArgument> arguments,
        List<ParameterSpecification> parameterSpecifications,
        String argumentName) {
      boolean argumentsPassedByName =
          arguments.stream().allMatch(argument -> argument.getName().isPresent());
      if (argumentsPassedByName) {
        return arguments.stream()
            .filter(
                argument ->
                    argumentName.equalsIgnoreCase(argument.getName().get().getCanonicalValue()))
            .findFirst();
      }

      for (int i = 0, size = parameterSpecifications.size(); i < size; i++) {
        if (argumentName.equalsIgnoreCase(parameterSpecifications.get(i).getName())) {
          if (i >= arguments.size()) {
            return Optional.empty();
          }
          return Optional.of(arguments.get(i));
        }
      }
      return Optional.empty();
    }

    private TableFunctionArgument findTableFunctionArgument(
        List<TableFunctionArgument> arguments,
        List<ParameterSpecification> parameterSpecifications,
        String argumentName) {
      if (arguments.isEmpty()) {
        throw new IllegalStateException(
            QueryMessages
                .EXCEPTION_ARGUMENTS_SHOULD_NEVER_BE_EMPTY_WHEN_RESOLVING_M4_MODE_F982EA1C);
      }

      boolean argumentsPassedByName =
          arguments.stream().allMatch(argument -> argument.getName().isPresent());
      if (argumentsPassedByName) {
        return arguments.stream()
            .filter(
                argument ->
                    argumentName.equalsIgnoreCase(argument.getName().get().getCanonicalValue()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            DataNodeQueryMessages.MISSING_REQUIRED_ARGUMENT_S, argumentName)));
      }

      for (int i = 0, size = parameterSpecifications.size(); i < size; i++) {
        if (argumentName.equalsIgnoreCase(parameterSpecifications.get(i).getName())) {
          if (i >= arguments.size()) {
            throw new IllegalStateException(
                String.format(
                    QueryMessages.EXCEPTION_MISSING_REQUIRED_ARGUMENT_ARG_1308D0DC, argumentName));
          }
          return arguments.get(i);
        }
      }
      throw new IllegalStateException(
          String.format(QueryMessages.EXCEPTION_UNKNOWN_ARGUMENT_ARG_F40E9394, argumentName));
    }

    // append order by time asc for built-in forecast tvf if user doesn't specify order by clause
    private void tryUpdateOrderByForForecastByName(
        String functionName,
        List<TableFunctionArgument> arguments,
        Map<String, ParameterSpecification> argumentSpecificationsByName) {
      if (TableBuiltinTableFunction.FORECAST.getFunctionName().equalsIgnoreCase(functionName)) {
        String timeColumn =
            (String)
                argumentSpecificationsByName.get(TIMECOL_PARAMETER_NAME).getDefaultValue().get();
        for (TableFunctionArgument argument : arguments) {
          if (TIMECOL_PARAMETER_NAME.equalsIgnoreCase(argument.getName().get().getValue())) {
            if (argument.getValue() instanceof StringLiteral) {
              timeColumn = ((StringLiteral) argument.getValue()).getValue();
            }
          }
        }
        tryUpdateOrderByForForecast(arguments, timeColumn);
      }
    }

    // append order by time asc for built-in forecast tvf if user doesn't specify order by clause
    private void tryUpdateOrderByForForecastByPosition(
        String functionName,
        List<TableFunctionArgument> arguments,
        List<ParameterSpecification> parameterSpecifications) {
      if (TableBuiltinTableFunction.FORECAST.getFunctionName().equalsIgnoreCase(functionName)) {
        int position = -1;
        String timeColumn = null;
        for (int i = 0, size = parameterSpecifications.size(); i < size; i++) {
          if (TIMECOL_PARAMETER_NAME.equalsIgnoreCase(parameterSpecifications.get(i).getName())) {
            position = i;
            timeColumn = (String) parameterSpecifications.get(i).getDefaultValue().get();
            break;
          }
        }
        if (position == -1) {
          throw new IllegalStateException(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_FORECASTTABLEFUNCTION_MUST_CONTAIN_FORECASTTABLEFUNCTION_DA5828E9);
        }
        if (position < arguments.size()
            && arguments.get(position).getValue() instanceof StringLiteral) {
          timeColumn = ((StringLiteral) arguments.get(position).getValue()).getValue();
        }
        tryUpdateOrderByForForecast(arguments, timeColumn);
      }
    }

    // append order by time asc for built-in forecast tvf if user doesn't specify order by clause
    private void tryUpdateOrderByForForecast(
        List<TableFunctionArgument> arguments, String timeColumn) {
      if (timeColumn == null || timeColumn.isEmpty()) {
        throw new SemanticException(
            String.format(
                QueryMessages.PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY_DOT, TIMECOL_PARAMETER_NAME));
      }
      for (TableFunctionArgument argument : arguments) {
        if (argument.getValue() instanceof TableFunctionTableArgument) {
          TableFunctionTableArgument input = (TableFunctionTableArgument) argument.getValue();
          if (!input.getOrderBy().isPresent()) {
            input.updateOrderBy(
                new OrderBy(
                    Collections.singletonList(
                        new SortItem(
                            new Identifier(timeColumn.toLowerCase(ENGLISH)),
                            SortItem.Ordering.ASCENDING,
                            SortItem.NullOrdering.FIRST))));
          }
        }
      }
    }

    private ArgumentAnalysis analyzeArgument(
        ParameterSpecification parameterSpecification,
        TableFunctionArgument argument,
        Optional<Scope> scope) {
      String actualType;
      if (argument.getValue() instanceof TableFunctionTableArgument) {
        actualType = "table";
      } else if (argument.getValue() instanceof Expression) {
        actualType = "expression";
      } else {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.UNEXPECTED_TABLE_FUNCTION_ARGUMENT_TYPE_S,
                argument.getClass().getSimpleName()));
      }

      if (parameterSpecification instanceof TableParameterSpecification) {
        if (!(argument.getValue() instanceof TableFunctionTableArgument)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.INVALID_ARGUMENT_S_EXPECTED_TABLE_ARGUMENT_GOT_S,
                  parameterSpecification.getName(),
                  actualType));
        }
        return analyzeTableArgument(
            (TableFunctionTableArgument) argument.getValue(),
            (TableParameterSpecification) parameterSpecification,
            scope);
      } else if (parameterSpecification instanceof ScalarParameterSpecification) {
        if (!(argument.getValue() instanceof Expression)) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.INVALID_ARGUMENT_S_EXPECTED_SCALAR_ARGUMENT_GOT_S,
                  parameterSpecification.getName(),
                  actualType));
        }
        return analyzeScalarArgument(
            (Expression) argument.getValue(),
            (ScalarParameterSpecification) parameterSpecification);
      } else {
        throw new IllegalStateException(
            String.format(
                DataNodeQueryMessages.QUERY_EXCEPTION_UNEXPECTED_ARGUMENT_SPECIFICATION_S_830154B1,
                parameterSpecification.getClass().getSimpleName()));
      }
    }

    private ArgumentAnalysis analyzeTableArgument(
        TableFunctionTableArgument tableArgument,
        TableParameterSpecification argumentSpecification,
        Optional<Scope> scope) {
      List<Optional<String>> fieldNames;
      List<org.apache.iotdb.udf.api.type.Type> fieldTypes;
      List<String> partitionBy = Collections.emptyList();
      List<String> orderBy = Collections.emptyList();

      TableArgumentAnalysis.Builder analysisBuilder = TableArgumentAnalysis.builder();
      analysisBuilder.withArgumentName(argumentSpecification.getName());

      // process the relation
      Relation relation = tableArgument.getTable();
      analysisBuilder.withRelation(relation);
      Scope argumentScope = process(relation, scope);
      QualifiedName relationName = analysis.getRelationName(relation);
      if (relationName != null) {
        analysisBuilder.withName(relationName);
      }

      // analyze field
      Collection<Field> fields = argumentScope.getRelationType().getVisibleFields();
      fieldNames = fields.stream().map(Field::getName).collect(toImmutableList());
      fieldTypes =
          fields.stream()
              .map(Field::getType)
              .map(UDFDataTypeTransformer::transformReadTypeToUDFDataType)
              .collect(toImmutableList());

      // analyze PARTITION BY
      if (tableArgument.getPartitionBy().isPresent()) {
        if (argumentSpecification.isRowSemantics()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .INVALID_ARGUMENT_S_PARTITIONING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW,
                  argumentSpecification.getName()));
        }
        List<Expression> partitionByExpression = tableArgument.getPartitionBy().get();
        analysisBuilder.withPartitionBy(partitionByExpression);
        partitionByExpression.forEach(
            partitioningColumn -> {
              validateAndGetInputField(partitioningColumn, argumentScope);
              Type type =
                  analyzeExpression(partitioningColumn, argumentScope).getType(partitioningColumn);
              if (!type.isComparable()) {
                throw new SemanticException(
                    String.format(
                        DataNodeQueryMessages
                            .S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_PARTITION_BY,
                        type));
              }
            });
        partitionBy =
            partitionByExpression.stream()
                .map(
                    expression -> {
                      if (expression instanceof Identifier) {
                        return ((Identifier) expression).getValue();
                      } else if (expression instanceof DereferenceExpression) {
                        return expression.toString();
                      } else {
                        throw new IllegalStateException(
                            String.format(
                                DataNodeQueryMessages
                                    .QUERY_EXCEPTION_UNEXPECTED_PARTITIONBY_EXPRESSION_S_8D74EB2D,
                                expression));
                      }
                    })
                .collect(toImmutableList());
      }

      // analyze ORDER BY
      if (tableArgument.getOrderBy().isPresent()) {
        if (argumentSpecification.isRowSemantics()) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages
                      .INVALID_ARGUMENT_S_ORDERING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW_SEMANTICS,
                  argumentSpecification.getName()));
        }
        OrderBy orderByExpression = tableArgument.getOrderBy().get();
        analysisBuilder.withOrderBy(orderByExpression);
        orderByExpression.getSortItems().stream()
            .map(SortItem::getSortKey)
            .forEach(
                orderingColumn -> {
                  validateAndGetInputField(orderingColumn, argumentScope);
                  Type type =
                      analyzeExpression(orderingColumn, argumentScope).getType(orderingColumn);
                  if (!type.isOrderable()) {
                    throw new SemanticException(
                        String.format(
                            DataNodeQueryMessages
                                .S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY,
                            type));
                  }
                });
        orderBy =
            orderByExpression.getSortItems().stream()
                .map(SortItem::getSortKey)
                .map(
                    expression -> {
                      if (expression instanceof Identifier) {
                        return ((Identifier) expression).getValue();
                      } else if (expression instanceof DereferenceExpression) {
                        return expression.toString();
                      } else {
                        throw new IllegalStateException(
                            String.format(
                                DataNodeQueryMessages
                                    .QUERY_EXCEPTION_UNEXPECTED_ORDERBY_EXPRESSION_S_9301B69E,
                                expression));
                      }
                    })
                .collect(toImmutableList());
      }

      // record remaining properties
      analysisBuilder.withRowSemantics(argumentSpecification.isRowSemantics());
      analysisBuilder.withPassThroughColumns(argumentSpecification.isPassThroughColumns());

      return new ArgumentAnalysis(
          new TableArgument(
              fieldNames, fieldTypes, partitionBy, orderBy, argumentSpecification.isRowSemantics()),
          Optional.of(analysisBuilder.build()));
    }

    private ArgumentAnalysis analyzeScalarArgument(
        Expression expression, ScalarParameterSpecification argumentSpecification) {
      if (expression instanceof TimeDurationLiteral) {
        if (((TimeDurationLiteral) expression).getValue().monthDuration != 0) {
          throw new SemanticException(
              DataNodeQueryMessages.SETTING_MONTHLY_INTERVALS_IS_NOT_SUPPORTED);
        }
      }

      // currently, only constant arguments are supported
      Object constantValue =
          evaluateConstantExpression(
              expression, new PlannerContext(metadata, typeManager), sessionContext);
      if (!argumentSpecification.getType().checkObjectType(constantValue)) {
        if ((argumentSpecification.getType().equals(org.apache.iotdb.udf.api.type.Type.STRING)
                || argumentSpecification.getType().equals(org.apache.iotdb.udf.api.type.Type.TEXT))
            && constantValue instanceof Binary) {
          constantValue = ((Binary) constantValue).getStringValue(TSFileConfig.STRING_CHARSET);
        } else if (argumentSpecification.getType().equals(org.apache.iotdb.udf.api.type.Type.INT32)
            && constantValue instanceof Long) {
          constantValue = ((Long) constantValue).intValue();
        } else {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.INVALID_SCALAR_ARGUMENT_S_EXPECTED_TYPE_S_GOT_S,
                  argumentSpecification.getName(),
                  argumentSpecification.getType(),
                  constantValue.getClass().getSimpleName()));
        }
      }
      for (Function<Object, String> checker : argumentSpecification.getCheckers()) {
        String errMsg = checker.apply(constantValue);
        if (errMsg != null) {
          throw new SemanticException(
              String.format(
                  DataNodeQueryMessages.INVALID_SCALAR_ARGUMENT_S_S,
                  argumentSpecification.getName(),
                  errMsg));
        }
      }
      return new ArgumentAnalysis(
          new ScalarArgument(argumentSpecification.getType(), constantValue), Optional.empty());
    }

    private Argument analyzeDefault(
        ParameterSpecification parameterSpecification, Node errorLocation) {
      if (parameterSpecification.isRequired()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.MISSING_REQUIRED_ARGUMENT_S,
                parameterSpecification.getName()));
      }
      checkArgument(
          !(parameterSpecification instanceof TableParameterSpecification),
          QueryMessages
              .EXCEPTION_TABLE_ARGUMENT_SPECIFICATION_CANNOT_HAVE_A_DEFAULT_VALUE_DOT_009A02CA);

      if (parameterSpecification instanceof ScalarParameterSpecification) {
        checkArgument(
            parameterSpecification.getDefaultValue().isPresent(),
            String.format(
                QueryMessages
                    .EXCEPTION_MISSING_DEFAULT_VALUE_FOR_SCALAR_ARGUMENT_COLON_ARG_8AF70F38,
                parameterSpecification.getName()));
        return new ScalarArgument(
            ((ScalarParameterSpecification) parameterSpecification).getType(),
            parameterSpecification.getDefaultValue().get());
      } else {
        throw new IllegalStateException(
            String.format(
                DataNodeQueryMessages.QUERY_EXCEPTION_UNEXPECTED_ARGUMENT_SPECIFICATION_S_830154B1,
                parameterSpecification.getClass().getSimpleName()));
      }
    }

    private Field validateAndGetInputField(Expression expression, Scope inputScope) {
      QualifiedName qualifiedName;
      if (expression instanceof Identifier) {
        qualifiedName = QualifiedName.of(ImmutableList.of((Identifier) expression));
      } else if (expression instanceof DereferenceExpression) {
        qualifiedName = getQualifiedName((DereferenceExpression) expression);
      } else {
        throw new SemanticException(
            String.format(DataNodeQueryMessages.EXPECTED_COLUMN_REFERENCE_ACTUAL_S, expression));
      }
      Optional<ResolvedField> field = inputScope.tryResolveField(expression, qualifiedName);
      if (!field.isPresent() || !field.get().isLocal()) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.COLUMN_S_IS_NOT_PRESENT_IN_THE_INPUT_RELATION, expression));
      }

      return field.get().getField();
    }
  }

  private static boolean hasScopeAsLocalParent(Scope root, Scope parent) {
    Scope scope = root;
    while (scope.getLocalParent().isPresent()) {
      scope = scope.getLocalParent().get();
      if (scope.equals(parent)) {
        return true;
      }
    }

    return false;
  }

  static void verifyNoAggregateWindowOrGroupingFunctions(Expression predicate, String clause) {
    List<FunctionCall> aggregates =
        ImmutableList.<FunctionCall>builder()
            .addAll(extractAggregateFunctions(ImmutableList.of(predicate)))
            .addAll(extractWindowFunctions(ImmutableList.of(predicate)))
            .build();

    if (!aggregates.isEmpty()) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages
                  .S_CANNOT_CONTAIN_AGGREGATIONS_WINDOW_FUNCTIONS_OR_GROUPING_OPERATIONS_S,
              clause,
              aggregates));
    }
  }
}
