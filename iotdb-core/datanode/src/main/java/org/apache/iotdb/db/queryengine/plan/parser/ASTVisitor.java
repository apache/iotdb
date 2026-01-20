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

package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.cache.CacheClearOptions;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterFactory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ConstantContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountDatabasesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountDevicesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountNodesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountTimeseriesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CreateFunctionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.DropFunctionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ExpressionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.GroupByAttributeClauseContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.IdentifierContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ProcessorAttributeClauseContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ShowFunctionsContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParserBaseVisitor;
import org.apache.iotdb.db.queryengine.execution.operator.window.WindowType;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.CountInferenceWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.HeadInferenceWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindow;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.TailInferenceWindow;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.builtin.BuiltInScalarFunctionHelperFactory;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataTypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NumericParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ViewFieldDefinition;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.component.FromComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByConditionComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByCountComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupBySessionComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTagComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByVariationComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.HavingCondition;
import org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.IntoItem;
import org.apache.iotdb.db.queryengine.plan.statement.component.NullOrdering;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultSetFormat;
import org.apache.iotdb.db.queryengine.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.literal.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.literal.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.literal.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.literal.Literal;
import org.apache.iotdb.db.queryengine.plan.statement.literal.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.literal.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterEncodingCompressorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesDataTypeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveAINodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveConfigNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowAvailableUrlsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowCurrentTimestampStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.externalservice.ShowExternalServiceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateTrainingStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.DropModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.LoadModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAIDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAINodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowLoadedModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.UnloadModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.AlterPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipePluginsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ExtendRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.MigrateRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ReconstructRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.RemoveRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.CreateTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropSubscriptionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowSubscriptionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowTopicsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.AlterSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateTableViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.ShowLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentUserStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StartRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StopRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.TestConnectionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.db.schemaengine.template.TemplateAlterOperationType;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.trigger.api.enums.TriggerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_RESULT_NODES;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression.getEscapeCharacter;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.canPushDownLimitOffsetToGroupByTime;
import static org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown.pushDownLimitOffsetToTimeParameter;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.parser.AstBuilder.lowerIdentifier;
import static org.apache.iotdb.db.utils.TimestampPrecisionUtils.TIMESTAMP_PRECISION;
import static org.apache.iotdb.db.utils.TimestampPrecisionUtils.currPrecision;
import static org.apache.iotdb.db.utils.constant.SqlConstant.CAST_FUNCTION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.CAST_TYPE;
import static org.apache.iotdb.db.utils.constant.SqlConstant.REPLACE_FROM;
import static org.apache.iotdb.db.utils.constant.SqlConstant.REPLACE_FUNCTION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.REPLACE_TO;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROUND_FUNCTION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROUND_PLACES;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_FUNCTION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_IS_STANDARD;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_LENGTH;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUBSTRING_START;

/** Parse AST to Statement. */
public class ASTVisitor extends IoTDBSqlParserBaseVisitor<Statement> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ASTVisitor.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String DELETE_RANGE_ERROR_MSG =
      "For delete statement, where clause can only contain atomic expressions like : "
          + "time > XXX, time <= XXX, or two atomic expressions connected by 'AND'";
  private static final String DELETE_ONLY_SUPPORT_TIME_EXP_ERROR_MSG =
      "For delete statement, where clause can only contain time expressions, "
          + "value filter is not currently supported.";

  private static final String GROUP_BY_COMMON_ONLY_ONE_MSG =
      "Only one of group by time or group by variation/series/session can be supported at a time";

  private static final String LIMIT_CONFIGURATION_ENABLED_ERROR_MSG =
      "Limit configuration is not enabled, please enable it first.";

  public static final String DELETE_RANGE_COMPARISON_ERROR_MSG =
      "For delete statement, where clause use a range comparison on the same field, the left value of the range cannot be greater than the right value of the range, it must be written like this : time > 5 and time < 10";

  private static final String NODE_NAME_IN_INTO_PATH_MATCHER = "([a-zA-Z0-9_${}\\u2E80-\\u9FFF]+)";
  private static final Pattern NODE_NAME_IN_INTO_PATH_PATTERN =
      Pattern.compile(NODE_NAME_IN_INTO_PATH_MATCHER);

  private static final String IGNORENULL = "IgnoreNull";
  private ZoneId zoneId;

  private boolean useWildcard = false;

  private boolean lastLevelUseWildcard = false;

  public static final String SERVICE_MANAGEMENT_NOT_SUPPORTED =
      "Service management SQLs are not supported now!";

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  /** Top Level Description. */
  @Override
  public Statement visitSingleStatement(IoTDBSqlParser.SingleStatementContext ctx) {
    Statement statement = visit(ctx.statement());
    if (ctx.DEBUG() != null) {
      statement.setDebug(true);
    }
    return statement;
  }

  /** Data Definition Language (DDL). */

  // Create Timeseries ========================================================================
  @Override
  public Statement visitCreateNonAlignedTimeseries(
      IoTDBSqlParser.CreateNonAlignedTimeseriesContext ctx) {
    CreateTimeSeriesStatement createTimeSeriesStatement = new CreateTimeSeriesStatement();
    createTimeSeriesStatement.setPath(parseFullPath(ctx.fullPath()));
    if (ctx.attributeClauses() != null) {
      parseAttributeClausesForCreateNonAlignedTimeSeries(
          ctx.attributeClauses(), createTimeSeriesStatement);
    }
    return createTimeSeriesStatement;
  }

  @Override
  public Statement visitCreateAlignedTimeseries(IoTDBSqlParser.CreateAlignedTimeseriesContext ctx) {
    CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement =
        new CreateAlignedTimeSeriesStatement();
    createAlignedTimeSeriesStatement.setDevicePath(parseAlignedDevice(ctx.fullPath()));
    parseAlignedMeasurements(ctx.alignedMeasurements(), createAlignedTimeSeriesStatement);
    return createAlignedTimeSeriesStatement;
  }

  public void parseAlignedMeasurements(
      IoTDBSqlParser.AlignedMeasurementsContext ctx,
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement) {
    for (int i = 0; i < ctx.nodeNameWithoutWildcard().size(); i++) {
      createAlignedTimeSeriesStatement.addMeasurement(
          parseNodeNameWithoutWildCard(ctx.nodeNameWithoutWildcard(i)));
      parseAttributeClausesForCreateAlignedTimeSeries(
          ctx.attributeClauses(i), createAlignedTimeSeriesStatement);
    }
  }

  public void parseAttributeClausesForCreateNonAlignedTimeSeries(
      IoTDBSqlParser.AttributeClausesContext ctx,
      CreateTimeSeriesStatement createTimeSeriesStatement) {
    if (ctx.aliasNodeName() != null) {
      createTimeSeriesStatement.setAlias(parseNodeName(ctx.aliasNodeName().nodeName()));
    }

    Map<String, String> props = new HashMap<>();
    TSDataType dataType = parseDataTypeAttribute(ctx);
    if (dataType != null) {
      props.put(
          IoTDBConstant.COLUMN_TIMESERIES_DATATYPE.toLowerCase(),
          dataType.toString().toLowerCase());
    }
    List<IoTDBSqlParser.AttributePairContext> attributePairs = ctx.attributePair();
    if (ctx.attributePair(0) != null) {
      for (IoTDBSqlParser.AttributePairContext attributePair : attributePairs) {
        props.put(
            parseAttributeKey(attributePair.attributeKey()).toLowerCase(),
            parseAttributeValue(attributePair.attributeValue()).toLowerCase());
      }
    }

    createTimeSeriesStatement.setProps(props);
    checkPropsInCreateTimeSeries(createTimeSeriesStatement);

    if (ctx.tagClause() != null) {
      parseTagClause(ctx.tagClause(), createTimeSeriesStatement);
    }
    if (ctx.attributeClause() != null) {
      parseAttributeClauseForTimeSeries(ctx.attributeClause(), createTimeSeriesStatement);
    }
  }

  /**
   * Check and set datatype, encoding, compressor.
   *
   * @throws SemanticException if encoding or dataType meets error handling
   */
  private void checkPropsInCreateTimeSeries(CreateTimeSeriesStatement createTimeSeriesStatement) {
    Map<String, String> props = createTimeSeriesStatement.getProps();
    if (props != null
        && props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE.toLowerCase())) {
      String datatypeString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE.toLowerCase()).toUpperCase();
      try {
        createTimeSeriesStatement.setDataType(TSDataType.valueOf(datatypeString));
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported datatype: %s", datatypeString));
      }
    }
    if (createTimeSeriesStatement.getDataType() == null) {
      throw new SemanticException("datatype must be declared");
    }

    final IoTDBDescriptor ioTDBDescriptor = IoTDBDescriptor.getInstance();
    createTimeSeriesStatement.setEncoding(
        ioTDBDescriptor.getDefaultEncodingByType(createTimeSeriesStatement.getDataType()));
    if (props != null && props.containsKey(COLUMN_TIMESERIES_ENCODING.toLowerCase())) {
      String encodingString = props.get(COLUMN_TIMESERIES_ENCODING.toLowerCase()).toUpperCase();
      try {
        createTimeSeriesStatement.setEncoding(TSEncoding.valueOf(encodingString));
        props.remove(COLUMN_TIMESERIES_ENCODING.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported encoding: %s", encodingString));
      }
    }

    createTimeSeriesStatement.setCompressor(
        TSFileDescriptor.getInstance()
            .getConfig()
            .getCompressor(createTimeSeriesStatement.getDataType()));
    if (props != null
        && props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase())) {
      String compressionString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase()).toUpperCase();
      try {
        createTimeSeriesStatement.setCompressor(CompressionType.valueOf(compressionString));
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(
            String.format("Unsupported compression: %s", compressionString));
      }
    } else if (props != null
        && props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase())) {
      String compressorString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase()).toUpperCase();
      try {
        createTimeSeriesStatement.setCompressor(CompressionType.valueOf(compressorString));
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported compression: %s", compressorString));
      }
    }
    createTimeSeriesStatement.setProps(props);
  }

  public void parseAttributeClausesForCreateAlignedTimeSeries(
      IoTDBSqlParser.AttributeClausesContext ctx,
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement) {
    if (ctx.aliasNodeName() != null) {
      createAlignedTimeSeriesStatement.addAliasList(parseNodeName(ctx.aliasNodeName().nodeName()));
    } else {
      createAlignedTimeSeriesStatement.addAliasList(null);
    }

    TSDataType dataType = parseDataTypeAttribute(ctx);
    createAlignedTimeSeriesStatement.addDataType(dataType);

    Map<String, String> props = new HashMap<>();
    if (ctx.attributePair() != null) {
      for (int i = 0; i < ctx.attributePair().size(); i++) {
        props.put(
            parseAttributeKey(ctx.attributePair(i).attributeKey()).toLowerCase(),
            parseAttributeValue(ctx.attributePair(i).attributeValue()));
      }
    }

    TSEncoding encoding = IoTDBDescriptor.getInstance().getDefaultEncodingByType(dataType);
    if (props.containsKey(COLUMN_TIMESERIES_ENCODING.toLowerCase())) {
      String encodingString = props.get(COLUMN_TIMESERIES_ENCODING.toLowerCase()).toUpperCase();
      try {
        encoding = TSEncoding.valueOf(encodingString);
        createAlignedTimeSeriesStatement.addEncoding(encoding);
        props.remove(COLUMN_TIMESERIES_ENCODING.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported encoding: %s", encodingString));
      }
    } else {
      createAlignedTimeSeriesStatement.addEncoding(encoding);
    }

    CompressionType compressor = TSFileDescriptor.getInstance().getConfig().getCompressor(dataType);
    if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase())) {
      String compressorString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase()).toUpperCase();
      try {
        compressor = CompressionType.valueOf(compressorString);
        createAlignedTimeSeriesStatement.addCompressor(compressor);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported compressor: %s", compressorString));
      }
    } else if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase())) {
      String compressionString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase()).toUpperCase();
      try {
        compressor = CompressionType.valueOf(compressionString);
        createAlignedTimeSeriesStatement.addCompressor(compressor);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(
            String.format("unsupported compression: %s", compressionString));
      }
    } else {
      createAlignedTimeSeriesStatement.addCompressor(compressor);
    }

    if (props.size() > 0) {
      throw new SemanticException("create aligned timeseries: property is not supported yet.");
    }

    if (ctx.tagClause() != null) {
      parseTagClause(ctx.tagClause(), createAlignedTimeSeriesStatement);
    } else {
      createAlignedTimeSeriesStatement.addTagsList(null);
    }

    if (ctx.attributeClause() != null) {
      parseAttributeClauseForTimeSeries(ctx.attributeClause(), createAlignedTimeSeriesStatement);
    } else {
      createAlignedTimeSeriesStatement.addAttributesList(null);
    }
  }

  // Tag & Property & Attribute

  public void parseTagClause(IoTDBSqlParser.TagClauseContext ctx, Statement statement) {
    Map<String, String> tags = extractMap(ctx.attributePair(), ctx.attributePair(0));
    if (statement instanceof CreateTimeSeriesStatement) {
      ((CreateTimeSeriesStatement) statement).setTags(tags);
    } else if (statement instanceof CreateAlignedTimeSeriesStatement) {
      ((CreateAlignedTimeSeriesStatement) statement).addTagsList(tags);
    } else if (statement instanceof AlterTimeSeriesStatement) {
      ((AlterTimeSeriesStatement) statement).setTagsMap(tags);
    }
  }

  public void parseAttributeClauseForTimeSeries(
      IoTDBSqlParser.AttributeClauseContext ctx, Statement statement) {
    Map<String, String> attributes = extractMap(ctx.attributePair(), ctx.attributePair(0));
    if (statement instanceof CreateTimeSeriesStatement) {
      ((CreateTimeSeriesStatement) statement).setAttributes(attributes);
    } else if (statement instanceof CreateAlignedTimeSeriesStatement) {
      ((CreateAlignedTimeSeriesStatement) statement).addAttributesList(attributes);
    } else if (statement instanceof AlterTimeSeriesStatement) {
      ((AlterTimeSeriesStatement) statement).setAttributesMap(attributes);
    }
  }

  // Alter Timeseries ========================================================================

  @Override
  public Statement visitAlterTimeseries(IoTDBSqlParser.AlterTimeseriesContext ctx) {
    AlterTimeSeriesStatement alterTimeSeriesStatement = new AlterTimeSeriesStatement();
    alterTimeSeriesStatement.setPath(parseFullPath(ctx.fullPath()));
    alterTimeSeriesStatement = parseAlterClause(ctx.alterClause(), alterTimeSeriesStatement);
    return alterTimeSeriesStatement;
  }

  private AlterTimeSeriesStatement parseAlterClause(
      IoTDBSqlParser.AlterClauseContext ctx, AlterTimeSeriesStatement alterTimeSeriesStatement) {
    Map<String, String> alterMap = new HashMap<>();
    // Rename
    if (ctx.RENAME() != null) {
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.RENAME);
      alterMap.put(parseAttributeKey(ctx.beforeName), parseAttributeKey(ctx.currentName));
    } else if (ctx.SET() != null) {
      if (ctx.DATA() != null && ctx.TYPE() != null) {
        MeasurementPath path = alterTimeSeriesStatement.getPath();
        alterTimeSeriesStatement = new AlterTimeSeriesDataTypeStatement();
        alterTimeSeriesStatement.setPath(path);
        // Set data type
        alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.SET_DATA_TYPE);
        setMap(ctx, alterMap);
        if (ctx.attributeValue() != null) {
          alterTimeSeriesStatement.setDataType(parseDataTypeAttribute(ctx.attributeValue()));
        }
      } else {
        // Set
        alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.SET);
        setMap(ctx, alterMap);
      }
    } else if (ctx.DROP() != null) {
      // Drop
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.DROP);
      for (int i = 0; i < ctx.attributeKey().size(); i++) {
        alterMap.put(parseAttributeKey(ctx.attributeKey().get(i)), null);
      }
    } else if (ctx.TAGS() != null) {
      // Add tag
      alterTimeSeriesStatement.setAlterType((AlterTimeSeriesStatement.AlterType.ADD_TAGS));
      setMap(ctx, alterMap);
    } else if (ctx.ATTRIBUTES() != null) {
      // Add attribute
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES);
      setMap(ctx, alterMap);
    } else if (ctx.UPSERT() != null) {
      // Upsert
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.UPSERT);
      if (ctx.aliasClause() != null) {
        parseAliasClause(ctx.aliasClause(), alterTimeSeriesStatement);
      }
      if (ctx.tagClause() != null) {
        parseTagClause(ctx.tagClause(), alterTimeSeriesStatement);
      }
      if (ctx.attributeClause() != null) {
        parseAttributeClauseForTimeSeries(ctx.attributeClause(), alterTimeSeriesStatement);
      }
    }
    alterTimeSeriesStatement.setAlterMap(alterMap);
    return alterTimeSeriesStatement;
  }

  public void parseAliasClause(
      IoTDBSqlParser.AliasClauseContext ctx, AlterTimeSeriesStatement alterTimeSeriesStatement) {
    if (alterTimeSeriesStatement != null && ctx.ALIAS() != null) {
      alterTimeSeriesStatement.setAlias(parseAliasNode(ctx.alias()));
    }
  }

  private TSDataType parseDataTypeAttribute(IoTDBSqlParser.AttributeValueContext ctx) {
    if (ctx == null) {
      throw new SemanticException("Incorrect Data type");
    }

    String dataTypeString = parseAttributeValue(ctx);
    TSDataType dataType = TSDataType.valueOf(dataTypeString);
    if (TSDataType.UNKNOWN.equals(dataType) || TSDataType.VECTOR.equals(dataType)) {
      throw new SemanticException(String.format("Unsupported datatype: %s", dataTypeString));
    }
    return dataType;
  }

  @Override
  public Statement visitAlterEncodingCompressor(
      final IoTDBSqlParser.AlterEncodingCompressorContext ctx) {
    final PathPatternTree tree = new PathPatternTree();
    ctx.prefixPath().forEach(path -> tree.appendPathPattern(parsePrefixPath(path)));
    tree.constructTree();
    TSEncoding encoding = null;
    CompressionType compressor = null;
    for (final IoTDBSqlParser.AttributePairContext pair : ctx.attributePair()) {
      final String key = parseAttributeKey(pair.key).toLowerCase(Locale.ENGLISH);
      final String value = parseAttributeValue(pair.value).toUpperCase(Locale.ENGLISH);
      switch (key) {
        case COLUMN_TIMESERIES_ENCODING:
          try {
            encoding = TSEncoding.valueOf(value);
          } catch (final Exception e) {
            throw new SemanticException(String.format("Unsupported encoding: %s", value));
          }
          break;
        case COLUMN_TIMESERIES_COMPRESSOR:
        case COLUMN_TIMESERIES_COMPRESSION:
          try {
            compressor = CompressionType.valueOf(value);
          } catch (final Exception e) {
            throw new SemanticException(String.format("Unsupported compressor: %s", value));
          }
          break;
        default:
          throw new SemanticException(String.format("property %s is unsupported yet.", key));
      }
    }

    if (tree.isEmpty()) {
      throw new SemanticException("The timeSeries shall not be root.");
    }
    return new AlterEncodingCompressorStatement(
        tree,
        encoding,
        compressor,
        Objects.nonNull(ctx.EXISTS()),
        Objects.nonNull(ctx.PERMITTED()));
  }

  // Drop Timeseries ======================================================================

  @Override
  public Statement visitDropTimeseries(IoTDBSqlParser.DropTimeseriesContext ctx) {
    DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
    List<PartialPath> partialPaths = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      partialPaths.add(parsePrefixPath(prefixPathContext));
    }
    deleteTimeSeriesStatement.setPathPatternList(partialPaths);
    return deleteTimeSeriesStatement;
  }

  // Show Timeseries ========================================================================

  @Override
  public Statement visitShowTimeseries(IoTDBSqlParser.ShowTimeseriesContext ctx) {
    boolean orderByHeat = ctx.LATEST() != null;
    ShowTimeSeriesStatement showTimeSeriesStatement;
    if (ctx.prefixPath() != null) {
      showTimeSeriesStatement =
          new ShowTimeSeriesStatement(parsePrefixPath(ctx.prefixPath()), orderByHeat);
    } else {
      showTimeSeriesStatement =
          new ShowTimeSeriesStatement(
              new PartialPath(SqlConstant.getSingleRootArray()), orderByHeat);
    }
    if (ctx.timeseriesWhereClause() != null) {
      if (ctx.timeConditionClause() != null) {
        throw new SemanticException(
            "TIMESERIES condition and TIME condition cannot be used at the same time.");
      }
      SchemaFilter schemaFilter = parseTimeseriesWhereClause(ctx.timeseriesWhereClause());
      showTimeSeriesStatement.setSchemaFilter(schemaFilter);
    }
    if (ctx.timeConditionClause() != null) {
      showTimeSeriesStatement.setTimeCondition(
          parseWhereClause(ctx.timeConditionClause().whereClause()));
    }
    if (ctx.rowPaginationClause() != null) {
      if (ctx.rowPaginationClause().limitClause() != null) {
        showTimeSeriesStatement.setLimit(parseLimitClause(ctx.rowPaginationClause().limitClause()));
      }
      if (ctx.rowPaginationClause().offsetClause() != null) {
        showTimeSeriesStatement.setOffset(
            parseOffsetClause(ctx.rowPaginationClause().offsetClause()));
      }
    }
    return showTimeSeriesStatement;
  }

  private SchemaFilter parseTimeseriesWhereClause(IoTDBSqlParser.TimeseriesWhereClauseContext ctx) {
    if (ctx.timeseriesContainsExpression() != null) {
      // path contains filter
      return SchemaFilterFactory.createPathContainsFilter(
          parseStringLiteral(ctx.timeseriesContainsExpression().value.getText()));
    } else if (ctx.columnEqualsExpression() != null) {
      return parseColumnEqualsExpressionContext(ctx.columnEqualsExpression());
    } else {
      // tag filter
      if (ctx.tagContainsExpression() != null) {
        return SchemaFilterFactory.createTagFilter(
            parseAttributeKey(ctx.tagContainsExpression().attributeKey()),
            parseStringLiteral(ctx.tagContainsExpression().value.getText()),
            true);
      } else {
        return SchemaFilterFactory.createTagFilter(
            parseAttributeKey(ctx.tagEqualsExpression().attributeKey()),
            parseAttributeValue(ctx.tagEqualsExpression().attributeValue()),
            false);
      }
    }
  }

  private SchemaFilter parseColumnEqualsExpressionContext(
      IoTDBSqlParser.ColumnEqualsExpressionContext ctx) {
    String column = parseAttributeKey(ctx.attributeKey());
    String value = parseAttributeValue(ctx.attributeValue());
    if (column.equalsIgnoreCase(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE)) {
      try {
        TSDataType dataType = TSDataType.valueOf(value.toUpperCase());
        return SchemaFilterFactory.createDataTypeFilter(dataType);
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported datatype: %s", value));
      }
    } else {
      throw new SemanticException("unexpected filter key");
    }
  }

  // SHOW DATABASES

  @Override
  public Statement visitShowDatabases(IoTDBSqlParser.ShowDatabasesContext ctx) {
    ShowDatabaseStatement showDatabaseStatement;

    // Parse prefixPath
    if (ctx.prefixPath() != null) {
      showDatabaseStatement = new ShowDatabaseStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      showDatabaseStatement =
          new ShowDatabaseStatement(new PartialPath(SqlConstant.getSingleRootArray()));
    }

    // Is detailed
    showDatabaseStatement.setDetailed(ctx.DETAILS() != null);

    return showDatabaseStatement;
  }

  // Show Devices ========================================================================

  @Override
  public Statement visitShowDevices(IoTDBSqlParser.ShowDevicesContext ctx) {
    ShowDevicesStatement showDevicesStatement;
    if (ctx.prefixPath() != null) {
      showDevicesStatement = new ShowDevicesStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      showDevicesStatement =
          new ShowDevicesStatement(new PartialPath(SqlConstant.getSingleRootArray()));
    }
    if (ctx.devicesWhereClause() != null) {
      if (ctx.timeConditionClause() != null) {
        throw new SemanticException(
            "DEVICE condition and TIME condition cannot be used at the same time.");
      }
      showDevicesStatement.setSchemaFilter(parseDevicesWhereClause(ctx.devicesWhereClause()));
    }
    if (ctx.timeConditionClause() != null) {
      showDevicesStatement.setTimeCondition(
          parseWhereClause(ctx.timeConditionClause().whereClause()));
    }
    if (ctx.rowPaginationClause() != null) {
      if (ctx.rowPaginationClause().limitClause() != null) {
        showDevicesStatement.setLimit(parseLimitClause(ctx.rowPaginationClause().limitClause()));
      }
      if (ctx.rowPaginationClause().offsetClause() != null) {
        showDevicesStatement.setOffset(parseOffsetClause(ctx.rowPaginationClause().offsetClause()));
      }
    }
    // show devices with database
    if (ctx.WITH() != null) {
      showDevicesStatement.setSgCol(true);
    }
    return showDevicesStatement;
  }

  private SchemaFilter parseDevicesWhereClause(IoTDBSqlParser.DevicesWhereClauseContext ctx) {
    // path contains filter
    if (ctx.deviceContainsExpression() != null) {
      return SchemaFilterFactory.createPathContainsFilter(
          parseStringLiteral(ctx.deviceContainsExpression().value.getText()));
    } else {
      String templateName = null;
      boolean isEqual = true;
      if (ctx.templateEqualExpression().operator_is() != null) {
        if (ctx.templateEqualExpression().operator_not() != null) {
          isEqual = false;
        }
      } else {
        templateName = parseStringLiteral(ctx.templateEqualExpression().templateName.getText());
        if (ctx.templateEqualExpression().OPERATOR_NEQ() != null) {
          isEqual = false;
        }
      }
      return SchemaFilterFactory.createTemplateNameFilter(templateName, isEqual);
    }
  }

  // Count Devices ========================================================================

  @Override
  public Statement visitCountDevices(CountDevicesContext ctx) {
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    CountDevicesStatement statement = new CountDevicesStatement(path);
    if (ctx.timeConditionClause() != null) {
      WhereCondition timeCondition = parseWhereClause(ctx.timeConditionClause().whereClause());
      statement.setTimeCondition(timeCondition);
    }
    return statement;
  }

  // Count TimeSeries ========================================================================
  @Override
  public Statement visitCountTimeseries(CountTimeseriesContext ctx) {
    Statement statement = null;
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    if (ctx.timeConditionClause() != null) {
      statement = new CountTimeSeriesStatement(path);
      WhereCondition timeCondition = parseWhereClause(ctx.timeConditionClause().whereClause());
      ((CountTimeSeriesStatement) statement).setTimeCondition(timeCondition);
    }
    if (ctx.INTEGER_LITERAL() != null) {
      if (ctx.timeConditionClause() != null) {
        throw new SemanticException(
            "TIME condition and GROUP BY LEVEL cannot be used at the same time.");
      }
      int level = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
      statement = new CountLevelTimeSeriesStatement(path, level);
    } else if (statement == null) {
      statement = new CountTimeSeriesStatement(path);
    }
    if (ctx.timeseriesWhereClause() != null) {
      if (ctx.timeConditionClause() != null) {
        throw new SemanticException(
            "TIMESERIES condition and TIME condition cannot be used at the same time.");
      }
      SchemaFilter schemaFilter = parseTimeseriesWhereClause(ctx.timeseriesWhereClause());
      if (statement instanceof CountTimeSeriesStatement) {
        ((CountTimeSeriesStatement) statement).setSchemaFilter(schemaFilter);
      } else if (statement instanceof CountLevelTimeSeriesStatement) {
        ((CountLevelTimeSeriesStatement) statement).setSchemaFilter(schemaFilter);
      }
    }
    return statement;
  }

  // Count Nodes ========================================================================
  @Override
  public Statement visitCountNodes(CountNodesContext ctx) {
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    int level = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    return new CountNodesStatement(path, level);
  }

  // Count Database ========================================================================
  @Override
  public Statement visitCountDatabases(CountDatabasesContext ctx) {
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    return new CountDatabaseStatement(path);
  }

  // Show version
  @Override
  public Statement visitShowVersion(IoTDBSqlParser.ShowVersionContext ctx) {
    return new ShowVersionStatement();
  }

  // Create Function
  @Override
  public Statement visitCreateFunction(CreateFunctionContext ctx) {
    if (ctx.uriClause() == null) {
      return new CreateFunctionStatement(
          parseIdentifier(ctx.udfName.getText()),
          parseStringLiteral(ctx.className.getText()),
          Optional.empty());
    } else {
      String uriString = parseAndValidateURI(ctx.uriClause());
      return new CreateFunctionStatement(
          parseIdentifier(ctx.udfName.getText()),
          parseStringLiteral(ctx.className.getText()),
          Optional.of(uriString));
    }
  }

  private String parseAndValidateURI(IoTDBSqlParser.UriClauseContext ctx) {
    String uriString = parseStringLiteral(ctx.uri().getText());
    if (StringUtils.isEmpty(uriString)) {
      throw new SemanticException("URI is empty, please specify the URI.");
    }
    try {
      new URI(uriString);
    } catch (URISyntaxException e) {
      throw new SemanticException(String.format("Invalid URI: %s", uriString));
    }
    return uriString;
  }

  // Drop Function
  @Override
  public Statement visitDropFunction(DropFunctionContext ctx) {
    return new DropFunctionStatement(parseIdentifier(ctx.udfName.getText()));
  }

  // Show Functions
  @Override
  public Statement visitShowFunctions(ShowFunctionsContext ctx) {
    return new ShowFunctionsStatement();
  }

  // Create Trigger =====================================================================
  @Override
  public Statement visitCreateTrigger(IoTDBSqlParser.CreateTriggerContext ctx) {
    if (ctx.triggerEventClause().DELETE() != null) {
      throw new SemanticException("Trigger does not support DELETE as TRIGGER_EVENT for now.");
    }
    if (ctx.triggerType() == null) {
      throw new SemanticException("Please specify trigger type: STATELESS or STATEFUL.");
    }
    Map<String, String> attributes = new HashMap<>();
    if (ctx.triggerAttributeClause() != null) {
      for (IoTDBSqlParser.TriggerAttributeContext triggerAttributeContext :
          ctx.triggerAttributeClause().triggerAttribute()) {
        attributes.put(
            parseAttributeKey(triggerAttributeContext.key),
            parseAttributeValue(triggerAttributeContext.value));
      }
    }
    if (ctx.uriClause() == null) {
      return new CreateTriggerStatement(
          parseIdentifier(ctx.triggerName.getText()),
          parseStringLiteral(ctx.className.getText()),
          "",
          false,
          ctx.triggerEventClause().BEFORE() != null
              ? TriggerEvent.BEFORE_INSERT
              : TriggerEvent.AFTER_INSERT,
          ctx.triggerType().STATELESS() != null ? TriggerType.STATELESS : TriggerType.STATEFUL,
          parsePrefixPath(ctx.prefixPath()),
          attributes);
    } else {
      String uriString = parseAndValidateURI(ctx.uriClause());
      return new CreateTriggerStatement(
          parseIdentifier(ctx.triggerName.getText()),
          parseStringLiteral(ctx.className.getText()),
          uriString,
          true,
          ctx.triggerEventClause().BEFORE() != null
              ? TriggerEvent.BEFORE_INSERT
              : TriggerEvent.AFTER_INSERT,
          ctx.triggerType().STATELESS() != null ? TriggerType.STATELESS : TriggerType.STATEFUL,
          parsePrefixPath(ctx.prefixPath()),
          attributes);
    }
  }

  // Drop Trigger =====================================================================
  @Override
  public Statement visitDropTrigger(IoTDBSqlParser.DropTriggerContext ctx) {
    return new DropTriggerStatement(parseIdentifier(ctx.triggerName.getText()));
  }

  // Show Trigger =====================================================================
  @Override
  public Statement visitShowTriggers(IoTDBSqlParser.ShowTriggersContext ctx) {
    return new ShowTriggersStatement();
  }

  @Override
  public Statement visitCreateService(IoTDBSqlParser.CreateServiceContext ctx) {
    throw new UnsupportedOperationException(SERVICE_MANAGEMENT_NOT_SUPPORTED);
  }

  @Override
  public Statement visitStartService(IoTDBSqlParser.StartServiceContext ctx) {
    throw new UnsupportedOperationException(SERVICE_MANAGEMENT_NOT_SUPPORTED);
  }

  @Override
  public Statement visitStopService(IoTDBSqlParser.StopServiceContext ctx) {
    throw new UnsupportedOperationException(SERVICE_MANAGEMENT_NOT_SUPPORTED);
  }

  @Override
  public Statement visitDropService(IoTDBSqlParser.DropServiceContext ctx) {
    throw new UnsupportedOperationException(SERVICE_MANAGEMENT_NOT_SUPPORTED);
  }

  @Override
  public Statement visitShowService(IoTDBSqlParser.ShowServiceContext ctx) {
    // show services on all DNs
    int dataNodeId = -1;
    if (ctx.ON() != null) {
      dataNodeId = Integer.parseInt(ctx.targetDataNodeId.getText());
    }
    return new ShowExternalServiceStatement(dataNodeId);
  }

  // Create PipePlugin =====================================================================
  @Override
  public Statement visitCreatePipePlugin(IoTDBSqlParser.CreatePipePluginContext ctx) {
    return new CreatePipePluginStatement(
        parseIdentifier(ctx.pluginName.getText()),
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null,
        parseStringLiteral(ctx.className.getText()),
        parseAndValidateURI(ctx.uriClause()));
  }

  // Drop PipePlugin =====================================================================
  @Override
  public Statement visitDropPipePlugin(IoTDBSqlParser.DropPipePluginContext ctx) {
    final DropPipePluginStatement dropPipePluginStatement = new DropPipePluginStatement();
    dropPipePluginStatement.setPluginName(parseIdentifier(ctx.pluginName.getText()));
    dropPipePluginStatement.setIfExists(ctx.IF() != null && ctx.EXISTS() != null);
    return dropPipePluginStatement;
  }

  // Show PipePlugins =====================================================================
  @Override
  public Statement visitShowPipePlugins(IoTDBSqlParser.ShowPipePluginsContext ctx) {
    return new ShowPipePluginsStatement();
  }

  // Show Child Paths =====================================================================
  @Override
  public Statement visitShowChildPaths(IoTDBSqlParser.ShowChildPathsContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowChildPathsStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowChildPathsStatement(new PartialPath(SqlConstant.getSingleRootArray()));
    }
  }

  // Show Child Nodes =====================================================================
  @Override
  public Statement visitShowChildNodes(IoTDBSqlParser.ShowChildNodesContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowChildNodesStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowChildNodesStatement(new PartialPath(SqlConstant.getSingleRootArray()));
    }
  }

  // Create CQ =====================================================================
  @Override
  public Statement visitCreateContinuousQuery(IoTDBSqlParser.CreateContinuousQueryContext ctx) {
    CreateContinuousQueryStatement statement = new CreateContinuousQueryStatement();

    statement.setCqId(parseIdentifier(ctx.cqId.getText()));

    QueryStatement queryBodyStatement =
        (QueryStatement) visitSelectStatement(ctx.selectStatement());
    queryBodyStatement.setCqQueryBody(true);
    statement.setQueryBodyStatement(queryBodyStatement);

    if (ctx.resampleClause() != null) {
      parseResampleClause(ctx.resampleClause(), statement);
    } else {
      QueryStatement queryStatement = statement.getQueryBodyStatement();
      if (!queryStatement.isGroupByTime()) {
        throw new SemanticException(
            "CQ: At least one of the parameters `every_interval` and `group_by_interval` needs to be specified.");
      }

      long interval =
          queryStatement.getGroupByTimeComponent().getInterval().getTotalDuration(currPrecision);
      statement.setEveryInterval(interval);
      statement.setStartTimeOffset(interval);
    }

    if (ctx.timeoutPolicyClause() != null) {
      parseTimeoutPolicyClause(ctx.timeoutPolicyClause(), statement);
    }

    return statement;
  }

  private void parseResampleClause(
      IoTDBSqlParser.ResampleClauseContext ctx, CreateContinuousQueryStatement statement) {
    if (ctx.EVERY() != null) {
      statement.setEveryInterval(
          DateTimeUtils.convertDurationStrToLong(ctx.everyInterval.getText()));
    } else {
      QueryStatement queryStatement = statement.getQueryBodyStatement();
      if (!queryStatement.isGroupByTime()) {
        throw new SemanticException(
            "CQ: At least one of the parameters `every_interval` and `group_by_interval` needs to be specified.");
      }
      statement.setEveryInterval(
          queryStatement.getGroupByTimeComponent().getInterval().getTotalDuration(currPrecision));
    }

    if (ctx.BOUNDARY() != null) {
      statement.setBoundaryTime(
          parseTimeValue(ctx.boundaryTime, CommonDateTimeUtils.currentTime()));
    }

    if (ctx.RANGE() != null) {
      statement.setStartTimeOffset(
          DateTimeUtils.convertDurationStrToLong(ctx.startTimeOffset.getText()));
      if (ctx.endTimeOffset != null) {
        statement.setEndTimeOffset(
            DateTimeUtils.convertDurationStrToLong(ctx.endTimeOffset.getText()));
      }
    } else {
      statement.setStartTimeOffset(statement.getEveryInterval());
    }
  }

  private void parseTimeoutPolicyClause(
      IoTDBSqlParser.TimeoutPolicyClauseContext ctx, CreateContinuousQueryStatement statement) {
    if (ctx.DISCARD() != null) {
      statement.setTimeoutPolicy(TimeoutPolicy.DISCARD);
    }
  }

  // Drop CQ =====================================================================
  @Override
  public Statement visitDropContinuousQuery(IoTDBSqlParser.DropContinuousQueryContext ctx) {
    return new DropContinuousQueryStatement(parseIdentifier(ctx.cqId.getText()));
  }

  // Show CQs =====================================================================
  @Override
  public Statement visitShowContinuousQueries(IoTDBSqlParser.ShowContinuousQueriesContext ctx) {
    return new ShowContinuousQueriesStatement();
  }

  // Create Logical View
  @Override
  public Statement visitCreateLogicalView(IoTDBSqlParser.CreateLogicalViewContext ctx) {
    CreateLogicalViewStatement createLogicalViewStatement = new CreateLogicalViewStatement();
    // parse target
    parseViewTargetPaths(
        ctx.viewTargetPaths(),
        createLogicalViewStatement::setTargetFullPaths,
        createLogicalViewStatement::setTargetPathsGroup,
        createLogicalViewStatement::setTargetIntoItem);
    // parse source
    parseViewSourcePaths(
        ctx.viewSourcePaths(),
        createLogicalViewStatement::setSourceFullPaths,
        createLogicalViewStatement::setSourcePathsGroup,
        createLogicalViewStatement::setSourceQueryStatement);

    return createLogicalViewStatement;
  }

  @Override
  public Statement visitDropLogicalView(IoTDBSqlParser.DropLogicalViewContext ctx) {
    DeleteLogicalViewStatement deleteLogicalViewStatement = new DeleteLogicalViewStatement();
    List<PartialPath> partialPaths = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      partialPaths.add(parsePrefixPath(prefixPathContext));
    }
    deleteLogicalViewStatement.setPathPatternList(partialPaths);
    return deleteLogicalViewStatement;
  }

  @Override
  public Statement visitShowLogicalView(IoTDBSqlParser.ShowLogicalViewContext ctx) {
    ShowLogicalViewStatement showLogicalViewStatement;
    if (ctx.prefixPath() != null) {
      showLogicalViewStatement = new ShowLogicalViewStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      showLogicalViewStatement =
          new ShowLogicalViewStatement(new PartialPath(SqlConstant.getSingleRootArray()));
    }
    if (ctx.timeseriesWhereClause() != null) {
      SchemaFilter schemaFilter = parseTimeseriesWhereClause(ctx.timeseriesWhereClause());
      showLogicalViewStatement.setSchemaFilter(schemaFilter);
    }
    if (ctx.rowPaginationClause() != null) {
      if (ctx.rowPaginationClause().limitClause() != null) {
        showLogicalViewStatement.setLimit(
            parseLimitClause(ctx.rowPaginationClause().limitClause()));
      }
      if (ctx.rowPaginationClause().offsetClause() != null) {
        showLogicalViewStatement.setOffset(
            parseOffsetClause(ctx.rowPaginationClause().offsetClause()));
      }
    }
    return showLogicalViewStatement;
  }

  @Override
  public Statement visitRenameLogicalView(IoTDBSqlParser.RenameLogicalViewContext ctx) {
    throw new SemanticException("Renaming view is not supported.");
  }

  @Override
  public Statement visitAlterLogicalView(IoTDBSqlParser.AlterLogicalViewContext ctx) {
    if (ctx.alterClause() == null) {
      final AlterLogicalViewStatement alterLogicalViewStatement = new AlterLogicalViewStatement();
      // parse target
      parseViewTargetPaths(
          ctx.viewTargetPaths(),
          alterLogicalViewStatement::setTargetFullPaths,
          alterLogicalViewStatement::setTargetPathsGroup,
          intoItem -> {
            if (intoItem != null) {
              throw new SemanticException(
                  "Can not use char '$' or into item in alter view statement.");
            }
          });
      // parse source
      parseViewSourcePaths(
          ctx.viewSourcePaths(),
          alterLogicalViewStatement::setSourceFullPaths,
          alterLogicalViewStatement::setSourcePathsGroup,
          alterLogicalViewStatement::setSourceQueryStatement);

      return alterLogicalViewStatement;
    } else {
      final AlterTimeSeriesStatement alterTimeSeriesStatement = new AlterTimeSeriesStatement(true);
      alterTimeSeriesStatement.setPath(parseFullPath(ctx.fullPath()));
      parseAlterClause(ctx.alterClause(), alterTimeSeriesStatement);
      if (alterTimeSeriesStatement.getAlias() != null) {
        throw new SemanticException("View doesn't support alias.");
      }
      return alterTimeSeriesStatement;
    }
  }

  // Parse suffix paths in logical view with into item
  private PartialPath parseViewPrefixPathWithInto(IoTDBSqlParser.PrefixPathContext ctx) {
    final List<IoTDBSqlParser.NodeNameContext> nodeNames = ctx.nodeName();
    final String[] path = new String[nodeNames.size() + 1];
    path[0] = ctx.ROOT().getText();
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i + 1] = parseNodeStringInIntoPath(nodeNames.get(i).getText());
    }
    return new PartialPath(path);
  }

  private PartialPath parseViewSuffixPatWithInto(IoTDBSqlParser.ViewSuffixPathsContext ctx) {
    final List<IoTDBSqlParser.NodeNameWithoutWildcardContext> nodeNamesWithoutStar =
        ctx.nodeNameWithoutWildcard();
    final String[] nodeList = new String[nodeNamesWithoutStar.size()];
    for (int i = 0; i < nodeNamesWithoutStar.size(); i++) {
      nodeList[i] = parseNodeStringInIntoPath(nodeNamesWithoutStar.get(i).getText());
    }
    return new PartialPath(nodeList);
  }

  private PartialPath parseViewSuffixPath(IoTDBSqlParser.ViewSuffixPathsContext ctx) {
    final List<IoTDBSqlParser.NodeNameWithoutWildcardContext> nodeNamesWithoutStar =
        ctx.nodeNameWithoutWildcard();
    final String[] nodeList = new String[nodeNamesWithoutStar.size()];
    for (int i = 0; i < nodeNamesWithoutStar.size(); i++) {
      nodeList[i] = parseNodeNameWithoutWildCard(nodeNamesWithoutStar.get(i));
    }
    return new PartialPath(nodeList);
  }

  // parse target paths in CreateLogicalView statement
  private void parseViewTargetPaths(
      IoTDBSqlParser.ViewTargetPathsContext ctx,
      Consumer<List<PartialPath>> setTargetFullPaths,
      BiConsumer<PartialPath, List<PartialPath>> setTargetPathsGroup,
      Consumer<IntoItem> setTargetIntoItem) {
    // Full paths
    if (ctx.fullPath() != null && !ctx.fullPath().isEmpty()) {
      final List<IoTDBSqlParser.FullPathContext> fullPathContextList = ctx.fullPath();
      final List<PartialPath> pathList = new ArrayList<>();
      for (IoTDBSqlParser.FullPathContext pathContext : fullPathContextList) {
        pathList.add(parseFullPath(pathContext));
      }
      setTargetFullPaths.accept(pathList);
    }
    // Prefix path and suffix paths
    if (ctx.prefixPath() != null
        && ctx.viewSuffixPaths() != null
        && !ctx.viewSuffixPaths().isEmpty()) {
      final IoTDBSqlParser.PrefixPathContext prefixPathContext = ctx.prefixPath();
      final List<IoTDBSqlParser.ViewSuffixPathsContext> suffixPathContextList =
          ctx.viewSuffixPaths();
      final List<PartialPath> suffixPathList = new ArrayList<>();
      PartialPath prefixPath = null;
      boolean isMultipleCreating = false;
      try {
        prefixPath = parsePrefixPath(prefixPathContext);
        for (IoTDBSqlParser.ViewSuffixPathsContext suffixPathContext : suffixPathContextList) {
          suffixPathList.add(parseViewSuffixPath(suffixPathContext));
        }
      } catch (SemanticException e) {
        // There is '$', '{', '}' in this statement
        isMultipleCreating = true;
        suffixPathList.clear();
      }
      if (!isMultipleCreating) {
        setTargetPathsGroup.accept(prefixPath, suffixPathList);
      } else {
        prefixPath = parseViewPrefixPathWithInto(prefixPathContext);
        for (final IoTDBSqlParser.ViewSuffixPathsContext suffixPathContext :
            suffixPathContextList) {
          suffixPathList.add(parseViewSuffixPatWithInto(suffixPathContext));
        }
        final List<String> intoMeasurementList = new ArrayList<>();
        for (PartialPath path : suffixPathList) {
          intoMeasurementList.add(path.toString());
        }
        final IntoItem intoItem = new IntoItem(prefixPath, intoMeasurementList, false);
        setTargetIntoItem.accept(intoItem);
      }
    }
  }

  // Parse source paths in CreateLogicalView statement
  private void parseViewSourcePaths(
      IoTDBSqlParser.ViewSourcePathsContext ctx,
      Consumer<List<PartialPath>> setSourceFullPaths,
      BiConsumer<PartialPath, List<PartialPath>> setSourcePathsGroup,
      Consumer<QueryStatement> setSourceQueryStatement) {
    // Full paths
    if (ctx.fullPath() != null && !ctx.fullPath().isEmpty()) {
      List<IoTDBSqlParser.FullPathContext> fullPathContextList = ctx.fullPath();
      List<PartialPath> pathList = new ArrayList<>();
      for (IoTDBSqlParser.FullPathContext pathContext : fullPathContextList) {
        pathList.add(parseFullPath(pathContext));
      }
      setSourceFullPaths.accept(pathList);
    }
    // prefix path and suffix paths
    if (ctx.prefixPath() != null
        && ctx.viewSuffixPaths() != null
        && !ctx.viewSuffixPaths().isEmpty()) {
      final IoTDBSqlParser.PrefixPathContext prefixPathContext = ctx.prefixPath();
      final PartialPath prefixPath = parsePrefixPath(prefixPathContext);
      final List<IoTDBSqlParser.ViewSuffixPathsContext> suffixPathContextList =
          ctx.viewSuffixPaths();
      final List<PartialPath> suffixPathList = new ArrayList<>();
      for (final IoTDBSqlParser.ViewSuffixPathsContext suffixPathContext : suffixPathContextList) {
        suffixPathList.add(parseViewSuffixPath(suffixPathContext));
      }
      setSourcePathsGroup.accept(prefixPath, suffixPathList);
    }
    if (ctx.selectClause() != null && ctx.fromClause() != null) {
      final QueryStatement queryStatement = new QueryStatement();
      queryStatement.setSelectComponent(parseSelectClause(ctx.selectClause(), queryStatement));
      queryStatement.setFromComponent(parseFromClause(ctx.fromClause()));
      setSourceQueryStatement.accept(queryStatement);
    }
  }

  // Create Model =====================================================================
  public static void validateModelId(String modelId) {
    if (modelId.length() < 2 || modelId.length() > 64) {
      throw new SemanticException("ModelId should be 2-64 characters");
    } else if (modelId.startsWith("_")) {
      throw new SemanticException("ModelId should not start with '_'");
    } else if (!modelId.matches("^[-\\w]*$")) {
      throw new SemanticException("ModelId can only contain letters, numbers, and underscores");
    }
  }

  private static List<String> convertToDeviceIdList(Token rawDeviceString) {
    String rawString = rawDeviceString.getText();
    rawString = rawString.substring(1, rawString.length() - 1);
    String[] deviceIdList = rawString.split(",");
    List<String> result = new ArrayList<>();
    for (String deviceId : deviceIdList) {
      deviceId = deviceId.trim();
      if (deviceId.equals("cpu")) {
        result.add("cpu");
        continue;
      }
      try {
        Integer.valueOf(deviceId);
      } catch (NumberFormatException e) {
        throw new SemanticException("Device id should be 'cpu' or integer");
      }
      result.add(deviceId);
    }
    return result;
  }

  @Override
  public Statement visitCreateModel(IoTDBSqlParser.CreateModelContext ctx) {
    if (ctx.uriClause() == null) {
      String modelId = ctx.modelId.getText();
      CreateTrainingStatement createTrainingStatement = new CreateTrainingStatement(modelId);
      if (ctx.hparamPair() != null) {
        Map<String, String> parameterList = new HashMap<>();
        for (IoTDBSqlParser.HparamPairContext hparamPairContext : ctx.hparamPair()) {
          parameterList.put(
              hparamPairContext.hparamKey.getText(), hparamPairContext.hparamValue().getText());
        }
        createTrainingStatement.setParameters(parameterList);
      }

      if (ctx.existingModelId != null) {
        createTrainingStatement.setExistingModelId(ctx.existingModelId.getText());
      }

      if (ctx.trainingData() == null) {
        throw new UnsupportedOperationException("data should not be set for model training");
      }

      List<PartialPath> targetPath = new ArrayList<>();
      List<List<Long>> timeRanges = new ArrayList<>();
      for (IoTDBSqlParser.DataElementContext dataElementContext :
          ctx.trainingData().dataElement()) {
        if (dataElementContext.timeRange() != null) {
          long currentTime = CommonDateTimeUtils.currentTime();
          long startTime = parseTimeValue(dataElementContext.timeRange().timeValue(0), currentTime);
          long endTime = parseTimeValue(dataElementContext.timeRange().timeValue(1), currentTime);
          timeRanges.add(Arrays.asList(startTime, endTime));
        } else {
          timeRanges.add(Collections.emptyList());
        }
        targetPath.add(parsePrefixPath(dataElementContext.pathPatternElement().prefixPath()));
      }
      createTrainingStatement.setTargetTimeRanges(timeRanges);
      createTrainingStatement.setTargetPathPatterns(targetPath);
      return createTrainingStatement;
    }
    String modelId = ctx.modelId.getText();
    validateModelId(modelId);
    CreateModelStatement createModelStatement = new CreateModelStatement();
    createModelStatement.setModelId(parseIdentifier(modelId));
    createModelStatement.setUri(parseAndValidateURI(ctx.uriClause()));
    return createModelStatement;
  }

  @Override
  public Statement visitLoadModel(IoTDBSqlParser.LoadModelContext ctx) {
    String modelId = parseIdentifier(ctx.existingModelId.getText());
    validateModelId(modelId);
    List<String> deviceIdList = convertToDeviceIdList(ctx.deviceIdList);
    return new LoadModelStatement(modelId, deviceIdList);
  }

  @Override
  public Statement visitUnloadModel(IoTDBSqlParser.UnloadModelContext ctx) {
    String modelId = parseIdentifier(ctx.existingModelId.getText());
    validateModelId(modelId);
    List<String> deviceIdList = convertToDeviceIdList(ctx.deviceIdList);
    return new UnloadModelStatement(modelId, deviceIdList);
  }

  // Drop Model =====================================================================
  @Override
  public Statement visitDropModel(IoTDBSqlParser.DropModelContext ctx) {
    return new DropModelStatement(parseIdentifier(ctx.modelId.getText()));
  }

  // Show Models =====================================================================
  @Override
  public Statement visitShowModels(IoTDBSqlParser.ShowModelsContext ctx) {
    ShowModelsStatement statement = new ShowModelsStatement();
    if (ctx.modelId != null) {
      statement.setModelId(parseIdentifier(ctx.modelId.getText()));
    }
    return statement;
  }

  @Override
  public Statement visitShowLoadedModels(IoTDBSqlParser.ShowLoadedModelsContext ctx) {
    return new ShowLoadedModelsStatement(
        ctx.deviceIdList != null ? convertToDeviceIdList(ctx.deviceIdList) : null);
  }

  public Statement visitShowAIDevices(IoTDBSqlParser.ShowAIDevicesContext ctx) {
    return new ShowAIDevicesStatement();
  }

  /** Data Manipulation Language (DML). */

  // Select Statement ========================================================================
  @Override
  public Statement visitSelectStatement(IoTDBSqlParser.SelectStatementContext ctx) {
    QueryStatement queryStatement = new QueryStatement();

    // parse SELECT & FROM
    queryStatement.setSelectComponent(parseSelectClause(ctx.selectClause(), queryStatement));
    queryStatement.setFromComponent(parseFromClause(ctx.fromClause()));

    // parse INTO
    if (ctx.intoClause() != null) {
      queryStatement.setIntoComponent(parseIntoClause(ctx.intoClause()));
    }

    // parse WHERE
    if (ctx.whereClause() != null) {
      queryStatement.setWhereCondition(parseWhereClause(ctx.whereClause()));
    }

    // parse GROUP BY
    if (ctx.groupByClause() != null) {
      Set<String> groupByKeys = new HashSet<>();
      List<IoTDBSqlParser.GroupByAttributeClauseContext> groupByAttributes =
          ctx.groupByClause().groupByAttributeClause();
      for (IoTDBSqlParser.GroupByAttributeClauseContext groupByAttribute : groupByAttributes) {
        if (groupByAttribute.TIME() != null || groupByAttribute.interval != null) {
          if (groupByKeys.contains("COMMON")) {
            throw new SemanticException(GROUP_BY_COMMON_ONLY_ONE_MSG);
          }

          groupByKeys.add("COMMON");
          queryStatement.setGroupByTimeComponent(parseGroupByTimeClause(groupByAttribute));
        } else if (groupByAttribute.LEVEL() != null) {
          if (groupByKeys.contains("LEVEL")) {
            throw new SemanticException("duplicated group by key: LEVEL");
          }

          groupByKeys.add("LEVEL");
          queryStatement.setGroupByLevelComponent(parseGroupByLevelClause(groupByAttribute));
        } else if (groupByAttribute.TAGS() != null) {
          if (groupByKeys.contains("TAGS")) {
            throw new SemanticException("duplicated group by key: TAGS");
          }

          groupByKeys.add("TAGS");
          queryStatement.setGroupByTagComponent(parseGroupByTagClause(groupByAttribute));
        } else if (groupByAttribute.VARIATION() != null) {
          if (groupByKeys.contains("COMMON")) {
            throw new SemanticException(GROUP_BY_COMMON_ONLY_ONE_MSG);
          }

          groupByKeys.add("COMMON");
          queryStatement.setGroupByComponent(
              parseGroupByClause(groupByAttribute, WindowType.VARIATION_WINDOW));
        } else if (groupByAttribute.CONDITION() != null) {
          if (groupByKeys.contains("COMMON")) {
            throw new SemanticException(GROUP_BY_COMMON_ONLY_ONE_MSG);
          }

          groupByKeys.add("COMMON");
          queryStatement.setGroupByComponent(
              parseGroupByClause(groupByAttribute, WindowType.CONDITION_WINDOW));
        } else if (groupByAttribute.SESSION() != null) {
          if (groupByKeys.contains("COMMON")) {
            throw new SemanticException(GROUP_BY_COMMON_ONLY_ONE_MSG);
          }

          groupByKeys.add("COMMON");
          queryStatement.setGroupByComponent(
              parseGroupByClause(groupByAttribute, WindowType.SESSION_WINDOW));
        } else if (groupByAttribute.COUNT() != null) {
          if (groupByKeys.contains("COMMON")) {
            throw new SemanticException(GROUP_BY_COMMON_ONLY_ONE_MSG);
          }

          groupByKeys.add("COMMON");
          queryStatement.setGroupByComponent(
              parseGroupByClause(groupByAttribute, WindowType.COUNT_WINDOW));

        } else {
          throw new SemanticException("Unknown GROUP BY type.");
        }
      }
    }

    // parse HAVING
    if (ctx.havingClause() != null) {
      queryStatement.setHavingCondition(parseHavingClause(ctx.havingClause()));
    }

    // parse ORDER BY
    if (ctx.orderByClause() != null) {
      queryStatement.setOrderByComponent(
          parseOrderByClause(
              ctx.orderByClause(),
              ImmutableSet.of(OrderByKey.TIME, OrderByKey.DEVICE, OrderByKey.TIMESERIES),
              true));
    }

    // parse FILL
    if (ctx.fillClause() != null) {
      queryStatement.setFillComponent(parseFillClause(ctx.fillClause()));
    }

    // parse ALIGN BY
    if (ctx.alignByClause() != null) {
      queryStatement.setResultSetFormat(parseAlignBy(ctx.alignByClause()));
    }

    if (ctx.paginationClause() != null) {
      // parse SLIMIT & SOFFSET
      if (ctx.paginationClause().seriesPaginationClause() != null) {
        if (ctx.paginationClause().seriesPaginationClause().slimitClause() != null) {
          queryStatement.setSeriesLimit(
              parseSLimitClause(ctx.paginationClause().seriesPaginationClause().slimitClause()));
        }
        if (ctx.paginationClause().seriesPaginationClause().soffsetClause() != null) {
          queryStatement.setSeriesOffset(
              parseSOffsetClause(ctx.paginationClause().seriesPaginationClause().soffsetClause()));
        }
      }

      // parse LIMIT & OFFSET
      if (ctx.paginationClause().rowPaginationClause() != null) {
        if (ctx.paginationClause().rowPaginationClause().limitClause() != null) {
          queryStatement.setRowLimit(
              parseLimitClause(ctx.paginationClause().rowPaginationClause().limitClause()));
        }
        if (ctx.paginationClause().rowPaginationClause().offsetClause() != null) {
          queryStatement.setRowOffset(
              parseOffsetClause(ctx.paginationClause().rowPaginationClause().offsetClause()));
        }
        if (canPushDownLimitOffsetToGroupByTime(queryStatement)) {
          pushDownLimitOffsetToTimeParameter(queryStatement, zoneId);
        }
      }
    }

    queryStatement.setUseWildcard(useWildcard);
    queryStatement.setLastLevelUseWildcard(lastLevelUseWildcard);
    return queryStatement;
  }

  // ---- Select Clause
  private SelectComponent parseSelectClause(
      IoTDBSqlParser.SelectClauseContext ctx, QueryStatement queryStatement) {
    SelectComponent selectComponent = new SelectComponent();

    // parse LAST
    if (ctx.LAST() != null) {
      selectComponent.setHasLast(true);
    }

    // parse resultColumn
    Map<String, Expression> aliasToColumnMap = new HashMap<>();
    for (IoTDBSqlParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      ResultColumn resultColumn = parseResultColumn(resultColumnContext);
      String columnName = resultColumn.getExpression().getExpressionString();
      // __endTime shouldn't be included in resultColumns
      if (columnName.equals(ColumnHeaderConstant.ENDTIME)) {
        queryStatement.setOutputEndTime(true);
        continue;
      }
      // don't support pure time in select
      if (columnName.equals(ColumnHeaderConstant.TIME)) {
        throw new SemanticException(
            "Time column is no need to appear in SELECT Clause explicitly, it will always be returned if possible");
      }
      if (resultColumn.hasAlias()) {
        String alias = resultColumn.getAlias();
        if (aliasToColumnMap.containsKey(alias)) {
          throw new SemanticException("duplicate alias in select clause");
        }
        aliasToColumnMap.put(alias, resultColumn.getExpression());
      }
      selectComponent.addResultColumn(resultColumn);
    }
    selectComponent.setAliasToColumnMap(aliasToColumnMap);

    return selectComponent;
  }

  private ResultColumn parseResultColumn(IoTDBSqlParser.ResultColumnContext resultColumnContext) {
    Expression expression = parseExpression(resultColumnContext.expression(), false);
    if (expression.isConstantOperand()) {
      throw new SemanticException("Constant operand is not allowed: " + expression);
    }
    String alias = null;
    if (resultColumnContext.AS() != null) {
      alias = parseAlias(resultColumnContext.alias());
    }
    ResultColumn.ColumnType columnType =
        ExpressionAnalyzer.identifyOutputColumnType(expression, true);
    return new ResultColumn(expression, alias, columnType);
  }

  // ---- From Clause
  private FromComponent parseFromClause(IoTDBSqlParser.FromClauseContext ctx) {
    FromComponent fromComponent = new FromComponent();
    List<IoTDBSqlParser.PrefixPathContext> prefixFromPaths = ctx.prefixPath();
    for (IoTDBSqlParser.PrefixPathContext prefixFromPath : prefixFromPaths) {
      PartialPath path = parsePrefixPath(prefixFromPath);
      fromComponent.addPrefixPath(path);
    }
    return fromComponent;
  }

  // ---- Into Clause
  private IntoComponent parseIntoClause(IoTDBSqlParser.IntoClauseContext ctx) {
    List<IntoItem> intoItems = new ArrayList<>();
    for (IoTDBSqlParser.IntoItemContext intoItemContext : ctx.intoItem()) {
      intoItems.add(parseIntoItem(intoItemContext));
    }
    return new IntoComponent(intoItems);
  }

  private IntoItem parseIntoItem(IoTDBSqlParser.IntoItemContext intoItemContext) {
    boolean isAligned = intoItemContext.ALIGNED() != null;
    PartialPath intoDevice = parseIntoPath(intoItemContext.intoPath());
    List<String> intoMeasurements =
        intoItemContext.nodeNameInIntoPath().stream()
            .map(this::parseNodeNameInIntoPath)
            .collect(Collectors.toList());
    return new IntoItem(intoDevice, intoMeasurements, isAligned);
  }

  private PartialPath parseIntoPath(IoTDBSqlParser.IntoPathContext intoPathContext) {
    if (intoPathContext instanceof IoTDBSqlParser.FullPathInIntoPathContext) {
      return parseFullPathInIntoPath((IoTDBSqlParser.FullPathInIntoPathContext) intoPathContext);
    } else {
      List<IoTDBSqlParser.NodeNameInIntoPathContext> nodeNames =
          ((IoTDBSqlParser.SuffixPathInIntoPathContext) intoPathContext).nodeNameInIntoPath();
      String[] path = new String[nodeNames.size()];
      for (int i = 0; i < nodeNames.size(); i++) {
        path[i] = parseNodeNameInIntoPath(nodeNames.get(i));
      }
      return new PartialPath(path);
    }
  }

  // ---- Where Clause
  private WhereCondition parseWhereClause(IoTDBSqlParser.WhereClauseContext ctx) {
    Expression predicate = parseExpression(ctx.expression(), true);
    return new WhereCondition(predicate);
  }

  // ---- Group By Clause
  private GroupByTimeComponent parseGroupByTimeClause(
      IoTDBSqlParser.GroupByAttributeClauseContext ctx) {
    GroupByTimeComponent groupByTimeComponent = new GroupByTimeComponent();

    // Parse time range
    if (ctx.timeRange() != null) {
      parseTimeRangeForGroupByTime(ctx.timeRange(), groupByTimeComponent);
      groupByTimeComponent.setLeftCRightO(ctx.timeRange().LS_BRACKET() != null);
    }

    // Parse time interval
    groupByTimeComponent.setInterval(DateTimeUtils.constructTimeDuration(ctx.interval.getText()));
    groupByTimeComponent.setOriginalInterval(ctx.interval.getText());
    if (groupByTimeComponent.getInterval().monthDuration == 0
        && groupByTimeComponent.getInterval().nonMonthDuration == 0) {
      throw new SemanticException(
          "The second parameter time interval should be a positive integer.");
    }

    // parse sliding step
    if (ctx.step != null) {
      groupByTimeComponent.setSlidingStep(DateTimeUtils.constructTimeDuration(ctx.step.getText()));
      groupByTimeComponent.setOriginalSlidingStep(ctx.step.getText());
    } else {
      groupByTimeComponent.setSlidingStep(groupByTimeComponent.getInterval());
      groupByTimeComponent.setOriginalSlidingStep(groupByTimeComponent.getOriginalInterval());
    }
    TimeDuration slidingStep = groupByTimeComponent.getSlidingStep();
    if (slidingStep.containsMonth()
        && Math.ceil(
                ((groupByTimeComponent.getEndTime() - groupByTimeComponent.getStartTime())
                    / (double) slidingStep.getMinTotalDuration(currPrecision)))
            >= 10000) {
      throw new SemanticException("The time windows may exceed 10000, please ensure your input.");
    }
    if (groupByTimeComponent.getSlidingStep().monthDuration == 0
        && groupByTimeComponent.getSlidingStep().nonMonthDuration == 0) {
      throw new SemanticException(
          "The third parameter time slidingStep should be a positive integer.");
    }
    return groupByTimeComponent;
  }

  /**
   * Parse time range (startTime and endTime) in group by time.
   *
   * @throws SemanticException if startTime is larger or equals to endTime in timeRange
   */
  private void parseTimeRangeForGroupByTime(
      IoTDBSqlParser.TimeRangeContext timeRange, GroupByTimeComponent groupByClauseComponent) {
    long currentTime = CommonDateTimeUtils.currentTime();
    long startTime = parseTimeValue(timeRange.timeValue(0), currentTime);
    long endTime = parseTimeValue(timeRange.timeValue(1), currentTime);
    groupByClauseComponent.setStartTime(startTime);
    groupByClauseComponent.setEndTime(endTime);
    if (startTime >= endTime) {
      throw new SemanticException("Start time should be smaller than endTime in GroupBy");
    }
  }

  private GroupByComponent parseGroupByClause(
      GroupByAttributeClauseContext ctx, WindowType windowType) {

    boolean ignoringNull = true;
    if (ctx.attributePair() != null
        && !ctx.attributePair().isEmpty()
        && ctx.attributePair().key.getText().equalsIgnoreCase(IGNORENULL)) {
      ignoringNull = Boolean.parseBoolean(ctx.attributePair().value.getText());
    }
    List<ExpressionContext> expressions = ctx.expression();
    if (windowType == WindowType.VARIATION_WINDOW) {
      ExpressionContext expressionContext = expressions.get(0);
      GroupByVariationComponent groupByVariationComponent = new GroupByVariationComponent();
      Expression expression = parseExpression(expressionContext, true);
      if (expression.isConstantOperand()) {
        throw new SemanticException(
            String.format(
                "Constant operand [%s] is not allowed in group by variation, there should be an expression",
                expression.getExpressionString()));
      }
      groupByVariationComponent.setControlColumnExpression(expression);
      groupByVariationComponent.setDelta(
          ctx.delta == null ? 0 : Double.parseDouble(ctx.delta.getText()));
      groupByVariationComponent.setIgnoringNull(ignoringNull);
      return groupByVariationComponent;
    } else if (windowType == WindowType.CONDITION_WINDOW) {
      ExpressionContext conditionExpressionContext = expressions.get(0);
      GroupByConditionComponent groupByConditionComponent = new GroupByConditionComponent();
      groupByConditionComponent.setControlColumnExpression(
          parseExpression(conditionExpressionContext, true));
      if (expressions.size() == 2) {
        groupByConditionComponent.setKeepExpression(parseExpression(expressions.get(1), true));
      } else {
        throw new SemanticException("Keep threshold in group by condition should be set");
      }
      groupByConditionComponent.setIgnoringNull(ignoringNull);
      return groupByConditionComponent;
    } else if (windowType == WindowType.SESSION_WINDOW) {
      long interval = DateTimeUtils.convertDurationStrToLong(ctx.timeInterval.getText());
      return new GroupBySessionComponent(interval);
    } else if (windowType == WindowType.COUNT_WINDOW) {
      ExpressionContext countExpressionContext = expressions.get(0);
      long countNumber = Long.parseLong(ctx.countNumber.getText());
      GroupByCountComponent groupByCountComponent = new GroupByCountComponent(countNumber);
      Expression expression = parseExpression(countExpressionContext, true);
      if (expression.isConstantOperand()) {
        throw new SemanticException(
            String.format(
                "Constant operand [%s] is not allowed in group by count, there should be an expression",
                expression.getExpressionString()));
      }
      groupByCountComponent.setControlColumnExpression(expression);
      groupByCountComponent.setIgnoringNull(ignoringNull);
      return groupByCountComponent;
    } else {
      throw new SemanticException("Unsupported window type");
    }
  }

  private GroupByLevelComponent parseGroupByLevelClause(
      IoTDBSqlParser.GroupByAttributeClauseContext ctx) {
    GroupByLevelComponent groupByLevelComponent = new GroupByLevelComponent();
    int[] levels = new int[ctx.INTEGER_LITERAL().size()];
    for (int i = 0; i < ctx.INTEGER_LITERAL().size(); i++) {
      levels[i] = Integer.parseInt(ctx.INTEGER_LITERAL().get(i).getText());
    }
    groupByLevelComponent.setLevels(levels);
    return groupByLevelComponent;
  }

  private GroupByTagComponent parseGroupByTagClause(
      IoTDBSqlParser.GroupByAttributeClauseContext ctx) {
    Set<String> tagKeys = new LinkedHashSet<>();
    for (IdentifierContext identifierContext : ctx.identifier()) {
      String key = parseIdentifier(identifierContext.getText());
      if (tagKeys.contains(key)) {
        throw new SemanticException("duplicated key in GROUP BY TAGS: " + key);
      }
      tagKeys.add(key);
    }
    return new GroupByTagComponent(new ArrayList<>(tagKeys));
  }

  // ---- Having Clause
  private HavingCondition parseHavingClause(IoTDBSqlParser.HavingClauseContext ctx) {
    Expression predicate = parseExpression(ctx.expression(), true);
    return new HavingCondition(predicate);
  }

  // ---- Order By Clause
  // all SortKeys should be contained by limitSet
  private OrderByComponent parseOrderByClause(
      IoTDBSqlParser.OrderByClauseContext ctx,
      ImmutableSet<String> limitSet,
      boolean allowExpression) {
    OrderByComponent orderByComponent = new OrderByComponent();
    Set<String> sortKeySet = new HashSet<>();
    for (IoTDBSqlParser.OrderByAttributeClauseContext orderByAttributeClauseContext :
        ctx.orderByAttributeClause()) {
      // if the order by clause is unique, then the following sort keys will be ignored
      if (orderByComponent.isUnique()) {
        break;
      }
      SortItem sortItem =
          parseOrderByAttributeClause(orderByAttributeClauseContext, limitSet, allowExpression);

      String sortKey = sortItem.getSortKey();
      if (sortKeySet.contains(sortKey)) {
        continue;
      } else {
        sortKeySet.add(sortKey);
      }

      if (sortItem.isExpression()) {
        orderByComponent.addExpressionSortItem(sortItem);
      } else {
        orderByComponent.addSortItem(sortItem);
      }
    }
    return orderByComponent;
  }

  private SortItem parseOrderByAttributeClause(
      IoTDBSqlParser.OrderByAttributeClauseContext ctx,
      ImmutableSet<String> limitSet,
      boolean allowExpression) {
    if (ctx.sortKey() != null) {
      String sortKey = ctx.sortKey().getText().toUpperCase();
      if (!limitSet.contains(sortKey)) {
        throw new SemanticException(
            String.format("ORDER BY: sort key[%s] is not contained in '%s'", sortKey, limitSet));
      }
      return new SortItem(sortKey, ctx.DESC() != null ? Ordering.DESC : Ordering.ASC);
    } else {
      if (!allowExpression) {
        throw new SemanticException(
            "ORDER BY expression is not supported for current statement, supported sort key: "
                + limitSet.toString());
      }
      Expression sortExpression = parseExpression(ctx.expression(), true);
      return new SortItem(
          sortExpression,
          ctx.DESC() != null ? Ordering.DESC : Ordering.ASC,
          ctx.FIRST() != null ? NullOrdering.FIRST : NullOrdering.LAST);
    }
  }

  // ---- Fill Clause
  public FillComponent parseFillClause(IoTDBSqlParser.FillClauseContext ctx) {
    FillComponent fillComponent = new FillComponent();
    if (ctx.LINEAR() != null) {
      fillComponent.setFillPolicy(FillPolicy.LINEAR);
    } else if (ctx.PREVIOUS() != null) {
      fillComponent.setFillPolicy(FillPolicy.PREVIOUS);

    } else if (ctx.constant() != null) {
      fillComponent.setFillPolicy(FillPolicy.CONSTANT);
      Literal fillValue = parseLiteral(ctx.constant());
      fillComponent.setFillValue(fillValue);
    } else {
      throw new SemanticException("Unknown FILL type.");
    }
    if (ctx.interval != null) {
      if (fillComponent.getFillPolicy() != FillPolicy.PREVIOUS) {
        throw new SemanticException(
            "Only FILL(PREVIOUS) support specifying the time duration threshold.");
      }
      fillComponent.setTimeDurationThreshold(
          DateTimeUtils.constructTimeDuration(ctx.interval.getText()));
    }
    return fillComponent;
  }

  private Literal parseLiteral(ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.boolean_literal() != null) {
      return new BooleanLiteral(text);
    } else if (constantContext.STRING_LITERAL() != null) {
      return new StringLiteral(parseStringLiteral(text));
    } else if (constantContext.INTEGER_LITERAL() != null) {
      return new LongLiteral(text);
    } else if (constantContext.realLiteral() != null) {
      return new DoubleLiteral(text);
    } else if (constantContext.dateExpression() != null) {
      return new LongLiteral(
          parseDateExpression(
              constantContext.dateExpression(),
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
    } else {
      throw new SemanticException("Unsupported constant value in FILL: " + text);
    }
  }

  // parse LIMIT & OFFSET
  private long parseLimitClause(IoTDBSqlParser.LimitClauseContext ctx) {
    long limit;
    try {
      limit = Long.parseLong(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException("Out of range. LIMIT <N>: N should be Int64.");
    }
    if (limit <= 0) {
      throw new SemanticException("LIMIT <N>: N should be greater than 0.");
    }
    return limit;
  }

  private long parseOffsetClause(IoTDBSqlParser.OffsetClauseContext ctx) {
    long offset;
    try {
      offset = Long.parseLong(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException(
          "Out of range. OFFSET <OFFSETValue>: OFFSETValue should be Int64.");
    }
    if (offset < 0) {
      throw new SemanticException("OFFSET <OFFSETValue>: OFFSETValue should >= 0.");
    }
    return offset;
  }

  // parse SLIMIT & SOFFSET
  private int parseSLimitClause(IoTDBSqlParser.SlimitClauseContext ctx) {
    int slimit;
    try {
      slimit = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException("Out of range. SLIMIT <SN>: SN should be Int32.");
    }
    if (slimit <= 0) {
      throw new SemanticException("SLIMIT <SN>: SN should be greater than 0.");
    }
    return slimit;
  }

  // parse SOFFSET
  public int parseSOffsetClause(IoTDBSqlParser.SoffsetClauseContext ctx) {
    int soffset;
    try {
      soffset = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException(
          "Out of range. SOFFSET <SOFFSETValue>: SOFFSETValue should be Int32.");
    }
    if (soffset < 0) {
      throw new SemanticException("SOFFSET <SOFFSETValue>: SOFFSETValue should >= 0.");
    }
    return soffset;
  }

  // ---- Align By Clause
  private ResultSetFormat parseAlignBy(IoTDBSqlParser.AlignByClauseContext ctx) {
    if (ctx.DEVICE() != null) {
      return ResultSetFormat.ALIGN_BY_DEVICE;
    } else {
      return ResultSetFormat.ALIGN_BY_TIME;
    }
  }

  // Insert Statement ========================================================================

  @Override
  public Statement visitInsertStatement(IoTDBSqlParser.InsertStatementContext ctx) {
    InsertStatement insertStatement = new InsertStatement();
    try {
      insertStatement.setDevice(
          DataNodeDevicePathCache.getInstance().getPartialPath(ctx.prefixPath().getText()));
    } catch (IllegalPathException e) {
      throw new SemanticException(e);
    }
    int timeIndex = parseInsertColumnSpec(ctx.insertColumnsSpec(), insertStatement);
    parseInsertValuesSpec(ctx.insertValuesSpec(), insertStatement, timeIndex);
    insertStatement.setAligned(ctx.ALIGNED() != null);
    return insertStatement;
  }

  private int parseInsertColumnSpec(
      IoTDBSqlParser.InsertColumnsSpecContext ctx, InsertStatement insertStatement) {
    List<String> measurementList = new ArrayList<>();
    int timeIndex = -1;

    for (int i = 0, size = ctx.insertColumn().size(); i < size; i++) {
      String measurement = parseInsertColumn(ctx.insertColumn(i));
      if ("time".equalsIgnoreCase(measurement) || "timestamp".equalsIgnoreCase(measurement)) {
        if (timeIndex != -1) {
          throw new SemanticException("One row should only have one time value");
        } else {
          timeIndex = i;
        }
      } else {
        measurementList.add(measurement);
      }
    }
    if (measurementList.isEmpty()) {
      throw new SemanticException("InsertStatement should contain at least one measurement");
    }
    insertStatement.setMeasurementList(measurementList.toArray(new String[0]));
    return timeIndex;
  }

  private String parseInsertColumn(IoTDBSqlParser.InsertColumnContext columnContext) {
    return parseNodeString(columnContext.getText());
  }

  private void parseInsertValuesSpec(
      IoTDBSqlParser.InsertValuesSpecContext ctx, InsertStatement insertStatement, int timeIndex) {
    List<IoTDBSqlParser.RowContext> rows = ctx.row();
    if (timeIndex == -1 && rows.size() != 1) {
      throw new SemanticException("need timestamps when insert multi rows");
    }
    List<Object[]> valuesList = new ArrayList<>();
    long[] timeArray = new long[rows.size()];
    for (int i = 0, size = rows.size(); i < size; i++) {
      IoTDBSqlParser.RowContext row = rows.get(i);
      // parse timestamp
      long timestamp;
      List<Object> valueList = new ArrayList<>();
      // using now() instead
      if (timeIndex == -1) {
        timestamp = CommonDateTimeUtils.currentTime();
      } else {
        timestamp = parseTimeValue(row.constant(timeIndex));
        TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
      }
      timeArray[i] = timestamp;

      // parse values
      List<ConstantContext> values = row.constant();
      for (int j = 0, columnCount = values.size(); j < columnCount; j++) {
        if (j != timeIndex) {
          if (values.get(j).dateExpression() != null) {
            valueList.add(
                parseDateExpression(
                    values.get(j).dateExpression(), CommonDateTimeUtils.currentTime()));
          } else if (values.get(j).STRING_LITERAL() != null) {
            valueList.add(parseStringLiteralInInsertValue(values.get(j).getText()));
          } else {
            valueList.add(values.get(j).getText());
          }
        }
      }

      valuesList.add(valueList.toArray(new Object[0]));
    }
    insertStatement.setTimes(timeArray);
    insertStatement.setValuesList(valuesList);
  }

  private long parseTimeValue(ConstantContext constant) {
    if (constant.INTEGER_LITERAL() != null) {
      try {
        if (constant.MINUS() != null) {
          return Long.parseLong("-" + constant.INTEGER_LITERAL().getText());
        }
        return Long.parseLong(constant.INTEGER_LITERAL().getText());
      } catch (NumberFormatException e) {
        throw new SemanticException(
            String.format(
                "Failed to parse the timestamp: "
                    + e.getMessage()
                    + "Current system timestamp precision is %s, "
                    + "please check whether the timestamp %s is correct.",
                TIMESTAMP_PRECISION,
                constant.INTEGER_LITERAL().getText()));
      }
    } else if (constant.dateExpression() != null) {
      return parseDateExpression(constant.dateExpression(), CommonDateTimeUtils.currentTime());
    } else {
      throw new SemanticException(String.format("Can not parse %s to time", constant));
    }
  }

  // Load File

  @Override
  public Statement visitLoadFile(IoTDBSqlParser.LoadFileContext ctx) {
    try {
      final LoadTsFileStatement loadTsFileStatement =
          new LoadTsFileStatement(parseStringLiteral(ctx.fileName.getText()));

      // if sql have with, return new load sql statement
      if (ctx.loadFileWithAttributeClauses() != null) {
        final Map<String, String> loadTsFileAttributes = new HashMap<>();
        for (IoTDBSqlParser.LoadFileWithAttributeClauseContext attributeContext :
            ctx.loadFileWithAttributeClauses().loadFileWithAttributeClause()) {
          final String key =
              parseStringLiteral(attributeContext.loadFileWithKey.getText()).trim().toLowerCase();
          final String value =
              parseStringLiteral(attributeContext.loadFileWithValue.getText()).trim().toLowerCase();

          LoadTsFileConfigurator.validateParameters(key, value);
          loadTsFileAttributes.put(key, value);
        }
        LoadTsFileConfigurator.validateSynonymParameters(loadTsFileAttributes);

        loadTsFileStatement.setLoadAttributes(loadTsFileAttributes);
        return loadTsFileStatement;
      }

      if (ctx.loadFileAttributeClauses() != null) {
        for (IoTDBSqlParser.LoadFileAttributeClauseContext attributeContext :
            ctx.loadFileAttributeClauses().loadFileAttributeClause()) {
          parseLoadFileAttributeClause(loadTsFileStatement, attributeContext);
        }
      }
      return loadTsFileStatement;
    } catch (FileNotFoundException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  /**
   * Used for parsing load tsfile, context will be one of "SCHEMA, LEVEL, METADATA", and maybe
   * followed by a recursion property statement.
   *
   * @param loadTsFileStatement the result statement, setting by clause context
   * @param ctx context of property statement
   * @throws SemanticException if AUTOREGISTER | SGLEVEL | VERIFY are not specified
   */
  private void parseLoadFileAttributeClause(
      LoadTsFileStatement loadTsFileStatement, IoTDBSqlParser.LoadFileAttributeClauseContext ctx) {
    if (ctx.ONSUCCESS() != null) {
      loadTsFileStatement.setDeleteAfterLoad(ctx.DELETE() != null);
    } else if (ctx.SGLEVEL() != null) {
      loadTsFileStatement.setDatabaseLevel(Integer.parseInt(ctx.INTEGER_LITERAL().getText()));
    } else if (ctx.VERIFY() != null) {
      loadTsFileStatement.setVerifySchema(Boolean.parseBoolean(ctx.boolean_literal().getText()));
    } else {
      throw new SemanticException(
          String.format(
              "Load tsfile format %s error, please input AUTOREGISTER | SGLEVEL | VERIFY.",
              ctx.getText()));
    }
  }

  /** Common Parsers. */

  // IoTDB Objects ========================================================================
  private MeasurementPath parseFullPath(IoTDBSqlParser.FullPathContext ctx) {
    List<IoTDBSqlParser.NodeNameWithoutWildcardContext> nodeNamesWithoutStar =
        ctx.nodeNameWithoutWildcard();
    String[] path = new String[nodeNamesWithoutStar.size() + 1];
    int i = 0;
    if (ctx.ROOT() != null) {
      path[0] = ctx.ROOT().getText();
    }
    for (IoTDBSqlParser.NodeNameWithoutWildcardContext nodeNameWithoutStar : nodeNamesWithoutStar) {
      i++;
      path[i] = parseNodeNameWithoutWildCard(nodeNameWithoutStar);
    }
    return new MeasurementPath(path);
  }

  // IoTDB Objects ========================================================================
  private PartialPath parseAlignedDevice(IoTDBSqlParser.FullPathContext ctx) {
    List<IoTDBSqlParser.NodeNameWithoutWildcardContext> nodeNamesWithoutStar =
        ctx.nodeNameWithoutWildcard();
    String[] path = new String[nodeNamesWithoutStar.size() + 1];
    int i = 0;
    if (ctx.ROOT() != null) {
      path[0] = ctx.ROOT().getText();
    }
    for (IoTDBSqlParser.NodeNameWithoutWildcardContext nodeNameWithoutStar : nodeNamesWithoutStar) {
      i++;
      path[i] = parseNodeNameWithoutWildCard(nodeNameWithoutStar);
    }
    return new PartialPath(path);
  }

  private PartialPath parseFullPathInExpression(
      IoTDBSqlParser.FullPathInExpressionContext ctx, boolean canUseFullPath) {
    List<IoTDBSqlParser.NodeNameContext> nodeNames = ctx.nodeName();
    int size = nodeNames.size();
    if (ctx.ROOT() != null) {
      if (!canUseFullPath) {
        // now full path cannot occur in SELECT only
        throw new SemanticException("Path can not start with root in select clause.");
      }
      size++;
    }
    String[] path = new String[size];
    if (ctx.ROOT() != null) {
      path[0] = ctx.ROOT().getText();
      for (int i = 0; i < nodeNames.size(); i++) {
        path[i + 1] = parseNodeName(nodeNames.get(i));
      }
    } else {
      for (int i = 0; i < nodeNames.size(); i++) {
        path[i] = parseNodeName(nodeNames.get(i));
      }
    }
    if (!lastLevelUseWildcard
        && !nodeNames.isEmpty()
        && !nodeNames.get(nodeNames.size() - 1).wildcard().isEmpty()) {
      lastLevelUseWildcard = true;
    }
    return new PartialPath(path);
  }

  private PartialPath parseFullPathInIntoPath(IoTDBSqlParser.FullPathInIntoPathContext ctx) {
    List<IoTDBSqlParser.NodeNameInIntoPathContext> nodeNames = ctx.nodeNameInIntoPath();
    String[] path = new String[nodeNames.size() + 1];
    int i = 0;
    if (ctx.ROOT() != null) {
      path[0] = ctx.ROOT().getText();
    }
    for (IoTDBSqlParser.NodeNameInIntoPathContext nodeName : nodeNames) {
      i++;
      path[i] = parseNodeNameInIntoPath(nodeName);
    }
    return new PartialPath(path);
  }

  private PartialPath parsePrefixPath(IoTDBSqlParser.PrefixPathContext ctx) {
    List<IoTDBSqlParser.NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size() + 1];
    path[0] = ctx.ROOT().getText();
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i + 1] = parseNodeName(nodeNames.get(i));
    }
    return new PartialPath(path);
  }

  private String parseNodeName(IoTDBSqlParser.NodeNameContext ctx) {
    if (!useWildcard && !ctx.wildcard().isEmpty()) {
      useWildcard = true;
    }
    return parseNodeString(ctx.getText());
  }

  private String parseNodeNameWithoutWildCard(IoTDBSqlParser.NodeNameWithoutWildcardContext ctx) {
    return parseNodeString(ctx.getText());
  }

  private String parseNodeNameInIntoPath(IoTDBSqlParser.NodeNameInIntoPathContext ctx) {
    return parseNodeStringInIntoPath(ctx.getText());
  }

  public static String parseNodeString(String nodeName) {
    if (nodeName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
        || nodeName.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
      return nodeName;
    }
    if (nodeName.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && nodeName.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      return PathUtils.removeBackQuotesIfNecessary(nodeName);
    }
    checkNodeName(nodeName);
    return nodeName;
  }

  private static String parseNodeStringInIntoPath(String nodeName) {
    if (nodeName.equals(IoTDBConstant.DOUBLE_COLONS)) {
      return nodeName;
    }
    if (nodeName.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && nodeName.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      // needn't remove back_quotes here, we will remove them after placeholders applied
      return nodeName;
    }
    checkNodeNameInIntoPath(nodeName);
    return nodeName;
  }

  private static void checkNodeName(String src) {
    // node name could start with * and end with *
    if (!TsFileConstant.NODE_NAME_PATTERN.matcher(src).matches()) {
      throw new SemanticException(
          String.format(
              "%s is illegal, unquoted node name can only consist of digits, characters and underscore, or start or end with wildcard",
              src));
    }
  }

  private static void checkNodeNameInIntoPath(String src) {
    // ${} are allowed
    if (!NODE_NAME_IN_INTO_PATH_PATTERN.matcher(src).matches()) {
      throw new SemanticException(
          String.format(
              "%s is illegal, unquoted node name in select into clause can only consist of digits, characters, $, { and }",
              src));
    }
  }

  private static void checkIdentifier(String src) {
    if (!TsFileConstant.IDENTIFIER_PATTERN.matcher(src).matches() || PathUtils.isRealNumber(src)) {
      throw new SemanticException(
          String.format(
              "%s is illegal, identifier not enclosed with backticks can only consist of digits, characters and underscore.",
              src));
    }
  }

  // Literals ========================================================================

  public long parseDateTimeFormat(String timestampStr) {
    if (timestampStr == null || "".equals(timestampStr.trim())) {
      throw new SemanticException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SqlConstant.NOW_FUNC)) {
      return CommonDateTimeUtils.currentTime();
    }
    try {
      return DateTimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SemanticException(
          String.format(
              "Input time format %s error. "
                  + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                  + "refer to user document for more info.",
              timestampStr));
    }
  }

  public static long parseDateTimeFormat(String timestampStr, long currentTime, ZoneId zoneId) {
    if (timestampStr == null || timestampStr.trim().isEmpty()) {
      throw new SemanticException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SqlConstant.NOW_FUNC)) {
      return currentTime;
    }
    try {
      return DateTimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SemanticException(
          String.format(
              "Input time format %s error. "
                  + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                  + "refer to user document for more info.",
              timestampStr));
    }
  }

  public static String parseStringLiteral(String src) {
    if (2 <= src.length()) {
      // do not unescape string
      String unWrappedString = src.substring(1, src.length() - 1);
      if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
        // replace "" with "
        String replaced = unWrappedString.replace("\"\"", "\"");
        return replaced.length() == 0 ? "" : replaced;
      }
      if ((src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'')) {
        // replace '' with '
        String replaced = unWrappedString.replace("''", "'");
        return replaced.length() == 0 ? "" : replaced;
      }
    }
    return src;
  }

  private String parseStringLiteralInInsertValue(String src) {
    if (2 <= src.length()
        && ((src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"')
            || (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\''))) {
      return "'" + parseStringLiteral(src) + "'";
    }
    return src;
  }

  public static String parseIdentifier(String src) {
    if (src.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && src.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      return src.substring(1, src.length() - 1)
          .replace(TsFileConstant.DOUBLE_BACK_QUOTE_STRING, TsFileConstant.BACK_QUOTE_STRING);
    }
    checkIdentifier(src);
    return src;
  }

  // Alias

  /** Function for parsing Alias of ResultColumn. */
  private String parseAlias(IoTDBSqlParser.AliasContext ctx) {
    String alias;
    if (ctx.constant() != null) {
      alias = parseConstant(ctx.constant());
    } else {
      alias = parseIdentifier(ctx.identifier().getText());
    }
    return alias;
  }

  /**
   * Function for parsing AliasNode.
   *
   * @throws SemanticException if the alias pattern is not supported
   */
  private String parseAliasNode(IoTDBSqlParser.AliasContext ctx) {
    String alias;
    if (ctx.constant() != null) {
      alias = parseConstant(ctx.constant());
      if (PathUtils.isRealNumber(alias)
          || !TsFileConstant.IDENTIFIER_PATTERN.matcher(alias).matches()) {
        throw new SemanticException("Not support for this alias, Please enclose in back quotes.");
      }
    } else {
      alias = parseNodeString(ctx.identifier().getText());
    }
    return alias;
  }

  /** Data Control Language (DCL). */
  private static String parseUsernameWithRoot(IoTDBSqlParser.UsernameWithRootContext ctx) {
    String src = ctx.identifier() != null ? ctx.identifier().getText() : "root";
    if (src.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && src.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      src =
          src.substring(1, src.length() - 1)
              .replace(TsFileConstant.DOUBLE_BACK_QUOTE_STRING, TsFileConstant.BACK_QUOTE_STRING);
    }
    return src;
  }

  // Create User
  @Override
  public Statement visitCreateUser(IoTDBSqlParser.CreateUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.CREATE_USER);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    authorStatement.setPassWord(parseStringLiteral(ctx.password.getText()));
    return authorStatement;
  }

  // Create Role
  @Override
  public Statement visitCreateRole(IoTDBSqlParser.CreateRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.CREATE_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    return authorStatement;
  }

  // Alter Password
  @Override
  public Statement visitAlterUser(IoTDBSqlParser.AlterUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.UPDATE_USER);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    authorStatement.setNewPassword(parseStringLiteral(ctx.password.getText()));
    return authorStatement;
  }

  @Override
  public Statement visitRenameUser(IoTDBSqlParser.RenameUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.RENAME_USER);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.username));
    authorStatement.setNewUsername(parseUsernameWithRoot(ctx.newUsername));
    return authorStatement;
  }

  // Unlock User
  @Override
  public Statement visitAlterUserAccountUnlock(IoTDBSqlParser.AlterUserAccountUnlockContext ctx) {
    String usernameWithRootWithOptionalHost = ctx.usernameWithRootWithOptionalHost().getText();
    String[] parts = usernameWithRootWithOptionalHost.split("@", 2);
    String username = parts[0];
    String host = parts.length > 1 ? parts[1] : null;

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.ACCOUNT_UNLOCK);
    authorStatement.setUserName(parseIdentifier(username));
    if (host != null) {
      authorStatement.setLoginAddr(parseStringLiteral(host));
    }
    return authorStatement;
  }

  // Grant User Privileges
  @Override
  public Statement visitGrantUser(IoTDBSqlParser.GrantUserContext ctx) {
    String[] privileges = parsePrivilege(ctx.privileges());
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream()
            .map(this::parsePrefixPath)
            .distinct()
            .collect(Collectors.toList());
    checkGrantRevokePrivileges(privileges, nodeNameList);
    String[] priviParsed = parsePrivilege(privileges);

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_USER);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    authorStatement.setPrivilegeList(priviParsed);
    authorStatement.setNodeNameList(nodeNameList);
    if (!CommonDescriptor.getInstance().getConfig().getEnableGrantOption()
        && ctx.grantOpt() != null) {
      throw new SemanticException(
          "Grant Option is disabled, Please check the parameter enable_grant_option.");
    }
    authorStatement.setGrantOpt(ctx.grantOpt() != null);

    return authorStatement;
  }

  // Grant Role Privileges

  @Override
  public Statement visitGrantRole(IoTDBSqlParser.GrantRoleContext ctx) {
    String[] privileges = parsePrivilege(ctx.privileges());
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream()
            .map(this::parsePrefixPath)
            .distinct()
            .collect(Collectors.toList());
    checkGrantRevokePrivileges(privileges, nodeNameList);
    String[] priviParsed = parsePrivilege(privileges);

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setPrivilegeList(priviParsed);
    authorStatement.setNodeNameList(nodeNameList);
    if (!CommonDescriptor.getInstance().getConfig().getEnableGrantOption()
        && ctx.grantOpt() != null) {
      throw new SemanticException(
          "Grant Option is disabled, Please check the parameter enable_grant_option.");
    }
    authorStatement.setGrantOpt(ctx.grantOpt() != null);
    return authorStatement;
  }

  // Grant User Role

  @Override
  public Statement visitGrantRoleToUser(IoTDBSqlParser.GrantRoleToUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    return authorStatement;
  }

  // Revoke User Privileges

  @Override
  public Statement visitRevokeUser(IoTDBSqlParser.RevokeUserContext ctx) {
    String[] privileges = parsePrivilege(ctx.privileges());
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream()
            .map(this::parsePrefixPath)
            .distinct()
            .collect(Collectors.toList());
    checkGrantRevokePrivileges(privileges, nodeNameList);
    String[] priviParsed = parsePrivilege(privileges);

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.REVOKE_USER);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    authorStatement.setPrivilegeList(priviParsed);
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  // Revoke Role Privileges

  @Override
  public Statement visitRevokeRole(IoTDBSqlParser.RevokeRoleContext ctx) {
    String[] privileges = parsePrivilege(ctx.privileges());
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream()
            .map(this::parsePrefixPath)
            .distinct()
            .collect(Collectors.toList());
    checkGrantRevokePrivileges(privileges, nodeNameList);
    String[] priviParsed = parsePrivilege(privileges);

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.REVOKE_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setPrivilegeList(priviParsed);
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  private void checkGrantRevokePrivileges(String[] privileges, List<PartialPath> nodeNameList) {
    // 1. all grant or revoke statements need target path.
    if (nodeNameList.isEmpty()) {
      throw new SemanticException("Statement needs target paths");
    }

    // 2. if privilege list has system privilege or "ALL", nodeNameList must only contain "root.**".
    boolean hasSystemPri = false;
    String errorPrivilegeName = "";

    for (String privilege : privileges) {
      if ("ALL".equalsIgnoreCase(privilege)
          || (!"READ".equalsIgnoreCase(privilege)
              && !"WRITE".equalsIgnoreCase(privilege)
              && !PrivilegeType.valueOf(privilege.toUpperCase()).isPathPrivilege())) {
        hasSystemPri = true;
        errorPrivilegeName = privilege.toUpperCase();
        break;
      }
    }
    if (hasSystemPri
        && !(nodeNameList.size() == 1
            && nodeNameList.contains(new PartialPath(ALL_RESULT_NODES)))) {
      throw new SemanticException(
          String.format("[%s] can only be set on path: root.**", errorPrivilegeName));
    }
  }

  private String[] parsePrivilege(String[] privileges) {
    Set<String> privSet = new HashSet<>();
    for (String priv : privileges) {
      if (priv.equalsIgnoreCase("READ")) {
        privSet.add("READ_SCHEMA");
        privSet.add("READ_DATA");
        continue;
      } else if (priv.equalsIgnoreCase("WRITE")) {
        privSet.add("WRITE_DATA");
        privSet.add("WRITE_SCHEMA");
        continue;
      } else if (priv.equalsIgnoreCase("ALL")) {
        for (PrivilegeType type : PrivilegeType.values()) {
          if (type.isRelationalPrivilege() || type.isDeprecated() || type.isHided()) {
            continue;
          }
          privSet.add(type.toString());
        }
        continue;
      }
      PrivilegeType privilegeType = PrivilegeType.valueOf(priv.toUpperCase());
      if (privilegeType.isDeprecated()) {
        throw new SemanticException(
            "Privilege type "
                + priv.toUpperCase()
                + " is deprecated, use "
                + privilegeType.getReplacedPrivilegeType()
                + " to instead it");
      }
      privSet.add(priv);
    }
    return privSet.toArray(new String[0]);
  }

  // Revoke Role From User
  @Override
  public Statement visitRevokeRoleFromUser(IoTDBSqlParser.RevokeRoleFromUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.REVOKE_USER_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    return authorStatement;
  }

  // Drop User

  @Override
  public Statement visitDropUser(IoTDBSqlParser.DropUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.DROP_USER);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    return authorStatement;
  }

  // Drop Role

  @Override
  public Statement visitDropRole(IoTDBSqlParser.DropRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.DROP_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    return authorStatement;
  }

  // List Users

  @Override
  public Statement visitListUser(IoTDBSqlParser.ListUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.LIST_USER);
    if (ctx.roleName != null) {
      authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    }
    return authorStatement;
  }

  // List Roles

  @Override
  public Statement visitListRole(IoTDBSqlParser.ListRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.LIST_ROLE);
    if (ctx.userName != null) {
      authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    }
    return authorStatement;
  }

  // List Privileges

  @Override
  public Statement visitListPrivilegesUser(IoTDBSqlParser.ListPrivilegesUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.LIST_USER_PRIVILEGE);
    authorStatement.setUserName(parseUsernameWithRoot(ctx.userName));
    return authorStatement;
  }

  // List Privileges of Roles On Specific Path

  @Override
  public Statement visitListPrivilegesRole(IoTDBSqlParser.ListPrivilegesRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.LIST_ROLE_PRIVILEGE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    return authorStatement;
  }

  private String[] parsePrivilege(IoTDBSqlParser.PrivilegesContext ctx) {
    List<IoTDBSqlParser.PrivilegeValueContext> privilegeList = ctx.privilegeValue();
    List<String> privileges = new ArrayList<>();
    for (IoTDBSqlParser.PrivilegeValueContext privilegeValue : privilegeList) {
      privileges.add(privilegeValue.getText());
    }
    return privileges.toArray(new String[0]);
  }

  // Create database
  @Override
  public Statement visitCreateDatabase(final IoTDBSqlParser.CreateDatabaseContext ctx) {
    final DatabaseSchemaStatement databaseSchemaStatement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
    final PartialPath path = parsePrefixPath(ctx.prefixPath());
    databaseSchemaStatement.setDatabasePath(path);
    if (ctx.databaseAttributesClause() != null) {
      parseDatabaseAttributesClause(databaseSchemaStatement, ctx.databaseAttributesClause());
    }
    return databaseSchemaStatement;
  }

  @Override
  public Statement visitAlterDatabase(IoTDBSqlParser.AlterDatabaseContext ctx) {
    DatabaseSchemaStatement databaseSchemaStatement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.ALTER);
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    databaseSchemaStatement.setDatabasePath(path);
    parseDatabaseAttributesClause(databaseSchemaStatement, ctx.databaseAttributesClause());
    return databaseSchemaStatement;
  }

  private void parseDatabaseAttributesClause(
      final DatabaseSchemaStatement databaseSchemaStatement,
      final IoTDBSqlParser.DatabaseAttributesClauseContext ctx) {
    for (final IoTDBSqlParser.DatabaseAttributeClauseContext attribute :
        ctx.databaseAttributeClause()) {
      final IoTDBSqlParser.DatabaseAttributeKeyContext attributeKey =
          attribute.databaseAttributeKey();
      if (attributeKey.TTL() != null) {
        final long ttl = Long.parseLong(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setTtl(ttl);
      } else if (attributeKey.TIME_PARTITION_INTERVAL() != null) {
        final long timePartitionInterval = Long.parseLong(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setTimePartitionInterval(timePartitionInterval);
      } else if (attributeKey.SCHEMA_REGION_GROUP_NUM() != null) {
        final int schemaRegionGroupNum = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setSchemaRegionGroupNum(schemaRegionGroupNum);
      } else if (attributeKey.DATA_REGION_GROUP_NUM() != null) {
        final int dataRegionGroupNum = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setDataRegionGroupNum(dataRegionGroupNum);
      }
    }
  }

  @Override
  public Statement visitSetTTL(IoTDBSqlParser.SetTTLContext ctx) {
    SetTTLStatement setTTLStatement = new SetTTLStatement();
    PartialPath path = parsePrefixPath(ctx.prefixPath());

    String ttlStr =
        ctx.INTEGER_LITERAL() != null ? ctx.INTEGER_LITERAL().getText() : ctx.INF().getText();
    long ttl =
        ttlStr.equalsIgnoreCase(IoTDBConstant.TTL_INFINITE)
            ? Long.MAX_VALUE
            : Long.parseLong(ttlStr);
    setTTLStatement.setPath(path);
    setTTLStatement.setTTL(ttl);
    return setTTLStatement;
  }

  @Override
  public Statement visitUnsetTTL(IoTDBSqlParser.UnsetTTLContext ctx) {
    UnSetTTLStatement unSetTTLStatement = new UnSetTTLStatement();
    PartialPath partialPath = parsePrefixPath(ctx.prefixPath());
    unSetTTLStatement.setPath(partialPath);
    return unSetTTLStatement;
  }

  @Override
  public Statement visitShowTTL(IoTDBSqlParser.ShowTTLContext ctx) {
    ShowTTLStatement showTTLStatement = new ShowTTLStatement();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      PartialPath partialPath = parsePrefixPath(prefixPathContext);
      showTTLStatement.addPathPatterns(partialPath);
    }
    return showTTLStatement;
  }

  @Override
  public Statement visitShowAllTTL(IoTDBSqlParser.ShowAllTTLContext ctx) {
    ShowTTLStatement showTTLStatement = new ShowTTLStatement();
    showTTLStatement.addPathPatterns(new PartialPath(SqlConstant.getSingleRootArray()));
    showTTLStatement.setShowAllTTL(true);
    return showTTLStatement;
  }

  @Override
  public Statement visitShowVariables(IoTDBSqlParser.ShowVariablesContext ctx) {
    return new ShowVariablesStatement();
  }

  @Override
  public Statement visitShowCluster(IoTDBSqlParser.ShowClusterContext ctx) {
    ShowClusterStatement showClusterStatement = new ShowClusterStatement();
    if (ctx.DETAILS() != null) {
      showClusterStatement.setDetails(true);
    }
    return showClusterStatement;
  }

  @Override
  public Statement visitShowClusterId(IoTDBSqlParser.ShowClusterIdContext ctx) {
    return new ShowClusterIdStatement();
  }

  @Override
  public Statement visitDropDatabase(IoTDBSqlParser.DropDatabaseContext ctx) {
    DeleteDatabaseStatement dropDatabaseStatement = new DeleteDatabaseStatement();
    List<IoTDBSqlParser.PrefixPathContext> prefixPathContexts = ctx.prefixPath();
    List<String> paths = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : prefixPathContexts) {
      paths.add(parsePrefixPath(prefixPathContext).getFullPath());
    }
    dropDatabaseStatement.setPrefixPath(paths);
    return dropDatabaseStatement;
  }

  // Explain ========================================================================
  @Override
  public Statement visitExplain(IoTDBSqlParser.ExplainContext ctx) {
    QueryStatement queryStatement = (QueryStatement) visitSelectStatement(ctx.selectStatement());
    if (ctx.ANALYZE() == null) {
      return new ExplainStatement(queryStatement);
    }
    ExplainAnalyzeStatement explainAnalyzeStatement = new ExplainAnalyzeStatement(queryStatement);
    if (ctx.VERBOSE() != null) {
      explainAnalyzeStatement.setVerbose(true);
    }
    return explainAnalyzeStatement;
  }

  @Override
  public Statement visitDeleteStatement(IoTDBSqlParser.DeleteStatementContext ctx) {
    DeleteDataStatement statement = new DeleteDataStatement();
    List<IoTDBSqlParser.PrefixPathContext> prefixPaths = ctx.prefixPath();
    List<MeasurementPath> pathList = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPath : prefixPaths) {
      pathList.add(new MeasurementPath(parsePrefixPath(prefixPath).getNodes()));
    }
    statement.setPathList(pathList);
    if (ctx.whereClause() != null) {
      WhereCondition whereCondition = parseWhereClause(ctx.whereClause());
      TimeRange timeRange = parseDeleteTimeRange(whereCondition.getPredicate());
      statement.setTimeRange(timeRange);
    } else {
      statement.setTimeRange(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }
    return statement;
  }

  private TimeRange parseDeleteTimeRange(Expression predicate) {
    if (predicate instanceof LogicAndExpression) {
      TimeRange leftTimeRange =
          parseDeleteTimeRange(((LogicAndExpression) predicate).getLeftExpression());
      TimeRange rightTimeRange =
          parseDeleteTimeRange(((LogicAndExpression) predicate).getRightExpression());
      long min = Math.max(leftTimeRange.getMin(), rightTimeRange.getMin());
      long max = Math.min(leftTimeRange.getMax(), rightTimeRange.getMax());
      if (min > max) {
        throw new SemanticException(DELETE_RANGE_COMPARISON_ERROR_MSG);
      }
      return new TimeRange(min, max);
    } else if (predicate instanceof CompareBinaryExpression) {
      if (((CompareBinaryExpression) predicate).getLeftExpression() instanceof TimestampOperand) {
        return parseTimeRangeForDeleteTimeRange(
            predicate.getExpressionType(),
            ((CompareBinaryExpression) predicate).getLeftExpression(),
            ((CompareBinaryExpression) predicate).getRightExpression());
      } else {
        return parseTimeRangeForDeleteTimeRange(
            predicate.getExpressionType(),
            ((CompareBinaryExpression) predicate).getRightExpression(),
            ((CompareBinaryExpression) predicate).getLeftExpression());
      }
    } else {
      throw new SemanticException(DELETE_RANGE_ERROR_MSG);
    }
  }

  private TimeRange parseTimeRangeForDeleteTimeRange(
      ExpressionType expressionType, Expression timeExpression, Expression valueExpression) {
    if (!(timeExpression instanceof TimestampOperand)
        || !(valueExpression instanceof ConstantOperand)) {
      throw new SemanticException(DELETE_ONLY_SUPPORT_TIME_EXP_ERROR_MSG);
    }

    if (((ConstantOperand) valueExpression).getDataType() != TSDataType.INT64) {
      throw new SemanticException("The datatype of timestamp should be LONG.");
    }

    long time = Long.parseLong(((ConstantOperand) valueExpression).getValueString());
    switch (expressionType) {
      case LESS_THAN:
        return new TimeRange(Long.MIN_VALUE, time - 1);
      case LESS_EQUAL:
        return new TimeRange(Long.MIN_VALUE, time);
      case GREATER_THAN:
        return new TimeRange(time + 1, Long.MAX_VALUE);
      case GREATER_EQUAL:
        return new TimeRange(time, Long.MAX_VALUE);
      case EQUAL_TO:
        return new TimeRange(time, time);
      default:
        throw new SemanticException(DELETE_RANGE_ERROR_MSG);
    }
  }

  /** function for parsing file path used by LOAD statement. */
  public String parseFilePath(String src) {
    return src.substring(1, src.length() - 1);
  }

  // Expression & Predicate ========================================================================

  private Expression parseExpression(
      IoTDBSqlParser.ExpressionContext context, boolean canUseFullPath) {
    if (context.unaryInBracket != null) {
      return parseExpression(context.unaryInBracket, canUseFullPath);
    }

    if (context.expressionAfterUnaryOperator != null) {
      if (context.MINUS() != null) {
        return new NegationExpression(
            parseExpression(context.expressionAfterUnaryOperator, canUseFullPath));
      }
      if (context.operator_not() != null) {
        return new LogicNotExpression(
            parseExpression(context.expressionAfterUnaryOperator, canUseFullPath));
      }
      return parseExpression(context.expressionAfterUnaryOperator, canUseFullPath);
    }

    if (context.leftExpression != null && context.rightExpression != null) {
      Expression leftExpression = parseExpression(context.leftExpression, canUseFullPath);
      Expression rightExpression = parseExpression(context.rightExpression, canUseFullPath);
      if (context.STAR() != null) {
        return new MultiplicationExpression(leftExpression, rightExpression);
      }
      if (context.DIV() != null) {
        return new DivisionExpression(leftExpression, rightExpression);
      }
      if (context.MOD() != null) {
        return new ModuloExpression(leftExpression, rightExpression);
      }
      if (context.PLUS() != null) {
        return new AdditionExpression(leftExpression, rightExpression);
      }
      if (context.MINUS() != null) {
        return new SubtractionExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_GT() != null) {
        return new GreaterThanExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_GTE() != null) {
        return new GreaterEqualExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_LT() != null) {
        return new LessThanExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_LTE() != null) {
        return new LessEqualExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_DEQ() != null || context.OPERATOR_SEQ() != null) {
        return new EqualToExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_NEQ() != null) {
        return new NonEqualExpression(leftExpression, rightExpression);
      }
      if (context.operator_and() != null) {
        return new LogicAndExpression(leftExpression, rightExpression);
      }
      if (context.operator_or() != null) {
        return new LogicOrExpression(leftExpression, rightExpression);
      }
      throw new UnsupportedOperationException();
    }

    if (context.unaryBeforeRegularOrLikeExpression != null) {
      if (context.REGEXP() != null) {
        return parseRegularExpression(context, canUseFullPath);
      }
      if (context.LIKE() != null) {
        return parseLikeExpression(context, canUseFullPath);
      }
      throw new UnsupportedOperationException();
    }

    if (context.unaryBeforeIsNullExpression != null) {
      return parseIsNullExpression(context, canUseFullPath);
    }

    if (context.firstExpression != null
        && context.secondExpression != null
        && context.thirdExpression != null) {
      Expression firstExpression = parseExpression(context.firstExpression, canUseFullPath);
      Expression secondExpression = parseExpression(context.secondExpression, canUseFullPath);
      Expression thirdExpression = parseExpression(context.thirdExpression, canUseFullPath);

      if (context.operator_between() != null) {
        return new BetweenExpression(
            firstExpression, secondExpression, thirdExpression, context.operator_not() != null);
      }
      throw new UnsupportedOperationException();
    }

    if (context.unaryBeforeInExpression != null) {
      return parseInExpression(context, canUseFullPath);
    }

    if (context.scalarFunctionExpression() != null) {
      return parseScalarFunctionExpression(context.scalarFunctionExpression(), canUseFullPath);
    }

    if (context.functionName() != null) {
      return parseFunctionExpression(context, canUseFullPath);
    }

    if (context.fullPathInExpression() != null) {
      return new TimeSeriesOperand(
          parseFullPathInExpression(context.fullPathInExpression(), canUseFullPath));
    }

    if (context.time != null) {
      return new TimestampOperand();
    }

    if (context.constant() != null && !context.constant().isEmpty()) {
      return parseConstantOperand(context.constant(0));
    }

    if (context.caseWhenThenExpression() != null) {
      return parseCaseWhenThenExpression(context.caseWhenThenExpression(), canUseFullPath);
    }

    throw new UnsupportedOperationException();
  }

  private Expression parseScalarFunctionExpression(
      IoTDBSqlParser.ScalarFunctionExpressionContext context, boolean canUseFullPath) {
    if (context.CAST() != null) {
      return parseCastFunction(context, canUseFullPath);
    } else if (context.REPLACE() != null) {
      return parseReplaceFunction(context, canUseFullPath);
    } else if (context.ROUND() != null) {
      return parseRoundFunction(context, canUseFullPath);
    } else if (context.SUBSTRING() != null) {
      return parseSubStrFunction(context, canUseFullPath);
    }
    throw new UnsupportedOperationException();
  }

  private Expression parseCastFunction(
      IoTDBSqlParser.ScalarFunctionExpressionContext castClause, boolean canUseFullPath) {
    FunctionExpression functionExpression = new FunctionExpression(CAST_FUNCTION);
    functionExpression.addExpression(parseExpression(castClause.castInput, canUseFullPath));
    functionExpression.addAttribute(CAST_TYPE, parseAttributeValue(castClause.attributeValue()));
    return functionExpression;
  }

  private Expression parseReplaceFunction(
      IoTDBSqlParser.ScalarFunctionExpressionContext replaceClause, boolean canUseFullPath) {
    FunctionExpression functionExpression = new FunctionExpression(REPLACE_FUNCTION);
    functionExpression.addExpression(parseExpression(replaceClause.text, canUseFullPath));
    functionExpression.addAttribute(REPLACE_FROM, parseStringLiteral(replaceClause.from.getText()));
    functionExpression.addAttribute(REPLACE_TO, parseStringLiteral(replaceClause.to.getText()));
    return functionExpression;
  }

  private Expression parseSubStrFunction(
      IoTDBSqlParser.ScalarFunctionExpressionContext subStrClause, boolean canUseFullPath) {
    FunctionExpression functionExpression = new FunctionExpression(SUBSTRING_FUNCTION);
    IoTDBSqlParser.SubStringExpressionContext subStringExpression =
        subStrClause.subStringExpression();
    functionExpression.addExpression(parseExpression(subStringExpression.input, canUseFullPath));
    if (subStringExpression.startPosition != null) {
      functionExpression.addAttribute(SUBSTRING_START, subStringExpression.startPosition.getText());
      if (subStringExpression.length != null) {
        functionExpression.addAttribute(SUBSTRING_LENGTH, subStringExpression.length.getText());
      }
    }
    if (subStringExpression.from != null) {
      functionExpression.addAttribute(SUBSTRING_IS_STANDARD, "0");
      functionExpression.addAttribute(
          SUBSTRING_START, parseStringLiteral(subStringExpression.from.getText()));
      if (subStringExpression.forLength != null) {
        functionExpression.addAttribute(SUBSTRING_LENGTH, subStringExpression.forLength.getText());
      }
    }
    return functionExpression;
  }

  private Expression parseRoundFunction(
      IoTDBSqlParser.ScalarFunctionExpressionContext roundClause, boolean canUseFullPath) {
    FunctionExpression functionExpression = new FunctionExpression(ROUND_FUNCTION);
    functionExpression.addExpression(parseExpression(roundClause.input, canUseFullPath));
    if (roundClause.places != null) {
      functionExpression.addAttribute(ROUND_PLACES, parseConstant(roundClause.constant()));
    }
    return functionExpression;
  }

  private CaseWhenThenExpression parseCaseWhenThenExpression(
      IoTDBSqlParser.CaseWhenThenExpressionContext context, boolean canUseFullPath) {
    // handle CASE
    Expression caseExpression = null;
    boolean simpleCase = false;
    if (context.caseExpression != null) {
      caseExpression = parseExpression(context.caseExpression, canUseFullPath);
      simpleCase = true;
    }
    // handle WHEN-THEN
    List<WhenThenExpression> whenThenList = new ArrayList<>();
    if (simpleCase) {
      for (IoTDBSqlParser.WhenThenExpressionContext whenThenExpressionContext :
          context.whenThenExpression()) {
        Expression when = parseExpression(whenThenExpressionContext.whenExpression, canUseFullPath);
        Expression then = parseExpression(whenThenExpressionContext.thenExpression, canUseFullPath);
        Expression comparison = new EqualToExpression(caseExpression, when);
        whenThenList.add(new WhenThenExpression(comparison, then));
      }
    } else {
      for (IoTDBSqlParser.WhenThenExpressionContext whenThenExpressionContext :
          context.whenThenExpression()) {
        whenThenList.add(
            new WhenThenExpression(
                parseExpression(whenThenExpressionContext.whenExpression, canUseFullPath),
                parseExpression(whenThenExpressionContext.thenExpression, canUseFullPath)));
      }
    }
    // handle ELSE
    Expression elseExpression = new NullOperand();
    if (context.elseExpression != null) {
      elseExpression = parseExpression(context.elseExpression, canUseFullPath);
    }
    return new CaseWhenThenExpression(whenThenList, elseExpression);
  }

  private Expression parseFunctionExpression(
      IoTDBSqlParser.ExpressionContext functionClause, boolean canUseFullPath) {
    FunctionExpression functionExpression =
        new FunctionExpression(parseIdentifier(functionClause.functionName().getText()));

    // expressions
    boolean hasNonPureConstantSubExpression = false;
    for (IoTDBSqlParser.ExpressionContext expression : functionClause.expression()) {
      Expression subexpression = parseExpression(expression, canUseFullPath);
      if (!subexpression.isConstantOperand()) {
        hasNonPureConstantSubExpression = true;
      }
      if (subexpression instanceof EqualToExpression) {
        Expression subLeftExpression = ((EqualToExpression) subexpression).getLeftExpression();
        Expression subRightExpression = ((EqualToExpression) subexpression).getRightExpression();
        if (subLeftExpression.isConstantOperand()
            && (!(subRightExpression.isConstantOperand()
                && ((ConstantOperand) subRightExpression).getDataType().equals(TSDataType.TEXT)))) {
          throw new SemanticException("Attributes of functions should be quoted with '' or \"\"");
        }
        if (subLeftExpression.isConstantOperand() && subRightExpression.isConstantOperand()) {
          // parse attribute
          functionExpression.addAttribute(
              ((ConstantOperand) subLeftExpression).getValueString(),
              ((ConstantOperand) subRightExpression).getValueString());
        } else {
          functionExpression.addExpression(subexpression);
        }
      } else {
        functionExpression.addExpression(subexpression);
      }
    }

    // It is not allowed to have function expressions like F(1, 1.0). There should be at least one
    // non-pure-constant sub-expression, otherwise the timestamp of the row cannot be inferred.
    if (!hasNonPureConstantSubExpression) {
      throw new SemanticException(
          "Invalid function expression, all the arguments are constant operands: "
              + functionClause.getText());
    }

    // check size of input expressions
    // type check of input expressions is put in ExpressionTypeAnalyzer
    if (functionExpression.isBuiltInAggregationFunctionExpression()) {
      checkAggregationFunctionInput(functionExpression);
    } else if (functionExpression.isBuiltInScalarFunctionExpression()) {
      checkBuiltInScalarFunctionInput(functionExpression);
    }
    return functionExpression;
  }

  private void checkAggregationFunctionInput(FunctionExpression functionExpression) {
    final String functionName = functionExpression.getFunctionName().toLowerCase();
    switch (functionName) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
      case SqlConstant.COUNT_TIME:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.EXTREME:
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.TIME_DURATION:
      case SqlConstant.MODE:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        checkFunctionExpressionInputSize(
            functionExpression.getExpressionString(),
            functionExpression.getExpressions().size(),
            1);
        return;
      case SqlConstant.COUNT_IF:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        checkFunctionExpressionInputSize(
            functionExpression.getExpressionString(),
            functionExpression.getExpressions().size(),
            2);
        return;
      default:
        throw new IllegalArgumentException(
            "Invalid Aggregation function: " + functionExpression.getFunctionName());
    }
  }

  private void checkBuiltInScalarFunctionInput(FunctionExpression functionExpression) {
    BuiltInScalarFunctionHelperFactory.createHelper(functionExpression.getFunctionName())
        .checkBuiltInScalarFunctionInputSize(functionExpression);
  }

  public static void checkFunctionExpressionInputSize(
      String expressionString, int actual, int... expected) {
    for (int expect : expected) {
      if (expect == actual) {
        return;
      }
    }
    throw new SemanticException(
        String.format(
            "Error size of input expressions. expression: %s, actual size: %s, expected size: %s.",
            expressionString, actual, Arrays.toString(expected)));
  }

  private Expression parseRegularExpression(ExpressionContext context, boolean canUseFullPath) {
    return new RegularExpression(
        parseExpression(context.unaryBeforeRegularOrLikeExpression, canUseFullPath),
        parseStringLiteral(String.valueOf(context.pattern.getText())),
        context.operator_not() != null);
  }

  private Expression parseLikeExpression(ExpressionContext context, boolean canUseFullPath) {
    return new LikeExpression(
        parseExpression(context.unaryBeforeRegularOrLikeExpression, canUseFullPath),
        parseStringLiteral(String.valueOf(context.pattern.getText())),
        context.ESCAPE() == null
            ? Optional.empty()
            : getEscapeCharacter(parseStringLiteral(String.valueOf(context.escapeSet.getText()))),
        context.operator_not() != null);
  }

  private Expression parseIsNullExpression(ExpressionContext context, boolean canUseFullPath) {
    return new IsNullExpression(
        parseExpression(context.unaryBeforeIsNullExpression, canUseFullPath),
        context.operator_not() != null);
  }

  private Expression parseInExpression(ExpressionContext context, boolean canUseFullPath) {
    Expression childExpression = parseExpression(context.unaryBeforeInExpression, canUseFullPath);
    LinkedHashSet<String> values = new LinkedHashSet<>();
    for (ConstantContext constantContext : context.constant()) {
      values.add(parseConstant(constantContext));
    }
    return new InExpression(childExpression, context.operator_not() != null, values);
  }

  private String parseConstant(ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.boolean_literal() != null
        || constantContext.INTEGER_LITERAL() != null
        || constantContext.realLiteral() != null) {
      return text;
    } else if (constantContext.STRING_LITERAL() != null) {
      return parseStringLiteral(text);
    } else if (constantContext.dateExpression() != null) {
      return String.valueOf(
          parseDateExpression(
              constantContext.dateExpression(),
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
    } else {
      throw new IllegalArgumentException("Unsupported constant value: " + text);
    }
  }

  private Expression parseConstantOperand(ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.boolean_literal() != null) {
      return new ConstantOperand(TSDataType.BOOLEAN, text);
    } else if (constantContext.STRING_LITERAL() != null) {
      return new ConstantOperand(TSDataType.TEXT, parseStringLiteral(text));
    } else if (constantContext.INTEGER_LITERAL() != null) {
      return new ConstantOperand(TSDataType.INT64, text);
    } else if (constantContext.realLiteral() != null) {
      return parseRealLiteral(text);
    } else if (constantContext.BINARY_LITERAL() != null) {
      BinaryLiteral binaryLiteral = new BinaryLiteral(text);
      return new ConstantOperand(
          TSDataType.BLOB, BaseEncoding.base16().encode(binaryLiteral.getValues()));
    } else if (constantContext.dateExpression() != null) {
      return new ConstantOperand(
          TSDataType.INT64,
          String.valueOf(
              parseDateExpression(
                  constantContext.dateExpression(),
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision())));
    } else {
      throw new SemanticException("Unsupported constant operand: " + text);
    }
  }

  private Expression parseRealLiteral(String value) {
    // 3.33 is float by default
    return new ConstantOperand(
        CONFIG.getFloatingStringInferType().equals(TSDataType.DOUBLE)
            ? TSDataType.DOUBLE
            : TSDataType.FLOAT,
        value);
  }

  /**
   * parse time expression, which is addition and subtraction expression of duration time, now() or
   * DataTimeFormat time.
   *
   * <p>eg. now() + 1d - 2h
   */
  public Long parseDateExpression(IoTDBSqlParser.DateExpressionContext ctx, String precision) {
    long time;
    time = parseDateTimeFormat(ctx.getChild(0).getText());
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time +=
            DateTimeUtils.convertDurationStrToLong(
                time, ctx.getChild(i + 1).getText(), precision, false);
      } else {
        time -=
            DateTimeUtils.convertDurationStrToLong(
                time, ctx.getChild(i + 1).getText(), precision, false);
      }
    }
    return time;
  }

  private Long parseDateExpression(IoTDBSqlParser.DateExpressionContext ctx, long currentTime) {
    long time;
    time = parseDateTimeFormat(ctx.getChild(0).getText(), currentTime, zoneId);
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time += DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText(), false);
      } else {
        time -= DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText(), false);
      }
    }
    return time;
  }

  private long parseTimeValue(IoTDBSqlParser.TimeValueContext ctx, long currentTime) {
    if (ctx.INTEGER_LITERAL() != null) {
      try {
        if (ctx.MINUS() != null) {
          return -Long.parseLong(ctx.INTEGER_LITERAL().getText());
        }
        return Long.parseLong(ctx.INTEGER_LITERAL().getText());
      } catch (NumberFormatException e) {
        throw new SemanticException(
            String.format("Can not parse %s to long value", ctx.INTEGER_LITERAL().getText()));
      }
    } else if (ctx.dateExpression() != null) {
      return parseDateExpression(ctx.dateExpression(), currentTime);
    } else {
      return parseDateTimeFormat(ctx.datetimeLiteral().getText(), currentTime, zoneId);
    }
  }

  /** Utils. */
  private void setMap(IoTDBSqlParser.AlterClauseContext ctx, Map<String, String> alterMap) {
    List<IoTDBSqlParser.AttributePairContext> tagsList = ctx.attributePair();
    String key;
    if (ctx.attributePair(0) != null) {
      for (IoTDBSqlParser.AttributePairContext attributePair : tagsList) {
        key = parseAttributeKey(attributePair.attributeKey());
        alterMap.computeIfPresent(
            key,
            (k, v) -> {
              throw new SemanticException(
                  String.format("There's duplicate [%s] in tag or attribute clause.", k));
            });
        alterMap.put(key, parseAttributeValue(attributePair.attributeValue()));
      }
    }
  }

  private Map<String, String> extractMap(
      List<IoTDBSqlParser.AttributePairContext> attributePair2,
      IoTDBSqlParser.AttributePairContext attributePair3) {
    Map<String, String> tags = new HashMap<>(attributePair2.size());
    if (attributePair3 != null) {
      String key;
      for (IoTDBSqlParser.AttributePairContext attributePair : attributePair2) {
        key = parseAttributeKey(attributePair.attributeKey());
        tags.computeIfPresent(
            key,
            (k, v) -> {
              throw new SemanticException(
                  String.format("There's duplicate [%s] in tag or attribute clause.", k));
            });
        tags.put(key, parseAttributeValue(attributePair.attributeValue()));
      }
    }
    return tags;
  }

  private String parseAttributeKey(IoTDBSqlParser.AttributeKeyContext ctx) {
    if (ctx.constant() != null) {
      return parseStringLiteral(ctx.getText());
    }
    return parseIdentifier(ctx.getText());
  }

  private String parseAttributeValue(IoTDBSqlParser.AttributeValueContext ctx) {
    if (ctx.constant() != null) {
      return parseStringLiteral(ctx.getText());
    }
    return parseIdentifier(ctx.getText());
  }

  // Flush

  @Override
  public Statement visitFlush(final IoTDBSqlParser.FlushContext ctx) {
    final FlushStatement flushStatement = new FlushStatement(StatementType.FLUSH);
    List<String> databases = null;
    if (ctx.boolean_literal() != null) {
      flushStatement.setSeq(Boolean.parseBoolean(ctx.boolean_literal().getText()));
    }
    flushStatement.setOnCluster(ctx.LOCAL() == null);
    if (ctx.prefixPath(0) != null) {
      databases = new ArrayList<>();
      for (final IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
        databases.add(parsePrefixPath(prefixPathContext).getFullPath());
      }
    }
    flushStatement.setDatabases(databases);
    return flushStatement;
  }

  // Clear Cache

  @Override
  public Statement visitClearCache(IoTDBSqlParser.ClearCacheContext ctx) {
    final ClearCacheStatement clearCacheStatement =
        new ClearCacheStatement(StatementType.CLEAR_CACHE);
    clearCacheStatement.setOnCluster(ctx.LOCAL() == null);

    if (ctx.SCHEMA() != null) {
      clearCacheStatement.setOptions(Collections.singleton(CacheClearOptions.TREE_SCHEMA));
    } else if (ctx.QUERY() != null) {
      clearCacheStatement.setOptions(Collections.singleton(CacheClearOptions.QUERY));
    } else if (ctx.ALL() != null) {
      clearCacheStatement.setOptions(
          new HashSet<>(
              Arrays.asList(
                  CacheClearOptions.TABLE_ATTRIBUTE,
                  CacheClearOptions.TREE_SCHEMA,
                  CacheClearOptions.QUERY)));
    } else {
      clearCacheStatement.setOptions(Collections.singleton(CacheClearOptions.DEFAULT));
    }

    return clearCacheStatement;
  }

  // Set Configuration

  @Override
  public Statement visitSetConfiguration(IoTDBSqlParser.SetConfigurationContext ctx) {
    SetConfigurationStatement setConfigurationStatement =
        new SetConfigurationStatement(StatementType.SET_CONFIGURATION);
    int nodeId =
        Integer.parseInt(ctx.INTEGER_LITERAL() == null ? "-1" : ctx.INTEGER_LITERAL().getText());
    Map<String, String> configItems = new HashMap<>();
    for (IoTDBSqlParser.SetConfigurationEntryContext entry : ctx.setConfigurationEntry()) {
      String key = parseStringLiteral(entry.STRING_LITERAL(0).getText()).trim();
      String value = parseStringLiteral(entry.STRING_LITERAL(1).getText()).trim();
      configItems.put(key, value);
    }
    setConfigurationStatement.setNodeId(nodeId);
    setConfigurationStatement.setConfigItems(configItems);
    return setConfigurationStatement;
  }

  // Start Repair Data

  @Override
  public Statement visitStartRepairData(IoTDBSqlParser.StartRepairDataContext ctx) {
    StartRepairDataStatement startRepairDataStatement =
        new StartRepairDataStatement(StatementType.START_REPAIR_DATA);
    startRepairDataStatement.setOnCluster(ctx.LOCAL() == null);
    return startRepairDataStatement;
  }

  // Stop Repair Data

  @Override
  public Statement visitStopRepairData(IoTDBSqlParser.StopRepairDataContext ctx) {
    StopRepairDataStatement stopRepairDataStatement =
        new StopRepairDataStatement(StatementType.STOP_REPAIR_DATA);
    stopRepairDataStatement.setOnCluster(ctx.LOCAL() == null);
    return stopRepairDataStatement;
  }

  // Load Configuration

  @Override
  public Statement visitLoadConfiguration(IoTDBSqlParser.LoadConfigurationContext ctx) {
    LoadConfigurationStatement loadConfigurationStatement =
        new LoadConfigurationStatement(StatementType.LOAD_CONFIGURATION);
    loadConfigurationStatement.setOnCluster(ctx.LOCAL() == null);
    return loadConfigurationStatement;
  }

  // Set System Status

  @Override
  public Statement visitSetSystemStatus(IoTDBSqlParser.SetSystemStatusContext ctx) {
    SetSystemStatusStatement setSystemStatusStatement = new SetSystemStatusStatement();
    setSystemStatusStatement.setOnCluster(ctx.LOCAL() == null);
    if (ctx.RUNNING() != null) {
      setSystemStatusStatement.setStatus(NodeStatus.Running);
    } else if (ctx.READONLY() != null) {
      setSystemStatusStatement.setStatus(NodeStatus.ReadOnly);
    } else {
      throw new SemanticException("Unknown system status in set system command.");
    }
    return setSystemStatusStatement;
  }

  // Kill Query
  @Override
  public Statement visitKillQuery(IoTDBSqlParser.KillQueryContext ctx) {
    if (ctx.queryId != null) {
      return new KillQueryStatement(parseStringLiteral(ctx.queryId.getText()));
    }
    return new KillQueryStatement();
  }

  // show query processlist

  @Override
  public Statement visitShowQueries(IoTDBSqlParser.ShowQueriesContext ctx) {
    ShowQueriesStatement showQueriesStatement = new ShowQueriesStatement();
    // parse WHERE
    if (ctx.whereClause() != null) {
      showQueriesStatement.setWhereCondition(parseWhereClause(ctx.whereClause()));
    }

    // parse ORDER BY
    if (ctx.orderByClause() != null) {
      showQueriesStatement.setOrderByComponent(
          parseOrderByClause(
              ctx.orderByClause(),
              ImmutableSet.of(
                  OrderByKey.TIME,
                  OrderByKey.QUERYID,
                  OrderByKey.DATANODEID,
                  OrderByKey.ELAPSEDTIME,
                  OrderByKey.STATEMENT),
              false));
    }

    // parse LIMIT & OFFSET
    if (ctx.rowPaginationClause() != null) {
      if (ctx.rowPaginationClause().limitClause() != null) {
        showQueriesStatement.setRowLimit(parseLimitClause(ctx.rowPaginationClause().limitClause()));
      }
      if (ctx.rowPaginationClause().offsetClause() != null) {
        showQueriesStatement.setRowOffset(
            parseOffsetClause(ctx.rowPaginationClause().offsetClause()));
      }
    }

    return showQueriesStatement;
  }

  // show region

  @Override
  public Statement visitShowRegions(IoTDBSqlParser.ShowRegionsContext ctx) {
    ShowRegionStatement showRegionStatement = new ShowRegionStatement();
    // TODO: Maybe add a show ConfigNode region in the future
    if (ctx.DATA() != null) {
      showRegionStatement.setRegionType(TConsensusGroupType.DataRegion);
    } else if (ctx.SCHEMA() != null) {
      showRegionStatement.setRegionType(TConsensusGroupType.SchemaRegion);
    } else {
      showRegionStatement.setRegionType(null);
    }

    if (ctx.OF() != null) {
      List<PartialPath> databases = null;
      if (ctx.prefixPath(0) != null) {
        databases = new ArrayList<>();
        for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
          databases.add(parsePrefixPath(prefixPathContext));
        }
      }
      showRegionStatement.setDatabases(databases);
    } else {
      showRegionStatement.setDatabases(null);
    }

    if (ctx.ON() != null) {
      List<Integer> nodeIds = new ArrayList<>();
      for (TerminalNode nodeid : ctx.INTEGER_LITERAL()) {
        nodeIds.add(Integer.parseInt(nodeid.getText()));
      }
      showRegionStatement.setNodeIds(nodeIds);
    } else {
      showRegionStatement.setNodeIds(null);
    }
    return showRegionStatement;
  }

  // show datanodes

  @Override
  public Statement visitShowDataNodes(IoTDBSqlParser.ShowDataNodesContext ctx) {
    return new ShowDataNodesStatement();
  }

  @Override
  public Statement visitShowAvailableUrls(IoTDBSqlParser.ShowAvailableUrlsContext ctx) {
    return new ShowAvailableUrlsStatement();
  }

  // show confignodes

  @Override
  public Statement visitShowConfigNodes(IoTDBSqlParser.ShowConfigNodesContext ctx) {
    return new ShowConfigNodesStatement();
  }

  @Override
  public Statement visitShowAINodes(IoTDBSqlParser.ShowAINodesContext ctx) {
    return new ShowAINodesStatement();
  }

  // device template

  @Override
  public Statement visitCreateSchemaTemplate(IoTDBSqlParser.CreateSchemaTemplateContext ctx) {
    String name = parseIdentifier(ctx.templateName.getText());
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> dataTypesList = new ArrayList<>();
    List<List<TSEncoding>> encodingsList = new ArrayList<>();
    List<List<CompressionType>> compressorsList = new ArrayList<>();

    if (ctx.ALIGNED() != null) {
      // aligned
      List<String> measurements = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      List<TSEncoding> encodings = new ArrayList<>();
      List<CompressionType> compressors = new ArrayList<>();
      for (IoTDBSqlParser.TemplateMeasurementClauseContext templateClauseContext :
          ctx.templateMeasurementClause()) {
        measurements.add(
            parseNodeNameWithoutWildCard(templateClauseContext.nodeNameWithoutWildcard()));
        parseAttributeClauseForSchemaTemplate(
            templateClauseContext.attributeClauses(), dataTypes, encodings, compressors);
      }
      measurementsList.add(measurements);
      dataTypesList.add(dataTypes);
      encodingsList.add(encodings);
      compressorsList.add(compressors);
    } else {
      // non-aligned
      for (IoTDBSqlParser.TemplateMeasurementClauseContext templateClauseContext :
          ctx.templateMeasurementClause()) {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressors = new ArrayList<>();
        measurements.add(
            parseNodeNameWithoutWildCard(templateClauseContext.nodeNameWithoutWildcard()));
        parseAttributeClauseForSchemaTemplate(
            templateClauseContext.attributeClauses(), dataTypes, encodings, compressors);
        measurementsList.add(measurements);
        dataTypesList.add(dataTypes);
        encodingsList.add(encodings);
        compressorsList.add(compressors);
      }
    }

    return new CreateSchemaTemplateStatement(
        name,
        measurementsList,
        dataTypesList,
        encodingsList,
        compressorsList,
        ctx.ALIGNED() != null);
  }

  @Override
  public Statement visitAlterSchemaTemplate(IoTDBSqlParser.AlterSchemaTemplateContext ctx) {
    String name = parseIdentifier(ctx.templateName.getText());
    List<String> measurements = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();

    for (IoTDBSqlParser.TemplateMeasurementClauseContext templateClauseContext :
        ctx.templateMeasurementClause()) {
      measurements.add(
          parseNodeNameWithoutWildCard(templateClauseContext.nodeNameWithoutWildcard()));
      parseAttributeClauseForSchemaTemplate(
          templateClauseContext.attributeClauses(), dataTypes, encodings, compressors);
    }

    return new AlterSchemaTemplateStatement(
        name,
        measurements,
        dataTypes,
        encodings,
        compressors,
        TemplateAlterOperationType.EXTEND_TEMPLATE);
  }

  void parseAttributeClauseForSchemaTemplate(
      IoTDBSqlParser.AttributeClausesContext ctx,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors) {
    if (ctx.aliasNodeName() != null) {
      throw new SemanticException("Device Template: alias is not supported yet.");
    }

    TSDataType dataType = parseDataTypeAttribute(ctx);
    dataTypes.add(dataType);

    Map<String, String> props = new HashMap<>();
    if (ctx.attributePair() != null) {
      for (int i = 0; i < ctx.attributePair().size(); i++) {
        props.put(
            parseAttributeKey(ctx.attributePair(i).attributeKey()).toLowerCase(),
            parseAttributeValue(ctx.attributePair(i).attributeValue()));
      }
    }

    TSEncoding encoding = IoTDBDescriptor.getInstance().getDefaultEncodingByType(dataType);
    if (props.containsKey(COLUMN_TIMESERIES_ENCODING.toLowerCase())) {
      String encodingString = props.get(COLUMN_TIMESERIES_ENCODING.toLowerCase()).toUpperCase();
      try {
        encoding = TSEncoding.valueOf(encodingString);
        encodings.add(encoding);
        props.remove(COLUMN_TIMESERIES_ENCODING.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported encoding: %s", encodingString));
      }
    } else {
      encodings.add(encoding);
    }

    CompressionType compressor = TSFileDescriptor.getInstance().getConfig().getCompressor(dataType);
    if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase())) {
      String compressorString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase()).toUpperCase();
      try {
        compressor = CompressionType.valueOf(compressorString);
        compressors.add(compressor);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported compressor: %s", compressorString));
      }
    } else if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase())) {
      String compressionString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase()).toUpperCase();
      try {
        compressor = CompressionType.valueOf(compressionString);
        compressors.add(compressor);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(
            String.format("Unsupported compression: %s", compressionString));
      }
    } else {
      compressors.add(compressor);
    }

    if (props.size() > 0) {
      throw new SemanticException("Device Template: property is not supported yet.");
    }

    if (ctx.tagClause() != null) {
      throw new SemanticException("Device Template: tag is not supported yet.");
    }

    if (ctx.attributeClause() != null) {
      throw new SemanticException("Device Template: attribute is not supported yet.");
    }
  }

  private TSDataType parseDataTypeAttribute(IoTDBSqlParser.AttributeClausesContext ctx) {
    TSDataType dataType = null;
    if (ctx.dataType != null) {
      if (ctx.attributeKey() != null
          && !parseAttributeKey(ctx.attributeKey())
              .equalsIgnoreCase(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE)) {
        throw new SemanticException("Expecting datatype");
      }
      String dataTypeString = ctx.dataType.getText().toUpperCase();
      try {
        dataType = TSDataType.valueOf(dataTypeString);
        if (TSDataType.UNKNOWN.equals(dataType) || TSDataType.VECTOR.equals(dataType)) {
          throw new SemanticException(String.format("Unsupported datatype: %s", dataTypeString));
        }
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported datatype: %s", dataTypeString));
      }
    }
    return dataType;
  }

  @Override
  public Statement visitShowSchemaTemplates(IoTDBSqlParser.ShowSchemaTemplatesContext ctx) {
    return new ShowSchemaTemplateStatement();
  }

  @Override
  public Statement visitShowNodesInSchemaTemplate(
      IoTDBSqlParser.ShowNodesInSchemaTemplateContext ctx) {
    String templateName = parseIdentifier(ctx.templateName.getText());
    return new ShowNodesInSchemaTemplateStatement(templateName);
  }

  @Override
  public Statement visitSetSchemaTemplate(IoTDBSqlParser.SetSchemaTemplateContext ctx) {
    String templateName = parseIdentifier(ctx.templateName.getText());
    return new SetSchemaTemplateStatement(templateName, parsePrefixPath(ctx.prefixPath()));
  }

  @Override
  public Statement visitShowPathsSetSchemaTemplate(
      IoTDBSqlParser.ShowPathsSetSchemaTemplateContext ctx) {
    String templateName = parseIdentifier(ctx.templateName.getText());
    return new ShowPathSetTemplateStatement(templateName);
  }

  @Override
  public Statement visitCreateTimeseriesUsingSchemaTemplate(
      IoTDBSqlParser.CreateTimeseriesUsingSchemaTemplateContext ctx) {
    ActivateTemplateStatement statement = new ActivateTemplateStatement();
    statement.setPath(parsePrefixPath(ctx.prefixPath()));
    return statement;
  }

  @Override
  public Statement visitShowPathsUsingSchemaTemplate(
      IoTDBSqlParser.ShowPathsUsingSchemaTemplateContext ctx) {
    PartialPath pathPattern;
    if (ctx.prefixPath() == null) {
      pathPattern = new PartialPath(SqlConstant.getSingleRootArray());
    } else {
      pathPattern = parsePrefixPath(ctx.prefixPath());
    }
    return new ShowPathsUsingTemplateStatement(
        pathPattern, parseIdentifier(ctx.templateName.getText()));
  }

  @Override
  public Statement visitDropTimeseriesOfSchemaTemplate(
      IoTDBSqlParser.DropTimeseriesOfSchemaTemplateContext ctx) {
    DeactivateTemplateStatement statement = new DeactivateTemplateStatement();
    if (ctx.templateName != null) {
      statement.setTemplateName(parseIdentifier(ctx.templateName.getText()));
    }
    List<PartialPath> pathPatternList = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      pathPatternList.add(parsePrefixPath(prefixPathContext));
    }
    statement.setPathPatternList(pathPatternList);
    return statement;
  }

  @Override
  public Statement visitUnsetSchemaTemplate(IoTDBSqlParser.UnsetSchemaTemplateContext ctx) {
    String templateName = parseIdentifier(ctx.templateName.getText());
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    return new UnsetSchemaTemplateStatement(templateName, path);
  }

  @Override
  public Statement visitDropSchemaTemplate(IoTDBSqlParser.DropSchemaTemplateContext ctx) {
    return new DropSchemaTemplateStatement(parseIdentifier(ctx.templateName.getText()));
  }

  // PIPE

  @Override
  public Statement visitCreatePipe(final IoTDBSqlParser.CreatePipeContext ctx) {
    final CreatePipeStatement createPipeStatement =
        new CreatePipeStatement(StatementType.CREATE_PIPE);

    final String pipeName;
    if (ctx.pipeName != null) {
      pipeName = parseIdentifier(ctx.pipeName.getText());
      createPipeStatement.setPipeName(pipeName);
    } else {
      throw new SemanticException(
          "Not support for this sql in CREATE PIPE, please enter pipe name.");
    }

    createPipeStatement.setIfNotExists(
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null);

    if (ctx.sourceAttributesClause() != null) {
      createPipeStatement.setSourceAttributes(
          parseSourceAttributesClause(ctx.sourceAttributesClause().sourceAttributeClause()));
    } else {
      createPipeStatement.setSourceAttributes(new HashMap<>());
    }
    if (ctx.processorAttributesClause() != null) {
      createPipeStatement.setProcessorAttributes(
          parseProcessorAttributesClause(
              ctx.processorAttributesClause().processorAttributeClause()));
    } else {
      createPipeStatement.setProcessorAttributes(new HashMap<>());
    }
    if (ctx.sinkAttributesClause() != null) {
      createPipeStatement.setSinkAttributes(
          parseSinkAttributesClause(ctx.sinkAttributesClause().sinkAttributeClause()));
    } else {
      createPipeStatement.setSinkAttributes(
          parseSinkAttributesClause(
              ctx.sinkAttributesWithoutWithSinkClause().sinkAttributeClause()));
    }

    return createPipeStatement;
  }

  @Override
  public Statement visitAlterPipe(IoTDBSqlParser.AlterPipeContext ctx) {
    final AlterPipeStatement alterPipeStatement = new AlterPipeStatement(StatementType.ALTER_PIPE);

    if (ctx.pipeName != null) {
      alterPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in ALTER PIPE, please enter pipe name.");
    }

    alterPipeStatement.setIfExists(ctx.IF() != null && ctx.EXISTS() != null);

    if (ctx.alterSourceAttributesClause() != null) {
      alterPipeStatement.setSourceAttributes(
          parseSourceAttributesClause(ctx.alterSourceAttributesClause().sourceAttributeClause()));
      alterPipeStatement.setReplaceAllSourceAttributes(
          Objects.nonNull(ctx.alterSourceAttributesClause().REPLACE()));
    } else {
      alterPipeStatement.setSourceAttributes(new HashMap<>());
      alterPipeStatement.setReplaceAllSourceAttributes(false);
    }

    if (ctx.alterProcessorAttributesClause() != null) {
      alterPipeStatement.setProcessorAttributes(
          parseProcessorAttributesClause(
              ctx.alterProcessorAttributesClause().processorAttributeClause()));
      alterPipeStatement.setReplaceAllProcessorAttributes(
          Objects.nonNull(ctx.alterProcessorAttributesClause().REPLACE()));
    } else {
      alterPipeStatement.setProcessorAttributes(new HashMap<>());
      alterPipeStatement.setReplaceAllProcessorAttributes(false);
    }

    if (ctx.alterSinkAttributesClause() != null) {
      alterPipeStatement.setSinkAttributes(
          parseSinkAttributesClause(ctx.alterSinkAttributesClause().sinkAttributeClause()));
      alterPipeStatement.setReplaceAllSinkAttributes(
          Objects.nonNull(ctx.alterSinkAttributesClause().REPLACE()));
    } else {
      alterPipeStatement.setSinkAttributes(new HashMap<>());
      alterPipeStatement.setReplaceAllSinkAttributes(false);
    }

    return alterPipeStatement;
  }

  private Map<String, String> parseSourceAttributesClause(
      List<IoTDBSqlParser.SourceAttributeClauseContext> contexts) {
    final Map<String, String> collectorMap = new HashMap<>();
    for (IoTDBSqlParser.SourceAttributeClauseContext context : contexts) {
      collectorMap.put(
          parseStringLiteral(context.sourceKey.getText()),
          parseStringLiteral(context.sourceValue.getText()));
    }
    return collectorMap;
  }

  private Map<String, String> parseProcessorAttributesClause(
      List<ProcessorAttributeClauseContext> contexts) {
    final Map<String, String> processorMap = new HashMap<>();
    for (IoTDBSqlParser.ProcessorAttributeClauseContext context : contexts) {
      processorMap.put(
          parseStringLiteral(context.processorKey.getText()),
          parseStringLiteral(context.processorValue.getText()));
    }
    return processorMap;
  }

  private Map<String, String> parseSinkAttributesClause(
      List<IoTDBSqlParser.SinkAttributeClauseContext> contexts) {
    final Map<String, String> SinkMap = new HashMap<>();
    for (IoTDBSqlParser.SinkAttributeClauseContext context : contexts) {
      SinkMap.put(
          parseStringLiteral(context.sinkKey.getText()),
          parseStringLiteral(context.sinkValue.getText()));
    }
    return SinkMap;
  }

  @Override
  public Statement visitDropPipe(IoTDBSqlParser.DropPipeContext ctx) {
    final DropPipeStatement dropPipeStatement = new DropPipeStatement(StatementType.DROP_PIPE);

    if (ctx.pipeName != null) {
      dropPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException("Not support for this sql in DROP PIPE, please enter pipename.");
    }

    dropPipeStatement.setIfExists(ctx.IF() != null && ctx.EXISTS() != null);

    return dropPipeStatement;
  }

  @Override
  public Statement visitStartPipe(IoTDBSqlParser.StartPipeContext ctx) {
    final StartPipeStatement startPipeStatement = new StartPipeStatement(StatementType.START_PIPE);

    if (ctx.pipeName != null) {
      startPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException("Not support for this sql in START PIPE, please enter pipename.");
    }

    return startPipeStatement;
  }

  @Override
  public Statement visitStopPipe(IoTDBSqlParser.StopPipeContext ctx) {
    final StopPipeStatement stopPipeStatement = new StopPipeStatement(StatementType.STOP_PIPE);

    if (ctx.pipeName != null) {
      stopPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException("Not support for this sql in STOP PIPE, please enter pipename.");
    }

    return stopPipeStatement;
  }

  @Override
  public Statement visitShowPipes(IoTDBSqlParser.ShowPipesContext ctx) {
    final ShowPipesStatement showPipesStatement = new ShowPipesStatement();

    if (ctx.pipeName != null) {
      showPipesStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    }
    showPipesStatement.setWhereClause(ctx.WHERE() != null);

    return showPipesStatement;
  }

  @Override
  public Statement visitCreateTopic(IoTDBSqlParser.CreateTopicContext ctx) {
    final CreateTopicStatement createTopicStatement = new CreateTopicStatement();

    if (ctx.topicName != null) {
      createTopicStatement.setTopicName(parseIdentifier(ctx.topicName.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in CREATE TOPIC, please enter topicName.");
    }

    createTopicStatement.setIfNotExists(
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null);

    if (ctx.topicAttributesClause() != null) {
      createTopicStatement.setTopicAttributes(
          parseTopicAttributesClause(ctx.topicAttributesClause().topicAttributeClause()));
    } else {
      createTopicStatement.setTopicAttributes(new HashMap<>());
    }

    return createTopicStatement;
  }

  private Map<String, String> parseTopicAttributesClause(
      List<IoTDBSqlParser.TopicAttributeClauseContext> contexts) {
    final Map<String, String> collectorMap = new HashMap<>();
    for (IoTDBSqlParser.TopicAttributeClauseContext context : contexts) {
      collectorMap.put(
          parseStringLiteral(context.topicKey.getText()),
          parseStringLiteral(context.topicValue.getText()));
    }
    return collectorMap;
  }

  @Override
  public Statement visitDropTopic(IoTDBSqlParser.DropTopicContext ctx) {
    final DropTopicStatement dropTopicStatement = new DropTopicStatement();

    if (ctx.topicName != null) {
      dropTopicStatement.setTopicName(parseIdentifier(ctx.topicName.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in DROP TOPIC, please enter topicName.");
    }

    dropTopicStatement.setIfExists(ctx.IF() != null && ctx.EXISTS() != null);

    return dropTopicStatement;
  }

  @Override
  public Statement visitShowTopics(IoTDBSqlParser.ShowTopicsContext ctx) {
    final ShowTopicsStatement showTopicsStatement = new ShowTopicsStatement();

    if (ctx.topicName != null) {
      showTopicsStatement.setTopicName(parseIdentifier(ctx.topicName.getText()));
    }

    return showTopicsStatement;
  }

  @Override
  public Statement visitShowSubscriptions(IoTDBSqlParser.ShowSubscriptionsContext ctx) {
    final ShowSubscriptionsStatement showSubscriptionsStatement = new ShowSubscriptionsStatement();

    if (ctx.topicName != null) {
      showSubscriptionsStatement.setTopicName(parseIdentifier(ctx.topicName.getText()));
    }

    return showSubscriptionsStatement;
  }

  @Override
  public Statement visitDropSubscription(IoTDBSqlParser.DropSubscriptionContext ctx) {
    final DropSubscriptionStatement dropSubscriptionStatement = new DropSubscriptionStatement();

    if (ctx.subscriptionId != null) {
      dropSubscriptionStatement.setSubscriptionId(parseIdentifier(ctx.subscriptionId.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in DROP SUBSCRIPTION, please enter subscriptionId.");
    }

    dropSubscriptionStatement.setIfExists(ctx.IF() != null && ctx.EXISTS() != null);

    return dropSubscriptionStatement;
  }

  @Override
  public Statement visitGetRegionId(IoTDBSqlParser.GetRegionIdContext ctx) {
    TConsensusGroupType type =
        ctx.DATA() == null ? TConsensusGroupType.SchemaRegion : TConsensusGroupType.DataRegion;
    GetRegionIdStatement getRegionIdStatement = new GetRegionIdStatement(type);
    if (ctx.database != null) {
      getRegionIdStatement.setDatabase(ctx.database.getText());
    } else {
      getRegionIdStatement.setDevice(Factory.DEFAULT_FACTORY.create(ctx.device.getText()));
    }
    getRegionIdStatement.setStartTimeStamp(-1L);
    getRegionIdStatement.setEndTimeStamp(Long.MAX_VALUE);

    if (ctx.timeRangeExpression != null) {
      Expression timeRangeExpression = parseExpression(ctx.timeRangeExpression, true);
      getRegionIdStatement = parseTimeRangeExpression(timeRangeExpression, getRegionIdStatement);
    }

    return getRegionIdStatement;
  }

  public GetRegionIdStatement parseTimeRangeExpression(
      Expression timeRangeExpression, GetRegionIdStatement getRegionIdStatement) {
    List<Expression> result = timeRangeExpression.getExpressions();
    if (timeRangeExpression.getExpressionType() == ExpressionType.LOGIC_AND) {
      getRegionIdStatement = parseTimeRangeExpression(result.get(0), getRegionIdStatement);
      getRegionIdStatement = parseTimeRangeExpression(result.get(1), getRegionIdStatement);
    } else if (result.get(0).getExpressionType() == ExpressionType.TIMESTAMP
        && result.get(1) instanceof ConstantOperand
        && ((ConstantOperand) result.get(1)).getDataType() == TSDataType.INT64) {
      ExpressionType tmpType = timeRangeExpression.getExpressionType();
      long timestamp = Long.parseLong(((ConstantOperand) result.get(1)).getValueString());
      switch (tmpType) {
        case EQUAL_TO:
          getRegionIdStatement.setStartTimeStamp(
              Math.max(getRegionIdStatement.getStartTimeStamp(), timestamp));
          getRegionIdStatement.setEndTimeStamp(
              Math.min(getRegionIdStatement.getEndTimeStamp(), timestamp));
          break;
        case GREATER_EQUAL:
          getRegionIdStatement.setStartTimeStamp(
              Math.max(getRegionIdStatement.getStartTimeStamp(), timestamp));
          break;
        case GREATER_THAN:
          getRegionIdStatement.setStartTimeStamp(
              Math.max(getRegionIdStatement.getStartTimeStamp(), timestamp + 1));
          break;
        case LESS_EQUAL:
          getRegionIdStatement.setEndTimeStamp(
              Math.min(getRegionIdStatement.getEndTimeStamp(), timestamp));
          break;
        case LESS_THAN:
          getRegionIdStatement.setEndTimeStamp(
              Math.min(getRegionIdStatement.getEndTimeStamp(), timestamp - 1));
          break;
        default:
          throw new UnsupportedOperationException();
      }
    } else {
      throw new SemanticException("Get region id statement‘ expression must be a time expression");
    }
    return getRegionIdStatement;
  }

  @Override
  public Statement visitGetSeriesSlotList(IoTDBSqlParser.GetSeriesSlotListContext ctx) {
    TConsensusGroupType type =
        ctx.DATA() == null ? TConsensusGroupType.SchemaRegion : TConsensusGroupType.DataRegion;
    return new GetSeriesSlotListStatement(ctx.database.getText(), type);
  }

  @Override
  public Statement visitGetTimeSlotList(IoTDBSqlParser.GetTimeSlotListContext ctx) {
    GetTimeSlotListStatement getTimeSlotListStatement = new GetTimeSlotListStatement();
    if (ctx.database != null) {
      getTimeSlotListStatement.setDatabase(ctx.database.getText());
    } else if (ctx.device != null) {
      getTimeSlotListStatement.setDevice(Factory.DEFAULT_FACTORY.create(ctx.device.getText()));
    } else if (ctx.regionId != null) {
      getTimeSlotListStatement.setRegionId(Integer.parseInt(ctx.regionId.getText()));
    }
    if (ctx.startTime != null) {
      long timestamp = parseTimeValue(ctx.startTime, CommonDateTimeUtils.currentTime());
      getTimeSlotListStatement.setStartTime(timestamp);
    }
    if (ctx.endTime != null) {
      long timestamp = parseTimeValue(ctx.endTime, CommonDateTimeUtils.currentTime());
      getTimeSlotListStatement.setEndTime(timestamp);
    }
    return getTimeSlotListStatement;
  }

  @Override
  public Statement visitCountTimeSlotList(IoTDBSqlParser.CountTimeSlotListContext ctx) {
    CountTimeSlotListStatement countTimeSlotListStatement = new CountTimeSlotListStatement();
    if (ctx.database != null) {
      countTimeSlotListStatement.setDatabase(ctx.database.getText());
    } else if (ctx.device != null) {
      countTimeSlotListStatement.setDevice(Factory.DEFAULT_FACTORY.create(ctx.device.getText()));
    } else if (ctx.regionId != null) {
      countTimeSlotListStatement.setRegionId(Integer.parseInt(ctx.regionId.getText()));
    }
    if (ctx.startTime != null) {
      countTimeSlotListStatement.setStartTime(Long.parseLong(ctx.startTime.getText()));
    }
    if (ctx.endTime != null) {
      countTimeSlotListStatement.setEndTime(Long.parseLong(ctx.endTime.getText()));
    }
    return countTimeSlotListStatement;
  }

  @Override
  public Statement visitMigrateRegion(IoTDBSqlParser.MigrateRegionContext ctx) {
    return new MigrateRegionStatement(
        Integer.parseInt(ctx.regionId.getText()),
        Integer.parseInt(ctx.fromId.getText()),
        Integer.parseInt(ctx.toId.getText()));
  }

  @Override
  public Statement visitReconstructRegion(IoTDBSqlParser.ReconstructRegionContext ctx) {
    int dataNodeId = Integer.parseInt(ctx.targetDataNodeId.getText());
    List<Integer> regionIds =
        ctx.regionIds.stream()
            .map(Token::getText)
            .map(Integer::parseInt)
            .collect(Collectors.toList());
    return new ReconstructRegionStatement(dataNodeId, regionIds);
  }

  @Override
  public Statement visitExtendRegion(IoTDBSqlParser.ExtendRegionContext ctx) {
    List<Integer> regionIds =
        ctx.regionIds.stream().map(token -> Integer.parseInt(token.getText())).collect(toList());
    return new ExtendRegionStatement(regionIds, Integer.parseInt(ctx.targetDataNodeId.getText()));
  }

  @Override
  public Statement visitRemoveRegion(IoTDBSqlParser.RemoveRegionContext ctx) {
    List<Integer> regionIds =
        ctx.regionIds.stream().map(token -> Integer.parseInt(token.getText())).collect(toList());
    return new RemoveRegionStatement(regionIds, Integer.parseInt(ctx.targetDataNodeId.getText()));
  }

  @Override
  public Statement visitRemoveDataNode(IoTDBSqlParser.RemoveDataNodeContext ctx) {
    List<Integer> nodeIds =
        Collections.singletonList(Integer.parseInt(ctx.INTEGER_LITERAL().getText()));
    return new RemoveDataNodeStatement(nodeIds);
  }

  @Override
  public Statement visitRemoveAINode(IoTDBSqlParser.RemoveAINodeContext ctx) {
    return new RemoveAINodeStatement();
  }

  @Override
  public Statement visitRemoveConfigNode(IoTDBSqlParser.RemoveConfigNodeContext ctx) {
    Integer nodeId = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    return new RemoveConfigNodeStatement(nodeId);
  }

  @Override
  public Statement visitVerifyConnection(IoTDBSqlParser.VerifyConnectionContext ctx) {
    return new TestConnectionStatement(ctx.DETAILS() != null);
  }

  // Quota
  @Override
  public Statement visitSetSpaceQuota(IoTDBSqlParser.SetSpaceQuotaContext ctx) {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      throw new SemanticException(LIMIT_CONFIGURATION_ENABLED_ERROR_MSG);
    }
    SetSpaceQuotaStatement setSpaceQuotaStatement = new SetSpaceQuotaStatement();
    List<IoTDBSqlParser.PrefixPathContext> prefixPathContexts = ctx.prefixPath();
    List<String> paths = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : prefixPathContexts) {
      paths.add(parsePrefixPath(prefixPathContext).getFullPath());
    }
    setSpaceQuotaStatement.setPrefixPathList(paths);

    Map<String, String> quotas = new HashMap<>();
    for (IoTDBSqlParser.AttributePairContext attributePair : ctx.attributePair()) {
      quotas.put(
          parseAttributeKey(attributePair.attributeKey()),
          parseAttributeValue(attributePair.attributeValue()));
    }

    quotas
        .keySet()
        .forEach(
            quotaType -> {
              switch (quotaType) {
                case IoTDBConstant.COLUMN_DEVICES:
                  break;
                case IoTDBConstant.COLUMN_TIMESERIES:
                  break;
                case IoTDBConstant.SPACE_QUOTA_DISK:
                  break;
                default:
                  throw new SemanticException("Wrong space quota type: " + quotaType);
              }
            });

    if (quotas.containsKey(IoTDBConstant.COLUMN_DEVICES)) {
      if (quotas.get(IoTDBConstant.COLUMN_DEVICES).equals(IoTDBConstant.QUOTA_UNLIMITED)) {
        setSpaceQuotaStatement.setDeviceNum(IoTDBConstant.UNLIMITED_VALUE);
      } else if (Long.parseLong(quotas.get(IoTDBConstant.COLUMN_DEVICES)) <= 0) {
        throw new SemanticException("Please set the number of devices greater than 0");
      } else {
        setSpaceQuotaStatement.setDeviceNum(
            Long.parseLong(quotas.get(IoTDBConstant.COLUMN_DEVICES)));
      }
    }
    if (quotas.containsKey(IoTDBConstant.COLUMN_TIMESERIES)) {
      if (quotas.get(IoTDBConstant.COLUMN_TIMESERIES).equals(IoTDBConstant.QUOTA_UNLIMITED)) {
        setSpaceQuotaStatement.setTimeSeriesNum(IoTDBConstant.UNLIMITED_VALUE);
      } else if (Long.parseLong(quotas.get(IoTDBConstant.COLUMN_TIMESERIES)) <= 0) {
        throw new SemanticException("Please set the number of timeseries greater than 0");
      } else {
        setSpaceQuotaStatement.setTimeSeriesNum(
            Long.parseLong(quotas.get(IoTDBConstant.COLUMN_TIMESERIES)));
      }
    }
    if (quotas.containsKey(IoTDBConstant.SPACE_QUOTA_DISK)) {
      if (quotas.get(IoTDBConstant.SPACE_QUOTA_DISK).equals(IoTDBConstant.QUOTA_UNLIMITED)) {
        setSpaceQuotaStatement.setDiskSize(IoTDBConstant.UNLIMITED_VALUE);
      } else {
        setSpaceQuotaStatement.setDiskSize(
            parseSpaceQuotaSizeUnit(quotas.get(IoTDBConstant.SPACE_QUOTA_DISK)));
      }
    }
    return setSpaceQuotaStatement;
  }

  @Override
  public Statement visitSetThrottleQuota(IoTDBSqlParser.SetThrottleQuotaContext ctx) {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      throw new SemanticException(LIMIT_CONFIGURATION_ENABLED_ERROR_MSG);
    }
    if (parseIdentifier(ctx.userName.getText()).equals(IoTDBConstant.PATH_ROOT)) {
      throw new SemanticException("Cannot set throttle quota for user root.");
    }
    SetThrottleQuotaStatement setThrottleQuotaStatement = new SetThrottleQuotaStatement();
    setThrottleQuotaStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    Map<String, String> quotas = new HashMap<>();
    Map<ThrottleType, TTimedQuota> throttleLimit = new HashMap<>();
    for (IoTDBSqlParser.AttributePairContext attributePair : ctx.attributePair()) {
      quotas.put(
          parseAttributeKey(attributePair.attributeKey()),
          parseAttributeValue(attributePair.attributeValue()));
    }
    if (quotas.containsKey(IoTDBConstant.REQUEST_NUM_PER_UNIT_TIME)) {
      TTimedQuota timedQuota;
      String request = quotas.get(IoTDBConstant.REQUEST_NUM_PER_UNIT_TIME);
      if (request.equals(IoTDBConstant.QUOTA_UNLIMITED)) {
        timedQuota = new TTimedQuota(IoTDBConstant.SEC, Long.MAX_VALUE);
      } else {
        String[] split = request.toLowerCase().split(IoTDBConstant.REQ_SPLIT_UNIT);
        if (Long.parseLong(split[0]) < 0) {
          throw new SemanticException("Please set the number of requests greater than 0");
        }
        timedQuota =
            new TTimedQuota(parseThrottleQuotaTimeUnit(split[1]), Long.parseLong(split[0]));
      }
      if (quotas.get(IoTDBConstant.REQUEST_TYPE) == null) {
        throttleLimit.put(ThrottleType.REQUEST_NUMBER, timedQuota);
      } else {
        switch (quotas.get(IoTDBConstant.REQUEST_TYPE)) {
          case IoTDBConstant.REQUEST_TYPE_READ:
            throttleLimit.put(ThrottleType.READ_NUMBER, timedQuota);
            break;
          case IoTDBConstant.REQUEST_TYPE_WRITE:
            throttleLimit.put(ThrottleType.WRITE_NUMBER, timedQuota);
            break;
          default:
            throw new SemanticException(
                "Please set the correct request type: " + quotas.get(IoTDBConstant.REQUEST_TYPE));
        }
      }
    }

    if (quotas.containsKey(IoTDBConstant.REQUEST_SIZE_PER_UNIT_TIME)) {
      TTimedQuota timedQuota;
      String size = quotas.get(IoTDBConstant.REQUEST_SIZE_PER_UNIT_TIME);
      if (size.equals(IoTDBConstant.QUOTA_UNLIMITED)) {
        timedQuota = new TTimedQuota(IoTDBConstant.SEC, Long.MAX_VALUE);
      } else {
        String[] split = size.toLowerCase().split("/");
        timedQuota =
            new TTimedQuota(
                parseThrottleQuotaTimeUnit(split[1]), parseThrottleQuotaSizeUnit(split[0]));
      }
      if (quotas.get(IoTDBConstant.REQUEST_TYPE) == null) {
        throttleLimit.put(ThrottleType.REQUEST_SIZE, timedQuota);
      } else {
        switch (quotas.get(IoTDBConstant.REQUEST_TYPE)) {
          case IoTDBConstant.REQUEST_TYPE_READ:
            throttleLimit.put(ThrottleType.READ_SIZE, timedQuota);
            break;
          case IoTDBConstant.REQUEST_TYPE_WRITE:
            throttleLimit.put(ThrottleType.WRITE_SIZE, timedQuota);
            break;
          default:
            throw new SemanticException(
                "Please set the correct request type: " + quotas.get(IoTDBConstant.REQUEST_TYPE));
        }
      }
    }

    if (quotas.containsKey(IoTDBConstant.MEMORY_SIZE_PER_READ)) {
      String mem = quotas.get(IoTDBConstant.MEMORY_SIZE_PER_READ);
      if (mem.equals(IoTDBConstant.QUOTA_UNLIMITED)) {
        setThrottleQuotaStatement.setMemLimit(IoTDBConstant.UNLIMITED_VALUE);
      } else {
        setThrottleQuotaStatement.setMemLimit(parseThrottleQuotaSizeUnit(mem));
      }
    }

    if (quotas.containsKey(IoTDBConstant.CPU_NUMBER_PER_READ)) {
      String cpuLimit = quotas.get(IoTDBConstant.CPU_NUMBER_PER_READ);
      if (cpuLimit.contains(IoTDBConstant.QUOTA_UNLIMITED)) {
        setThrottleQuotaStatement.setCpuLimit(IoTDBConstant.UNLIMITED_VALUE);
      } else {
        int cpuNum = Integer.parseInt(cpuLimit);
        if (cpuNum <= 0) {
          throw new SemanticException("Please set the number of cpu greater than 0");
        }
        setThrottleQuotaStatement.setCpuLimit(cpuNum);
      }
    }
    setThrottleQuotaStatement.setThrottleLimit(throttleLimit);
    return setThrottleQuotaStatement;
  }

  @Override
  public Statement visitShowThrottleQuota(IoTDBSqlParser.ShowThrottleQuotaContext ctx) {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      throw new SemanticException(LIMIT_CONFIGURATION_ENABLED_ERROR_MSG);
    }
    ShowThrottleQuotaStatement showThrottleQuotaStatement = new ShowThrottleQuotaStatement();
    if (ctx.userName != null) {
      showThrottleQuotaStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    }
    return showThrottleQuotaStatement;
  }

  private long parseThrottleQuotaTimeUnit(String timeUnit) {
    switch (timeUnit.toLowerCase()) {
      case IoTDBConstant.SEC_UNIT:
        return IoTDBConstant.SEC;
      case IoTDBConstant.MIN_UNIT:
        return IoTDBConstant.MIN;
      case IoTDBConstant.HOUR_UNIT:
        return IoTDBConstant.HOUR;
      case IoTDBConstant.DAY_UNIT:
        return IoTDBConstant.DAY;
      default:
        throw new SemanticException(
            "When setting the request, the unit is incorrect. Please use 'sec', 'min', 'hour', 'day' as the unit");
    }
  }

  private long parseThrottleQuotaSizeUnit(String data) {
    String unit = data.substring(data.length() - 1);
    long size = Long.parseLong(data.substring(0, data.length() - 1));
    if (size <= 0) {
      throw new SemanticException("Please set the size greater than 0");
    }
    switch (unit.toUpperCase()) {
      case IoTDBConstant.B_UNIT:
        return size;
      case IoTDBConstant.KB_UNIT:
        return size * IoTDBConstant.KB;
      case IoTDBConstant.MB_UNIT:
        return size * IoTDBConstant.MB;
      case IoTDBConstant.GB_UNIT:
        return size * IoTDBConstant.GB;
      case IoTDBConstant.TB_UNIT:
        return size * IoTDBConstant.TB;
      case IoTDBConstant.PB_UNIT:
        return size * IoTDBConstant.PB;
      default:
        throw new SemanticException(
            "When setting the size/time, the unit is incorrect. Please use 'B', 'K', 'M', 'G', 'P', 'T' as the unit");
    }
  }

  private long parseSpaceQuotaSizeUnit(String data) {
    String unit = data.substring(data.length() - 1);
    long disk = Long.parseLong(data.substring(0, data.length() - 1));
    if (disk <= 0) {
      throw new SemanticException("Please set the disk size greater than 0");
    }
    switch (unit.toUpperCase()) {
      case IoTDBConstant.MB_UNIT:
        return disk;
      case IoTDBConstant.GB_UNIT:
        return disk * IoTDBConstant.KB;
      case IoTDBConstant.TB_UNIT:
        return disk * IoTDBConstant.MB;
      case IoTDBConstant.PB_UNIT:
        return disk * IoTDBConstant.GB;
      default:
        throw new SemanticException(
            "When setting the disk size, the unit is incorrect. Please use 'M', 'G', 'P', 'T' as the unit");
    }
  }

  @Override
  public Statement visitShowSpaceQuota(IoTDBSqlParser.ShowSpaceQuotaContext ctx) {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      throw new SemanticException(LIMIT_CONFIGURATION_ENABLED_ERROR_MSG);
    }
    ShowSpaceQuotaStatement showSpaceQuotaStatement = new ShowSpaceQuotaStatement();
    if (ctx.prefixPath() != null) {
      List<PartialPath> databases = new ArrayList<>();
      for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
        databases.add(parsePrefixPath(prefixPathContext));
      }
      showSpaceQuotaStatement.setDatabases(databases);
    } else {
      showSpaceQuotaStatement.setDatabases(null);
    }
    return showSpaceQuotaStatement;
  }

  @Override
  public Statement visitCallInference(IoTDBSqlParser.CallInferenceContext ctx) {
    String sql = ctx.inputSql.getText();
    QueryStatement statement =
        (QueryStatement)
            StatementGenerator.createStatement(sql.substring(1, sql.length() - 1), zoneId);

    statement.setModelId(parseIdentifier(ctx.modelId.getText()));
    statement.setHasModelInference(true);

    if (ctx.hparamPair() != null) {
      for (IoTDBSqlParser.HparamPairContext context : ctx.hparamPair()) {
        IoTDBSqlParser.HparamValueContext valueContext = context.hparamValue();
        String paramKey = context.hparamKey.getText();
        if (paramKey.equalsIgnoreCase("WINDOW")) {
          if (statement.isSetInferenceWindow()) {
            throw new SemanticException("There should be only one window in CALL INFERENCE.");
          }
          if (valueContext.windowFunction().isEmpty()) {
            throw new SemanticException(
                "Window Function(e.g. HEAD, TAIL, COUNT) should be set in value when key is 'WINDOW' in CALL INFERENCE");
          }
          parseWindowFunctionInInference(valueContext.windowFunction(), statement);
        } else if (paramKey.equalsIgnoreCase("GENERATETIME")) {
          statement.setGenerateTime(
              Boolean.parseBoolean(parseAttributeValue(valueContext.attributeValue())));
        } else {
          statement.addInferenceAttribute(
              paramKey, parseAttributeValue(valueContext.attributeValue()));
        }
      }
    }

    return statement;
  }

  private void parseWindowFunctionInInference(
      IoTDBSqlParser.WindowFunctionContext windowContext, QueryStatement statement) {
    InferenceWindow inferenceWindow = null;
    if (windowContext.TAIL() != null) {
      inferenceWindow =
          new TailInferenceWindow(Integer.parseInt(windowContext.windowSize.getText()));
    } else if (windowContext.HEAD() != null) {
      inferenceWindow =
          new HeadInferenceWindow(Integer.parseInt(windowContext.windowSize.getText()));
    } else if (windowContext.COUNT() != null) {
      inferenceWindow =
          new CountInferenceWindow(
              Integer.parseInt(windowContext.interval.getText()),
              Integer.parseInt(windowContext.step.getText()));
    }
    statement.setInferenceWindow(inferenceWindow);
  }

  @Override
  public Statement visitCreateTableView(final IoTDBSqlParser.CreateTableViewContext ctx) {
    if (true) {
      throw new SemanticException("The 'CreateTableView' is unsupported in tree sql-dialect.");
    }
    return new CreateTableViewStatement(
        new CreateView(
            null,
            getQualifiedName(ctx.qualifiedName()),
            ctx.viewColumnDefinition().stream()
                .map(this::parseViewColumnDefinition)
                .collect(Collectors.toList()),
            null,
            ctx.comment() == null
                ? null
                : parseStringLiteral(ctx.comment().STRING_LITERAL().getText()),
            Objects.nonNull(ctx.properties())
                ? ctx.properties().propertyAssignments().property().stream()
                    .map(this::parseProperty)
                    .collect(Collectors.toList())
                : ImmutableList.of(),
            parsePrefixPath(ctx.prefixPath()),
            Objects.nonNull(ctx.REPLACE()),
            Objects.nonNull(ctx.RESTRICT())));
  }

  public ColumnDefinition parseViewColumnDefinition(
      final IoTDBSqlParser.ViewColumnDefinitionContext ctx) {
    final Identifier rawColumnName =
        new Identifier(parseIdentifier(ctx.identifier().get(0).getText()));
    final Identifier columnName = lowerIdentifier(rawColumnName);
    final TsTableColumnCategory columnCategory = getColumnCategory(ctx.columnCategory);
    Identifier originalMeasurement = null;

    if (Objects.nonNull(ctx.FROM())) {
      originalMeasurement = new Identifier(parseIdentifier(ctx.original_measurement.getText()));
    } else if (columnCategory == FIELD && !columnName.equals(rawColumnName)) {
      originalMeasurement = rawColumnName;
    }

    return columnCategory == FIELD
        ? new ViewFieldDefinition(
            null,
            columnName,
            Objects.nonNull(ctx.type())
                ? parseGenericType((IoTDBSqlParser.GenericTypeContext) ctx.type())
                : null,
            null,
            ctx.comment() == null
                ? null
                : parseStringLiteral(ctx.comment().STRING_LITERAL().getText()),
            originalMeasurement)
        : new ColumnDefinition(
            null,
            columnName,
            Objects.nonNull(ctx.type())
                ? parseGenericType((IoTDBSqlParser.GenericTypeContext) ctx.type())
                : null,
            columnCategory,
            null,
            ctx.comment() == null
                ? null
                : parseStringLiteral(ctx.comment().STRING_LITERAL().getText()));
  }

  public DataType parseGenericType(final IoTDBSqlParser.GenericTypeContext ctx) {
    return new GenericDataType(
        new Identifier(parseIdentifier(ctx.identifier().getText())),
        ctx.typeParameter().stream()
            .map(this::parseTypeParameter)
            .map(DataTypeParameter.class::cast)
            .collect(toImmutableList()));
  }

  public Node parseTypeParameter(final IoTDBSqlParser.TypeParameterContext ctx) {
    if (ctx.INTEGER_LITERAL() != null) {
      return new NumericParameter(ctx.getText());
    }

    return new TypeParameter(parseGenericType((IoTDBSqlParser.GenericTypeContext) ctx.type()));
  }

  private Property parseProperty(final IoTDBSqlParser.PropertyContext ctx) {
    final Identifier name = new Identifier(parseIdentifier(ctx.identifier().getText()));
    final IoTDBSqlParser.PropertyValueContext valueContext = ctx.propertyValue();
    return valueContext instanceof IoTDBSqlParser.DefaultPropertyValueContext
        ? new Property(name)
        : new Property(
            name,
            parseExpression(
                ((IoTDBSqlParser.NonDefaultPropertyValueContext) valueContext)
                    .literalExpression()));
  }

  private org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression parseExpression(
      final IoTDBSqlParser.LiteralExpressionContext context) {
    if (context.STRING_LITERAL() != null) {
      return new org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral(
          parseStringLiteral(context.getText()));
    } else if (context.INTEGER_LITERAL() != null) {
      return new org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral(
          context.getText());
    }
    throw new UnsupportedOperationException("Currently other expressions are not supported");
  }

  private QualifiedName getQualifiedName(final IoTDBSqlParser.QualifiedNameContext context) {
    return QualifiedName.of(
        context.identifier().stream()
            .map(identifierContext -> new Identifier(parseIdentifier(identifierContext.getText())))
            .collect(Collectors.toList()));
  }

  public static TsTableColumnCategory getColumnCategory(final Token category) {
    if (category == null) {
      return FIELD;
    }
    switch (category.getType()) {
      case IoTDBSqlParser.TAG:
        return TAG;
      case IoTDBSqlParser.TIME:
        return TIME;
      case IoTDBSqlParser.FIELD:
        return FIELD;
      default:
        throw new UnsupportedOperationException(
            "Unsupported ColumnCategory: " + category.getText());
    }
  }

  @Override
  public Statement visitShowCurrentTimestamp(IoTDBSqlParser.ShowCurrentTimestampContext ctx) {
    return new ShowCurrentTimestampStatement();
  }

  @Override
  public Statement visitSetSqlDialect(IoTDBSqlParser.SetSqlDialectContext ctx) {
    return new SetSqlDialectStatement(
        ctx.TABLE() == null ? IClientSession.SqlDialect.TREE : IClientSession.SqlDialect.TABLE);
  }

  @Override
  public Statement visitShowCurrentSqlDialect(IoTDBSqlParser.ShowCurrentSqlDialectContext ctx) {
    return new ShowCurrentSqlDialectStatement();
  }

  @Override
  public Statement visitShowCurrentUser(IoTDBSqlParser.ShowCurrentUserContext ctx) {
    return new ShowCurrentUserStatement();
  }
}
