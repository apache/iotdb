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

package org.apache.iotdb.db.mpp.plan.parser;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.cq.TimeoutPolicy;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.BuiltInScalarFunctionHelperFactory;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.statement.AuthorType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByConditionComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupBySessionComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTagComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByVariationComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.HavingCondition;
import org.apache.iotdb.db.mpp.plan.statement.component.IntoComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.IntoItem;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultSetFormat;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.literal.BooleanLiteral;
import org.apache.iotdb.db.mpp.plan.statement.literal.DoubleLiteral;
import org.apache.iotdb.db.mpp.plan.statement.literal.Literal;
import org.apache.iotdb.db.mpp.plan.statement.literal.LongLiteral;
import org.apache.iotdb.db.mpp.plan.statement.literal.StringLiteral;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreatePipePluginStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropPipePluginStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropTriggerStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.MigrateRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowPipePluginsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.MergeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.CreatePipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.DropPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.ShowPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.StartPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.pipe.StopPipeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.DropPipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.ShowPipeSinkTypeStatement;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ConstantContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountDevicesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountNodesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountStorageGroupContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountTimeseriesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CreateFunctionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.DropFunctionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ExpressionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.GroupByAttributeClauseContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.IdentifierContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ShowFunctionsContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParserBaseVisitor;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.trigger.api.enums.TriggerType;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.constant.SqlConstant.CAST_FUNCTION;
import static org.apache.iotdb.db.constant.SqlConstant.CAST_TYPE;
import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_RESULT_NODES;

/** Parse AST to Statement. */
public class ASTVisitor extends IoTDBSqlParserBaseVisitor<Statement> {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String DELETE_RANGE_ERROR_MSG =
      "For delete statement, where clause can only contain atomic expressions like : "
          + "time > XXX, time <= XXX, or two atomic expressions connected by 'AND'";
  private static final String DELETE_ONLY_SUPPORT_TIME_EXP_ERROR_MSG =
      "For delete statement, where clause can only contain time expressions, "
          + "value filter is not currently supported.";

  private static final String GROUP_BY_COMMON_ONLY_ONE_MSG =
      "Only one of group by time or group by variation/series/session can be supported at a time";

  private static final String IGNORENULL = "IgnoreNull";
  private ZoneId zoneId;

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  /** Top Level Description */
  @Override
  public Statement visitSingleStatement(IoTDBSqlParser.SingleStatementContext ctx) {
    Statement statement = visit(ctx.statement());
    if (ctx.DEBUG() != null) {
      statement.setDebug(true);
    }
    return statement;
  }

  /** Data Definition Language (DDL) */

  // Create Timeseries ========================================================================
  @Override
  public Statement visitCreateNonAlignedTimeseries(
      IoTDBSqlParser.CreateNonAlignedTimeseriesContext ctx) {
    CreateTimeSeriesStatement createTimeSeriesStatement = new CreateTimeSeriesStatement();
    createTimeSeriesStatement.setPath(parseFullPath(ctx.fullPath()));
    if (ctx.attributeClauses() != null) {
      parseAttributeClauses(ctx.attributeClauses(), createTimeSeriesStatement);
    }
    return createTimeSeriesStatement;
  }

  @Override
  public Statement visitCreateAlignedTimeseries(IoTDBSqlParser.CreateAlignedTimeseriesContext ctx) {
    CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement =
        new CreateAlignedTimeSeriesStatement();
    createAlignedTimeSeriesStatement.setDevicePath(parseFullPath(ctx.fullPath()));
    parseAlignedMeasurements(ctx.alignedMeasurements(), createAlignedTimeSeriesStatement);
    return createAlignedTimeSeriesStatement;
  }

  public void parseAlignedMeasurements(
      IoTDBSqlParser.AlignedMeasurementsContext ctx,
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement) {
    for (int i = 0; i < ctx.nodeNameWithoutWildcard().size(); i++) {
      createAlignedTimeSeriesStatement.addMeasurement(
          parseNodeNameWithoutWildCard(ctx.nodeNameWithoutWildcard(i)));
      parseAttributeClauses(ctx.attributeClauses(i), createAlignedTimeSeriesStatement);
    }
  }

  public void parseAttributeClauses(
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
      parseAttributeClause(ctx.attributeClause(), createTimeSeriesStatement);
    }
  }

  /** check and set datatype, encoding, compressor */
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
    if (props != null
        && props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase())) {
      String encodingString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase()).toUpperCase();
      try {
        createTimeSeriesStatement.setEncoding(TSEncoding.valueOf(encodingString));
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("Unsupported encoding: %s", encodingString));
      }
    }

    createTimeSeriesStatement.setCompressor(
        TSFileDescriptor.getInstance().getConfig().getCompressor());
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

  public void parseAttributeClauses(
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
    if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase())) {
      String encodingString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase()).toUpperCase();
      try {
        encoding = TSEncoding.valueOf(encodingString);
        createAlignedTimeSeriesStatement.addEncoding(encoding);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported encoding: %s", encodingString));
      }
    } else {
      createAlignedTimeSeriesStatement.addEncoding(encoding);
    }

    CompressionType compressor = TSFileDescriptor.getInstance().getConfig().getCompressor();
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
      parseAttributeClause(ctx.attributeClause(), createAlignedTimeSeriesStatement);
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

  public void parseAttributeClause(IoTDBSqlParser.AttributeClauseContext ctx, Statement statement) {
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
    parseAlterClause(ctx.alterClause(), alterTimeSeriesStatement);
    return alterTimeSeriesStatement;
  }

  private void parseAlterClause(
      IoTDBSqlParser.AlterClauseContext ctx, AlterTimeSeriesStatement alterTimeSeriesStatement) {
    Map<String, String> alterMap = new HashMap<>();
    // rename
    if (ctx.RENAME() != null) {
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.RENAME);
      alterMap.put(parseAttributeKey(ctx.beforeName), parseAttributeKey(ctx.currentName));
    } else if (ctx.SET() != null) {
      // set
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.SET);
      setMap(ctx, alterMap);
    } else if (ctx.DROP() != null) {
      // drop
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.DROP);
      for (int i = 0; i < ctx.attributeKey().size(); i++) {
        alterMap.put(parseAttributeKey(ctx.attributeKey().get(i)), null);
      }
    } else if (ctx.TAGS() != null) {
      // add tag
      alterTimeSeriesStatement.setAlterType((AlterTimeSeriesStatement.AlterType.ADD_TAGS));
      setMap(ctx, alterMap);
    } else if (ctx.ATTRIBUTES() != null) {
      // add attribute
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES);
      setMap(ctx, alterMap);
    } else {
      // upsert
      alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.UPSERT);
      if (ctx.aliasClause() != null) {
        parseAliasClause(ctx.aliasClause(), alterTimeSeriesStatement);
      }
      if (ctx.tagClause() != null) {
        parseTagClause(ctx.tagClause(), alterTimeSeriesStatement);
      }
      if (ctx.attributeClause() != null) {
        parseAttributeClause(ctx.attributeClause(), alterTimeSeriesStatement);
      }
    }
    alterTimeSeriesStatement.setAlterMap(alterMap);
  }

  public void parseAliasClause(
      IoTDBSqlParser.AliasClauseContext ctx, AlterTimeSeriesStatement alterTimeSeriesStatement) {
    if (alterTimeSeriesStatement != null && ctx.ALIAS() != null) {
      alterTimeSeriesStatement.setAlias(parseAliasNode(ctx.alias()));
    }
  }

  // Delete Timeseries ======================================================================

  @Override
  public Statement visitDeleteTimeseries(IoTDBSqlParser.DeleteTimeseriesContext ctx) {
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
    if (ctx.tagWhereClause() != null) {
      parseTagWhereClause(ctx.tagWhereClause(), showTimeSeriesStatement);
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

  private void parseTagWhereClause(IoTDBSqlParser.TagWhereClauseContext ctx, Statement statement) {
    IoTDBSqlParser.AttributeValueContext attributeValueContext;
    String key;
    String value;
    boolean isContains;
    if (ctx.containsExpression() != null) {
      isContains = true;
      attributeValueContext = ctx.containsExpression().attributeValue();
      key = parseAttributeKey(ctx.containsExpression().attributeKey());
    } else {
      isContains = false;
      attributeValueContext = ctx.attributePair().attributeValue();
      key = parseAttributeKey(ctx.attributePair().attributeKey());
    }
    value = parseAttributeValue(attributeValueContext);
    if (statement instanceof ShowTimeSeriesStatement) {
      ((ShowTimeSeriesStatement) statement).setContains(isContains);
      ((ShowTimeSeriesStatement) statement).setKey(key);
      ((ShowTimeSeriesStatement) statement).setValue(value);
    } else if (statement instanceof CountTimeSeriesStatement) {
      ((CountTimeSeriesStatement) statement).setContains(isContains);
      ((CountTimeSeriesStatement) statement).setKey(key);
      ((CountTimeSeriesStatement) statement).setValue(value);
    } else if (statement instanceof CountLevelTimeSeriesStatement) {
      ((CountLevelTimeSeriesStatement) statement).setContains(isContains);
      ((CountLevelTimeSeriesStatement) statement).setKey(key);
      ((CountLevelTimeSeriesStatement) statement).setValue(value);
    }
  }

  // SHOW DATABASES

  @Override
  public Statement visitShowStorageGroup(IoTDBSqlParser.ShowStorageGroupContext ctx) {
    ShowStorageGroupStatement showStorageGroupStatement;

    // Parse prefixPath
    if (ctx.prefixPath() != null) {
      showStorageGroupStatement = new ShowStorageGroupStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      showStorageGroupStatement =
          new ShowStorageGroupStatement(new PartialPath(SqlConstant.getSingleRootArray()));
    }

    // Is detailed
    showStorageGroupStatement.setDetailed(ctx.DETAILS() != null);

    return showStorageGroupStatement;
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

  // Count Devices ========================================================================

  @Override
  public Statement visitCountDevices(CountDevicesContext ctx) {
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    return new CountDevicesStatement(path);
  }

  // Count TimeSeries ========================================================================
  @Override
  public Statement visitCountTimeseries(CountTimeseriesContext ctx) {
    Statement statement;
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    if (ctx.INTEGER_LITERAL() != null) {
      int level = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
      statement = new CountLevelTimeSeriesStatement(path, level);
    } else {
      statement = new CountTimeSeriesStatement(path);
    }
    if (ctx.tagWhereClause() != null) {
      parseTagWhereClause(ctx.tagWhereClause(), statement);
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

  // Count StorageGroup ========================================================================
  @Override
  public Statement visitCountStorageGroup(CountStorageGroupContext ctx) {
    PartialPath path;
    if (ctx.prefixPath() != null) {
      path = parsePrefixPath(ctx.prefixPath());
    } else {
      path = new PartialPath(SqlConstant.getSingleRootArray());
    }
    return new CountStorageGroupStatement(path);
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
          false);
    } else {
      String uriString = parseAndValidateURI(ctx.uriClause());
      return new CreateFunctionStatement(
          parseIdentifier(ctx.udfName.getText()),
          parseStringLiteral(ctx.className.getText()),
          true,
          uriString);
    }
  }

  private String parseAndValidateURI(IoTDBSqlParser.UriClauseContext ctx) {
    String uriString = parseStringLiteral(ctx.uri().getText());
    try {
      URI uri = new URI(uriString);
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

  // Create PipePlugin =====================================================================
  @Override
  public Statement visitCreatePipePlugin(IoTDBSqlParser.CreatePipePluginContext ctx) {
    return new CreatePipePluginStatement(
        parseIdentifier(ctx.pluginName.getText()),
        parseStringLiteral(ctx.className.getText()),
        parseAndValidateURI(ctx.uriClause()));
  }

  // Drop PipePlugin =====================================================================
  @Override
  public Statement visitDropPipePlugin(IoTDBSqlParser.DropPipePluginContext ctx) {
    return new DropPipePluginStatement(parseIdentifier(ctx.pluginName.getText()));
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

      long interval = queryStatement.getGroupByTimeComponent().getInterval();
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
      statement.setEveryInterval(queryStatement.getGroupByTimeComponent().getInterval());
    }

    if (ctx.BOUNDARY() != null) {
      statement.setBoundaryTime(parseTimeValue(ctx.boundaryTime, DateTimeUtils.currentTime()));
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

  /** Data Manipulation Language (DML) */

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
              ImmutableSet.of(SortKey.TIME, SortKey.DEVICE, SortKey.TIMESERIES)));
    }

    // parse FILL
    if (ctx.fillClause() != null) {
      queryStatement.setFillComponent(parseFillClause(ctx.fillClause()));
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
      }
    }

    // parse ALIGN BY
    if (ctx.alignByClause() != null) {
      queryStatement.setResultSetFormat(parseAlignBy(ctx.alignByClause()));
    }

    return queryStatement;
  }

  // ---- Select Clause
  private SelectComponent parseSelectClause(
      IoTDBSqlParser.SelectClauseContext ctx, QueryStatement queryStatement) {
    SelectComponent selectComponent = new SelectComponent(zoneId);

    // parse LAST
    if (ctx.LAST() != null) {
      selectComponent.setHasLast(true);
    }

    // parse resultColumn
    Map<String, Expression> aliasToColumnMap = new HashMap<>();
    for (IoTDBSqlParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      ResultColumn resultColumn = parseResultColumn(resultColumnContext);
      // __endTime shouldn't be included in resultColumns
      if (resultColumn.getExpression().getExpressionString().equals(ColumnHeaderConstant.ENDTIME)) {
        queryStatement.setOutputEndTime(true);
        continue;
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

    // parse time range
    if (ctx.timeRange() != null) {
      parseTimeRange(ctx.timeRange(), groupByTimeComponent);
      groupByTimeComponent.setLeftCRightO(ctx.timeRange().LS_BRACKET() != null);
    }

    // parse time interval
    groupByTimeComponent.setInterval(
        parseTimeIntervalOrSlidingStep(ctx.interval.getText(), true, groupByTimeComponent));
    if (groupByTimeComponent.getInterval() <= 0) {
      throw new SemanticException(
          "The second parameter time interval should be a positive integer.");
    }

    // parse sliding step
    if (ctx.step != null) {
      groupByTimeComponent.setSlidingStep(
          parseTimeIntervalOrSlidingStep(ctx.step.getText(), false, groupByTimeComponent));
    } else {
      groupByTimeComponent.setSlidingStep(groupByTimeComponent.getInterval());
      groupByTimeComponent.setSlidingStepByMonth(groupByTimeComponent.isIntervalByMonth());
    }

    return groupByTimeComponent;
  }

  /** parse time range (startTime and endTime) in group by time. */
  private void parseTimeRange(
      IoTDBSqlParser.TimeRangeContext timeRange, GroupByTimeComponent groupByClauseComponent) {
    long currentTime = DateTimeUtils.currentTime();
    long startTime = parseTimeValue(timeRange.timeValue(0), currentTime);
    long endTime = parseTimeValue(timeRange.timeValue(1), currentTime);
    groupByClauseComponent.setStartTime(startTime);
    groupByClauseComponent.setEndTime(endTime);
    if (startTime >= endTime) {
      throw new SemanticException("Start time should be smaller than endTime in GroupBy");
    }
  }

  /**
   * parse time interval or sliding step in group by query.
   *
   * @param duration represent duration string like: 12d8m9ns, 1y1d, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  private long parseTimeIntervalOrSlidingStep(
      String duration, boolean isParsingTimeInterval, GroupByTimeComponent groupByTimeComponent) {
    if (duration.toLowerCase().contains("mo")) {
      if (isParsingTimeInterval) {
        groupByTimeComponent.setIntervalByMonth(true);
      } else {
        groupByTimeComponent.setSlidingStepByMonth(true);
      }
    }
    return DateTimeUtils.convertDurationStrToLong(duration);
  }

  private GroupByComponent parseGroupByClause(
      GroupByAttributeClauseContext ctx, WindowType windowType) {

    boolean ignoringNull = true;
    if (ctx.attributePair() != null && !ctx.attributePair().isEmpty()) {
      if (ctx.attributePair().key.getText().equalsIgnoreCase(IGNORENULL)) {
        ignoringNull = Boolean.parseBoolean(ctx.attributePair().value.getText());
      }
    }
    List<ExpressionContext> expressions = ctx.expression();
    if (windowType == WindowType.VARIATION_WINDOW) {
      ExpressionContext expressionContext = expressions.get(0);
      GroupByVariationComponent groupByVariationComponent = new GroupByVariationComponent();
      groupByVariationComponent.setControlColumnExpression(
          parseExpression(expressionContext, true));
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
      }
      groupByConditionComponent.setIgnoringNull(ignoringNull);
      return groupByConditionComponent;
    } else if (windowType == WindowType.SESSION_WINDOW) {
      long interval = DateTimeUtils.convertDurationStrToLong(ctx.timeInterval.getText());
      return new GroupBySessionComponent(interval);
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
      IoTDBSqlParser.OrderByClauseContext ctx, ImmutableSet<SortKey> limitSet) {
    OrderByComponent orderByComponent = new OrderByComponent();
    Set<SortKey> sortKeySet = new HashSet<>();
    for (IoTDBSqlParser.OrderByAttributeClauseContext orderByAttributeClauseContext :
        ctx.orderByAttributeClause()) {
      SortItem sortItem = parseOrderByAttributeClause(orderByAttributeClauseContext);

      SortKey sortKey = sortItem.getSortKey();
      if (!limitSet.contains(sortKey)) {
        throw new SemanticException(
            String.format("ORDER BY: sort key[%s] is not contained in '%s'", sortKey, limitSet));
      }
      if (sortKeySet.contains(sortKey)) {
        throw new SemanticException(String.format("ORDER BY: duplicate sort key '%s'", sortKey));
      } else {
        sortKeySet.add(sortKey);
        orderByComponent.addSortItem(sortItem);
      }
    }
    return orderByComponent;
  }

  private SortItem parseOrderByAttributeClause(IoTDBSqlParser.OrderByAttributeClauseContext ctx) {
    return new SortItem(
        SortKey.valueOf(ctx.sortKey().getText().toUpperCase()),
        ctx.DESC() != null ? Ordering.DESC : Ordering.ASC);
  }

  // ---- Fill Clause
  public FillComponent parseFillClause(IoTDBSqlParser.FillClauseContext ctx) {
    FillComponent fillComponent = new FillComponent();
    if (ctx.LINEAR() != null) {
      fillComponent.setFillPolicy(FillPolicy.LINEAR);
    } else if (ctx.PREVIOUS() != null) {
      fillComponent.setFillPolicy(FillPolicy.PREVIOUS);
    } else if (ctx.constant() != null) {
      fillComponent.setFillPolicy(FillPolicy.VALUE);
      Literal fillValue = parseLiteral(ctx.constant());
      fillComponent.setFillValue(fillValue);
    } else {
      throw new SemanticException("Unknown FILL type.");
    }
    return fillComponent;
  }

  private Literal parseLiteral(ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.BOOLEAN_LITERAL() != null) {
      return new BooleanLiteral(text);
    } else if (constantContext.STRING_LITERAL() != null) {
      return new StringLiteral(parseStringLiteral(text));
    } else if (constantContext.INTEGER_LITERAL() != null) {
      return new LongLiteral(text);
    } else if (constantContext.realLiteral() != null) {
      return new DoubleLiteral(text);
    } else if (constantContext.dateExpression() != null) {
      return new LongLiteral(parseDateExpression(constantContext.dateExpression()));
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
    insertStatement.setDevice(parsePrefixPath(ctx.prefixPath()));
    boolean isTimeDefault = parseInsertColumnSpec(ctx.insertColumnsSpec(), insertStatement);
    parseInsertValuesSpec(ctx.insertValuesSpec(), insertStatement, isTimeDefault);
    insertStatement.setAligned(ctx.ALIGNED() != null);
    return insertStatement;
  }

  private boolean parseInsertColumnSpec(
      IoTDBSqlParser.InsertColumnsSpecContext ctx, InsertStatement insertStatement) {
    List<String> measurementList = new ArrayList<>();
    for (IoTDBSqlParser.NodeNameWithoutWildcardContext measurementName :
        ctx.nodeNameWithoutWildcard()) {
      measurementList.add(parseNodeNameWithoutWildCard(measurementName));
    }
    insertStatement.setMeasurementList(measurementList.toArray(new String[0]));
    return (ctx.TIME() == null && ctx.TIMESTAMP() == null);
  }

  private void parseInsertValuesSpec(
      IoTDBSqlParser.InsertValuesSpecContext ctx,
      InsertStatement insertStatement,
      boolean isTimeDefault) {
    List<IoTDBSqlParser.InsertMultiValueContext> insertMultiValues = ctx.insertMultiValue();
    List<String[]> valuesList = new ArrayList<>();
    long[] timeArray = new long[insertMultiValues.size()];
    for (int i = 0; i < insertMultiValues.size(); i++) {
      // parse timestamp
      long timestamp;
      List<String> valueList = new ArrayList<>();

      if (insertMultiValues.get(i).timeValue() != null) {
        if (isTimeDefault) {
          if (insertMultiValues.size() != 1) {
            throw new SemanticException("need timestamps when insert multi rows");
          }
          valueList.add(insertMultiValues.get(i).timeValue().getText());
          timestamp = DateTimeUtils.currentTime();
        } else {
          timestamp =
              parseTimeValue(insertMultiValues.get(i).timeValue(), DateTimeUtils.currentTime());
        }
      } else {
        if (!isTimeDefault) {
          throw new SemanticException(
              "the measurementList's size is not consistent with the valueList's size");
        }
        if (insertMultiValues.size() != 1) {
          throw new SemanticException("need timestamps when insert multi rows");
        }
        timestamp = parseDateFormat(SqlConstant.NOW_FUNC);
      }
      timeArray[i] = timestamp;

      // parse values
      List<IoTDBSqlParser.MeasurementValueContext> values =
          insertMultiValues.get(i).measurementValue();
      for (IoTDBSqlParser.MeasurementValueContext value : values) {
        for (IoTDBSqlParser.ConstantContext constant : value.constant()) {
          if (constant.STRING_LITERAL() != null) {
            valueList.add(parseStringLiteralInInsertValue(constant.getText()));
          } else {
            valueList.add(constant.getText());
          }
        }
      }
      valuesList.add(valueList.toArray(new String[0]));
    }
    insertStatement.setTimes(timeArray);
    insertStatement.setValuesList(valuesList);
  }

  // Load File

  @Override
  public Statement visitLoadFile(IoTDBSqlParser.LoadFileContext ctx) {
    try {
      LoadTsFileStatement loadTsFileStatement =
          new LoadTsFileStatement(parseStringLiteral(ctx.fileName.getText()));
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
   * used for parsing load tsfile, context will be one of "SCHEMA, LEVEL, METADATA", and maybe
   * followed by a recursion property statement
   *
   * @param loadTsFileStatement the result statement, setting by clause context
   * @param ctx context of property statement
   */
  private void parseLoadFileAttributeClause(
      LoadTsFileStatement loadTsFileStatement, IoTDBSqlParser.LoadFileAttributeClauseContext ctx) {
    if (ctx.ONSUCCESS() != null) {
      loadTsFileStatement.setDeleteAfterLoad(ctx.DELETE() != null);
    } else if (ctx.SGLEVEL() != null) {
      loadTsFileStatement.setSgLevel(Integer.parseInt(ctx.INTEGER_LITERAL().getText()));
    } else if (ctx.VERIFY() != null) {
      loadTsFileStatement.setVerifySchema(Boolean.parseBoolean(ctx.BOOLEAN_LITERAL().getText()));
    } else {
      throw new SemanticException(
          String.format(
              "load tsfile format %s error, please input AUTOREGISTER | SGLEVEL | VERIFY.",
              ctx.getText()));
    }
  }

  /** Common Parsers */

  // IoTDB Objects ========================================================================
  private PartialPath parseFullPath(IoTDBSqlParser.FullPathContext ctx) {
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

  private PartialPath parseSuffixPath(IoTDBSqlParser.SuffixPathContext ctx) {
    List<IoTDBSqlParser.NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size()];
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i] = parseNodeName(nodeNames.get(i));
    }
    return new PartialPath(path);
  }

  private PartialPath convertConstantToPath(String src) throws IllegalPathException {
    return new PartialPath(src);
  }

  private String parseNodeName(IoTDBSqlParser.NodeNameContext ctx) {
    return parseNodeString(ctx.getText());
  }

  private String parseNodeNameWithoutWildCard(IoTDBSqlParser.NodeNameWithoutWildcardContext ctx) {
    return parseNodeString(ctx.getText());
  }

  private String parseNodeNameInIntoPath(IoTDBSqlParser.NodeNameInIntoPathContext ctx) {
    return parseNodeStringInIntoPath(ctx.getText());
  }

  private String parseNodeString(String nodeName) {
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

  private String parseNodeStringInIntoPath(String nodeName) {
    if (nodeName.equals(IoTDBConstant.DOUBLE_COLONS)) {
      return nodeName;
    }
    if (nodeName.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && nodeName.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      return PathUtils.removeBackQuotesIfNecessary(nodeName);
    }
    checkNodeNameInIntoPath(nodeName);
    return nodeName;
  }

  private void checkNodeName(String src) {
    // node name could start with * and end with *
    if (!TsFileConstant.NODE_NAME_PATTERN.matcher(src).matches()) {
      throw new SemanticException(
          String.format(
              "%s is illegal, unquoted node name can only consist of digits, characters and underscore, or start or end with wildcard",
              src));
    }
  }

  private void checkNodeNameInIntoPath(String src) {
    // ${} are allowed
    if (!TsFileConstant.NODE_NAME_IN_INTO_PATH_PATTERN.matcher(src).matches()) {
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

  public long parseDateFormat(String timestampStr) {
    if (timestampStr == null || "".equals(timestampStr.trim())) {
      throw new SemanticException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SqlConstant.NOW_FUNC)) {
      return DateTimeUtils.currentTime();
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

  public long parseDateFormat(String timestampStr, long currentTime) {
    if (timestampStr == null || "".equals(timestampStr.trim())) {
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

  private String parseStringLiteral(String src) {
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
    if (2 <= src.length()) {
      if ((src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"')
          || (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'')) {
        return "'" + parseStringLiteral(src) + "'";
      }
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

  // alias

  /** function for parsing Alias of ResultColumn . */
  private String parseAlias(IoTDBSqlParser.AliasContext ctx) {
    String alias;
    if (ctx.constant() != null) {
      alias = parseConstant(ctx.constant());
    } else {
      alias = parseIdentifier(ctx.identifier().getText());
    }
    return alias;
  }

  /** function for parsing AliasNode. */
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

  /** Data Control Language (DCL) */

  // Create User
  @Override
  public Statement visitCreateUser(IoTDBSqlParser.CreateUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.CREATE_USER);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
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
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    authorStatement.setNewPassword(parseStringLiteral(ctx.password.getText()));
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_USER);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    authorStatement.setPrivilegeList(privileges);
    authorStatement.setNodeNameList(nodeNameList);
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setPrivilegeList(privileges);
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  // Grant User Role

  @Override
  public Statement visitGrantRoleToUser(IoTDBSqlParser.GrantRoleToUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.REVOKE_USER);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    authorStatement.setPrivilegeList(privileges);
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorType.REVOKE_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setPrivilegeList(privileges);
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  private void checkGrantRevokePrivileges(String[] privileges, List<PartialPath> nodeNameList) {
    if (nodeNameList.isEmpty()) {
      nodeNameList.add(new PartialPath(ALL_RESULT_NODES));
      return;
    }
    boolean pathRelevant = true;
    String errorPrivilegeName = "";
    for (String privilege : privileges) {
      if ("SET_STORAGE_GROUP".equalsIgnoreCase(privilege)) {
        privilege = PrivilegeType.CREATE_DATABASE.name();
      }
      if ("DELETE_STORAGE_GROUP".equalsIgnoreCase(privilege)) {
        privilege = PrivilegeType.DELETE_DATABASE.name();
      }
      if (!PrivilegeType.valueOf(privilege.toUpperCase()).isPathRelevant()) {
        pathRelevant = false;
        errorPrivilegeName = privilege.toUpperCase();
        break;
      }
    }
    if (!(pathRelevant
        || (nodeNameList.size() == 1
            && nodeNameList.contains(new PartialPath(ALL_RESULT_NODES))))) {
      throw new SemanticException(
          String.format(
              "path independent privilege: [%s] can only be set on path: root.**",
              errorPrivilegeName));
    }
  }

  // Revoke Role From User

  @Override
  public Statement visitRevokeRoleFromUser(IoTDBSqlParser.RevokeRoleFromUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.REVOKE_USER_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    return authorStatement;
  }

  // Drop User

  @Override
  public Statement visitDropUser(IoTDBSqlParser.DropUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.DROP_USER);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
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
      authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    }
    return authorStatement;
  }

  // List Privileges

  @Override
  public Statement visitListPrivilegesUser(IoTDBSqlParser.ListPrivilegesUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.LIST_USER_PRIVILEGE);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream().map(this::parsePrefixPath).collect(Collectors.toList());
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  // List Privileges of Roles On Specific Path

  @Override
  public Statement visitListPrivilegesRole(IoTDBSqlParser.ListPrivilegesRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorType.LIST_ROLE_PRIVILEGE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream().map(this::parsePrefixPath).collect(Collectors.toList());
    authorStatement.setNodeNameList(nodeNameList);
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
  public Statement visitCreateStorageGroup(IoTDBSqlParser.CreateStorageGroupContext ctx) {
    DatabaseSchemaStatement databaseSchemaStatement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    databaseSchemaStatement.setStorageGroupPath(path);
    if (ctx.storageGroupAttributesClause() != null) {
      parseStorageGroupAttributesClause(
          databaseSchemaStatement, ctx.storageGroupAttributesClause());
    }
    return databaseSchemaStatement;
  }

  @Override
  public Statement visitAlterStorageGroup(IoTDBSqlParser.AlterStorageGroupContext ctx) {
    DatabaseSchemaStatement databaseSchemaStatement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.ALTER);
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    databaseSchemaStatement.setStorageGroupPath(path);
    parseStorageGroupAttributesClause(databaseSchemaStatement, ctx.storageGroupAttributesClause());
    return databaseSchemaStatement;
  }

  private void parseStorageGroupAttributesClause(
      DatabaseSchemaStatement databaseSchemaStatement,
      IoTDBSqlParser.StorageGroupAttributesClauseContext ctx) {
    for (IoTDBSqlParser.StorageGroupAttributeClauseContext attribute :
        ctx.storageGroupAttributeClause()) {
      if (attribute.TTL() != null) {
        long ttl = Long.parseLong(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setTTL(ttl);
      } else if (attribute.SCHEMA_REPLICATION_FACTOR() != null) {
        int schemaReplicationFactor = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setSchemaReplicationFactor(schemaReplicationFactor);
      } else if (attribute.DATA_REPLICATION_FACTOR() != null) {
        int dataReplicationFactor = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setDataReplicationFactor(dataReplicationFactor);
      } else if (attribute.TIME_PARTITION_INTERVAL() != null) {
        long timePartitionInterval = Long.parseLong(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setTimePartitionInterval(timePartitionInterval);
      } else if (attribute.SCHEMA_REGION_GROUP_NUM() != null) {
        int schemaRegionGroupNum = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setSchemaRegionGroupNum(schemaRegionGroupNum);
      } else if (attribute.DATA_REGION_GROUP_NUM() != null) {
        int dataRegionGroupNum = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        databaseSchemaStatement.setDataRegionGroupNum(dataRegionGroupNum);
      }
    }
  }

  @Override
  public Statement visitSetTTL(IoTDBSqlParser.SetTTLContext ctx) {
    SetTTLStatement setTTLStatement = new SetTTLStatement();
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    long ttl = Long.parseLong(ctx.INTEGER_LITERAL().getText());
    setTTLStatement.setStorageGroupPath(path);
    setTTLStatement.setTTL(ttl);
    return setTTLStatement;
  }

  @Override
  public Statement visitUnsetTTL(IoTDBSqlParser.UnsetTTLContext ctx) {
    UnSetTTLStatement unSetTTLStatement = new UnSetTTLStatement();
    PartialPath partialPath = parsePrefixPath(ctx.prefixPath());
    unSetTTLStatement.setStorageGroupPath(partialPath);
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
    showTTLStatement.setAll(true);
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
  public Statement visitDeleteStorageGroup(IoTDBSqlParser.DeleteStorageGroupContext ctx) {
    DeleteStorageGroupStatement deleteStorageGroupStatement = new DeleteStorageGroupStatement();
    List<IoTDBSqlParser.PrefixPathContext> prefixPathContexts = ctx.prefixPath();
    List<String> paths = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPathContext : prefixPathContexts) {
      paths.add(parsePrefixPath(prefixPathContext).getFullPath());
    }
    deleteStorageGroupStatement.setPrefixPath(paths);
    return deleteStorageGroupStatement;
  }

  // Explain ========================================================================
  @Override
  public Statement visitExplain(IoTDBSqlParser.ExplainContext ctx) {
    QueryStatement queryStatement = (QueryStatement) visitSelectStatement(ctx.selectStatement());
    return new ExplainStatement(queryStatement);
  }

  @Override
  public Statement visitDeleteStatement(IoTDBSqlParser.DeleteStatementContext ctx) {
    DeleteDataStatement statement = new DeleteDataStatement();
    List<IoTDBSqlParser.PrefixPathContext> prefixPaths = ctx.prefixPath();
    List<PartialPath> pathList = new ArrayList<>();
    for (IoTDBSqlParser.PrefixPathContext prefixPath : prefixPaths) {
      pathList.add(parsePrefixPath(prefixPath));
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
      return new TimeRange(
          Math.max(leftTimeRange.getMin(), rightTimeRange.getMin()),
          Math.min(leftTimeRange.getMax(), rightTimeRange.getMax()));
    } else if (predicate instanceof CompareBinaryExpression) {
      if (((CompareBinaryExpression) predicate).getLeftExpression() instanceof TimestampOperand) {
        return parseTimeRange(
            predicate.getExpressionType(),
            ((CompareBinaryExpression) predicate).getLeftExpression(),
            ((CompareBinaryExpression) predicate).getRightExpression());
      } else {
        return parseTimeRange(
            predicate.getExpressionType(),
            ((CompareBinaryExpression) predicate).getRightExpression(),
            ((CompareBinaryExpression) predicate).getLeftExpression());
      }
    } else {
      throw new SemanticException(DELETE_RANGE_ERROR_MSG);
    }
  }

  private TimeRange parseTimeRange(
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
      if (context.OPERATOR_NOT() != null) {
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
      if (context.OPERATOR_AND() != null) {
        return new LogicAndExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_OR() != null) {
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

      if (context.OPERATOR_BETWEEN() != null) {
        return new BetweenExpression(
            firstExpression, secondExpression, thirdExpression, context.OPERATOR_NOT() != null);
      }
      throw new UnsupportedOperationException();
    }

    if (context.unaryBeforeInExpression != null) {
      return parseInExpression(context, canUseFullPath);
    }

    if (context.castInput != null) {
      return parseCastFunction(context, canUseFullPath);
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

    throw new UnsupportedOperationException();
  }

  private Expression parseCastFunction(
      IoTDBSqlParser.ExpressionContext castClause, boolean canUseFullPath) {
    FunctionExpression functionExpression = new FunctionExpression(CAST_FUNCTION);
    functionExpression.addExpression(parseExpression(castClause.castInput, canUseFullPath));
    functionExpression.addAttribute(CAST_TYPE, parseAttributeValue(castClause.attributeValue()));
    return functionExpression;
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
    } else if (functionExpression.isBuiltInScalarFunction()) {
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
      case SqlConstant.MIN_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.EXTREME:
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.TIME_DURATION:
        checkFunctionExpressionInputSize(
            functionExpression.getExpressionString(),
            functionExpression.getExpressions().size(),
            1);
        return;
      case SqlConstant.COUNT_IF:
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
        parseStringLiteral(context.STRING_LITERAL().getText()));
  }

  private Expression parseLikeExpression(ExpressionContext context, boolean canUseFullPath) {
    return new LikeExpression(
        parseExpression(context.unaryBeforeRegularOrLikeExpression, canUseFullPath),
        parseStringLiteral(context.STRING_LITERAL().getText()));
  }

  private Expression parseIsNullExpression(ExpressionContext context, boolean canUseFullPath) {
    return new IsNullExpression(
        parseExpression(context.unaryBeforeIsNullExpression, canUseFullPath),
        context.OPERATOR_NOT() != null);
  }

  private Expression parseInExpression(ExpressionContext context, boolean canUseFullPath) {
    Expression childExpression = parseExpression(context.unaryBeforeInExpression, canUseFullPath);
    LinkedHashSet<String> values = new LinkedHashSet<>();
    for (ConstantContext constantContext : context.constant()) {
      values.add(parseConstant(constantContext));
    }
    return new InExpression(childExpression, context.OPERATOR_NOT() != null, values);
  }

  private String parseConstant(ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.BOOLEAN_LITERAL() != null
        || constantContext.INTEGER_LITERAL() != null
        || constantContext.realLiteral() != null) {
      return text;
    } else if (constantContext.STRING_LITERAL() != null) {
      return parseStringLiteral(text);
    } else if (constantContext.dateExpression() != null) {
      return String.valueOf(parseDateExpression(constantContext.dateExpression()));
    } else {
      throw new IllegalArgumentException("Unsupported constant value: " + text);
    }
  }

  private Expression parseConstantOperand(ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.BOOLEAN_LITERAL() != null) {
      return new ConstantOperand(TSDataType.BOOLEAN, text);
    } else if (constantContext.STRING_LITERAL() != null) {
      return new ConstantOperand(TSDataType.TEXT, parseStringLiteral(text));
    } else if (constantContext.INTEGER_LITERAL() != null) {
      return new ConstantOperand(TSDataType.INT64, text);
    } else if (constantContext.realLiteral() != null) {
      return parseRealLiteral(text);
    } else if (constantContext.dateExpression() != null) {
      return new ConstantOperand(
          TSDataType.INT64, String.valueOf(parseDateExpression(constantContext.dateExpression())));
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
  private Long parseDateExpression(IoTDBSqlParser.DateExpressionContext ctx) {
    long time;
    time = parseDateFormat(ctx.getChild(0).getText());
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time += DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  private Long parseDateExpression(IoTDBSqlParser.DateExpressionContext ctx, long currentTime) {
    long time;
    time = parseDateFormat(ctx.getChild(0).getText(), currentTime);
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time += DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
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
      return parseDateFormat(ctx.datetimeLiteral().getText(), currentTime);
    }
  }

  /** Utils */
  private void setMap(IoTDBSqlParser.AlterClauseContext ctx, Map<String, String> alterMap) {
    List<IoTDBSqlParser.AttributePairContext> tagsList = ctx.attributePair();
    if (ctx.attributePair(0) != null) {
      for (IoTDBSqlParser.AttributePairContext attributePair : tagsList) {
        String value;
        value = parseAttributeValue(attributePair.attributeValue());
        alterMap.put(parseAttributeKey(attributePair.attributeKey()), value);
      }
    }
  }

  private Map<String, String> extractMap(
      List<IoTDBSqlParser.AttributePairContext> attributePair2,
      IoTDBSqlParser.AttributePairContext attributePair3) {
    Map<String, String> tags = new HashMap<>(attributePair2.size());
    if (attributePair3 != null) {
      for (IoTDBSqlParser.AttributePairContext attributePair : attributePair2) {
        tags.put(
            parseAttributeKey(attributePair.attributeKey()),
            parseAttributeValue(attributePair.attributeValue()));
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

  // Merge
  @Override
  public Statement visitMerge(IoTDBSqlParser.MergeContext ctx) {
    MergeStatement mergeStatement = new MergeStatement(StatementType.MERGE);
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException("MERGE ON CLUSTER is not supported in standalone mode");
    }
    mergeStatement.setOnCluster(ctx.LOCAL() == null);
    return mergeStatement;
  }

  @Override
  public Statement visitFullMerge(IoTDBSqlParser.FullMergeContext ctx) {
    MergeStatement mergeStatement = new MergeStatement(StatementType.FULL_MERGE);
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException("FULL MERGE ON CLUSTER is not supported in standalone mode");
    }
    mergeStatement.setOnCluster(ctx.LOCAL() == null);
    return mergeStatement;
  }

  // Flush

  @Override
  public Statement visitFlush(IoTDBSqlParser.FlushContext ctx) {
    FlushStatement flushStatement = new FlushStatement(StatementType.FLUSH);
    List<PartialPath> storageGroups = null;
    if (ctx.BOOLEAN_LITERAL() != null) {
      flushStatement.setSeq(Boolean.parseBoolean(ctx.BOOLEAN_LITERAL().getText()));
    }
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException("FLUSH ON CLUSTER is not supported in standalone mode");
    }
    flushStatement.setOnCluster(ctx.LOCAL() == null);
    if (ctx.prefixPath(0) != null) {
      storageGroups = new ArrayList<>();
      for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
        storageGroups.add(parsePrefixPath(prefixPathContext));
      }
    }
    flushStatement.setStorageGroups(storageGroups);
    return flushStatement;
  }

  // Clear Cache

  @Override
  public Statement visitClearCache(IoTDBSqlParser.ClearCacheContext ctx) {
    ClearCacheStatement clearCacheStatement = new ClearCacheStatement(StatementType.CLEAR_CACHE);
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException("CLEAR CACHE ON CLUSTER is not supported in standalone mode");
    }
    clearCacheStatement.setOnCluster(ctx.LOCAL() == null);
    return clearCacheStatement;
  }

  // Load Configuration

  @Override
  public Statement visitLoadConfiguration(IoTDBSqlParser.LoadConfigurationContext ctx) {
    LoadConfigurationStatement loadConfigurationStatement =
        new LoadConfigurationStatement(StatementType.LOAD_CONFIGURATION);
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException(
          "LOAD CONFIGURATION ON CLUSTER is not supported in standalone mode");
    }
    loadConfigurationStatement.setOnCluster(ctx.LOCAL() == null);
    return loadConfigurationStatement;
  }

  // Set System Status

  @Override
  public Statement visitSetSystemStatus(IoTDBSqlParser.SetSystemStatusContext ctx) {
    SetSystemStatusStatement setSystemStatusStatement = new SetSystemStatusStatement();
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException(
          "SET SYSTEM STATUS ON CLUSTER is not supported in standalone mode");
    }
    setSystemStatusStatement.setOnCluster(ctx.LOCAL() == null);
    if (ctx.RUNNING() != null) {
      setSystemStatusStatement.setStatus(NodeStatus.Running);
    } else if (ctx.READONLY() != null) {
      setSystemStatusStatement.setStatus(NodeStatus.ReadOnly);
    } else {
      throw new RuntimeException("Unknown system status in set system command.");
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
                  SortKey.TIME,
                  SortKey.QUERYID,
                  SortKey.DATANODEID,
                  SortKey.ELAPSEDTIME,
                  SortKey.STATEMENT)));
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

    showQueriesStatement.setZoneId(zoneId);
    return showQueriesStatement;
  }

  // show region

  @Override
  public Statement visitShowRegion(IoTDBSqlParser.ShowRegionContext ctx) {
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
      List<PartialPath> storageGroups = null;
      if (ctx.prefixPath(0) != null) {
        storageGroups = new ArrayList<>();
        for (IoTDBSqlParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
          storageGroups.add(parsePrefixPath(prefixPathContext));
        }
      }
      showRegionStatement.setStorageGroups(storageGroups);
    } else {
      showRegionStatement.setStorageGroups(null);
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

  // show confignodes

  @Override
  public Statement visitShowConfigNodes(IoTDBSqlParser.ShowConfigNodesContext ctx) {
    return new ShowConfigNodesStatement();
  }

  // schema template

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
        parseAttributeClause(
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
        parseAttributeClause(
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

  void parseAttributeClause(
      IoTDBSqlParser.AttributeClausesContext ctx,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors) {
    if (ctx.aliasNodeName() != null) {
      throw new SemanticException("schema template: alias is not supported yet.");
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
    if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase())) {
      String encodingString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase()).toUpperCase();
      try {
        encoding = TSEncoding.valueOf(encodingString);
        encodings.add(encoding);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_ENCODING.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported encoding: %s", encodingString));
      }
    } else {
      encodings.add(encoding);
    }

    CompressionType compressor = TSFileDescriptor.getInstance().getConfig().getCompressor();
    if (props.containsKey(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase())) {
      String compressorString =
          props.get(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase()).toUpperCase();
      try {
        compressor = CompressionType.valueOf(compressorString);
        compressors.add(compressor);
        props.remove(IoTDBConstant.COLUMN_TIMESERIES_COMPRESSOR.toLowerCase());
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported compressor: %s", compressorString));
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
            String.format("unsupported compression: %s", compressionString));
      }
    } else {
      compressors.add(compressor);
    }

    if (props.size() > 0) {
      throw new SemanticException("schema template: property is not supported yet.");
    }

    if (ctx.tagClause() != null) {
      throw new SemanticException("schema template: tag is not supported yet.");
    }

    if (ctx.attributeClause() != null) {
      throw new SemanticException("schema template: attribute is not supported yet.");
    }
  }

  private TSDataType parseDataTypeAttribute(IoTDBSqlParser.AttributeClausesContext ctx) {
    TSDataType dataType = null;
    if (ctx.dataType != null) {
      if (ctx.attributeKey() != null
          && !parseAttributeKey(ctx.attributeKey())
              .equalsIgnoreCase(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE)) {
        throw new SemanticException("expecting datatype");
      }
      String dataTypeString = ctx.dataType.getText().toUpperCase();
      try {
        dataType = TSDataType.valueOf(dataTypeString);
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported datatype: %s", dataTypeString));
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
  public Statement visitCreateTimeseriesOfSchemaTemplate(
      IoTDBSqlParser.CreateTimeseriesOfSchemaTemplateContext ctx) {
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
  public Statement visitDeleteTimeseriesOfSchemaTemplate(
      IoTDBSqlParser.DeleteTimeseriesOfSchemaTemplateContext ctx) {
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

  public Map<String, String> parseSyncAttributeClauses(
      IoTDBSqlParser.SyncAttributeClausesContext ctx) {

    Map<String, String> attributes = new HashMap<>();

    List<IoTDBSqlParser.AttributePairContext> attributePairs = ctx.attributePair();
    if (ctx.attributePair(0) != null) {
      for (IoTDBSqlParser.AttributePairContext attributePair : attributePairs) {
        attributes.put(
            parseAttributeKey(attributePair.attributeKey()).toLowerCase(),
            parseAttributeValue(attributePair.attributeValue()).toLowerCase());
      }
    }

    return attributes;
  }

  private PartialPath parsePathFromExpression(Expression expression) {
    if (expression instanceof TimeSeriesOperand) {
      return ((TimeSeriesOperand) expression).getPath();
    } else if (expression instanceof TimestampOperand) {
      return SqlConstant.TIME_PATH;
    } else {
      throw new IllegalArgumentException(
          "Unsupported expression type: " + expression.getExpressionType());
    }
  }

  // PIPE

  @Override
  public Statement visitShowPipe(IoTDBSqlParser.ShowPipeContext ctx) {
    ShowPipeStatement showPipeStatement = new ShowPipeStatement();
    if (ctx.pipeName != null) {
      showPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    }
    showPipeStatement.setWhereClause(ctx.CONNECTOR() != null);
    return showPipeStatement;
  }

  @Override
  public Statement visitCreatePipe(IoTDBSqlParser.CreatePipeContext ctx) {

    CreatePipeStatement createPipeStatement = new CreatePipeStatement(StatementType.CREATE_PIPE);

    if (ctx.pipeName != null) {
      createPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in CREATEPIPE, please enter pipename or pipesinkname.");
    }
    if (ctx.collectorAttributesClause() != null) {
      createPipeStatement.setCollectorAttributes(
          parseCollectorAttributesClause(ctx.collectorAttributesClause()));
    } else {
      createPipeStatement.setCollectorAttributes(new HashMap<>());
    }
    if (ctx.processorAttributesClause() != null) {
      createPipeStatement.setProcessorAttributes(
          parseProcessorAttributesClause(ctx.processorAttributesClause()));
    } else {
      createPipeStatement.setProcessorAttributes(new HashMap<>());
    }
    createPipeStatement.setConnectorAttributes(
        parseConnectorAttributesClause(ctx.connectorAttributesClause()));
    return createPipeStatement;
  }

  private Map<String, String> parseCollectorAttributesClause(
      IoTDBSqlParser.CollectorAttributesClauseContext ctx) {
    Map<String, String> collectorMap = new HashMap<>();
    for (IoTDBSqlParser.CollectorAttributeClauseContext singleCtx :
        ctx.collectorAttributeClause()) {
      collectorMap.put(singleCtx.collectorKey.getText(), singleCtx.collectorValue.getText());
    }
    return collectorMap;
  }

  private Map<String, String> parseProcessorAttributesClause(
      IoTDBSqlParser.ProcessorAttributesClauseContext ctx) {
    Map<String, String> processorMap = new HashMap<>();
    for (IoTDBSqlParser.ProcessorAttributeClauseContext singleCtx :
        ctx.processorAttributeClause()) {
      processorMap.put(singleCtx.processorKey.getText(), singleCtx.processorValue.getText());
    }
    return processorMap;
  }

  private Map<String, String> parseConnectorAttributesClause(
      IoTDBSqlParser.ConnectorAttributesClauseContext ctx) {
    Map<String, String> connectorMap = new HashMap<>();
    for (IoTDBSqlParser.ConnectorAttributeClauseContext singleCtx :
        ctx.connectorAttributeClause()) {
      connectorMap.put(singleCtx.connectorKey.getText(), singleCtx.connectorValue.getText());
    }
    return connectorMap;
  }

  @Override
  public Statement visitStartPipe(IoTDBSqlParser.StartPipeContext ctx) {
    StartPipeStatement startPipeStatement = new StartPipeStatement(StatementType.START_PIPE);

    if (ctx.pipeName != null) {
      startPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException("Not support for this sql in STARTPIPE, please enter pipename.");
    }
    return startPipeStatement;
  }

  @Override
  public Statement visitStopPipe(IoTDBSqlParser.StopPipeContext ctx) {
    StopPipeStatement stopPipeStatement = new StopPipeStatement(StatementType.STOP_PIPE);

    if (ctx.pipeName != null) {
      stopPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException("Not support for this sql in STOPPIPE, please enter pipename.");
    }
    return stopPipeStatement;
  }

  @Override
  public Statement visitDropPipe(IoTDBSqlParser.DropPipeContext ctx) {

    DropPipeStatement dropPipeStatement = new DropPipeStatement(StatementType.DROP_PIPE);

    if (ctx.pipeName != null) {
      dropPipeStatement.setPipeName(parseIdentifier(ctx.pipeName.getText()));
    } else {
      throw new SemanticException("Not support for this sql in DROPPIPE, please enter pipename.");
    }
    return dropPipeStatement;
  }

  // pipeSink

  @Override
  public Statement visitShowPipeSink(IoTDBSqlParser.ShowPipeSinkContext ctx) {
    ShowPipeSinkStatement showPipeSinkStatement = new ShowPipeSinkStatement();
    if (ctx.pipeSinkName != null) {
      showPipeSinkStatement.setPipeSinkName(parseIdentifier(ctx.pipeSinkName.getText()));
    }
    return showPipeSinkStatement;
  }

  @Override
  public Statement visitShowPipeSinkType(IoTDBSqlParser.ShowPipeSinkTypeContext ctx) {
    return new ShowPipeSinkTypeStatement();
  }

  @Override
  public Statement visitCreatePipeSink(IoTDBSqlParser.CreatePipeSinkContext ctx) {

    CreatePipeSinkStatement createPipeSinkStatement = new CreatePipeSinkStatement();

    if (ctx.pipeSinkName != null) {
      createPipeSinkStatement.setPipeSinkName(parseIdentifier(ctx.pipeSinkName.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in CREATEPIPESINK, please enter pipesinkname.");
    }
    if (ctx.pipeSinkType != null) {
      createPipeSinkStatement.setPipeSinkType(parseIdentifier(ctx.pipeSinkType.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in CREATEPIPESINK, please enter pipesinktype.");
    }
    if (ctx.syncAttributeClauses() != null) {
      createPipeSinkStatement.setAttributes(parseSyncAttributeClauses(ctx.syncAttributeClauses()));
    } else {
      createPipeSinkStatement.setAttributes(new HashMap<>());
    }

    return createPipeSinkStatement;
  }

  @Override
  public Statement visitDropPipeSink(IoTDBSqlParser.DropPipeSinkContext ctx) {

    DropPipeSinkStatement dropPipeSinkStatement =
        new DropPipeSinkStatement(StatementType.DROP_PIPESINK);

    if (ctx.pipeSinkName != null) {
      dropPipeSinkStatement.setPipeSinkName(parseIdentifier(ctx.pipeSinkName.getText()));
    } else {
      throw new SemanticException(
          "Not support for this sql in DROPPIPESINK, please enter pipesinkname.");
    }
    return dropPipeSinkStatement;
  }

  @Override
  public Statement visitGetRegionId(IoTDBSqlParser.GetRegionIdContext ctx) {
    TConsensusGroupType type =
        ctx.DATA() == null ? TConsensusGroupType.SchemaRegion : TConsensusGroupType.DataRegion;
    GetRegionIdStatement getRegionIdStatement = new GetRegionIdStatement(ctx.path.getText(), type);
    if (ctx.seriesSlot != null) {
      getRegionIdStatement.setSeriesSlotId(
          new TSeriesPartitionSlot(Integer.parseInt(ctx.seriesSlot.getText())));
    } else {
      getRegionIdStatement.setDeviceId(ctx.deviceId.getText());
    }
    if (ctx.timeSlot != null) {
      getRegionIdStatement.setTimeSlotId(
          new TTimePartitionSlot(
              Long.parseLong(ctx.timeSlot.getText()) * CONFIG.getTimePartitionInterval()));
    } else if (ctx.timeStamp != null) {
      getRegionIdStatement.setTimeStamp(Long.parseLong(ctx.timeStamp.getText()));
    }
    return getRegionIdStatement;
  }

  @Override
  public Statement visitGetSeriesSlotList(IoTDBSqlParser.GetSeriesSlotListContext ctx) {
    GetSeriesSlotListStatement getSeriesSlotListStatement =
        new GetSeriesSlotListStatement(ctx.prefixPath().getText());
    if (ctx.DATA() != null) {
      getSeriesSlotListStatement.setPartitionType(TConsensusGroupType.DataRegion);
    } else if (ctx.SCHEMA() != null) {
      getSeriesSlotListStatement.setPartitionType(TConsensusGroupType.SchemaRegion);
    }
    return getSeriesSlotListStatement;
  }

  @Override
  public Statement visitGetTimeSlotList(IoTDBSqlParser.GetTimeSlotListContext ctx) {
    GetTimeSlotListStatement getTimeSlotListStatement =
        new GetTimeSlotListStatement(
            ctx.prefixPath().getText(),
            new TSeriesPartitionSlot(Integer.parseInt(ctx.seriesSlot.getText())));
    if (ctx.startTime != null) {
      getTimeSlotListStatement.setStartTime(Long.parseLong(ctx.startTime.getText()));
    }
    if (ctx.endTime != null) {
      getTimeSlotListStatement.setEndTime(Long.parseLong(ctx.endTime.getText()));
    }
    return getTimeSlotListStatement;
  }

  @Override
  public Statement visitMigrateRegion(IoTDBSqlParser.MigrateRegionContext ctx) {
    return new MigrateRegionStatement(
        Integer.parseInt(ctx.regionId.getText()),
        Integer.parseInt(ctx.fromId.getText()),
        Integer.parseInt(ctx.toId.getText()));
  }
}
