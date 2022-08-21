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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.filter.BasicFunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
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
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.FillComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.HavingCondition;
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
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ExplainStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.MergeStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowVersionStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ConstantContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountDevicesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountNodesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountStorageGroupContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CountTimeseriesContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.CreateFunctionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.DropFunctionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ExpressionContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.ShowFunctionsContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser.UriContext;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParserBaseVisitor;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

  /** For create-cq clause and select-into clause, used to match "{x}", where x is an integer. */
  private static final Pattern leveledPathNodePattern = Pattern.compile("\\$\\{\\w+}");

  // TODO: add comment
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
    if (ctx.dataType != null) {
      if (ctx.attributeKey() != null) {
        if (!parseAttributeKey(ctx.attributeKey())
            .equalsIgnoreCase(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE)) {
          throw new SQLParserException("expecting datatype");
        }
      }
      props.put(
          IoTDBConstant.COLUMN_TIMESERIES_DATATYPE.toLowerCase(),
          parseAttributeValue(ctx.dataType).toLowerCase());
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

    TSDataType dataType = null;
    if (ctx.dataType != null) {
      if (ctx.attributeKey() != null) {
        if (!parseAttributeKey(ctx.attributeKey())
            .equalsIgnoreCase(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE)) {
          throw new SQLParserException("expecting datatype");
        }
      }
      String dataTypeString = ctx.dataType.getText().toUpperCase();
      try {
        dataType = TSDataType.valueOf(dataTypeString);
        createAlignedTimeSeriesStatement.addDataType(dataType);
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported datatype: %s", dataTypeString));
      }
    }

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
      throw new SQLParserException("create aligned timeseries: property is not supported yet.");
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
      alterTimeSeriesStatement.setAlias(parseAlias(ctx.alias()));
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
    deleteTimeSeriesStatement.setPartialPaths(partialPaths);
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
              new PartialPath(SQLConstant.getSingleRootArray()), orderByHeat);
    }
    if (ctx.tagWhereClause() != null) {
      parseTagWhereClause(ctx.tagWhereClause(), showTimeSeriesStatement);
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), showTimeSeriesStatement);
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

  // Show Storage Group

  @Override
  public Statement visitShowStorageGroup(IoTDBSqlParser.ShowStorageGroupContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowStorageGroupStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowStorageGroupStatement(new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  // Show Devices ========================================================================

  @Override
  public Statement visitShowDevices(IoTDBSqlParser.ShowDevicesContext ctx) {
    ShowDevicesStatement showDevicesStatement;
    if (ctx.prefixPath() != null) {
      showDevicesStatement = new ShowDevicesStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      showDevicesStatement =
          new ShowDevicesStatement(new PartialPath(SQLConstant.getSingleRootArray()));
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), showDevicesStatement);
    }
    // show devices wtih storage group
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
      path = new PartialPath(SQLConstant.getSingleRootArray());
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
      path = new PartialPath(SQLConstant.getSingleRootArray());
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
      path = new PartialPath(SQLConstant.getSingleRootArray());
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
      path = new PartialPath(SQLConstant.getSingleRootArray());
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
    return new CreateFunctionStatement(
        parseIdentifier(ctx.udfName.getText()),
        parseStringLiteral(ctx.className.getText()),
        parseUris(ctx.uri()));
  }

  private List<URI> parseUris(List<UriContext> uriContexts) {
    List<URI> uris = new ArrayList<>();
    if (uriContexts != null) {
      for (UriContext uriContext : uriContexts) {
        final String uriString = uriContext.getText();
        try {
          uris.add(new URI(parseStringLiteral(uriString)));
        } catch (URISyntaxException e) {
          throw new SemanticException(String.format("'%s' is not a legal URI.", uriString));
        }
      }
    }
    return uris;
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

  // Show Child Paths =====================================================================
  @Override
  public Statement visitShowChildPaths(IoTDBSqlParser.ShowChildPathsContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowChildPathsStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowChildPathsStatement(new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  // Show Child Nodes =====================================================================
  @Override
  public Statement visitShowChildNodes(IoTDBSqlParser.ShowChildNodesContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowChildNodesStatement(parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowChildNodesStatement(new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  /** Data Manipulation Language (DML) */

  // Select Statement ========================================================================

  private QueryStatement queryStatement;

  @Override
  public Statement visitSelectStatement(IoTDBSqlParser.SelectStatementContext ctx) {
    if (ctx.intoClause() != null) {
      throw new SemanticException(
          "The SELECT-INTO statement is not supported in the current version.");
    }

    // initialize query statement
    queryStatement = new QueryStatement();

    // parser select, from
    parseSelectClause(ctx.selectClause());
    parseFromClause(ctx.fromClause());

    // parse where clause
    if (ctx.whereClause() != null) {
      WhereCondition whereCondition = parseWhereClause(ctx.whereClause());
      queryStatement.setWhereCondition(whereCondition);
    }

    // parser special clause
    if (ctx.specialClause() != null) {
      queryStatement = (QueryStatement) visit(ctx.specialClause());
    }
    return queryStatement;
  }

  // Select Clause

  public void parseSelectClause(IoTDBSqlParser.SelectClauseContext ctx) {
    SelectComponent selectComponent = new SelectComponent(zoneId);

    // parse LAST
    if (ctx.LAST() != null) {
      selectComponent.setHasLast(true);
    }

    // parse resultColumn
    Map<String, Expression> aliasToColumnMap = new HashMap<>();
    for (IoTDBSqlParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      ResultColumn resultColumn = parseResultColumn(resultColumnContext);
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

    // set selectComponent
    queryStatement.setSelectComponent(selectComponent);
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

  // From Clause

  public void parseFromClause(IoTDBSqlParser.FromClauseContext ctx) {
    FromComponent fromComponent = new FromComponent();
    List<IoTDBSqlParser.PrefixPathContext> prefixFromPaths = ctx.prefixPath();
    for (IoTDBSqlParser.PrefixPathContext prefixFromPath : prefixFromPaths) {
      PartialPath path = parsePrefixPath(prefixFromPath);
      fromComponent.addPrefixPath(path);
    }
    queryStatement.setFromComponent(fromComponent);
  }

  // Where Clause

  public WhereCondition parseWhereClause(IoTDBSqlParser.WhereClauseContext ctx) {
    Expression predicate =
        parseExpression(ctx.expression(), ctx.expression().OPERATOR_NOT() == null);
    return new WhereCondition(predicate);
  }

  // Group By Time Clause

  @Override
  public Statement visitGroupByTimeStatement(IoTDBSqlParser.GroupByTimeStatementContext ctx) {
    // parse group by time clause
    parseGroupByTimeClause(ctx.groupByTimeClause());

    // parse order by time
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause());
    }

    // parse Having
    if (ctx.havingClause() != null) {
      parseHavingClause(ctx.havingClause());
    }

    // parse limit & offset
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }

    return queryStatement;
  }

  @Override
  public Statement visitGroupByFillStatement(IoTDBSqlParser.GroupByFillStatementContext ctx) {
    // parse group by time clause & fill clause
    parseGroupByTimeClause(ctx.groupByFillClause());

    // parse order by time
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause());
    }

    // parse Having
    if (ctx.havingClause() != null) {
      parseHavingClause(ctx.havingClause());
    }

    // parse limit & offset
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryStatement;
  }

  private void parseGroupByTimeClause(IoTDBSqlParser.GroupByTimeClauseContext ctx) {
    GroupByTimeComponent groupByTimeComponent = new GroupByTimeComponent();

    // parse time range
    parseTimeRange(ctx.timeRange(), groupByTimeComponent);
    groupByTimeComponent.setLeftCRightO(ctx.timeRange().LS_BRACKET() != null);

    // parse time interval
    groupByTimeComponent.setInterval(
        parseTimeIntervalOrSlidingStep(
            ctx.DURATION_LITERAL(0).getText(), true, groupByTimeComponent));
    if (groupByTimeComponent.getInterval() <= 0) {
      throw new SemanticException(
          "The second parameter time interval should be a positive integer.");
    }

    // parse sliding step
    if (ctx.DURATION_LITERAL().size() == 2) {
      groupByTimeComponent.setSlidingStep(
          parseTimeIntervalOrSlidingStep(
              ctx.DURATION_LITERAL(1).getText(), false, groupByTimeComponent));
    } else {
      groupByTimeComponent.setSlidingStep(groupByTimeComponent.getInterval());
      groupByTimeComponent.setSlidingStepByMonth(groupByTimeComponent.isIntervalByMonth());
    }

    // parse GroupByLevel clause
    if (ctx.LEVEL() != null && ctx.INTEGER_LITERAL() != null) {
      GroupByLevelComponent groupByLevelComponent = new GroupByLevelComponent();
      int[] levels = new int[ctx.INTEGER_LITERAL().size()];
      for (int i = 0; i < ctx.INTEGER_LITERAL().size(); i++) {
        levels[i] = Integer.parseInt(ctx.INTEGER_LITERAL().get(i).getText());
      }
      groupByLevelComponent.setLevels(levels);
      queryStatement.setGroupByLevelComponent(groupByLevelComponent);
    }

    // parse fill clause
    if (ctx.fillClause() != null) {
      parseFillClause(ctx.fillClause());
    }

    // set groupByTimeComponent
    queryStatement.setGroupByTimeComponent(groupByTimeComponent);
  }

  private void parseGroupByTimeClause(IoTDBSqlParser.GroupByFillClauseContext ctx) {
    GroupByTimeComponent groupByTimeComponent = new GroupByTimeComponent();

    // parse time range
    parseTimeRange(ctx.timeRange(), groupByTimeComponent);
    groupByTimeComponent.setLeftCRightO(ctx.timeRange().LS_BRACKET() != null);

    // parse time interval
    groupByTimeComponent.setInterval(
        parseTimeIntervalOrSlidingStep(
            ctx.DURATION_LITERAL(0).getText(), true, groupByTimeComponent));
    if (groupByTimeComponent.getInterval() <= 0) {
      throw new SemanticException(
          "The second parameter time interval should be a positive integer.");
    }

    // parse sliding step
    if (ctx.DURATION_LITERAL().size() == 2) {
      groupByTimeComponent.setSlidingStep(
          parseTimeIntervalOrSlidingStep(
              ctx.DURATION_LITERAL(1).getText(), false, groupByTimeComponent));
    } else {
      groupByTimeComponent.setSlidingStep(groupByTimeComponent.getInterval());
      groupByTimeComponent.setSlidingStepByMonth(groupByTimeComponent.isIntervalByMonth());
    }

    // parse fill clause
    if (ctx.fillClause() != null) {
      parseFillClause(ctx.fillClause());
    }

    // set groupByTimeComponent
    queryStatement.setGroupByTimeComponent(groupByTimeComponent);
  }

  /** parse time range (startTime and endTime) in group by query. */
  private void parseTimeRange(
      IoTDBSqlParser.TimeRangeContext timeRange, GroupByTimeComponent groupByClauseComponent) {
    long currentTime = DatetimeUtils.currentTime();
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
    return DatetimeUtils.convertDurationStrToLong(duration);
  }

  // Group By Level Clause
  @Override
  public Statement visitGroupByLevelStatement(IoTDBSqlParser.GroupByLevelStatementContext ctx) {
    // parse GroupByLevel clause
    parseGroupByLevelClause(ctx.groupByLevelClause());

    // parse order by time
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause());
    }

    // parse Having
    if (ctx.havingClause() != null) {
      parseHavingClause(ctx.havingClause());
    }

    // parse limit & offset
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryStatement;
  }

  public void parseGroupByLevelClause(IoTDBSqlParser.GroupByLevelClauseContext ctx) {
    GroupByLevelComponent groupByLevelComponent = new GroupByLevelComponent();
    int[] levels = new int[ctx.INTEGER_LITERAL().size()];
    for (int i = 0; i < ctx.INTEGER_LITERAL().size(); i++) {
      levels[i] = Integer.parseInt(ctx.INTEGER_LITERAL().get(i).getText());
    }
    groupByLevelComponent.setLevels(levels);

    // parse fill clause
    if (ctx.fillClause() != null) {
      parseFillClause(ctx.fillClause());
    }

    queryStatement.setGroupByLevelComponent(groupByLevelComponent);
  }

  // HAVING Clause
  public void parseHavingClause(IoTDBSqlParser.HavingClauseContext ctx) {
    Expression predicate =
        parseExpression(ctx.expression(), ctx.expression().OPERATOR_NOT() == null);
    queryStatement.setHavingCondition(new HavingCondition(predicate));
  }

  // Fill Clause
  @Override
  public Statement visitFillStatement(IoTDBSqlParser.FillStatementContext ctx) {
    // parse fill
    parseFillClause(ctx.fillClause());

    // parse order by time
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause());
    }

    // parse limit & offset
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryStatement;
  }

  public void parseFillClause(IoTDBSqlParser.FillClauseContext ctx) {
    FillComponent fillComponent = new FillComponent();
    if (ctx.linearClause() != null) {
      if (ctx.linearClause().DURATION_LITERAL().size() > 0) {
        throw new SemanticException("The specified fill time range is not supported.");
      }
      fillComponent.setFillPolicy(FillPolicy.LINEAR);
    } else if (ctx.previousClause() != null) {
      if (ctx.previousClause().DURATION_LITERAL() != null) {
        throw new SemanticException("The specified fill time range is not supported.");
      }
      fillComponent.setFillPolicy(FillPolicy.PREVIOUS);
    } else if (ctx.specificValueClause() != null) {
      fillComponent.setFillPolicy(FillPolicy.VALUE);
      if (ctx.specificValueClause().constant() != null) {
        Literal fillValue = parseLiteral(ctx.specificValueClause().constant());
        fillComponent.setFillValue(fillValue);
      } else {
        throw new SemanticException("fill value cannot be null");
      }
    } else if (ctx.previousUntilLastClause() != null) {
      throw new SemanticException("PREVIOUSUNTILLAST fill is not supported yet.");
    } else if (ctx.oldTypeClause() != null) {
      throw new SemanticException("The specified fill datatype is not supported.");
    }
    queryStatement.setFillComponent(fillComponent);
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
      throw new SQLParserException("Unsupported constant operand: " + text);
    }
  }

  // Other Clauses
  @Override
  public Statement visitSpecialLimitStatement(IoTDBSqlParser.SpecialLimitStatementContext ctx) {
    return visit(ctx.specialLimit());
  }

  @Override
  public Statement visitLimitStatement(IoTDBSqlParser.LimitStatementContext ctx) {
    // parse LIMIT
    parseLimitClause(ctx.limitClause(), queryStatement);

    // parse SLIMIT
    if (ctx.slimitClause() != null) {
      parseSlimitClause(ctx.slimitClause());
    }

    // parse ALIGN BY DEVICE or DISABLE ALIGN
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryStatement;
  }

  // parse LIMIT & OFFSET
  private void parseLimitClause(IoTDBSqlParser.LimitClauseContext ctx, Statement statement) {
    int limit;
    try {
      limit = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException("Out of range. LIMIT <N>: N should be Int32.");
    }
    if (limit <= 0) {
      throw new SemanticException("LIMIT <N>: N should be greater than 0.");
    }
    if (statement instanceof ShowTimeSeriesStatement) {
      ((ShowTimeSeriesStatement) statement).setLimit(limit);
    } else if (statement instanceof ShowDevicesStatement) {
      ((ShowDevicesStatement) statement).setLimit(limit);
    } else {
      queryStatement.setRowLimit(limit);
    }

    // parse OFFSET
    if (ctx.offsetClause() != null) {
      parseOffsetClause(ctx.offsetClause(), statement);
    }
  }

  // parse OFFSET
  private void parseOffsetClause(IoTDBSqlParser.OffsetClauseContext ctx, Statement statement) {
    int offset;
    try {
      offset = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException(
          "Out of range. OFFSET <OFFSETValue>: OFFSETValue should be Int32.");
    }
    if (offset < 0) {
      throw new SemanticException("OFFSET <OFFSETValue>: OFFSETValue should >= 0.");
    }
    if (statement instanceof ShowTimeSeriesStatement) {
      ((ShowTimeSeriesStatement) statement).setOffset(offset);
    } else if (statement instanceof ShowDevicesStatement) {
      ((ShowDevicesStatement) statement).setOffset(offset);
    } else {
      queryStatement.setRowOffset(offset);
    }
  }

  @Override
  public Statement visitSlimitStatement(IoTDBSqlParser.SlimitStatementContext ctx) {
    // parse SLIMIT
    parseSlimitClause(ctx.slimitClause());

    // parse LIMIT
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), queryStatement);
    }

    // parse ALIGN BY DEVICE or DISABLE ALIGN
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryStatement;
  }

  // parse SLIMIT & SOFFSET
  private void parseSlimitClause(IoTDBSqlParser.SlimitClauseContext ctx) {
    int slimit;
    try {
      slimit = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    } catch (NumberFormatException e) {
      throw new SemanticException("Out of range. SLIMIT <SN>: SN should be Int32.");
    }
    if (slimit <= 0) {
      throw new SemanticException("SLIMIT <SN>: SN should be greater than 0.");
    }
    queryStatement.setSeriesLimit(slimit);

    // parse SOFFSET
    if (ctx.soffsetClause() != null) {
      parseSoffsetClause(ctx.soffsetClause());
    }
  }

  // parse SOFFSET
  public void parseSoffsetClause(IoTDBSqlParser.SoffsetClauseContext ctx) {
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
    queryStatement.setSeriesOffset(soffset);
  }

  // WITHOUT NULL Clause

  @Override
  public Statement visitWithoutNullStatement(IoTDBSqlParser.WithoutNullStatementContext ctx) {
    // parse WITHOUT NULL
    parseWithoutNullClause(ctx.withoutNullClause());

    // parse LIMIT & OFFSET
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), queryStatement);
    }

    // parse SLIMIT & SOFFSET
    if (ctx.slimitClause() != null) {
      parseSlimitClause(ctx.slimitClause());
    }

    // parse ALIGN BY DEVICE or DISABLE ALIGN
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryStatement;
  }

  // parse WITHOUT NULL
  private void parseWithoutNullClause(IoTDBSqlParser.WithoutNullClauseContext ctx) {
    throw new SemanticException("WITHOUT NULL clause is not supported yet.");
  }

  // ORDER BY TIME Clause

  @Override
  public Statement visitOrderByTimeStatement(IoTDBSqlParser.OrderByTimeStatementContext ctx) {
    // parse ORDER BY TIME
    parseOrderByClause(ctx.orderByClause());

    // parse others
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryStatement;
  }

  // parse ORDER BY TIME
  private void parseOrderByClause(IoTDBSqlParser.OrderByClauseContext ctx) {
    OrderByComponent orderByComponent = new OrderByComponent();
    Set<SortKey> sortKeySet = new HashSet<>();
    for (IoTDBSqlParser.OrderByAttributeClauseContext orderByAttributeClauseContext :
        ctx.orderByAttributeClause()) {
      SortItem sortItem = parseOrderByAttributeClause(orderByAttributeClauseContext);

      SortKey sortKey = sortItem.getSortKey();
      if (sortKeySet.contains(sortKey)) {
        throw new SemanticException(String.format("ORDER BY: duplicate sort key '%s'", sortKey));
      } else {
        sortKeySet.add(sortKey);
        orderByComponent.addSortItem(sortItem);
      }
    }
    queryStatement.setOrderByComponent(orderByComponent);
  }

  private SortItem parseOrderByAttributeClause(IoTDBSqlParser.OrderByAttributeClauseContext ctx) {
    return new SortItem(
        SortKey.valueOf(ctx.sortKey().getText().toUpperCase()),
        ctx.DESC() != null ? Ordering.DESC : Ordering.ASC);
  }

  // ResultSetFormat Clause

  @Override
  public Statement visitAlignByDeviceClauseOrDisableAlignStatement(
      IoTDBSqlParser.AlignByDeviceClauseOrDisableAlignStatementContext ctx) {
    parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    return queryStatement;
  }

  private void parseAlignByDeviceClauseOrDisableAlign(
      IoTDBSqlParser.AlignByDeviceClauseOrDisableAlignContext ctx) {
    if (ctx.alignByDeviceClause() != null) {
      queryStatement.setResultSetFormat(ResultSetFormat.ALIGN_BY_DEVICE);
    } else {
      queryStatement.setResultSetFormat(ResultSetFormat.DISABLE_ALIGN);
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
          timestamp = DatetimeUtils.currentTime();
        } else {
          timestamp =
              parseTimeValue(insertMultiValues.get(i).timeValue(), DatetimeUtils.currentTime());
        }
      } else {
        if (!isTimeDefault) {
          throw new SemanticException(
              "the measurementList's size is not consistent with the valueList's size");
        }
        if (insertMultiValues.size() != 1) {
          throw new SemanticException("need timestamps when insert multi rows");
        }
        timestamp = parseDateFormat(SQLConstant.NOW_FUNC);
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

  /** path of expression in withoutNull clause can start with root. */
  private PartialPath parseFullPathInExpression(
      IoTDBSqlParser.FullPathInExpressionContext ctx, boolean inWithoutNull)
      throws SQLParserException {
    List<IoTDBSqlParser.NodeNameContext> nodeNames = ctx.nodeName();
    int size = nodeNames.size();
    if (ctx.ROOT() != null) {
      if (!inWithoutNull) {
        throw new SQLParserException("Path can not start with root in select clause.");
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

  private String parseNodeString(String nodeName) {
    if (nodeName.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
        || nodeName.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
      return nodeName;
    }
    if (nodeName.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && nodeName.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      String unWrapped = nodeName.substring(1, nodeName.length() - 1);
      if (StringUtils.isNumeric(unWrapped)
          || !TsFileConstant.IDENTIFIER_PATTERN.matcher(unWrapped).matches()) {
        return nodeName;
      }
      return unWrapped;
    }
    checkNodeName(nodeName);
    return nodeName;
  }

  private void checkNodeName(String src) {
    // node name could start with * and end with *
    if (!TsFileConstant.NODE_NAME_PATTERN.matcher(src).matches()) {
      throw new SQLParserException(
          String.format(
              "%s is illegal, unquoted node name can only consist of digits, characters and underscore, or start or end with wildcard",
              src));
    }
  }

  private void checkIdentifier(String src) {
    if (!TsFileConstant.IDENTIFIER_PATTERN.matcher(src).matches()) {
      throw new SQLParserException(
          String.format(
              "%s is illegal, unquoted identifier can only consist of digits, characters and underscore",
              src));
    }
  }

  // Literals ========================================================================

  public long parseDateFormat(String timestampStr) throws SQLParserException {
    if (timestampStr == null || "".equals(timestampStr.trim())) {
      throw new SemanticException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return DatetimeUtils.currentTime();
    }
    try {
      return DatetimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SQLParserException(
          String.format(
              "Input time format %s error. "
                  + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                  + "refer to user document for more info.",
              timestampStr));
    }
  }

  public long parseDateFormat(String timestampStr, long currentTime) throws SQLParserException {
    if (timestampStr == null || "".equals(timestampStr.trim())) {
      throw new SemanticException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return currentTime;
    }
    try {
      return DatetimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SQLParserException(
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
      String unWrappedString =
          src.substring(1, src.length() - 1).replace("\\\"", "\"").replace("\\'", "'");
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

  private String parseStringLiteralInLikeOrRegular(String src) {
    if (2 <= src.length()) {
      String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
      if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
        // replace "" with "
        String replaced = unescapeString.replace("\"\"", "\"");
        return replaced.length() == 0 ? "" : replaced;
      }
      if ((src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'')) {
        // replace '' with '
        String replaced = unescapeString.replace("''", "'");
        return replaced.length() == 0 ? "" : replaced;
      }
    }
    return src;
  }

  private String parseIdentifier(String src) {
    if (src.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && src.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      return src.substring(1, src.length() - 1)
          .replace(TsFileConstant.DOUBLE_BACK_QUOTE_STRING, TsFileConstant.BACK_QUOTE_STRING);
    }
    checkIdentifier(src);
    return src;
  }

  /** function for parsing Alias. */
  private String parseAlias(IoTDBSqlParser.AliasContext ctx) {
    String alias;
    if (ctx.constant() != null) {
      alias = parseStringLiteral(ctx.constant().getText());
    } else {
      alias = parseIdentifier(ctx.identifier().getText());
    }
    return alias;
  }

  /** Data Control Language (DCL) */

  // Create User

  @Override
  public Statement visitCreateUser(IoTDBSqlParser.CreateUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.CREATE_USER);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    authorStatement.setPassWord(parseStringLiteral(ctx.password.getText()));
    return authorStatement;
  }

  // Create Role

  @Override
  public Statement visitCreateRole(IoTDBSqlParser.CreateRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.CREATE_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    return authorStatement;
  }

  // Alter Password

  @Override
  public Statement visitAlterUser(IoTDBSqlParser.AlterUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.UPDATE_USER);
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.GRANT_USER);
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.GRANT_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setPrivilegeList(privileges);
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  // Grant User Role

  @Override
  public Statement visitGrantRoleToUser(IoTDBSqlParser.GrantRoleToUserContext ctx) {
    AuthorStatement authorStatement =
        new AuthorStatement(AuthorOperator.AuthorType.GRANT_ROLE_TO_USER);
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.REVOKE_USER);
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

    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.REVOKE_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setPrivilegeList(privileges);
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  private void checkGrantRevokePrivileges(String[] privileges, List<PartialPath> nodeNameList) {
    if (nodeNameList.isEmpty()) {
      nodeNameList.addAll(Collections.singletonList(new PartialPath(ALL_RESULT_NODES)));
      return;
    }
    boolean pathRelevant = true;
    String errorPrivilegeName = "";
    for (String privilege : privileges) {
      if (!PrivilegeType.valueOf(privilege.toUpperCase()).isPathRelevant()) {
        pathRelevant = false;
        errorPrivilegeName = privilege.toUpperCase();
        break;
      }
    }
    if (!(pathRelevant
        || (nodeNameList.size() == 1
            && nodeNameList.contains(new PartialPath(ALL_RESULT_NODES))))) {
      throw new SQLParserException(
          String.format(
              "path independent privilege: [%s] can only be set on path: root.**",
              errorPrivilegeName));
    }
  }

  // Revoke Role From User

  @Override
  public Statement visitRevokeRoleFromUser(IoTDBSqlParser.RevokeRoleFromUserContext ctx) {
    AuthorStatement authorStatement =
        new AuthorStatement(AuthorOperator.AuthorType.REVOKE_ROLE_FROM_USER);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    return authorStatement;
  }

  // Drop User

  @Override
  public Statement visitDropUser(IoTDBSqlParser.DropUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.DROP_USER);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    return authorStatement;
  }

  // Drop Role

  @Override
  public Statement visitDropRole(IoTDBSqlParser.DropRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.DROP_ROLE);
    authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    return authorStatement;
  }

  // List Users

  @Override
  public Statement visitListUser(IoTDBSqlParser.ListUserContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.LIST_USER);
    if (ctx.roleName != null) {
      authorStatement.setRoleName(parseIdentifier(ctx.roleName.getText()));
    }
    return authorStatement;
  }

  // List Roles

  @Override
  public Statement visitListRole(IoTDBSqlParser.ListRoleContext ctx) {
    AuthorStatement authorStatement = new AuthorStatement(AuthorOperator.AuthorType.LIST_ROLE);
    if (ctx.userName != null) {
      authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    }
    return authorStatement;
  }

  // List Privileges

  @Override
  public Statement visitListPrivilegesUser(IoTDBSqlParser.ListPrivilegesUserContext ctx) {
    AuthorStatement authorStatement =
        new AuthorStatement(AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    authorStatement.setUserName(parseIdentifier(ctx.userName.getText()));
    List<PartialPath> nodeNameList =
        ctx.prefixPath().stream().map(this::parsePrefixPath).collect(Collectors.toList());
    authorStatement.setNodeNameList(nodeNameList);
    return authorStatement;
  }

  // List Privileges of Roles On Specific Path

  @Override
  public Statement visitListPrivilegesRole(IoTDBSqlParser.ListPrivilegesRoleContext ctx) {
    AuthorStatement authorStatement =
        new AuthorStatement(AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
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

  // Create Storage Group

  @Override
  public Statement visitSetStorageGroup(IoTDBSqlParser.SetStorageGroupContext ctx) {
    SetStorageGroupStatement setStorageGroupStatement = new SetStorageGroupStatement();
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    setStorageGroupStatement.setStorageGroupPath(path);
    if (ctx.storageGroupAttributesClause() != null) {
      parseStorageGroupAttributesClause(
          setStorageGroupStatement, ctx.storageGroupAttributesClause());
    }
    return setStorageGroupStatement;
  }

  @Override
  public Statement visitCreateStorageGroup(IoTDBSqlParser.CreateStorageGroupContext ctx) {
    SetStorageGroupStatement setStorageGroupStatement = new SetStorageGroupStatement();
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    setStorageGroupStatement.setStorageGroupPath(path);
    if (ctx.storageGroupAttributesClause() != null) {
      parseStorageGroupAttributesClause(
          setStorageGroupStatement, ctx.storageGroupAttributesClause());
    }
    return setStorageGroupStatement;
  }

  private void parseStorageGroupAttributesClause(
      SetStorageGroupStatement setStorageGroupStatement,
      IoTDBSqlParser.StorageGroupAttributesClauseContext ctx) {
    if (ctx.storageGroupAttributeClause().size() != 0) {
      throw new RuntimeException(
          "Currently not support set ttl, schemaReplication factor, dataReplication factor, time partition interval to specific storage group.");
    }
    for (IoTDBSqlParser.StorageGroupAttributeClauseContext attribute :
        ctx.storageGroupAttributeClause()) {
      if (attribute.TTL() != null) {
        long ttl = Long.parseLong(attribute.INTEGER_LITERAL().getText());
        setStorageGroupStatement.setTtl(ttl);
      } else if (attribute.SCHEMA_REPLICATION_FACTOR() != null) {
        int schemaReplicationFactor = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        setStorageGroupStatement.setSchemaReplicationFactor(schemaReplicationFactor);
      } else if (attribute.DATA_REPLICATION_FACTOR() != null) {
        int dataReplicationFactor = Integer.parseInt(attribute.INTEGER_LITERAL().getText());
        setStorageGroupStatement.setDataReplicationFactor(dataReplicationFactor);
      } else if (attribute.TIME_PARTITION_INTERVAL() != null) {
        long timePartitionInterval = Long.parseLong(attribute.INTEGER_LITERAL().getText());
        setStorageGroupStatement.setTimePartitionInterval(timePartitionInterval);
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
  public Statement visitShowCluster(IoTDBSqlParser.ShowClusterContext ctx) {
    ShowClusterStatement showClusterStatement = new ShowClusterStatement();
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
      IoTDBSqlParser.ExpressionContext context, boolean inWithoutNull) {
    if (context.unaryInBracket != null) {
      return parseExpression(context.unaryInBracket, inWithoutNull);
    }

    if (context.expressionAfterUnaryOperator != null) {
      if (context.MINUS() != null) {
        return new NegationExpression(
            parseExpression(context.expressionAfterUnaryOperator, inWithoutNull));
      }
      if (context.OPERATOR_NOT() != null) {
        return new LogicNotExpression(
            parseExpression(context.expressionAfterUnaryOperator, inWithoutNull));
      }
      return parseExpression(context.expressionAfterUnaryOperator, inWithoutNull);
    }

    if (context.leftExpression != null && context.rightExpression != null) {
      Expression leftExpression = parseExpression(context.leftExpression, inWithoutNull);
      Expression rightExpression = parseExpression(context.rightExpression, inWithoutNull);
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
        return parseRegularExpression(context, inWithoutNull);
      }
      if (context.LIKE() != null) {
        return parseLikeExpression(context, inWithoutNull);
      }
      throw new UnsupportedOperationException();
    }

    if (context.unaryBeforeIsNullExpression != null) {
      return parseIsNullExpression(context, inWithoutNull);
    }

    if (context.firstExpression != null
        && context.secondExpression != null
        && context.thirdExpression != null) {
      Expression firstExpression = parseExpression(context.firstExpression, inWithoutNull);
      Expression secondExpression = parseExpression(context.secondExpression, inWithoutNull);
      Expression thirdExpression = parseExpression(context.thirdExpression, inWithoutNull);

      if (context.OPERATOR_BETWEEN() != null) {
        return new BetweenExpression(
            firstExpression, secondExpression, thirdExpression, context.OPERATOR_NOT() != null);
      }
      throw new UnsupportedOperationException();
    }

    if (context.unaryBeforeInExpression != null) {
      return parseInExpression(context, inWithoutNull);
    }

    if (context.functionName() != null) {
      return parseFunctionExpression(context, inWithoutNull);
    }

    if (context.fullPathInExpression() != null) {
      return new TimeSeriesOperand(
          parseFullPathInExpression(context.fullPathInExpression(), inWithoutNull));
    }

    if (context.time != null) {
      return new TimestampOperand();
    }

    if (context.constant() != null && !context.constant().isEmpty()) {
      return parseConstantOperand(context.constant(0));
    }

    throw new UnsupportedOperationException();
  }

  private Expression parseFunctionExpression(
      IoTDBSqlParser.ExpressionContext functionClause, boolean inWithoutNull) {
    FunctionExpression functionExpression =
        new FunctionExpression(parseIdentifier(functionClause.functionName().getText()));

    // expressions
    boolean hasNonPureConstantSubExpression = false;
    for (IoTDBSqlParser.ExpressionContext expression : functionClause.expression()) {
      Expression subexpression = parseExpression(expression, inWithoutNull);
      if (!subexpression.isConstantOperand()) {
        hasNonPureConstantSubExpression = true;
      }
      if (subexpression instanceof EqualToExpression
          && ((EqualToExpression) subexpression).getLeftExpression().isConstantOperand()
          && ((EqualToExpression) subexpression).getRightExpression().isConstantOperand()) {
        // parse attribute
        functionExpression.addAttribute(
            ((ConstantOperand) ((EqualToExpression) subexpression).getLeftExpression())
                .getValueString(),
            ((ConstantOperand) ((EqualToExpression) subexpression).getRightExpression())
                .getValueString());
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
    return functionExpression;
  }

  private Expression parseRegularExpression(ExpressionContext context, boolean inWithoutNull) {
    return new RegularExpression(
        parseExpression(context.unaryBeforeRegularOrLikeExpression, inWithoutNull),
        parseStringLiteralInLikeOrRegular(context.STRING_LITERAL().getText()));
  }

  private Expression parseLikeExpression(ExpressionContext context, boolean inWithoutNull) {
    return new LikeExpression(
        parseExpression(context.unaryBeforeRegularOrLikeExpression, inWithoutNull),
        parseStringLiteralInLikeOrRegular(context.STRING_LITERAL().getText()));
  }

  private Expression parseIsNullExpression(ExpressionContext context, boolean inWithoutNull) {
    return new IsNullExpression(
        parseExpression(context.unaryBeforeIsNullExpression, inWithoutNull),
        context.OPERATOR_NOT() != null);
  }

  private Expression parseInExpression(ExpressionContext context, boolean inWithoutNull) {
    Expression childExpression = parseExpression(context.unaryBeforeInExpression, inWithoutNull);
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
      throw new IllegalArgumentException("Unsupported constant operand: " + text);
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
      throw new SQLParserException("Unsupported constant operand: " + text);
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
        time += DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  private Long parseDateExpression(IoTDBSqlParser.DateExpressionContext ctx, long currentTime) {
    long time;
    time = parseDateFormat(ctx.getChild(0).getText(), currentTime);
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time += DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  private long parseTimeValue(IoTDBSqlParser.TimeValueContext ctx, long currentTime) {
    if (ctx.INTEGER_LITERAL() != null) {
      if (ctx.MINUS() != null) {
        return -Long.parseLong(ctx.INTEGER_LITERAL().getText());
      }
      return Long.parseLong(ctx.INTEGER_LITERAL().getText());
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

  private Pair<Long, Long> calcOperatorInterval(QueryFilter queryFilter) {

    if (queryFilter.getSinglePath() != null
        && !IoTDBConstant.TIME.equals(queryFilter.getSinglePath().getMeasurement())) {
      throw new SemanticException(DELETE_ONLY_SUPPORT_TIME_EXP_ERROR_MSG);
    }

    long time = Long.parseLong(((BasicFunctionFilter) queryFilter).getValue());
    switch (queryFilter.getFilterType()) {
      case LESSTHAN:
        return new Pair<>(Long.MIN_VALUE, time - 1);
      case LESSTHANOREQUALTO:
        return new Pair<>(Long.MIN_VALUE, time);
      case GREATERTHAN:
        return new Pair<>(time + 1, Long.MAX_VALUE);
      case GREATERTHANOREQUALTO:
        return new Pair<>(time, Long.MAX_VALUE);
      case EQUAL:
        return new Pair<>(time, time);
      default:
        throw new SemanticException(DELETE_RANGE_ERROR_MSG);
    }
  }

  // Merge
  @Override
  public Statement visitMerge(IoTDBSqlParser.MergeContext ctx) {
    MergeStatement mergeStatement = new MergeStatement(StatementType.MERGE);
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException("MERGE ON CLUSTER is not supported in standalone mode");
    }
    if (ctx.LOCAL() != null) {
      mergeStatement.setCluster(false);
    } else {
      mergeStatement.setCluster(true);
    }
    return mergeStatement;
  }

  @Override
  public Statement visitFullMerge(IoTDBSqlParser.FullMergeContext ctx) {
    MergeStatement mergeStatement = new MergeStatement(StatementType.FULL_MERGE);
    if (ctx.CLUSTER() != null && !IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      throw new SemanticException("FULL MERGE ON CLUSTER is not supported in standalone mode");
    }
    if (ctx.LOCAL() != null) {
      mergeStatement.setCluster(false);
    } else {
      mergeStatement.setCluster(true);
    }
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
    if (ctx.LOCAL() != null) {
      flushStatement.setCluster(false);
    } else {
      flushStatement.setCluster(true);
    }
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
    if (ctx.LOCAL() != null) {
      clearCacheStatement.setCluster(false);
    } else {
      clearCacheStatement.setCluster(true);
    }
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
    if (ctx.LOCAL() != null) {
      loadConfigurationStatement.setCluster(false);
    } else {
      loadConfigurationStatement.setCluster(true);
    }
    return loadConfigurationStatement;
  }

  // show region

  @Override
  public Statement visitShowRegion(IoTDBSqlParser.ShowRegionContext ctx) {
    ShowRegionStatement showRegionStatement = new ShowRegionStatement();
    // TODO: Maybe add a show partition region in the future
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
    String name = parseIdentifier(parseIdentifier(ctx.templateName.getText()));
    List<List<String>> measurementsList = new ArrayList<List<String>>();
    List<List<TSDataType>> dataTypesList = new ArrayList<List<TSDataType>>();
    List<List<TSEncoding>> encodingsList = new ArrayList<List<TSEncoding>>();
    List<List<CompressionType>> compressorsList = new ArrayList<List<CompressionType>>();

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
        name, measurementsList, dataTypesList, encodingsList, compressorsList);
  }

  void parseAttributeClause(
      IoTDBSqlParser.AttributeClausesContext ctx,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors) {
    if (ctx.aliasNodeName() != null) {
      throw new SQLParserException("schema template: alias is not supported yet.");
    }

    TSDataType dataType = null;
    if (ctx.dataType != null) {
      if (ctx.attributeKey() != null) {
        if (!parseAttributeKey(ctx.attributeKey())
            .equalsIgnoreCase(IoTDBConstant.COLUMN_TIMESERIES_DATATYPE)) {
          throw new SQLParserException("expecting datatype");
        }
      }
      String dataTypeString = ctx.dataType.getText().toUpperCase();
      try {
        dataType = TSDataType.valueOf(dataTypeString);
        dataTypes.add(dataType);
      } catch (Exception e) {
        throw new SemanticException(String.format("unsupported datatype: %s", dataTypeString));
      }
    }

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
      throw new SQLParserException("schema template: property is not supported yet.");
    }

    if (ctx.tagClause() != null) {
      throw new SQLParserException("schema template: tag is not supported yet.");
    }

    if (ctx.attributeClause() != null) {
      throw new SQLParserException("schema template: attribute is not supported yet.");
    }
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
    return new ShowPathsUsingTemplateStatement(parseIdentifier(ctx.templateName.getText()));
  }

  // schema template
}
