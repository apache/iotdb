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

package org.apache.iotdb.db.mpp.sql.parser;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.filter.*;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant;
import org.apache.iotdb.db.mpp.sql.statement.*;
import org.apache.iotdb.db.mpp.sql.statement.component.*;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParserBaseVisitor;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.executor.fill.ValueFill;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.binary.*;
import org.apache.iotdb.db.query.expression.unary.ConstantOperand;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang.StringEscapeUtils;

import java.time.ZoneId;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.index.common.IndexConstant.*;
import static org.apache.iotdb.db.qp.constant.SQLConstant.TIME_PATH;

/** Parse AST to Statement. */
public class ASTVisitor extends IoTDBSqlParserBaseVisitor<Statement> {

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

  // TODO: add comment
  private IoTDBConstant.ClientVersion clientVersion = IoTDBConstant.ClientVersion.V_0_13;

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public void setClientVersion(IoTDBConstant.ClientVersion clientVersion) {
    this.clientVersion = clientVersion;
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
    createAlignedTimeSeriesStatement.setDeviceId(parseFullPath(ctx.fullPath()));
    parseAlignedMeasurements(ctx.alignedMeasurements(), createAlignedTimeSeriesStatement);
    return createAlignedTimeSeriesStatement;
  }

  public void parseAlignedMeasurements(
      IoTDBSqlParser.AlignedMeasurementsContext ctx,
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement) {
    for (int i = 0; i < ctx.nodeNameWithoutWildcard().size(); i++) {
      createAlignedTimeSeriesStatement.addMeasurement(
          parseNodeName(ctx.nodeNameWithoutWildcard(i).getText()));
      parseAttributeClauses(ctx.attributeClauses(i), createAlignedTimeSeriesStatement);
    }
  }

  public void parseAttributeClauses(
      IoTDBSqlParser.AttributeClausesContext ctx,
      CreateTimeSeriesStatement createTimeSeriesStatement) {
    if (ctx.alias() != null) {
      createTimeSeriesStatement.setAlias(parseNodeName(ctx.alias().nodeNameCanInExpr().getText()));
    }
    final String dataType = ctx.dataType.getText().toUpperCase();
    final TSDataType tsDataType = TSDataType.valueOf(dataType);
    createTimeSeriesStatement.setDataType(tsDataType);

    final IoTDBDescriptor ioTDBDescriptor = IoTDBDescriptor.getInstance();
    TSEncoding encoding = ioTDBDescriptor.getDefaultEncodingByType(tsDataType);
    if (Objects.nonNull(ctx.encoding)) {
      String encodingString = ctx.encoding.getText().toUpperCase();
      encoding = TSEncoding.valueOf(encodingString);
    }
    createTimeSeriesStatement.setEncoding(encoding);

    CompressionType compressor;
    List<IoTDBSqlParser.PropertyClauseContext> properties = ctx.propertyClause();
    if (ctx.compressor != null) {
      compressor = CompressionType.valueOf(ctx.compressor.getText().toUpperCase());
    } else {
      compressor = TSFileDescriptor.getInstance().getConfig().getCompressor();
    }
    Map<String, String> props = null;
    if (ctx.propertyClause(0) != null) {
      props = new HashMap<>(properties.size());
      for (IoTDBSqlParser.PropertyClauseContext property : properties) {
        props.put(
            parseIdentifier(property.identifier().getText()).toLowerCase(),
            parseStringLiteral(property.propertyValue().getText().toLowerCase()));
      }
    }
    createTimeSeriesStatement.setCompressor(compressor);
    createTimeSeriesStatement.setProps(props);
    if (ctx.tagClause() != null) {
      parseTagClause(ctx.tagClause(), createTimeSeriesStatement);
    }
    if (ctx.attributeClause() != null) {
      parseAttributeClause(ctx.attributeClause(), createTimeSeriesStatement);
    }
  }

  public void parseAttributeClauses(
      IoTDBSqlParser.AttributeClausesContext ctx,
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement) {
    if (ctx.alias() != null) {
      createAlignedTimeSeriesStatement.addAliasList(
          parseNodeName(ctx.alias().nodeNameCanInExpr().getText()));
    } else {
      createAlignedTimeSeriesStatement.addAliasList(null);
    }

    String dataTypeString = ctx.dataType.getText().toUpperCase();
    TSDataType dataType = TSDataType.valueOf(dataTypeString);
    createAlignedTimeSeriesStatement.addDataType(dataType);

    TSEncoding encoding = IoTDBDescriptor.getInstance().getDefaultEncodingByType(dataType);
    if (Objects.nonNull(ctx.encoding)) {
      String encodingString = ctx.encoding.getText().toUpperCase();
      encoding = TSEncoding.valueOf(encodingString);
    }
    createAlignedTimeSeriesStatement.addEncoding(encoding);

    CompressionType compressor = TSFileDescriptor.getInstance().getConfig().getCompressor();
    if (ctx.compressor != null) {
      String compressorString = ctx.compressor.getText().toUpperCase();
      compressor = CompressionType.valueOf(compressorString);
    }
    createAlignedTimeSeriesStatement.addCompressor(compressor);

    if (ctx.propertyClause(0) != null) {
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
    Map<String, String> tags = extractMap(ctx.propertyClause(), ctx.propertyClause(0));
    if (statement instanceof CreateTimeSeriesStatement) {
      ((CreateTimeSeriesStatement) statement).setTags(tags);
    } else if (statement instanceof CreateAlignedTimeSeriesStatement) {
      ((CreateAlignedTimeSeriesStatement) statement).addTagsList(tags);
    }
    // TODO: remove comments
    //    else if (statement instanceof AlterTimeSeriesStatement) {
    //      ((AlterTimeSeriesStatement) statement).setTagsMap(tags);
    //    }
  }

  public void parseAttributeClause(IoTDBSqlParser.AttributeClauseContext ctx, Statement statement) {
    Map<String, String> attributes = extractMap(ctx.propertyClause(), ctx.propertyClause(0));
    if (statement instanceof CreateTimeSeriesStatement) {
      ((CreateTimeSeriesStatement) statement).setAttributes(attributes);
    } else if (statement instanceof CreateAlignedTimeSeriesStatement) {
      ((CreateAlignedTimeSeriesStatement) statement).addAttributesList(attributes);
    }
    // TODO: remove comments
    //    else if (operator instanceof AlterTimeSeriesOperator) {
    //      ((AlterTimeSeriesOperator) operator).setAttributesMap(attributes);
    //    }
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
    if (ctx.showWhereClause() != null) {
      parseShowWhereClause(ctx.showWhereClause(), showTimeSeriesStatement);
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), showTimeSeriesStatement);
    }
    return showTimeSeriesStatement;
  }

  private void parseShowWhereClause(
      IoTDBSqlParser.ShowWhereClauseContext ctx, ShowTimeSeriesStatement statement) {
    IoTDBSqlParser.PropertyValueContext propertyValueContext;
    if (ctx.containsExpression() != null) {
      statement.setContains(true);
      propertyValueContext = ctx.containsExpression().propertyValue();
      statement.setKey(parseIdentifier(ctx.containsExpression().identifier().getText()));
    } else {
      statement.setContains(false);
      propertyValueContext = ctx.propertyClause().propertyValue();
      statement.setKey(parseIdentifier(ctx.propertyClause().identifier().getText()));
    }
    statement.setValue(parseStringLiteral(propertyValueContext.getText()));
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

  /** Data Manipulation Language (DML) */

  // Select Statement ========================================================================

  private QueryStatement queryStatement;

  @Override
  public Statement visitSelectStatement(IoTDBSqlParser.SelectStatementContext ctx) {
    // visit special clause first to initialize different query statement
    if (ctx.specialClause() != null) {
      queryStatement = (QueryStatement) visit(ctx.specialClause());
    }

    // there is no special clause in query statement
    if (queryStatement == null) {
      queryStatement = new QueryStatement();
    }

    // parser select, from, where clauses
    parseSelectClause(ctx.selectClause());
    parseFromClause(ctx.fromClause());
    if (ctx.whereClause() != null) {
      WhereCondition whereCondition = parseWhereClause(ctx.whereClause());
      if (whereCondition != null) {
        queryStatement.setWhereCondition(whereCondition);
      }
    }
    return queryStatement;
  }

  // Select Clause

  public void parseSelectClause(IoTDBSqlParser.SelectClauseContext ctx) {
    SelectComponent selectComponent = new SelectComponent(zoneId);

    // parse TOP_N
    if (ctx.topClause() != null) {
      // TODO: parse info of top clause into selectOp
      visitTopClause(ctx.topClause());
    }

    // parse LAST
    if (ctx.LAST() != null) {
      queryStatement = new LastQueryStatement(queryStatement);
    }

    // parse resultColumn
    Set<String> aliasSet = new HashSet<>();
    for (IoTDBSqlParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      ResultColumn resultColumn = parseResultColumn(resultColumnContext);
      if (resultColumn.hasAlias()) {
        aliasSet.add(resultColumn.getAlias());
      }
      selectComponent.addResultColumn(resultColumn);
    }
    selectComponent.setAliasSet(aliasSet);

    // judge query type
    if (!(queryStatement instanceof GroupByQueryStatement)
        && !(queryStatement instanceof FillQueryStatement)
        && !(queryStatement instanceof LastQueryStatement)
        && !(queryStatement instanceof AggregationQueryStatement)
        && !(queryStatement instanceof UDTFQueryStatement)
        && !(queryStatement instanceof UDAFQueryStatement)) {
      if (selectComponent.isHasUserDefinedAggregationFunction()) {
        queryStatement = new UDAFQueryStatement(new AggregationQueryStatement(queryStatement));
      } else if (selectComponent.isHasBuiltInAggregationFunction()) {
        queryStatement = new AggregationQueryStatement(queryStatement);
      } else if (selectComponent.isHasTimeSeriesGeneratingFunction()) {
        queryStatement = new UDTFQueryStatement(queryStatement);
      }
    } else if (selectComponent.isHasUserDefinedAggregationFunction()) {
      queryStatement = new UDAFQueryStatement((AggregationQueryStatement) (queryStatement));
    }

    // set selectComponent
    queryStatement.setSelectComponent(selectComponent);
  }

  @Override
  public Statement visitTopClause(IoTDBSqlParser.TopClauseContext ctx) {
    int top = Integer.parseInt(ctx.INTEGER_LITERAL().getText());
    if (top <= 0 || top > 1000) {
      throw new SemanticException(
          String.format(
              "TOP <N>: N should be greater than 0 and less than 1000, current N is %d", top));
    }
    queryStatement.addProp(TOP_K, top);
    return queryStatement;
  }

  private ResultColumn parseResultColumn(IoTDBSqlParser.ResultColumnContext resultColumnContext) {
    Expression expression = parseExpression(resultColumnContext.expression());
    if (expression.isConstantOperand()) {
      throw new SemanticException("Constant operand is not allowed: " + expression);
    }
    return new ResultColumn(
        expression,
        resultColumnContext.AS() == null
            ? null
            : parseIdentifier(resultColumnContext.identifier().getText()));
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
    if (ctx.indexPredicateClause() != null) {
      parseIndexPredicate(ctx.indexPredicateClause());
      return null;
    }
    QueryFilter queryFilter = new QueryFilter();
    queryFilter.addChildOperator(parseOrExpression(ctx.orExpression()));
    return new WhereCondition(queryFilter.getChildren().get(0));
  }

  // Group By Time Clause

  @Override
  public Statement visitGroupByTimeStatement(IoTDBSqlParser.GroupByTimeStatementContext ctx) {
    queryStatement = new GroupByQueryStatement();

    // parse group by time clause
    parseGroupByTimeClause(ctx.groupByTimeClause());

    // parse order by time
    if (ctx.orderByTimeClause() != null) {
      parseOrderByTimeClause(ctx.orderByTimeClause());
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
      ((AggregationQueryStatement) queryStatement).setGroupByLevelComponent(groupByLevelComponent);
    }

    // set groupByTimeComponent
    ((GroupByQueryStatement) queryStatement).setGroupByTimeComponent(groupByTimeComponent);
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
    queryStatement = new AggregationQueryStatement();

    // parse GroupByLevel clause
    parseGroupByLevelClause(ctx.groupByLevelClause());

    // parse order by time
    if (ctx.orderByTimeClause() != null) {
      parseOrderByTimeClause(ctx.orderByTimeClause());
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
    ((AggregationQueryStatement) queryStatement).setGroupByLevelComponent(groupByLevelComponent);
  }

  // Fill Clause

  @Override
  public Statement visitFillStatement(IoTDBSqlParser.FillStatementContext ctx) {
    queryStatement = new FillQueryStatement();

    // parse fill clause
    parseFillClause(ctx.fillClause());

    // parse slimit & soffset
    if (ctx.slimitClause() != null) {
      parseSlimitClause(ctx.slimitClause());
    }

    // parse AlignByDevice or DisableAlign
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }

    return queryStatement;
  }

  public void parseFillClause(IoTDBSqlParser.FillClauseContext ctx) {
    FillComponent fillComponent = new FillComponent();
    if (ctx.oldTypeClause().size() > 0) {
      // old type fill logic
      List<IoTDBSqlParser.OldTypeClauseContext> list = ctx.oldTypeClause();
      Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
      for (IoTDBSqlParser.OldTypeClauseContext typeClause : list) {
        if (typeClause.ALL() != null) {
          if (typeClause.linearClause() != null) {
            throw new SemanticException("fill all doesn't support linear fill");
          }
          parseAllTypeClause(typeClause, fillTypes);
          break;
        } else {
          parsePrimitiveTypeClause(typeClause, fillTypes);
        }
      }
      fillComponent.setFillTypes(fillTypes);
    } else {
      // new single fill logic
      fillComponent.setSingleFill(getSingleIFill(ctx));
    }
    ((FillQueryStatement) queryStatement).setFillComponent(fillComponent);
  }

  // Group By Time with Fill Clause

  @Override
  public Statement visitGroupByFillStatement(IoTDBSqlParser.GroupByFillStatementContext ctx) {
    queryStatement = new GroupByFillQueryStatement();

    // parse GroupByTime with FIll
    parseGroupByFillClause(ctx.groupByFillClause());

    // parse OrderByTime
    if (ctx.orderByTimeClause() != null) {
      parseOrderByTimeClause(ctx.orderByTimeClause());
    }

    // parse limit & offset
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }

    return queryStatement;
  }

  private void parseGroupByFillClause(IoTDBSqlParser.GroupByFillClauseContext ctx) {
    GroupByTimeComponent groupByTimeComponent = new GroupByTimeComponent();
    FillComponent fillComponent = new FillComponent();

    // parse time range (start time & end time)
    parseTimeRange(ctx.timeRange(), groupByTimeComponent);
    groupByTimeComponent.setLeftCRightO(ctx.timeRange().LS_BRACKET() != null);

    // parse time interval
    groupByTimeComponent.setInterval(
        parseTimeIntervalOrSlidingStep(
            ctx.DURATION_LITERAL(0).getText(), true, groupByTimeComponent));

    // parse sliding step
    if (ctx.DURATION_LITERAL().size() == 2) {
      groupByTimeComponent.setSlidingStep(
          parseTimeIntervalOrSlidingStep(
              ctx.DURATION_LITERAL(1).getText(), false, groupByTimeComponent));
    } else {
      groupByTimeComponent.setSlidingStep(groupByTimeComponent.getInterval());
      groupByTimeComponent.setSlidingStepByMonth(groupByTimeComponent.isIntervalByMonth());
    }

    if (ctx.fillClause().oldTypeClause().size() > 0) {
      // old type fill logic
      List<IoTDBSqlParser.OldTypeClauseContext> list = ctx.fillClause().oldTypeClause();
      Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
      for (IoTDBSqlParser.OldTypeClauseContext typeClause : list) {
        if (typeClause.ALL() != null) {
          parseAllTypeClause(typeClause, fillTypes);
        } else {
          parsePrimitiveTypeClause(typeClause, fillTypes);
        }
      }

      int usePrevious = 0;
      int useLinear = 0;
      int useValue = 0;
      for (IFill iFill : fillTypes.values()) {
        if (iFill instanceof PreviousFill) {
          usePrevious = 1;
        }
        if (iFill instanceof LinearFill) {
          useLinear = 1;
        }
        if (iFill instanceof ValueFill) {
          useValue = 1;
        }
      }
      if (usePrevious + useLinear + useValue > 1) {
        throw new SemanticException("The old type logic could only use one type of fill");
      }

      fillComponent.setFillTypes(fillTypes);
    } else {
      fillComponent.setSingleFill(getSingleIFill(ctx.fillClause()));
    }

    ((GroupByFillQueryStatement) queryStatement).setGroupByTimeComponent(groupByTimeComponent);
    ((GroupByFillQueryStatement) queryStatement).setFillComponent(fillComponent);
  }

  private IFill getSingleIFill(IoTDBSqlParser.FillClauseContext ctx) {
    int defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    if (ctx.linearClause() != null) { // linear
      if (ctx.linearClause().DURATION_LITERAL(0) != null) {
        String beforeStr = ctx.linearClause().DURATION_LITERAL(0).getText();
        String afterStr = ctx.linearClause().DURATION_LITERAL(1).getText();
        return new LinearFill(beforeStr, afterStr);
      } else {
        return new LinearFill(defaultFillInterval, defaultFillInterval);
      }
    } else if (ctx.previousClause() != null) { // previous
      if (ctx.previousClause().DURATION_LITERAL() != null) {
        String preRangeStr = ctx.previousClause().DURATION_LITERAL().getText();
        return new PreviousFill(preRangeStr);
      } else {
        return new PreviousFill(defaultFillInterval);
      }
    } else if (ctx.specificValueClause() != null) { // value
      if (ctx.specificValueClause().constant() != null) {
        return new ValueFill(ctx.specificValueClause().constant().getText());
      } else {
        throw new SemanticException("fill value cannot be null");
      }
    } else if (ctx.previousUntilLastClause() != null) { // previous until last
      if (ctx.previousUntilLastClause().DURATION_LITERAL() != null) {
        String preRangeStr = ctx.previousUntilLastClause().DURATION_LITERAL().getText();
        return new PreviousFill(preRangeStr, true);
      } else {
        return new PreviousFill(defaultFillInterval, true);
      }
    } else {
      throw new SemanticException("unknown single fill type");
    }
  }

  private void parseAllTypeClause(
      IoTDBSqlParser.OldTypeClauseContext ctx, Map<TSDataType, IFill> fillTypes) {
    IFill fill;
    int defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();

    if (ctx.linearClause() != null) { // linear
      if (ctx.linearClause().DURATION_LITERAL(0) != null) {
        String beforeStr = ctx.linearClause().DURATION_LITERAL(0).getText();
        String afterStr = ctx.linearClause().DURATION_LITERAL(1).getText();
        fill = new LinearFill(beforeStr, afterStr);
      } else {
        fill = new LinearFill(defaultFillInterval, defaultFillInterval);
      }
    } else if (ctx.previousClause() != null) { // previous
      if (ctx.previousClause().DURATION_LITERAL() != null) {
        String preRangeStr = ctx.previousClause().DURATION_LITERAL().getText();
        fill = new PreviousFill(preRangeStr);
      } else {
        fill = new PreviousFill(defaultFillInterval);
      }
    } else if (ctx.specificValueClause() != null) {
      throw new SemanticException("fill all doesn't support value fill");
    } else { // previous until last
      if (ctx.previousUntilLastClause().DURATION_LITERAL() != null) {
        String preRangeStr = ctx.previousUntilLastClause().DURATION_LITERAL().getText();
        fill = new PreviousFill(preRangeStr, true);
      } else {
        fill = new PreviousFill(defaultFillInterval, true);
      }
    }

    for (TSDataType tsDataType : TSDataType.values()) {
      if (fill instanceof LinearFill
          && (tsDataType == TSDataType.BOOLEAN || tsDataType == TSDataType.TEXT)) {
        continue;
      }
      fillTypes.put(tsDataType, fill.copy());
    }
  }

  private void parsePrimitiveTypeClause(
      IoTDBSqlParser.OldTypeClauseContext ctx, Map<TSDataType, IFill> fillTypes) {
    TSDataType dataType = parseType(ctx.dataType.getText());
    if (dataType == TSDataType.VECTOR) {
      throw new SemanticException(String.format("type %s cannot use fill function", dataType));
    }

    if (ctx.linearClause() != null
        && (dataType == TSDataType.TEXT || dataType == TSDataType.BOOLEAN)) {
      throw new SemanticException(
          String.format(
              "type %s cannot use %s fill function",
              dataType, ctx.linearClause().LINEAR().getText()));
    }

    int defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();

    if (ctx.linearClause() != null) { // linear
      if (ctx.linearClause().DURATION_LITERAL(0) != null) {
        String beforeRangeStr = ctx.linearClause().DURATION_LITERAL(0).getText();
        String afterRangeStr = ctx.linearClause().DURATION_LITERAL(1).getText();
        LinearFill fill = new LinearFill(beforeRangeStr, afterRangeStr);
        fillTypes.put(dataType, fill);
      } else {
        fillTypes.put(dataType, new LinearFill(defaultFillInterval, defaultFillInterval));
      }
    } else if (ctx.previousClause() != null) { // previous
      if (ctx.previousClause().DURATION_LITERAL() != null) {
        String beforeStr = ctx.previousClause().DURATION_LITERAL().getText();
        fillTypes.put(dataType, new PreviousFill(beforeStr));
      } else {
        fillTypes.put(dataType, new PreviousFill(defaultFillInterval));
      }
    } else if (ctx.specificValueClause() != null) { // value
      if (ctx.specificValueClause().constant() != null) {
        fillTypes.put(
            dataType, new ValueFill(ctx.specificValueClause().constant().getText(), dataType));
      } else {
        throw new SemanticException("fill value cannot be null");
      }
    } else { // previous until last
      if (ctx.previousUntilLastClause().DURATION_LITERAL() != null) {
        String preRangeStr = ctx.previousUntilLastClause().DURATION_LITERAL().getText();
        fillTypes.put(dataType, new PreviousFill(preRangeStr, true));
      } else {
        fillTypes.put(dataType, new PreviousFill(defaultFillInterval, true));
      }
    }
  }

  // parse DataType
  private TSDataType parseType(String datatype) {
    String type = datatype.toLowerCase();
    switch (type) {
      case "int32":
        return TSDataType.INT32;
      case "int64":
        return TSDataType.INT64;
      case "float":
        return TSDataType.FLOAT;
      case "double":
        return TSDataType.DOUBLE;
      case "boolean":
        return TSDataType.BOOLEAN;
      case "text":
        return TSDataType.TEXT;
      default:
        throw new SemanticException("not a valid fill type : " + type);
    }
  }

  // Other Clauses
  @Override
  public Statement visitSpecialLimitStatement(IoTDBSqlParser.SpecialLimitStatementContext ctx) {
    return visit(ctx.specialLimit());
  }

  @Override
  public Statement visitLimitStatement(IoTDBSqlParser.LimitStatementContext ctx) {
    if (queryStatement == null) {
      queryStatement = new QueryStatement();
    }

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
    if (queryStatement == null) {
      queryStatement = new QueryStatement();
    }

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
    if (queryStatement == null) {
      queryStatement = new QueryStatement();
    }

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
    FilterNullComponent filterNullComponent = new FilterNullComponent();

    // add without null columns
    List<IoTDBSqlParser.ExpressionContext> expressionContexts = ctx.expression();
    for (IoTDBSqlParser.ExpressionContext expressionContext : expressionContexts) {
      filterNullComponent.addWithoutNullColumn(parseExpression(expressionContext));
    }

    // set without null policy
    if (ctx.ANY() != null) {
      filterNullComponent.setWithoutPolicyType(FilterNullPolicy.CONTAINS_NULL);
    } else if (ctx.ALL() != null) {
      filterNullComponent.setWithoutPolicyType(FilterNullPolicy.ALL_NULL);
    } else {
      filterNullComponent.setWithoutPolicyType(FilterNullPolicy.NO_FILTER);
    }

    queryStatement.setFilterNullComponent(filterNullComponent);
  }

  // ORDER BY TIME Clause

  @Override
  public Statement visitOrderByTimeStatement(IoTDBSqlParser.OrderByTimeStatementContext ctx) {
    queryStatement = new QueryStatement();

    // parse ORDER BY TIME
    parseOrderByTimeClause(ctx.orderByTimeClause());

    // parse others
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryStatement;
  }

  // parse ORDER BY TIME
  private void parseOrderByTimeClause(IoTDBSqlParser.OrderByTimeClauseContext ctx) {
    if (ctx.DESC() != null) {
      queryStatement.setResultOrder(OrderBy.TIMESTAMP_DESC);
    }
  }

  // ResultSetFormat Clause

  @Override
  public Statement visitAlignByDeviceClauseOrDisableAlignStatement(
      IoTDBSqlParser.AlignByDeviceClauseOrDisableAlignStatementContext ctx) {
    if (queryStatement == null) {
      queryStatement = new QueryStatement();
    }
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
      measurementList.add(parseNodeName(measurementName.getText()));
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
      if (insertMultiValues.get(i).timeValue() != null) {
        if (isTimeDefault) {
          throw new SemanticException(
              "the measurementList's size is not consistent with the valueList's size");
        }
        timestamp =
            parseTimeValue(insertMultiValues.get(i).timeValue(), DatetimeUtils.currentTime());
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
      List<String> valueList = new ArrayList<>();
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
      path[i] = parseNodeName(nodeNameWithoutStar.getText());
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

  private PartialPath parseSuffixPathCanInExpr(IoTDBSqlParser.SuffixPathCanInExprContext ctx) {
    List<IoTDBSqlParser.NodeNameCanInExprContext> nodeNames = ctx.nodeNameCanInExpr();
    String[] path = new String[nodeNames.size()];
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i] = parseNodeName(nodeNames.get(i).getText());
    }
    return new PartialPath(path);
  }

  private PartialPath convertConstantToPath(String src) throws IllegalPathException {
    return new PartialPath(src);
  }

  public String parseNodeName(IoTDBSqlParser.NodeNameContext ctx) {
    String src = ctx.getText();
    if (2 <= src.length() && src.charAt(0) == '`' && src.charAt(src.length() - 1) == '`') {
      return src.substring(1, src.length() - 1);
    }
    return src;
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
      if ((src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"')
          || (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'')) {
        String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
        return unescapeString.length() == 0 ? "" : unescapeString;
      }
    }
    return src;
  }

  private String parseStringLiteralInInsertValue(String src) {
    if (2 <= src.length()) {
      if ((src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"')
          || (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'')) {
        return StringEscapeUtils.unescapeJava(src);
      }
    }
    return src;
  }

  private String parseIdentifier(String src) {
    if (2 <= src.length() && src.charAt(0) == '`' && src.charAt(src.length() - 1) == '`') {
      return StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
    }
    return src;
  }

  private String parseNodeName(String src) {
    if (2 <= src.length() && src.charAt(0) == '`' && src.charAt(src.length() - 1) == '`') {
      return src.substring(1, src.length() - 1);
    }
    return src;
  }

  /** function for parsing file path used by LOAD statement. */
  public String parseFilePath(String src) {
    return src.substring(1, src.length() - 1);
  }

  // Expression & Predicate ========================================================================

  private Expression parseExpression(IoTDBSqlParser.ExpressionContext context) {
    // LR_BRACKET unaryInBracket=expression RR_BRACKET
    if (context.unaryInBracket != null) {
      return parseExpression(context.unaryInBracket);
    }

    // (PLUS | MINUS) unaryAfterSign=expression
    if (context.unaryAfterSign != null) {
      return context.MINUS() != null
          ? new NegationExpression(parseExpression(context.unaryAfterSign))
          : parseExpression(context.unaryAfterSign);
    }

    // leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
    // leftExpression=expression (PLUS | MINUS) rightExpression=expression
    if (context.leftExpression != null && context.rightExpression != null) {
      Expression leftExpression = parseExpression(context.leftExpression);
      Expression rightExpression = parseExpression(context.rightExpression);
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
    }

    // functionName=suffixPath LR_BRACKET expression (COMMA expression)* functionAttribute*
    // RR_BRACKET
    if (context.functionName() != null) {
      return parseFunctionExpression(context);
    }

    // suffixPath
    if (context.suffixPathCanInExpr() != null) {
      return new TimeSeriesOperand(parseSuffixPathCanInExpr(context.suffixPathCanInExpr()));
    }

    if (context.constant() != null) {
      try {
        IoTDBSqlParser.ConstantContext constantContext = context.constant();
        if (clientVersion.equals(IoTDBConstant.ClientVersion.V_0_13)) {
          if (constantContext.BOOLEAN_LITERAL() != null) {
            return new ConstantOperand(
                TSDataType.BOOLEAN, constantContext.BOOLEAN_LITERAL().getText());
          } else if (constantContext.STRING_LITERAL() != null) {
            String text = constantContext.STRING_LITERAL().getText();
            return new ConstantOperand(TSDataType.TEXT, parseStringLiteral(text));
          } else if (constantContext.INTEGER_LITERAL() != null) {
            return new ConstantOperand(
                TSDataType.INT64, constantContext.INTEGER_LITERAL().getText());
          } else if (constantContext.realLiteral() != null) {
            return new ConstantOperand(TSDataType.DOUBLE, constantContext.realLiteral().getText());
          } else {
            throw new SemanticException(
                "Unsupported constant operand: " + constantContext.getText());
          }
        } else if (clientVersion.equals(IoTDBConstant.ClientVersion.V_0_12)) {
          // if client version is before 0.13, node name in expression may be a constant
          return new TimeSeriesOperand(convertConstantToPath(context.constant().getText()));
        }
      } catch (QueryProcessException | IllegalPathException e) {
        throw new SQLParserException(e.getMessage());
      }
    }

    throw new UnsupportedOperationException();
  }

  private Expression parseFunctionExpression(IoTDBSqlParser.ExpressionContext functionClause) {
    FunctionExpression functionExpression =
        new FunctionExpression(parseIdentifier(functionClause.functionName().getText()));

    // expressions
    boolean hasNonPureConstantSubExpression = false;
    for (IoTDBSqlParser.ExpressionContext expression : functionClause.expression()) {
      Expression subexpression = parseExpression(expression);
      if (!subexpression.isConstantOperand()) {
        hasNonPureConstantSubExpression = true;
      }
      functionExpression.addExpression(subexpression);
    }

    // It is not allowed to have function expressions like F(1, 1.0). There should be at least one
    // non-pure-constant sub-expression, otherwise the timestamp of the row cannot be inferred.
    if (!hasNonPureConstantSubExpression) {
      throw new SemanticException(
          "Invalid function expression, all the arguments are constant operands: "
              + functionClause.getText());
    }

    // attributes
    for (IoTDBSqlParser.FunctionAttributeContext functionAttribute :
        functionClause.functionAttribute()) {
      functionExpression.addAttribute(
          parseStringLiteral(functionAttribute.functionAttributeKey.getText()),
          parseStringLiteral(functionAttribute.functionAttributeValue.getText()));
    }

    return functionExpression;
  }

  private QueryFilter parseOrExpression(IoTDBSqlParser.OrExpressionContext ctx) {
    if (ctx.andExpression().size() == 1) {
      return parseAndExpression(ctx.andExpression(0));
    }
    QueryFilter binaryOp = new QueryFilter(FilterConstant.FilterType.KW_OR);
    if (ctx.andExpression().size() > 2) {
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(0)));
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(1)));
      for (int i = 2; i < ctx.andExpression().size(); i++) {
        QueryFilter op = new QueryFilter(FilterConstant.FilterType.KW_OR);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parseAndExpression(ctx.andExpression(i)));
        binaryOp = op;
      }
    } else {
      for (IoTDBSqlParser.AndExpressionContext andExpressionContext : ctx.andExpression()) {
        binaryOp.addChildOperator(parseAndExpression(andExpressionContext));
      }
    }
    return binaryOp;
  }

  private QueryFilter parseAndExpression(IoTDBSqlParser.AndExpressionContext ctx) {
    if (ctx.predicate().size() == 1) {
      return parsePredicate(ctx.predicate(0));
    }
    QueryFilter binaryOp = new QueryFilter(FilterConstant.FilterType.KW_AND);
    int size = ctx.predicate().size();
    if (size > 2) {
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(0)));
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(1)));
      for (int i = 2; i < size; i++) {
        QueryFilter op = new QueryFilter(FilterConstant.FilterType.KW_AND);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parsePredicate(ctx.predicate(i)));
        binaryOp = op;
      }
    } else {
      for (IoTDBSqlParser.PredicateContext predicateContext : ctx.predicate()) {
        binaryOp.addChildOperator(parsePredicate(predicateContext));
      }
    }
    return binaryOp;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private QueryFilter parsePredicate(IoTDBSqlParser.PredicateContext ctx) {
    PartialPath path = null;
    if (ctx.OPERATOR_NOT() != null) {
      QueryFilter notOp = new QueryFilter(FilterConstant.FilterType.KW_NOT);
      notOp.addChildOperator(parseOrExpression(ctx.orExpression()));
      return notOp;
    } else if (ctx.LR_BRACKET() != null && ctx.OPERATOR_NOT() == null) {
      return parseOrExpression(ctx.orExpression());
    } else if (ctx.REGEXP() != null || ctx.LIKE() != null) {
      if (ctx.suffixPath() != null) {
        path = parseSuffixPath(ctx.suffixPath());
      } else if (ctx.fullPath() != null) {
        path = parseFullPath(ctx.fullPath());
      }
      if (path == null) {
        throw new SQLParserException("Path is null, please check the sql.");
      }
      return ctx.REGEXP() != null
          ? new RegexpFilter(FilterConstant.FilterType.REGEXP, path, ctx.STRING_LITERAL().getText())
          : new LikeFilter(FilterConstant.FilterType.LIKE, path, ctx.STRING_LITERAL().getText());
    } else {
      if (ctx.TIME() != null || ctx.TIMESTAMP() != null) {
        path = new PartialPath(SQLConstant.getSingleTimeArray());
      }
      if (ctx.fullPath() != null) {
        path = parseFullPath(ctx.fullPath());
      }
      if (ctx.suffixPath() != null) {
        path = parseSuffixPath(ctx.suffixPath());
      }
      if (path == null) {
        throw new SQLParserException("Path is null, please check the sql.");
      }
      if (ctx.inClause() != null) {
        return parseInOperator(ctx.inClause(), path);
      } else {
        return parseBasicFunctionOperator(ctx, path);
      }
    }
  }

  private QueryFilter parseBasicFunctionOperator(
      IoTDBSqlParser.PredicateContext ctx, PartialPath path) {
    BasicFunctionFilter basic;
    if (ctx.constant().dateExpression() != null) {
      if (!path.equals(TIME_PATH)) {
        throw new SQLParserException(path.getFullPath(), "Date can only be used to time");
      }
      basic =
          new BasicFunctionFilter(
              FilterConstant.lexerToFilterType.get(ctx.comparisonOperator().type.getType()),
              path,
              Long.toString(parseDateExpression(ctx.constant().dateExpression())));
    } else {
      basic =
          new BasicFunctionFilter(
              FilterConstant.lexerToFilterType.get(ctx.comparisonOperator().type.getType()),
              path,
              parseStringLiteral(ctx.constant().getText()));
    }
    return basic;
  }

  private QueryFilter parseInOperator(IoTDBSqlParser.InClauseContext ctx, PartialPath path) {
    Set<String> values = new HashSet<>();
    boolean not = ctx.OPERATOR_NOT() != null;
    for (IoTDBSqlParser.ConstantContext constant : ctx.constant()) {
      if (constant.dateExpression() != null) {
        if (!path.equals(TIME_PATH)) {
          throw new SQLParserException(path.getFullPath(), "Date can only be used to time");
        }
        values.add(Long.toString(parseDateExpression(constant.dateExpression())));
      } else {
        values.add(constant.getText());
      }
    }
    return new InFilter(FilterConstant.FilterType.IN, path, not, values);
  }

  private void parseIndexPredicate(IoTDBSqlParser.IndexPredicateClauseContext ctx) {
    Map<String, Object> props;
    PartialPath path;
    if (ctx.suffixPath() != null) {
      path = parseSuffixPath(ctx.suffixPath());
    } else {
      path = parseFullPath(ctx.fullPath());
    }
    if (ctx.LIKE() != null) {
      // whole matching case
      if (queryStatement.getSelectComponent().getResultColumns().size() != 1) {
        throw new SemanticException("Index query statement allows only one select path");
      }
      if (!path.equals(
          queryStatement
              .getSelectComponent()
              .getResultColumns()
              .get(0)
              .getExpression()
              .toString())) {
        throw new SemanticException(
            "In the index query statement, "
                + "the path in select element and the index predicate should be same");
      }
      queryStatement.addProp(PATTERN, parseSequence(ctx.sequenceClause(0)));
      queryStatement.setIndexType(IndexType.RTREE_PAA);
    } else if (ctx.CONTAIN() != null) {
      // subsequence matching case
      List<double[]> compositePattern = new ArrayList<>();
      List<Double> thresholds = new ArrayList<>();
      for (int i = 0; i < ctx.sequenceClause().size(); i++) {
        compositePattern.add(parseSequence(ctx.sequenceClause(i)));
        thresholds.add(Double.parseDouble(ctx.constant(i).getText()));
      }

      List<ResultColumn> resultColumns = new ArrayList<>();
      resultColumns.add(new ResultColumn(new TimeSeriesOperand(path)));
      queryStatement.getSelectComponent().setResultColumns(resultColumns);
      queryStatement.addProp(PATTERN, compositePattern);
      queryStatement.addProp(THRESHOLD, thresholds);
      queryStatement.setIndexType(IndexType.ELB_INDEX);
    } else {
      throw new SQLParserException("Unknown index predicate: " + ctx);
    }
  }

  private double[] parseSequence(IoTDBSqlParser.SequenceClauseContext ctx) {
    int seqLen = ctx.constant().size();
    double[] sequence = new double[seqLen];
    for (int i = 0; i < seqLen; i++) {
      sequence[i] = Double.parseDouble(ctx.constant(i).getText());
    }
    return sequence;
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
      return Long.parseLong(ctx.INTEGER_LITERAL().getText());
    } else if (ctx.dateExpression() != null) {
      return parseDateExpression(ctx.dateExpression(), currentTime);
    } else {
      return parseDateFormat(ctx.datetimeLiteral().getText(), currentTime);
    }
  }

  /** Utils */
  private void setMap(IoTDBSqlParser.AlterClauseContext ctx, Map<String, String> alterMap) {
    List<IoTDBSqlParser.PropertyClauseContext> tagsList = ctx.propertyClause();
    if (ctx.propertyClause(0) != null) {
      for (IoTDBSqlParser.PropertyClauseContext property : tagsList) {
        String value;
        value = parseStringLiteral(property.propertyValue().getText());
        alterMap.put(parseIdentifier(property.identifier().getText()), value);
      }
    }
  }

  private Map<String, String> extractMap(
      List<IoTDBSqlParser.PropertyClauseContext> property2,
      IoTDBSqlParser.PropertyClauseContext property3) {
    Map<String, String> tags = new HashMap<>(property2.size());
    if (property3 != null) {
      for (IoTDBSqlParser.PropertyClauseContext property : property2) {
        tags.put(
            parseIdentifier(property.identifier().getText()),
            parseStringLiteral(property.propertyValue().getText()));
      }
    }
    return tags;
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
}
