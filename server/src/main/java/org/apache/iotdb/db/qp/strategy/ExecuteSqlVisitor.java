/**
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

package org.apache.iotdb.db.qp.strategy;

import main.antlr4.org.apache.iotdb.db.sql.parse.TSParser;
import main.antlr4.org.apache.iotdb.db.sql.parse.TSParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.constant.TSParserConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.*;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.LinearFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

import java.time.ZoneId;
import java.util.*;

import static org.apache.iotdb.db.qp.constant.SQLConstant.*;

public class ExecuteSqlVisitor extends TSParserBaseVisitor {
  private static final String ERR_INCORRECT_AUTHOR_COMMAND = "illegal ast tree in grant author "
          + "command, please check you SQL statement";

  private RootOperator initializedOperator = null;
  private SelectOperator selectOp;
  private FilterOperator whereOp;
  private ZoneId zoneId;
  private Map<TSDataType, IFill> fillTypes;
  private TSDataType fillClauseDataType;
  private Path whereSeriesPath;

  private boolean isAndWhereClause = false;
  private boolean isOrWhereClause = false;
  private boolean isNotWhereClause = false;

  public ExecuteSqlVisitor(ZoneId zoneId) {
    this.zoneId = zoneId;
  }


  @Override
  public Object visitStatement(TSParser.StatementContext ctx) {
    return visit(ctx.execStatement());
  }

  @Override
  public RootOperator visitQueryStatement(TSParser.QueryStatementContext ctx) {
    initializedOperator = new QueryOperator(SQLConstant.TOK_QUERY);
    visit(ctx.selectClause());
    if (ctx.whereClause() != null) {
      visit(ctx.whereClause());
    }
    if (ctx.specialClause() != null) {
      visit(ctx.specialClause());
    }
    return initializedOperator;
  }

  @Override
  public Object visitX_positiveFloat(TSParser.X_positiveFloatContext ctx) {
    return ctx.PositiveFloat().getText();
  }

  @Override
  public Object visitX_negativeInteger(TSParser.X_negativeIntegerContext ctx) {
    return ctx.NegativeFloat().getText();
  }

  @Override
  public Object visitX_positiveIntegerDot(TSParser.X_positiveIntegerDotContext ctx) {
    return ctx.PositiveInteger().getText();
  }

  @Override
  public Object visitX_negativeIntegerDot(TSParser.X_negativeIntegerDotContext ctx) {
    return ctx.NegativeInteger().getText();
  }

  @Override
  public Object visitX_unsignedIntegerDotUnsignedInteger(TSParser.X_unsignedIntegerDotUnsignedIntegerContext ctx) {
    return ctx.getText();
  }

  @Override
  public Object visitX_unsignedIntegerDot(TSParser.X_unsignedIntegerDotContext ctx) {
    return ctx.UnsignedInteger().getText();
  }

  @Override
  public Object visitX_dotUnsignedInteger(TSParser.X_dotUnsignedIntegerContext ctx) {
    return ctx.getText();
  }

  @Override
  public Object visitX_unsignedIntegerDoubleInScientificNotationSuffix(TSParser.X_unsignedIntegerDoubleInScientificNotationSuffixContext ctx) {
    return ctx.getText();
  }

  @Override
  public Object visitX_doubleInScientificNotationSuffix(TSParser.X_doubleInScientificNotationSuffixContext ctx) {
    return ctx.getText();
  }


  @Override
  public Object visitIntegerString(TSParser.IntegerStringContext ctx) {
    return ctx.integer().getText();
  }

  @Override
  public Object visitFloatString(TSParser.FloatStringContext ctx) {
    return visit(ctx.floatValue());
  }

  @Override
  public Object visitBooleanString(TSParser.BooleanStringContext ctx) {
    return ctx.getText();
  }

  @Override
  public RootOperator visitInsertStatement(TSParser.InsertStatementContext ctx) {
    InsertOperator insertOp = new InsertOperator(SQLConstant.TOK_INSERT);
    initializedOperator = insertOp;

    // set select path
    Path selectPath = parsePrefixPath(ctx.prefixPath());
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    selectOp.addSelectPath(selectPath);
    ((SFWOperator) initializedOperator).setSelectOperator(selectOp);

    // set time
    long timestamp = (long) visit(ctx.multiValue().time);
    insertOp.setTime(timestamp);

    // set measurement list
    String[] measurementList = new String[ctx.multidentifier().identifier().size()];
    for (int i = 0; i < measurementList.length; i++) {
      measurementList[i] = ctx.multidentifier().identifier(i).getText();
    }
    insertOp.setMeasurementList(measurementList);

    // set value list
    String[] valueList = new String[ctx.multiValue().numberOrStringWidely().size()];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = ctx.multiValue().numberOrStringWidely(i).getText();
    }
    insertOp.setValueList(valueList);

    return initializedOperator;
  }

  public long parseTimeFormat(String timestampStr) {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new SqlParseException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    try {
      return DatetimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SqlParseException(String
              .format("Input time format %s error. "
                      + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                      + "refer to user document for more info.", timestampStr));
    }
  }


  @Override
  public Object visitSelectIndex(TSParser.SelectIndexContext ctx) {
    throw new SqlParseException("Not supported TSParser type SELECT_INDEX");
  }

  @Override
  public Object visitSelectSimple(TSParser.SelectSimpleContext ctx) {
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    for (TSParser.ClusteredPathContext cp : ctx.clusteredPath()) {
      visit(cp);
    }
    visit(ctx.fromClause());
    ((SFWOperator) initializedOperator).setSelectOperator(selectOp);
    return initializedOperator;
  }

  @Override
  public Object visitAggregateCommandPath(TSParser.AggregateCommandPathContext ctx) {
    Path path = parseSuffixPath(ctx.suffixPath());
    String aggregation = ctx.identifier().getText();
    selectOp.addClusterPath(path, aggregation);
    return selectOp;
  }

  @Override
  public Object visitSimpleSuffixPath(TSParser.SimpleSuffixPathContext ctx) {
    Path path = parseSuffixPath(ctx.suffixPath());
    selectOp.addSelectPath(path);
    return selectOp;
  }

  @Override
  public Object visitFromClause(TSParser.FromClauseContext ctx) {
    FromOperator from = new FromOperator(SQLConstant.TOK_FROM);
    for (TSParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      Path path = parsePrefixPath(prefixPathContext);
      from.addPrefixTablePath(path);
    }
    ((SFWOperator) initializedOperator).setFromOperator(from);
    return initializedOperator;
  }

  @Override
  public Object visitWhereClause(TSParser.WhereClauseContext ctx) {
    whereOp = new FilterOperator(SQLConstant.TOK_WHERE);
    visit(ctx.searchCondition());
    ((SFWOperator) initializedOperator).setFilterOperator(whereOp.getChildren().get(0));
    return initializedOperator;
  }

  @Override
  public Object visitSearchCondition(TSParser.SearchConditionContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public Object visitExpression(TSParser.ExpressionContext ctx) {
    return visit(ctx.precedenceOrExpression());
  }

  @Override
  public Object visitPrecedenceOrExpression(TSParser.PrecedenceOrExpressionContext ctx) {
    if (ctx.precedenceAndExpression().size() == 1) {
      isOrWhereClause = false;
      return visit(ctx.precedenceAndExpression(0));
    }
    isOrWhereClause = true;
    FilterOperator binaryOp = new FilterOperator(
            TSParserConstant.getTSTokenIntType(TSParser.KW_OR));
    int size = ctx.precedenceAndExpression().size();
    if (size > 2) {
      binaryOp.addChildOperator((FilterOperator) visit(ctx.precedenceAndExpression(0)));
      binaryOp.addChildOperator((FilterOperator) visit(ctx.precedenceAndExpression(1)));
      for (int i = 2; i < size; i++) {
        FilterOperator op = new FilterOperator(TSParserConstant.getTSTokenIntType(TSParser.KW_OR));
        op.addChildOperator(binaryOp);
        op.addChildOperator((FilterOperator) visit(ctx.precedenceAndExpression(i)));
        binaryOp = op;
      }
    } else {
      for (TSParser.PrecedenceAndExpressionContext precedenceAndExpressionContext : ctx.precedenceAndExpression()) {
        binaryOp.addChildOperator((FilterOperator) visit(precedenceAndExpressionContext));
      }
    }
    whereOp.addChildOperator(binaryOp);
    return binaryOp;
  }


  @Override
  public Object visitPrecedenceAndExpression(TSParser.PrecedenceAndExpressionContext ctx) {
    if (ctx.precedenceNotExpression().size() == 1) {
      isAndWhereClause = false;
      return visit(ctx.precedenceNotExpression(0));
    }
    isAndWhereClause = true;
    FilterOperator binaryOp = new FilterOperator(
            TSParserConstant.getTSTokenIntType(TSParser.KW_AND));
    if (!isOrWhereClause) {
      whereOp.addChildOperator(binaryOp);
    }
    int size = ctx.precedenceNotExpression().size();
    if (size > 2) {
      binaryOp.addChildOperator((FilterOperator) visit(ctx.precedenceNotExpression(0)));
      binaryOp.addChildOperator((FilterOperator) visit(ctx.precedenceNotExpression(1)));
      for (int i = 2; i < size; i++) {
        FilterOperator op = new FilterOperator(
                TSParserConstant.getTSTokenIntType(TSParser.KW_AND));
        op.addChildOperator(binaryOp);
        op.addChildOperator((FilterOperator) visit(ctx.precedenceNotExpression(i)));
        binaryOp = op;
      }
      whereOp.getChildren().set(whereOp.getChildren().size()-1, binaryOp);
    } else {
      for (TSParser.PrecedenceNotExpressionContext precedenceNotExpressionContext : ctx.precedenceNotExpression()) {
        binaryOp.addChildOperator((FilterOperator) visit(precedenceNotExpressionContext));
      }
    }
    return binaryOp;
  }

  @Override
  public Object visitWithNot(TSParser.WithNotContext ctx) {
    isNotWhereClause = true;
    FilterOperator notOp = new FilterOperator(SQLConstant.KW_NOT);
    if (!isAndWhereClause) {
      whereOp.addChildOperator(notOp);
    }
    notOp.addChildOperator((FilterOperator) visit(ctx.precedenceNotExpression()));
    return notOp;
  }

  @Override
  public Object visitWithoutNot(TSParser.WithoutNotContext ctx) {
    return visit(ctx.precedenceEqualExpressionSingle());
  }

  @Override
  public Object visitSlimitClause(TSParser.SlimitClauseContext ctx) {
    int seriesLimit;
    try {
      seriesLimit = Integer.parseInt(ctx.nonNegativeInteger().getText().trim());
    } catch (NumberFormatException e) {
      throw new SqlParseException("SLIMIT <SN>: SN should be Int32.");
    }
    if (seriesLimit <= 0) {
      // seriesLimit is ensured to be a non negative integer after the lexical examination,
      // and seriesLimit is further required to be a positive integer here.
      throw new SqlParseException(
              "SLIMIT <SN>: SN must be a positive integer and can not be zero.");
    }
    if (ctx.soffsetClause() != null) {
      visit(ctx.soffsetClause());
    }
    ((QueryOperator) initializedOperator).setSeriesLimit(seriesLimit);
    return initializedOperator;
  }

  @Override
  public Object visitSoffsetClause(TSParser.SoffsetClauseContext ctx) {
    try {
      ((QueryOperator) initializedOperator)
              .setSeriesOffset(Integer.parseInt(ctx.nonNegativeInteger().getText().trim()));
    } catch (NumberFormatException e) {
      throw new SqlParseException("SOFFSET <SOFFSETValue>: SOFFSETValue should be Int32.");
    }
    return initializedOperator;
  }


  @Override
  public Object visitLimitClause(TSParser.LimitClauseContext ctx) {
    int rowsLimit;
    try {
      rowsLimit = Integer.parseInt(ctx.nonNegativeInteger().getText().trim());
    } catch (NumberFormatException e) {
      throw new SqlParseException("LIMIT <N>: N should be Int32.");
    }
    if (rowsLimit <= 0) {
//      // seriesLimit is ensured to be a non negative integer after the lexical examination,
//      // and seriesLimit is further required to be a positive integer here.
      throw new SqlParseException(
              "LIMIT <N>: N must be a positive integer and can not be zero.");
    }
    if (ctx.offsetClause() != null) {
      visit(ctx.offsetClause());
    }
    return initializedOperator;
  }

  @Override
  public Object visitOffsetClause(TSParser.OffsetClauseContext ctx) {
//    throw new SqlParseException("Not supported TSParser type KW_OFFSET");
    return null;
  }

  @Override
  public Object visitGroupbyClause(TSParser.GroupbyClauseContext ctx) {
    selectOp = ((QueryOperator) initializedOperator).getSelectOperator();
    if (selectOp.getSuffixPaths().size() != selectOp.getAggregations().size()) {
      throw new SqlParseException(
              "Group by must bind each seriesPath with an aggregation function");
    }
    ((QueryOperator) initializedOperator).setGroupBy(true);
    long value = parseTimeUnit(ctx.value.getText(), ctx.unit.getText());
    ((QueryOperator) initializedOperator).setUnit(value);

    List<Pair<Long, Long>> intervals = new ArrayList<>();
    for (TSParser.TimeIntervalContext interval : ctx.timeInterval()) {
      intervals.add((Pair<Long, Long>) visit(interval));
    }
    ((QueryOperator) initializedOperator).setIntervals(intervals);

    long originTime = 0;
    if (ctx.timeOrigin == null) {
      originTime = parseTimeFormat(SQLConstant.START_TIME_STR);
    } else {
      originTime = (long) visit(ctx.timeOrigin);
    }
    ((QueryOperator) initializedOperator).setOrigin(originTime);
    return initializedOperator;
  }

  @Override
  public Object visitTimeInterval(TSParser.TimeIntervalContext ctx) {
    long startTime = (long) visit(ctx.dateFormatWithNumber(0));
    long endTime = (long) visit(ctx.dateFormatWithNumber(1));
    return new Pair<>(startTime, endTime);
  }

  @Override
  public Long visitGetDateTimeFromDateFormat(TSParser.GetDateTimeFromDateFormatContext ctx) {
    return parseTimeFormat((String) visit(ctx.dateFormat()));
  }


  @Override
  public String visitGetDateTime(TSParser.GetDateTimeContext ctx) {
    return ctx.DATETIME().getText();
  }

  @Override
  public String visitGetIdentifier(TSParser.GetIdentifierContext ctx) {
    return ctx.Identifier().getText();
  }

  @Override
  public Object visitFillClause(TSParser.FillClauseContext ctx) {
    FilterOperator filterOperator = ((SFWOperator) initializedOperator).getFilterOperator();
    fillTypes = new EnumMap<>(TSDataType.class);
    for (TSParser.TypeClauseContext typeClauseContext : ctx.typeClause()) {
      try {
        fillClauseDataType = parseType(typeClauseContext.Identifier().getText());
        visit(typeClauseContext.interTypeClause());
      } catch (LogicalOperatorException e) {
        e.printStackTrace();
      }
    }
    ((QueryOperator) initializedOperator).setFillTypes(fillTypes);
    ((QueryOperator) initializedOperator).setFill(true);
    return initializedOperator;
  }

//  "select sensor1 from root.vehicle.device1 where time = 50 Fill(int32[linear, 5m, 5m], boolean[previous, 5m])"

  @Override
  public Object visitLinear(TSParser.LinearContext ctx) {
    if (fillClauseDataType == TSDataType.BOOLEAN || fillClauseDataType == TSDataType.TEXT) {
      throw new SqlParseException(String.format("type %s cannot use %s fill function", fillClauseDataType,
              "KW_LINEAR"));
    }
    if (!ctx.integer().isEmpty()) {
      long beforeRange = 0;
      long afterRange = 0;
      beforeRange = parseTimeUnit(ctx.value1.getText(), ctx.unit1.getText());
      afterRange = parseTimeUnit(ctx.value2.getText(), ctx.unit2.getText());
      fillTypes.put(fillClauseDataType, new LinearFill(beforeRange, afterRange));
    } else {
      fillTypes.put(fillClauseDataType, new LinearFill(-1, -1));
    }
    return initializedOperator;
  }

  @Override
  public Object visitPrevious(TSParser.PreviousContext ctx) {
    if (ctx.value1 != null) {
      long preRange = parseTimeUnit(ctx.value1.getText(), ctx.unit1.getText());
      fillTypes.put(fillClauseDataType, new PreviousFill(preRange));
    } else {
      fillTypes.put(fillClauseDataType, new PreviousFill(-1));
    }
    return initializedOperator;
  }

  private long parseTimeUnit(String time, String unit) {
    long timeInterval = Long.parseLong(time);
    if (timeInterval <= 0) {
      throw new SqlParseException("Interval must more than 0.");
    }
    switch (unit) {
      case "w":
        timeInterval *= 7;
      case "d":
        timeInterval *= 24;
      case "h":
        timeInterval *= 60;
      case "m":
        timeInterval *= 60;
      case "s":
        timeInterval *= 1000;
      default:
        break;
    }
    return timeInterval;
  }

  @Override
  public Long visitGetDateTimeFromInteger(TSParser.GetDateTimeFromIntegerContext ctx) {
    return Long.parseLong(ctx.integer().getText().trim());
  }

  private TSDataType parseType(String type) throws LogicalOperatorException {
    type = type.toLowerCase();
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
        throw new LogicalOperatorException("not a valid fill type : " + type);
    }
  }

  @Override
  public Object visitUpdateAccount(TSParser.UpdateAccountContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_UPDATE_USER,
            AuthorOperator.AuthorType.UPDATE_USER);
    authorOperator.setUserName(ctx.userName.getText());
    authorOperator.setNewPassword(ctx.psw.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitUpdateRecord(TSParser.UpdateRecordContext ctx) {
    if (ctx.prefixPath().size() > 1 || ctx.setClause().setExpression().size() > 1) {
      throw new SqlParseException("UPDATE clause doesn't support multi-update yet.");
    }
    UpdateOperator updateOp = new UpdateOperator(SQLConstant.TOK_UPDATE);
    initializedOperator = updateOp;
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    fromOp.addPrefixTablePath(parsePrefixPath(ctx.prefixPath(0)));
    updateOp.setFromOperator(fromOp);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    selectOp.addSelectPath(parseSuffixPath(ctx.setClause().setExpression(0).suffixPath()));
    updateOp.setSelectOperator(selectOp);
    updateOp.setValue(ctx.setClause().setExpression(0).numberOrStringWidely().getText());
    if (ctx.whereClause() != null) {
      visit(ctx.whereClause());
    }
    return initializedOperator;
  }

  private Path parsePrefixPath(TSParser.PrefixPathContext prefixPathContext) {
    int nodeNameNumber = 0;
    if (prefixPathContext.nodeName() != null) {
      nodeNameNumber = prefixPathContext.nodeName().size();
    }
    String[] paths = new String[nodeNameNumber + 1];
    paths[0] = SQLConstant.ROOT;
    if (nodeNameNumber > 0) {
      int i = 1;
      for (TSParser.NodeNameContext nodeName : prefixPathContext.nodeName()) {
        paths[i++] = nodeName.getText();
      }
    }
    return new Path(new StringContainer(paths, TsFileConstant.PATH_SEPARATOR));
  }

  private Path parseSuffixPath(TSParser.SuffixPathContext suffixPathContext) {
    int nodeNameNumber = suffixPathContext.nodeName().size();
    String[] paths = new String[nodeNameNumber];
    int i = 0;
    for (TSParser.NodeNameContext nodeNameContext : suffixPathContext.nodeName()) {
      paths[i++] = nodeNameContext.getText();
    }
    return new Path(new StringContainer(paths, TsFileConstant.PATH_SEPARATOR));
  }

  @Override
  public Object visitDeleteStatement(TSParser.DeleteStatementContext ctx) {
    initializedOperator = new DeleteOperator(SQLConstant.TOK_DELETE);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    for (TSParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      Path tablePath = parsePrefixPath(prefixPathContext);
      selectOp.addSelectPath(tablePath);
    }
    ((SFWOperator) initializedOperator).setSelectOperator(selectOp);
    if (ctx.whereClause() != null) {

      visit(ctx.whereClause());
    }
    long deleteTime = parseDeleteTimeFilter((DeleteOperator) initializedOperator);
    ((DeleteOperator) initializedOperator).setTime(deleteTime);
    return initializedOperator;
  }

  /**
   * for delete command, time should only have an end time.
   *
   * @param operator delete logical plan
   */
  private long parseDeleteTimeFilter(DeleteOperator operator) {
    FilterOperator filterOperator = operator.getFilterOperator();

    if (!(filterOperator.isLeaf())) {
      throw new SqlParseException(
              "For delete command, where clause must be like : time < XXX or time <= XXX");
    }
    if (filterOperator.getTokenIntType() != LESSTHAN
            && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
      throw new SqlParseException(
              "For delete command, where clause must be like : time < XXX or time <= XXX");
    }

    long time = Long.parseLong(((BasicFunctionOperator) filterOperator).getValue());
    if (filterOperator.getTokenIntType() == LESSTHAN) {
      time = time - 1;
    }
    // time must greater than 0 now
    if (time <= 0) {
      throw new SqlParseException("delete Time:" + time + ", time must > 0");
    }
    return time;
  }

  @Override
  public Object visitDeleteTimeseries(TSParser.DeleteTimeseriesContext ctx) {
    List<Path> deletePaths = new ArrayList<>();
    for (TSParser.PrefixPathContext prefixPathContext : ctx.prefixPath()) {
      deletePaths.add(parsePrefixPath(prefixPathContext));
    }
    MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_DELETE,
            MetadataOperator.NamespaceType.DELETE_PATH);
    metadataOperator.setDeletePathList(deletePaths);
    initializedOperator = metadataOperator;
    return initializedOperator;
  }

  @Override
  public Object visitShowMetadata(TSParser.ShowMetadataContext ctx) {
    throw new SqlParseException("Not supported TSParser SHOW METADATA");
  }

  @Override
  public Object visitDescribePath(TSParser.DescribePathContext ctx) {
    throw new SqlParseException("Not supported TSParser DESCRIBE PATH");
  }

  @Override
  public Object visitQuitStatement(TSParser.QuitStatementContext ctx) {
    throw new SqlParseException("Not supported TSParser QUIT");
  }

  @Override
  public Object visitDeleteALabelFromPropertyTree(TSParser.DeleteALabelFromPropertyTreeContext ctx) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_DELETE_LABEL,
            PropertyOperator.PropertyType.DELETE_PROPERTY_LABEL);
    String label = ctx.label.getText();
    String property = ctx.property.getText();
    Path propertyLabel = new Path(new String[]{property, label});
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
    return initializedOperator;
  }

  @Override
  public Object visitCreateTimeseries(TSParser.CreateTimeseriesContext ctx) {
    Path series = parseTimeseriesPath(ctx.timeseries());
    String dataType = ctx.propertyClauses().propertyName.getText();
    String encodeType = ctx.propertyClauses().pv.getText();
    String compressor;
    int offset = 2;
    if (ctx.propertyClauses().compressor != null) {
      compressor = ctx.propertyClauses().compressor.getText();
      offset++;
    } else {
      compressor = TSFileConfig.compressor;
    }
    checkMetadataArgs(dataType, encodeType, compressor);
    Map<String, String> props = new HashMap<>(ctx.propertyClauses().propertyClause().size() + 1, 1);
    for (TSParser.PropertyClauseContext propertyClauseContext : ctx.propertyClauses().propertyClause()) {
      props.put(propertyClauseContext.propertyName.getText().toLowerCase(), propertyClauseContext.pv.getText());
    }
    MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_CREATE,
            MetadataOperator.NamespaceType.ADD_PATH);
    metadataOperator.setPath(series);
    metadataOperator.setDataType(TSDataType.valueOf(dataType));
    metadataOperator.setEncoding(TSEncoding.valueOf(encodeType));
    metadataOperator.setProps(props);
    metadataOperator.setCompressor(CompressionType.valueOf(compressor));
    initializedOperator = metadataOperator;
    return initializedOperator;
  }

  private Path parseTimeseriesPath(TSParser.TimeseriesContext timeseriesContext) {
    String[] paths = new String[timeseriesContext.identifier().size() + 1];
    paths[0] = SQLConstant.ROOT;
    int i = 1;
    for (TSParser.IdentifierContext identifierContext : timeseriesContext.identifier()) {
      paths[i++] = identifierContext.getText();
    }
    return new Path(new StringContainer(paths, TsFileConstant.PATH_SEPARATOR));
  }

  private void checkMetadataArgs(String dataType, String encoding, String compressor) {
//    final String rle = "RLE";
//    final String plain = "PLAIN";
//    final String ts2Diff = "TS_2DIFF";
//    final String bitmap = "BITMAP";
//    final String gorilla = "GORILLA";
    TSDataType tsDataType;
    TSEncoding tsEncoding;
    if (dataType == null) {
      throw new SqlParseException("data type cannot be null");
    }

    try {
      tsDataType = TSDataType.valueOf(dataType);
    } catch (Exception e) {
      throw new SqlParseException(String.format("data type %s not support", dataType));
    }

    if (encoding == null) {
      throw new SqlParseException("encoding type cannot be null");
    }

    try {
      tsEncoding = TSEncoding.valueOf(encoding);
    } catch (Exception e) {
      throw new SqlParseException(String.format("encoding %s is not support", encoding));
    }

    try {
      CompressionType.valueOf(compressor);
    } catch (Exception e) {
      throw new SqlParseException(String.format("compressor %s is not support", compressor));
    }
    checkDataTypeEncoding(tsDataType, tsEncoding);
  }

  private void checkDataTypeEncoding(TSDataType tsDataType, TSEncoding tsEncoding) {
    boolean throwExp = false;
    switch (tsDataType) {
      case BOOLEAN:
        if (!(tsEncoding.equals(TSEncoding.RLE) || tsEncoding.equals(TSEncoding.PLAIN))) {
          throwExp = true;
        }
        break;
      case INT32:
      case INT64:
        if (!(tsEncoding.equals(TSEncoding.RLE) || tsEncoding.equals(TSEncoding.PLAIN)
                || tsEncoding.equals(TSEncoding.TS_2DIFF))) {
          throwExp = true;
        }
        break;
      case FLOAT:
      case DOUBLE:
        if (!(tsEncoding.equals(TSEncoding.RLE) || tsEncoding.equals(TSEncoding.PLAIN)
                || tsEncoding.equals(TSEncoding.TS_2DIFF) || tsEncoding.equals(TSEncoding.GORILLA))) {
          throwExp = true;
        }
        break;
      case TEXT:
        if (!tsEncoding.equals(TSEncoding.PLAIN)) {
          throwExp = true;
        }
        break;
      default:
        throwExp = true;
    }
    if (throwExp) {
      throw new SqlParseException(
              String.format("encoding %s does not support %s", tsEncoding, tsDataType));
    }
  }

  @Override
  public Object visitSetStorageGroup(TSParser.SetStorageGroupContext ctx) {
    MetadataOperator metadataOperator = new MetadataOperator(
            SQLConstant.TOK_METADATA_SET_FILE_LEVEL,
            MetadataOperator.NamespaceType.SET_STORAGE_GROUP);
    Path path = parsePrefixPath(ctx.prefixPath());
    metadataOperator.setPath(path);
    initializedOperator = metadataOperator;
    return initializedOperator;
  }

  @Override
  public Object visitAddALabelProperty(TSParser.AddALabelPropertyContext ctx) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_ADD_LABEL,
            PropertyOperator.PropertyType.ADD_PROPERTY_LABEL);
    String label = ctx.label.getText();
    String property = ctx.property.getText();
    Path propertyLabel = new Path(new String[]{property, label});
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
    return initializedOperator;
  }

  @Override
  public Object visitLinkMetadataToPropertyTree(TSParser.LinkMetadataToPropertyTreeContext ctx) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_LINK,
            PropertyOperator.PropertyType.ADD_PROPERTY_TO_METADATA);
    Path metaPath = parsePrefixPath(ctx.prefixPath());
    propertyOperator.setMetadataPath(metaPath);
    String label = ctx.propertyPath().label.getText();
    String property = ctx.propertyPath().property.getText();
    Path propertyLabel = new Path(new String[]{property, label});
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
    return initializedOperator;
  }

  @Override
  public Object visitUnlinkMetadataNodeFromPropertyTree(TSParser.UnlinkMetadataNodeFromPropertyTreeContext ctx) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_UNLINK,
            PropertyOperator.PropertyType.DEL_PROPERTY_FROM_METADATA);
    Path metaPath = parsePrefixPath(ctx.prefixPath());
    propertyOperator.setMetadataPath(metaPath);
    String label = ctx.propertyPath().label.getText();
    String property = ctx.propertyPath().property.getText();
    Path propertyLabel = new Path(new String[]{property, label});
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
    return initializedOperator;
  }

  @Override
  public Object visitCreateUser(TSParser.CreateUserContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE,
            AuthorOperator.AuthorType.CREATE_USER);
    authorOperator.setUserName(ctx.userName.getText());
    authorOperator.setPassWord(ctx.password.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitCreateRole(TSParser.CreateRoleContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE,
            AuthorOperator.AuthorType.CREATE_ROLE);
    authorOperator.setRoleName(ctx.roleName.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitAddAPropertyTree(TSParser.AddAPropertyTreeContext ctx) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_CREATE,
            PropertyOperator.PropertyType.ADD_TREE);
    propertyOperator.setPropertyPath(new Path(ctx.property.getText()));
    initializedOperator = propertyOperator;
    return initializedOperator;
  }

  @Override
  public Object visitDropUser(TSParser.DropUserContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP,
            AuthorOperator.AuthorType.DROP_USER);
    authorOperator.setUserName(ctx.userName.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitDropRole(TSParser.DropRoleContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP,
            AuthorOperator.AuthorType.DROP_ROLE);
    authorOperator.setRoleName(ctx.roleName.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitGrantUser(TSParser.GrantUserContext ctx) {
    String[] privileges = new String[ctx.privileges().StringLiteral().size()];
    int i = 0;
    for (TerminalNode privilege : ctx.privileges().StringLiteral()) {
      privileges[i++] = parseStringWithQuoto(privilege.getText());
    }
    Path nodePath = parsePrefixPath(ctx.prefixPath());
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
            AuthorOperator.AuthorType.GRANT_USER);
    authorOperator.setUserName(ctx.userName.getText());
    authorOperator.setPrivilegeList(privileges);
    authorOperator.setNodeNameList(nodePath);
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitGrantRole(TSParser.GrantRoleContext ctx) {
    String[] privileges = new String[ctx.privileges().StringLiteral().size()];
    int i = 0;
    for (TerminalNode privilege : ctx.privileges().StringLiteral()) {
      privileges[i++] = parseStringWithQuoto(privilege.getText());
    }
    Path nodePath = parsePrefixPath(ctx.prefixPath());
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
            AuthorOperator.AuthorType.GRANT_ROLE);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setPrivilegeList(privileges);
    authorOperator.setNodeNameList(nodePath);
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitGrantRoleToUser(TSParser.GrantRoleToUserContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
            AuthorOperator.AuthorType.GRANT_ROLE_TO_USER);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setUserName(ctx.userName.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  private String parseStringWithQuoto(String src) {
    if (src.length() < 3 || src.charAt(0) != '\'' || src.charAt(src.length() - 1) != '\'') {
      throw new SqlParseException("error format for string with quoto:" + src);
    }
    return src.substring(1, src.length() - 1);
  }

  @Override
  public Object visitRevokeUser(TSParser.RevokeUserContext ctx) {
    String[] privileges = new String[ctx.privileges().StringLiteral().size()];
    int i = 0;
    for (TerminalNode privilege : ctx.privileges().StringLiteral()) {
      privileges[i++] = parseStringWithQuoto(privilege.getText());
    }
    Path nodePath = parsePrefixPath(ctx.prefixPath());
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE,
            AuthorOperator.AuthorType.REVOKE_USER);
    authorOperator.setUserName(ctx.userName.getText());
    authorOperator.setPrivilegeList(privileges);
    authorOperator.setNodeNameList(nodePath);
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitRevokeRole(TSParser.RevokeRoleContext ctx) {
    String[] privileges = new String[ctx.privileges().StringLiteral().size()];
    int i = 0;
    for (TerminalNode privilege : ctx.privileges().StringLiteral()) {
      privileges[i++] = parseStringWithQuoto(privilege.getText());
    }
    Path nodePath = parsePrefixPath(ctx.prefixPath());
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE,
            AuthorOperator.AuthorType.REVOKE_ROLE);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setPrivilegeList(privileges);
    authorOperator.setNodeNameList(nodePath);
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitRevokeRoleFromUser(TSParser.RevokeRoleFromUserContext ctx) {
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE,
            AuthorOperator.AuthorType.REVOKE_ROLE_FROM_USER);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setUserName(ctx.userName.getText());
    initializedOperator = authorOperator;
    return initializedOperator;
  }

  @Override
  public Object visitLoadStatement(TSParser.LoadStatementContext ctx) {
    String csvPath = ctx.fileName.getText();
    if (!SQLConstant.ROOT.equals(ctx.identifier(0).getText())) {
      throw new SqlParseException("Missing root.");
    }
    if (csvPath.length() < 3 || csvPath.charAt(0) != '\''
            || csvPath.charAt(csvPath.length() - 1) != '\'') {
      throw new SqlParseException("data load: error format csvPath:" + csvPath);
    }
    StringContainer sc = new StringContainer(TsFileConstant.PATH_SEPARATOR);
    sc.addTail(SQLConstant.ROOT);
    for (int i = 1; i < ctx.identifier().size(); i++) {
      String pathNode = ctx.identifier(i).getText();
      sc.addTail(pathNode);
    }
    initializedOperator = new LoadDataOperator(SQLConstant.TOK_DATALOAD,
            csvPath.substring(1, csvPath.length() - 1),
            sc.toString());
    return initializedOperator;
  }

  @Override
  public Object visitListUser(TSParser.ListUserContext ctx) {
    initializedOperator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_USER);
    return initializedOperator;
  }

  @Override
  public Object visitListRole(TSParser.ListRoleContext ctx) {
    initializedOperator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_ROLE);
    return initializedOperator;
  }

  @Override
  public Object visitListPrivilegesUser(TSParser.ListPrivilegesUserContext ctx) {
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    initializedOperator = operator;
    operator.setUserName(ctx.username.getText());
    operator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return initializedOperator;
  }

  @Override
  public Object visitListPrivilegesRole(TSParser.ListPrivilegesRoleContext ctx) {
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
    initializedOperator = operator;
    operator.setRoleName(ctx.roleName.getText());
    operator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return initializedOperator;
  }

  @Override
  public Object visitListUserPrivileges(TSParser.ListUserPrivilegesContext ctx) {
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    initializedOperator = operator;
    operator.setUserName(ctx.username.getText());
    return initializedOperator;
  }

  @Override
  public Object visitListRolePrivileges(TSParser.ListRolePrivilegesContext ctx) {
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
    initializedOperator = operator;
    operator.setRoleName(ctx.roleName.getText());
    return initializedOperator;
  }

  @Override
  public Object visitListAllRoles(TSParser.ListAllRolesContext ctx) {
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_USER_ROLES);
    initializedOperator = operator;
    operator.setUserName(ctx.username.getText());
    return initializedOperator;
  }

  @Override
  public Object visitListAllUsers(TSParser.ListAllUsersContext ctx) {
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_ROLE_USERS);
    initializedOperator = operator;
    operator.setRoleName(ctx.roleName.getText());
    return initializedOperator;
  }


  @Override
  public Object visitPrecedenceEqualExpressionSingle(TSParser.PrecedenceEqualExpressionSingleContext ctx) {

    if (ctx.atomExpression() == null) return visit(ctx.left);

    whereSeriesPath = (Path) visit(ctx.left);
    String seriesValue = (String) visit(ctx.equalExpr);
    Pair<Path, String> pair = new Pair<>(whereSeriesPath, seriesValue);
    BasicFunctionOperator basic = null;
    try {
      basic = new BasicFunctionOperator(
              TSParserConstant.getTSTokenIntType((Integer) visit(ctx.precedenceEqualOperator())),
              pair.left, pair.right);
      if (!isNotWhereClause && !isAndWhereClause && !isOrWhereClause) {
        whereOp.addChildOperator(basic);
      }
    } catch (LogicalOperatorException e) {
      throw new SqlParseException(e.getLocalizedMessage());
    }
    return basic;
  }

  @Override
  public Object visitExpNull(TSParser.ExpNullContext ctx) {

    throw new SqlParseException("null");
  }

  @Override
  public Object visitExpTime(TSParser.ExpTimeContext ctx) {

    String[] path = new String[1];
    path[0] = ctx.KW_TIME().getText();
    return new Path(new StringContainer(path, TsFileConstant.PATH_SEPARATOR));
  }

  @Override
  public Object visitExpConstant(TSParser.ExpConstantContext ctx) {

    String path = ctx.getText();
    return new Path(path);
  }

  @Override
  public Object visitExpPrefixPath(TSParser.ExpPrefixPathContext ctx) {
    return parsePrefixPath(ctx.prefixPath());
  }

  @Override
  public Object visitExpSuffixPath(TSParser.ExpSuffixPathContext ctx) {
    return parseSuffixPath(ctx.suffixPath());
  }

  @Override
  public Object visitExpExpression(TSParser.ExpExpressionContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public Object visitAtomNull(TSParser.AtomNullContext ctx) {

    return ctx.KW_NULL().getText();
  }

  @Override
  public Object visitAtomTime(TSParser.AtomTimeContext ctx) {

    return ctx.KW_TIME().getText();
  }


  @Override
  public Object visitAtomConstant(TSParser.AtomConstantContext ctx) {

    return visit(ctx.constant());
  }

  @Override
  public Object visitAtomPrefixPath(TSParser.AtomPrefixPathContext ctx) {

    return parsePrefixPath(ctx.prefixPath()).getFullPath();
  }

  @Override
  public Object visitAtomSuffixPath(TSParser.AtomSuffixPathContext ctx) {
    return parseSuffixPath(ctx.suffixPath()).getFullPath();
  }


  @Override
  public String visitConstantString(TSParser.ConstantStringContext ctx) {
    return ctx.StringLiteral().getText();
  }

  @Override
  public String visitConstantDate(TSParser.ConstantDateContext ctx) {

    if (!whereSeriesPath.equals(RESERVED_TIME)) {
      throw new SqlParseException("Date can only be used to time");
    }
    return parseTimeFormat((String) visit(ctx.dateFormat())) + "";
  }

  @Override
  public String visitConstantNumber(TSParser.ConstantNumberContext ctx) {
    return (String) visit(ctx.number());
  }

  @Override
  public Object visitInteger(TSParser.IntegerContext ctx) {
    return ctx.getText();
  }


  @Override
  public Object visitPreEqual(TSParser.PreEqualContext ctx) {
    return TSParser.EQUAL;
  }

  @Override
  public Object visitPreEqual_NS(TSParser.PreEqual_NSContext ctx) {
    return TSParser.EQUAL_NS;
  }

  @Override
  public Object visitPreNotEqual(TSParser.PreNotEqualContext ctx) {
    return TSParser.NOTEQUAL;
  }

  @Override
  public Object visitPreLessThanOrEqualTo(TSParser.PreLessThanOrEqualToContext ctx) {
    return TSParser.LESSTHANOREQUALTO;
  }

  @Override
  public Object visitPreLessThan(TSParser.PreLessThanContext ctx) {
    return TSParser.LESSTHAN;
  }

  @Override
  public Object visitPreGreaterThanOrEqualTo(TSParser.PreGreaterThanOrEqualToContext ctx) {
    return TSParser.GREATERTHANOREQUALTO;
  }

  @Override
  public Object visitPreGreaterThan(TSParser.PreGreaterThanContext ctx) {
    return TSParser.GREATERTHAN;
  }

  @Override
  public Object visitExeAuthorStat(TSParser.ExeAuthorStatContext ctx) {
    return visit(ctx.authorStatement());
  }

  @Override
  public Object visitExeDeleteStat(TSParser.ExeDeleteStatContext ctx) {
    return visit(ctx.deleteStatement());
  }

  @Override
  public Object visitExeUpdateStat(TSParser.ExeUpdateStatContext ctx) {
    return visit(ctx.updateStatement());
  }

  @Override
  public Object visitExeInsertStat(TSParser.ExeInsertStatContext ctx) {
    return visit(ctx.insertStatement());
  }

  @Override
  public Object visitExeQueryStat(TSParser.ExeQueryStatContext ctx) {
    return visit(ctx.queryStatement());
  }

  @Override
  public Object visitExeMetaStat(TSParser.ExeMetaStatContext ctx) {
    return visit(ctx.metadataStatement());
  }

  @Override
  public Object visitExeMergeStat(TSParser.ExeMergeStatContext ctx) {
    return visit(ctx.mergeStatement());
  }

  @Override
  public Object visitExeIndexStat(TSParser.ExeIndexStatContext ctx) {
    return visit(ctx.indexStatement());
  }

  @Override
  public Object visitExeQuitStat(TSParser.ExeQuitStatContext ctx) {
    return visit(ctx.quitStatement());
  }

  @Override
  public Object visitExeListStat(TSParser.ExeListStatContext ctx) {
    return visit(ctx.listStatement());
  }
}
