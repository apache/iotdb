/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.strategy;

import static org.apache.iotdb.db.qp.constant.SQLConstant.LESSTHAN;
import static org.apache.iotdb.db.qp.constant.SQLConstant.LESSTHANOREQUALTO;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.antlr.runtime.Token;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.qp.IllegalASTFormatException;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.constant.TSParserConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.qp.logical.crud.UpdateOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.LinearFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.db.sql.parse.AstNode;
import org.apache.iotdb.db.sql.parse.Node;
import org.apache.iotdb.db.sql.parse.TSParser;
import org.apache.iotdb.tsfile.common.constant.SystemConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class receives an AstNode and transform it to an operator which is a logical plan.
 */
public class LogicalGenerator {

  private Logger log = LoggerFactory.getLogger(LogicalGenerator.class);
  private RootOperator initializedOperator = null;
  private ZoneId zoneId;

  public LogicalGenerator(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public RootOperator getLogicalPlan(AstNode astNode)
      throws QueryProcessorException, ArgsErrorException {
    analyze(astNode);
    return initializedOperator;
  }

  /**
   * input an astNode parsing by {@code antlr} and analyze it.
   *
   * @throws QueryProcessorException exception in query process
   * @throws ArgsErrorException args error
   */
  private void analyze(AstNode astNode) throws QueryProcessorException, ArgsErrorException {
    Token token = astNode.getToken();
    if (token == null) {
      throw new QueryProcessorException("given token is null");
    }
    int tokenIntType = token.getType();
    switch (tokenIntType) {
      case TSParser.TOK_INSERT:
        analyzeInsert(astNode);
        return;
      case TSParser.TOK_SELECT:
        analyzeSelectedPath(astNode);
        return;
      case TSParser.TOK_FROM:
        analyzeFrom(astNode);
        return;
      case TSParser.TOK_WHERE:
        analyzeWhere(astNode);
        return;
      case TSParser.TOK_GROUPBY:
        analyzeGroupBy(astNode);
        return;
      case TSParser.TOK_FILL:
        analyzeFill(astNode);
        return;
      case TSParser.TOK_UPDATE:
        if (astNode.getChild(0).getType() == TSParser.TOK_UPDATE_PSWD) {
          analyzeAuthorUpdate(astNode);
          return;
        }
        analyzeUpdate(astNode);
        return;
      case TSParser.TOK_DELETE:
        switch (astNode.getChild(0).getType()) {
          case TSParser.TOK_TIMESERIES:
            analyzeMetadataDelete(astNode);
            break;
          case TSParser.TOK_LABEL:
            analyzePropertyDeleteLabel(astNode);
            break;
          default:
            analyzeDelete(astNode);
            break;
        }
        return;
      case TSParser.TOK_SET:
        analyzeMetadataSetFileLevel(astNode);
        return;
      case TSParser.TOK_ADD:
        analyzePropertyAddLabel(astNode);
        return;
      case TSParser.TOK_LINK:
        analyzePropertyLink(astNode);
        return;
      case TSParser.TOK_UNLINK:
        analyzePropertyUnLink(astNode);
        return;
      case TSParser.TOK_CREATE:
        switch (astNode.getChild(0).getType()) {
          case TSParser.TOK_USER:
          case TSParser.TOK_ROLE:
            analyzeAuthorCreate(astNode);
            break;
          case TSParser.TOK_TIMESERIES:
            analyzeMetadataCreate(astNode);
            break;
          case TSParser.TOK_PROPERTY:
            analyzePropertyCreate(astNode);
            break;
          default:
            break;
        }
        return;
      case TSParser.TOK_DROP:
        switch (astNode.getChild(0).getType()) {
          case TSParser.TOK_USER:
          case TSParser.TOK_ROLE:
            analyzeAuthorDrop(astNode);
            break;
          default:
            break;
        }
        return;
      case TSParser.TOK_GRANT:
        analyzeAuthorGrant(astNode);
        return;
      case TSParser.TOK_REVOKE:
        analyzeAuthorRevoke(astNode);
        return;
      case TSParser.TOK_LOAD:
        analyzeDataLoad(astNode);
        return;
      case TSParser.TOK_QUERY:
        // for TSParser.TOK_QUERY might appear in both query and insert
        // command. Thus, do
        // nothing and call analyze() with children nodes recursively.
        initializedOperator = new QueryOperator(SQLConstant.TOK_QUERY);
        break;
      case TSParser.TOK_LIST:
        analyzeList(astNode);
        return;
      case TSParser.TOK_LIMIT:
        analyzeLimit(astNode);
        return;
      case TSParser.TOK_SLIMIT:
        analyzeSlimit(astNode);
        return;
      case TSParser.TOK_SOFFSET:
        analyzeSoffset(astNode);
        return;
      default:
        throw new QueryProcessorException("Not supported TSParser type" + tokenIntType);
    }
    for (Node node : astNode.getChildren()) {
      analyze((AstNode) node);
    }
  }

  private void analyzeSlimit(AstNode astNode) throws LogicalOperatorException {
    AstNode unit = astNode.getChild(0);
    int seriesLimit;
    try {
      seriesLimit = Integer.parseInt(unit.getText().trim());
    } catch (NumberFormatException e) {
      throw new LogicalOperatorException("SLIMIT <SN>: SN should be Int32.");
    }
    if (seriesLimit <= 0) {
      // seriesLimit is ensured to be a non negative integer after the lexical examination,
      // and seriesLimit is further required to be a positive integer here.
      throw new LogicalOperatorException(
          "SLIMIT <SN>: SN must be a positive integer and can not be zero.");
    }
    ((QueryOperator) initializedOperator).setSeriesLimit(seriesLimit);
  }

  private void analyzeSoffset(AstNode astNode) throws LogicalOperatorException {
    AstNode unit = astNode.getChild(0);
    try {
      // NOTE seriesOffset is ensured to be a non negative integer after the lexical examination.
      ((QueryOperator) initializedOperator)
          .setSeriesOffset(Integer.parseInt(unit.getText().trim()));
    } catch (NumberFormatException e) {
      throw new LogicalOperatorException("SOFFSET <SOFFSETValue>: SOFFSETValue should be Int32.");
    }
  }

  private void analyzeLimit(AstNode astNode) throws LogicalOperatorException {
    AstNode unit = astNode.getChild(0);
    int rowsLimit;
    try {
      rowsLimit = Integer.parseInt(unit.getText().trim());
    } catch (NumberFormatException e) {
      throw new LogicalOperatorException("LIMIT <N>: N should be Int32.");
    }
    if (rowsLimit <= 0) {
      // rowsLimit is ensured to be a non negative integer after the lexical examination,
      // and rowsLimit is further required to be a positive integer here.
      throw new LogicalOperatorException(
          "LIMIT <N>: N must be a positive integer and can not be zero.");
    }
  }

  private void analyzeList(AstNode astNode) {
    int childrenSize = astNode.getChildren().size();
    if (childrenSize == 1) {
      int tokenType = astNode.getChild(0).getType();
      if (tokenType == TSParser.TOK_USER) {
        // list all users
        initializedOperator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_USER);
      } else if (tokenType == TSParser.TOK_ROLE) {
        // list all roles
        initializedOperator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_ROLE);
      }
    } else if (childrenSize == 3) {
      int tokenType = astNode.getChild(1).getType();
      if (tokenType == TSParser.TOK_USER) {
        // list user privileges on seriesPath
        AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
        initializedOperator = operator;
        operator.setUserName(astNode.getChild(1).getChild(0).getText());
        operator.setNodeNameList(parsePath(astNode.getChild(2)));
      } else if (tokenType == TSParser.TOK_ROLE) {
        // list role privileges on seriesPath
        AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
            AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
        initializedOperator = operator;
        operator.setRoleName(astNode.getChild(1).getChild(0).getText());
        operator.setNodeNameList(parsePath(astNode.getChild(2)));
      } else if (tokenType == TSParser.TOK_ALL) {
        tokenType = astNode.getChild(0).getType();
        if (tokenType == TSParser.TOK_PRIVILEGES) {
          tokenType = astNode.getChild(2).getType();
          if (tokenType == TSParser.TOK_USER) {
            // list all privileges of a user
            AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
                AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
            initializedOperator = operator;
            operator.setUserName(astNode.getChild(2).getChild(0).getText());
          } else if (tokenType == TSParser.TOK_ROLE) {
            // list all privileges of a role
            AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
                AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
            initializedOperator = operator;
            operator.setRoleName(astNode.getChild(2).getChild(0).getText());
          }
        } else {
          tokenType = astNode.getChild(2).getType();
          if (tokenType == TSParser.TOK_USER) {
            // list all roles of a user
            AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
                AuthorOperator.AuthorType.LIST_USER_ROLES);
            initializedOperator = operator;
            operator.setUserName(astNode.getChild(2).getChild(0).getText());
          } else if (tokenType == TSParser.TOK_ROLE) {
            // list all users of a role
            AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
                AuthorOperator.AuthorType.LIST_ROLE_USERS);
            initializedOperator = operator;
            operator.setRoleName(astNode.getChild(2).getChild(0).getText());
          }
        }
      }
    }
  }

  private void analyzePropertyCreate(AstNode astNode) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_CREATE,
        PropertyOperator.PropertyType.ADD_TREE);
    propertyOperator.setPropertyPath(new Path(astNode.getChild(0).getChild(0).getText()));
    initializedOperator = propertyOperator;
  }

  private void analyzePropertyAddLabel(AstNode astNode) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_ADD_LABEL,
        PropertyOperator.PropertyType.ADD_PROPERTY_LABEL);
    Path propertyLabel = parsePropertyAndLabel(astNode, 0);
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
  }

  private void analyzePropertyDeleteLabel(AstNode astNode) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_DELETE_LABEL,
        PropertyOperator.PropertyType.DELETE_PROPERTY_LABEL);
    Path propertyLabel = parsePropertyAndLabel(astNode, 0);
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
  }

  private Path parsePropertyAndLabel(AstNode astNode, int startIndex) {
    String label = astNode.getChild(startIndex).getChild(0).getText();
    String property = astNode.getChild(startIndex + 1).getChild(0).getText();
    return new Path(new String[]{property, label});
  }

  private void analyzePropertyLink(AstNode astNode) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_LINK,
        PropertyOperator.PropertyType.ADD_PROPERTY_TO_METADATA);
    Path metaPath = parsePath(astNode.getChild(0));
    propertyOperator.setMetadataPath(metaPath);
    Path propertyLabel = parsePropertyAndLabel(astNode, 1);
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
  }

  private void analyzePropertyUnLink(AstNode astNode) {
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_UNLINK,
        PropertyOperator.PropertyType.DEL_PROPERTY_FROM_METADATA);
    Path metaPath = parsePath(astNode.getChild(0));
    propertyOperator.setMetadataPath(metaPath);
    Path propertyLabel = parsePropertyAndLabel(astNode, 1);
    propertyOperator.setPropertyPath(propertyLabel);
    initializedOperator = propertyOperator;
  }

  private void analyzeMetadataCreate(AstNode astNode) throws MetadataArgsErrorException {
    Path series = parsePath(astNode.getChild(0).getChild(0));
    AstNode paramNode = astNode.getChild(1);
    String dataType = paramNode.getChild(0).getChild(0).getText();
    String encodingType = paramNode.getChild(1).getChild(0).getText();
    checkMetadataArgs(dataType, encodingType);
    String[] paramStrings = new String[paramNode.getChildCount() - 2];
    for (int i = 0; i < paramStrings.length; i++) {
      AstNode node = paramNode.getChild(i + 2);
      paramStrings[i] = node.getChild(0) + SQLConstant.METADATA_PARAM_EQUAL + node.getChild(1);
    }
    MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_CREATE,
        MetadataOperator.NamespaceType.ADD_PATH);
    metadataOperator.setPath(series);
    metadataOperator.setDataType(dataType);
    metadataOperator.setEncoding(encodingType);
    metadataOperator.setEncodingArgs(paramStrings);
    initializedOperator = metadataOperator;
  }

  private void analyzeMetadataDelete(AstNode astNode) {
    List<Path> deletePaths = new ArrayList<>();
    for (int i = 0; i < astNode.getChild(0).getChildCount(); i++) {
      deletePaths.add(parsePath(astNode.getChild(0).getChild(i)));
    }
    MetadataOperator metadataOperator = new MetadataOperator(SQLConstant.TOK_METADATA_DELETE,
        MetadataOperator.NamespaceType.DELETE_PATH);
    metadataOperator.setDeletePathList(deletePaths);
    initializedOperator = metadataOperator;
  }

  private void analyzeMetadataSetFileLevel(AstNode astNode) {
    MetadataOperator metadataOperator = new MetadataOperator(
        SQLConstant.TOK_METADATA_SET_FILE_LEVEL,
        MetadataOperator.NamespaceType.SET_FILE_LEVEL);
    Path path = parsePath(astNode.getChild(0).getChild(0));
    metadataOperator.setPath(path);
    initializedOperator = metadataOperator;
  }

  private void analyzeInsert(AstNode astNode) throws QueryProcessorException {
    InsertOperator insertOp = new InsertOperator(SQLConstant.TOK_INSERT);
    initializedOperator = insertOp;
    analyzeSelectedPath(astNode.getChild(0));
    long timestamp;
    AstNode timeChild;
    try {
      timeChild = astNode.getChild(1).getChild(0);
      if (timeChild.getToken().getType() != TSParser.TOK_TIME) {
        throw new LogicalOperatorException("need keyword 'timestamp'");
      }
      AstNode timeValue = astNode.getChild(2).getChild(0);
      if (timeValue.getType() == TSParser.TOK_DATETIME) {
        timestamp = Long.valueOf(parseTokenTime(timeValue));
      } else {
        timestamp = Long.valueOf(astNode.getChild(2).getChild(0).getText());
      }
    } catch (NumberFormatException e) {
      throw new LogicalOperatorException(
          "need a long value in insert clause, but given:" + astNode.getChild(2).getChild(0)
              .getText());
    }
    if (astNode.getChild(1).getChildCount() != astNode.getChild(2).getChildCount()) {
      throw new QueryProcessorException(
          "number of measurement is NOT EQUAL TO the number of values");
    }
    insertOp.setTime(timestamp);
    List<String> measurementList = new ArrayList<>();
    for (int i = 1; i < astNode.getChild(1).getChildCount(); i++) {
      measurementList.add(astNode.getChild(1).getChild(i).getText());
    }
    insertOp.setMeasurementList(measurementList);

    List<String> valueList = new ArrayList<>();
    for (int i = 1; i < astNode.getChild(2).getChildCount(); i++) {
      valueList.add(astNode.getChild(2).getChild(i).getText());
    }
    insertOp.setValueList(valueList);
  }

  private void analyzeUpdate(AstNode astNode) throws QueryProcessorException {
    if (astNode.getChildCount() > 3) {
      throw new LogicalOperatorException("UPDATE clause doesn't support multi-update yet.");
    }
    UpdateOperator updateOp = new UpdateOperator(SQLConstant.TOK_UPDATE);
    initializedOperator = updateOp;
    FromOperator fromOp = new FromOperator(TSParser.TOK_FROM);
    fromOp.addPrefixTablePath(parsePath(astNode.getChild(0)));
    updateOp.setFromOperator(fromOp);
    SelectOperator selectOp = new SelectOperator(TSParser.TOK_SELECT);
    selectOp.addSelectPath(parsePath(astNode.getChild(1).getChild(0)));
    updateOp.setSelectOperator(selectOp);
    updateOp.setValue(astNode.getChild(1).getChild(1).getText());
    analyzeWhere(astNode.getChild(2));
  }

  private void analyzeDelete(AstNode astNode) throws LogicalOperatorException {
    initializedOperator = new DeleteOperator(SQLConstant.TOK_DELETE);
    SelectOperator selectOp = new SelectOperator(TSParser.TOK_SELECT);
    int selChildCount = astNode.getChildCount() - 1;
    for (int i = 0; i < selChildCount; i++) {
      AstNode child = astNode.getChild(i);
      if (child.getType() != TSParser.TOK_PATH) {
        throw new LogicalOperatorException(
            "children FROM clause except last one must all be seriesPath like root.a.b, actual:"
                + child.getText());
      }
      Path tablePath = parsePath(child);
      selectOp.addSelectPath(tablePath);
    }
    ((SFWOperator) initializedOperator).setSelectOperator(selectOp);
    analyzeWhere(astNode.getChild(selChildCount));
    long deleteTime = parseDeleteTimeFilter((DeleteOperator) initializedOperator);
    ((DeleteOperator) initializedOperator).setTime(deleteTime);
  }

  /**
   * for delete command, time should only have an end time.
   *
   * @param operator delete logical plan
   */
  private long parseDeleteTimeFilter(DeleteOperator operator) throws LogicalOperatorException {
    FilterOperator filterOperator = operator.getFilterOperator();
    if (!(filterOperator.isLeaf())) {
      throw new LogicalOperatorException(
          "For delete command, where clause must be like : time < XXX or time <= XXX");
    }
    if (filterOperator.getTokenIntType() != LESSTHAN
        && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
      throw new LogicalOperatorException(
          "For delete command, time filter must be less than or less than or equal to");
    }
    long time = Long.valueOf(((BasicFunctionOperator) filterOperator).getValue());
    if (filterOperator.getTokenIntType() == LESSTHAN) {
      time = time - 1;
    }
    // time must greater than 0 now
    if (time <= 0) {
      throw new LogicalOperatorException("delete Time:" + time + ", time must > 0");
    }
    return time;
  }

  private void analyzeFrom(AstNode node) throws LogicalOperatorException {
    int selChildCount = node.getChildCount();
    FromOperator from = new FromOperator(SQLConstant.TOK_FROM);
    for (int i = 0; i < selChildCount; i++) {
      AstNode child = node.getChild(i);
      if (child.getType() != TSParser.TOK_PATH) {
        throw new LogicalOperatorException(
            "children FROM clause must all be seriesPath like root.a.b, actual:" + child.getText());
      }
      Path tablePath = parsePath(child);
      from.addPrefixTablePath(tablePath);
    }
    ((SFWOperator) initializedOperator).setFromOperator(from);
  }

  private void analyzeSelectedPath(AstNode astNode) throws LogicalOperatorException {
    int tokenIntType = astNode.getType();
    SelectOperator selectOp = new SelectOperator(TSParser.TOK_SELECT);
    if (tokenIntType == TSParser.TOK_SELECT) {
      int selChildCount = astNode.getChildCount();
      for (int i = 0; i < selChildCount; i++) {
        AstNode child = astNode.getChild(i);
        if (child.getChild(0).getType() == TSParser.TOK_CLUSTER) {
          AstNode cluster = child.getChild(0);
          AstNode pathChild = cluster.getChild(0);
          Path selectPath = parsePath(pathChild);
          String aggregation = cluster.getChild(1).getText();
          selectOp.addClusterPath(selectPath, aggregation);
        } else {
          Path selectPath = parsePath(child);
          selectOp.addSelectPath(selectPath);
        }
      }
    } else if (tokenIntType == TSParser.TOK_PATH) {
      Path selectPath = parsePath(astNode);
      selectOp.addSelectPath(selectPath);
    } else {
      throw new LogicalOperatorException(
          "children SELECT clause must all be seriesPath like root.a.b, actual:" + astNode.dump());
    }
    ((SFWOperator) initializedOperator).setSelectOperator(selectOp);
  }

  private void analyzeWhere(AstNode astNode) throws LogicalOperatorException {
    if (astNode.getType() != TSParser.TOK_WHERE) {
      throw new LogicalOperatorException(
          "given node is not WHERE! please check whether SQL statement is correct.");
    }
    if (astNode.getChildCount() != 1) {
      throw new LogicalOperatorException("where clause has" + astNode.getChildCount()
          + " child, please check whether SQL grammar is correct.");
    }
    FilterOperator whereOp = new FilterOperator(SQLConstant.TOK_WHERE);
    AstNode child = astNode.getChild(0);
    analyzeWhere(child, child.getType(), whereOp);
    ((SFWOperator) initializedOperator).setFilterOperator(whereOp.getChildren().get(0));
  }

  private void analyzeWhere(AstNode ast, int tokenIntType, FilterOperator filterOp)
      throws LogicalOperatorException {
    int childCount = ast.getChildCount();
    switch (tokenIntType) {
      case TSParser.KW_NOT:
        if (childCount != 1) {
          throw new LogicalOperatorException(
              "parsing where clause failed: NOT operator requries one param");
        }
        FilterOperator notOp = new FilterOperator(SQLConstant.KW_NOT);
        filterOp.addChildOperator(notOp);
        AstNode childAstNode = ast.getChild(0);
        int childNodeTokenType = childAstNode.getToken().getType();
        analyzeWhere(childAstNode, childNodeTokenType, notOp);
        break;
      case TSParser.KW_AND:
      case TSParser.KW_OR:
        if (childCount != 2) {
          throw new LogicalOperatorException(
              "parsing where clause failed! node has " + childCount + " paramter.");
        }
        FilterOperator binaryOp = new FilterOperator(
            TSParserConstant.getTSTokenIntType(tokenIntType));
        filterOp.addChildOperator(binaryOp);
        for (int i = 0; i < childCount; i++) {
          childAstNode = ast.getChild(i);
          childNodeTokenType = childAstNode.getToken().getType();
          analyzeWhere(childAstNode, childNodeTokenType, binaryOp);
        }
        break;
      case TSParser.LESSTHAN:
      case TSParser.LESSTHANOREQUALTO:
      case TSParser.EQUAL:
      case TSParser.EQUAL_NS:
      case TSParser.GREATERTHAN:
      case TSParser.GREATERTHANOREQUALTO:
      case TSParser.NOTEQUAL:
        Pair<Path, String> pair = parseLeafNode(ast);
        BasicFunctionOperator basic = new BasicFunctionOperator(
            TSParserConstant.getTSTokenIntType(tokenIntType),
            pair.left, pair.right);
        filterOp.addChildOperator(basic);
        break;
      default:
        throw new LogicalOperatorException("unsupported token:" + tokenIntType);
    }
  }

  private void analyzeGroupBy(AstNode astNode) throws LogicalOperatorException {
    SelectOperator selectOp = ((QueryOperator) initializedOperator).getSelectOperator();

    if (selectOp.getSuffixPaths().size() != selectOp.getAggregations().size()) {
      throw new LogicalOperatorException(
          "Group by must bind each seriesPath with an aggregation function");
    }
    ((QueryOperator) initializedOperator).setGroupBy(true);
    int childCount = astNode.getChildCount();

    // parse timeUnit
    AstNode unit = astNode.getChild(0);
    long value = parseTimeUnit(unit);
    ((QueryOperator) initializedOperator).setUnit(value);

    // parse show intervals
    AstNode intervalsNode = astNode.getChild(childCount - 1);
    int intervalCount = intervalsNode.getChildCount();
    List<Pair<Long, Long>> intervals = new ArrayList<>();
    AstNode intervalNode;
    long startTime;
    long endTime;
    for (int i = 0; i < intervalCount; i++) {
      intervalNode = intervalsNode.getChild(i);
      AstNode startNode = intervalNode.getChild(0);
      if (startNode.getType() == TSParser.TOK_DATETIME) {
        startTime = Long.valueOf(parseTokenTime(startNode));
      } else {
        startTime = Long.valueOf(startNode.getText());
      }
      AstNode endNode = intervalNode.getChild(1);
      if (endNode.getType() == TSParser.TOK_DATETIME) {
        endTime = Long.valueOf(parseTokenTime(endNode));
      } else {
        endTime = Long.valueOf(endNode.getText());
      }
      intervals.add(new Pair<>(startTime, endTime));
    }

    ((QueryOperator) initializedOperator).setIntervals(intervals);

    // parse time origin
    long originTime;
    if (childCount == 3) {
      AstNode originNode = astNode.getChild(1).getChild(0);
      if (originNode.getType() == TSParser.TOK_DATETIME) {
        originTime = Long.valueOf(parseTokenTime(originNode));
      } else {
        originTime = Long.valueOf(originNode.getText());
      }
    } else {
      originTime = parseTimeFormat(SQLConstant.START_TIME_STR);
    }
    ((QueryOperator) initializedOperator).setOrigin(originTime);
  }

  /**
   * analyze fill type clause.
   *
   * <P>PreviousClause : PREVIOUS COMMA < ValidPreviousTime > LinearClause : LINEAR COMMA <
   * ValidPreviousTime > COMMA < ValidBehindTime >
   */
  private void analyzeFill(AstNode node) throws LogicalOperatorException {
    FilterOperator filterOperator = ((SFWOperator) initializedOperator).getFilterOperator();
    if (!filterOperator.isLeaf() || filterOperator.getTokenIntType() != SQLConstant.EQUAL) {
      throw new LogicalOperatorException("Only \"=\" can be used in fill function");
    }

    Map<TSDataType, IFill> fillTypes = new HashMap<>();
    int childNum = node.getChildCount();
    for (int i = 0; i < childNum; i++) {
      AstNode childNode = node.getChild(i);
      TSDataType dataType = parseTypeNode(childNode.getChild(0));
      AstNode fillTypeNode = childNode.getChild(1);
      switch (fillTypeNode.getType()) {
        case TSParser.TOK_LINEAR:
          checkTypeFill(dataType, TSParser.TOK_LINEAR);
          if (fillTypeNode.getChildCount() == 2) {
            long beforeRange = parseTimeUnit(fillTypeNode.getChild(0));
            long afterRange = parseTimeUnit(fillTypeNode.getChild(1));
            fillTypes.put(dataType, new LinearFill(beforeRange, afterRange));
          } else if (fillTypeNode.getChildCount() == 0) {
            fillTypes.put(dataType, new LinearFill(-1, -1));
          } else {
            throw new LogicalOperatorException(
                "Linear fill type must have 0 or 2 valid time ranges");
          }
          break;
        case TSParser.TOK_PREVIOUS:
          checkTypeFill(dataType, TSParser.TOK_PREVIOUS);
          if (fillTypeNode.getChildCount() == 1) {
            long preRange = parseTimeUnit(fillTypeNode.getChild(0));
            fillTypes.put(dataType, new PreviousFill(preRange));
          } else if (fillTypeNode.getChildCount() == 0) {
            fillTypes.put(dataType, new PreviousFill(-1));
          } else {
            throw new LogicalOperatorException(
                "Previous fill type must have 0 or 1 valid time range");
          }
          break;
        default:
          break;
      }
    }

    ((QueryOperator) initializedOperator).setFillTypes(fillTypes);
    ((QueryOperator) initializedOperator).setFill(true);
  }

  private void checkTypeFill(TSDataType dataType, int type) throws LogicalOperatorException {
    switch (dataType) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        if (type != TSParser.TOK_LINEAR && type != TSParser.TOK_PREVIOUS) {
          throw new LogicalOperatorException(
              String.format("type %s cannot use %s fill function", dataType,
                  TSParser.tokenNames[type]));
        }
        return;
      case BOOLEAN:
      case TEXT:
        if (type != TSParser.TOK_PREVIOUS) {
          throw new LogicalOperatorException(
              String.format("type %s cannot use %s fill function", dataType,
                  TSParser.tokenNames[type]));
        }
        return;
      default:
        break;
    }
  }

  /**
   * parse datatype node.
   */
  private TSDataType parseTypeNode(AstNode typeNode) throws LogicalOperatorException {
    String type = typeNode.getText().toLowerCase();
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

  private long parseTimeUnit(AstNode node) throws LogicalOperatorException {
    long timeInterval = Long.valueOf(node.getChild(0).getText());
    if (timeInterval <= 0) {
      throw new LogicalOperatorException("Interval must more than 0.");
    }
    String granu = node.getChild(1).getText();
    switch (granu) {
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

  private Pair<Path, String> parseLeafNode(AstNode node) throws LogicalOperatorException {
    if (node.getChildCount() != 2) {
      throw new LogicalOperatorException(
          "error format in SQL statement, please check whether SQL statement is correct.");
    }
    AstNode col = node.getChild(0);
    if (col.getType() != TSParser.TOK_PATH) {
      throw new LogicalOperatorException(
          "error format in SQL statement, please check whether SQL statement is correct.");
    }
    Path seriesPath = parsePath(col);
    AstNode rightKey = node.getChild(1);
    String seriesValue;
    if (rightKey.getType() == TSParser.TOK_PATH) {
      seriesValue = parsePath(rightKey).getFullPath();
    } else if (rightKey.getType() == TSParser.TOK_DATETIME) {
      if (!seriesPath.equals(SQLConstant.RESERVED_TIME)) {
        throw new LogicalOperatorException("Date can only be used to time");
      }
      seriesValue = parseTokenTime(rightKey);
    } else {
      seriesValue = rightKey.getText();
    }
    return new Pair<>(seriesPath, seriesValue);
  }

  private String parseTokenTime(AstNode astNode) throws LogicalOperatorException {
    StringContainer sc = new StringContainer();
    for (int i = 0; i < astNode.getChildCount(); i++) {
      sc.addTail(astNode.getChild(i).getText());
    }
    return parseTimeFormat(sc.toString()) + "";
  }

  /**
   * function for parsing time format.
   */
  public long parseTimeFormat(String timestampStr) throws LogicalOperatorException {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new LogicalOperatorException("input timestamp cannot be empty");
    }
    if (timestampStr.toLowerCase().equals(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    try {
      return DatetimeUtils.convertDatetimeStrToMillisecond(timestampStr, zoneId);
    } catch (Exception e) {
      throw new LogicalOperatorException(String
          .format("Input time format %s error. "
              + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
              + "refer to user document for more info.", timestampStr));
    }
  }

  private Path parsePath(AstNode node) {
    int childCount = node.getChildCount();
    String[] path;

    if (childCount == 1 && node.getChild(0).getType() == TSParser.TOK_ROOT) {
      AstNode childNode = node.getChild(0);
      childCount = childNode.getChildCount();
      path = new String[childCount + 1];
      path[0] = SQLConstant.ROOT;
      for (int i = 0; i < childCount; i++) {
        path[i + 1] = childNode.getChild(i).getText();
      }
    } else {
      path = new String[childCount];
      for (int i = 0; i < childCount; i++) {
        path[i] = node.getChild(i).getText();
      }
    }
    return new Path(new StringContainer(path, SystemConstant.PATH_SEPARATOR));
  }

  private String parseStringWithQuoto(String src) throws IllegalASTFormatException {
    if (src.length() < 3 || src.charAt(0) != '\'' || src.charAt(src.length() - 1) != '\'') {
      throw new IllegalASTFormatException("error format for string with quoto:" + src);
    }
    return src.substring(1, src.length() - 1);
  }

  private void analyzeDataLoad(AstNode astNode) throws IllegalASTFormatException {
    int childCount = astNode.getChildCount();
    // node seriesPath should have more than one level and first node level must
    // be root
    // if (childCount < 3 ||
    // !SQLConstant.ROOT.equals(astNode.getChild(1).getText().toLowerCase()))
    if (childCount < 3 || !SQLConstant.ROOT.equals(astNode.getChild(1).getText())) {
      throw new IllegalASTFormatException("data load command: child count < 3\n" + astNode.dump());
    }
    String csvPath = astNode.getChild(0).getText();
    if (csvPath.length() < 3 || csvPath.charAt(0) != '\''
        || csvPath.charAt(csvPath.length() - 1) != '\'') {
      throw new IllegalASTFormatException("data load: error format csvPath:" + csvPath);
    }
    StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
    sc.addTail(SQLConstant.ROOT);
    for (int i = 2; i < childCount; i++) {
      // String pathNode = astNode.getChild(i).getText().toLowerCase();
      String pathNode = astNode.getChild(i).getText();
      sc.addTail(pathNode);
    }
    initializedOperator = new LoadDataOperator(SQLConstant.TOK_DATALOAD,
        csvPath.substring(1, csvPath.length() - 1),
        sc.toString());
  }

  private void analyzeAuthorCreate(AstNode astNode) throws IllegalASTFormatException {
    int childCount = astNode.getChildCount();
    AuthorOperator authorOperator;
    if (childCount == 2) {
      // create user
      authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE,
          AuthorOperator.AuthorType.CREATE_USER);
      authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
      authorOperator.setPassWord(astNode.getChild(1).getChild(0).getText());
    } else if (childCount == 1) {
      // create role
      authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE,
          AuthorOperator.AuthorType.CREATE_ROLE);
      authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
    } else {
      throw new IllegalASTFormatException(
          "illegal ast tree in grant author command, please check you SQL statement");
    }
    initializedOperator = authorOperator;
  }

  private void analyzeAuthorUpdate(AstNode astNode) throws IllegalASTFormatException {
    int childCount = astNode.getChildCount();
    AuthorOperator authorOperator;
    if (childCount == 1) {
      authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_UPDATE_USER,
          AuthorOperator.AuthorType.UPDATE_USER);
      AstNode user = astNode.getChild(0);
      if (user.getChildCount() != 2) {
        throw new IllegalASTFormatException(
            "illegal ast tree in update password command, please check you SQL statement");
      }
      authorOperator.setUserName(user.getChild(0).getText());
      authorOperator.setNewPassword(user.getChild(1).getText());
    } else {
      throw new IllegalASTFormatException(
          "illegal ast tree in update password command, please check you SQL statement");
    }
    initializedOperator = authorOperator;
  }

  private void analyzeAuthorDrop(AstNode astNode) throws IllegalASTFormatException {
    int childCount = astNode.getChildCount();
    AuthorOperator authorOperator;
    if (childCount == 1) {
      // drop user or role
      switch (astNode.getChild(0).getType()) {
        case TSParser.TOK_USER:
          authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP,
              AuthorOperator.AuthorType.DROP_USER);
          authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
          break;
        case TSParser.TOK_ROLE:
          authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP,
              AuthorOperator.AuthorType.DROP_ROLE);
          authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
          break;
        default:
          throw new IllegalASTFormatException(
              "illegal ast tree in grant author command, please check you SQL statement");
      }
    } else {
      throw new IllegalASTFormatException(
          "illegal ast tree in grant author command, please check you SQL statement");
    }
    initializedOperator = authorOperator;
  }

  private void analyzeAuthorGrant(AstNode astNode) throws IllegalASTFormatException {
    int childCount = astNode.getChildCount();
    AuthorOperator authorOperator;
    if (childCount == 2) {
      // grant role to user
      authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
          AuthorOperator.AuthorType.GRANT_ROLE_TO_USER);
      authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
      authorOperator.setUserName(astNode.getChild(1).getChild(0).getText());
    } else if (childCount == 3) {
      AstNode privilegesNode = astNode.getChild(1);
      String[] privileges = new String[privilegesNode.getChildCount()];
      for (int i = 0; i < privileges.length; i++) {
        privileges[i] = parseStringWithQuoto(privilegesNode.getChild(i).getText());
      }
      Path nodePath = parsePath(astNode.getChild(2));
      if (astNode.getChild(0).getType() == TSParser.TOK_USER) {
        // grant user
        authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
            AuthorOperator.AuthorType.GRANT_USER);
        authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
        authorOperator.setPrivilegeList(privileges);
        authorOperator.setNodeNameList(nodePath);
      } else if (astNode.getChild(0).getType() == TSParser.TOK_ROLE) {
        // grant role
        authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
            AuthorOperator.AuthorType.GRANT_ROLE);
        authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
        authorOperator.setPrivilegeList(privileges);
        authorOperator.setNodeNameList(nodePath);
      } else {
        throw new IllegalASTFormatException(
            "illegal ast tree in grant author command, please check you SQL statement");
      }
    } else {
      throw new IllegalASTFormatException(
          "illegal ast tree in grant author command, please check you SQL statement");
    }
    initializedOperator = authorOperator;
  }

  private void analyzeAuthorRevoke(AstNode astNode) throws IllegalASTFormatException {
    int childCount = astNode.getChildCount();
    AuthorOperator authorOperator;
    if (childCount == 2) {
      // revoke role to user
      authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE,
          AuthorOperator.AuthorType.REVOKE_ROLE_FROM_USER);
      authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
      authorOperator.setUserName(astNode.getChild(1).getChild(0).getText());
    } else if (childCount == 3) {
      AstNode privilegesNode = astNode.getChild(1);
      String[] privileges = new String[privilegesNode.getChildCount()];
      for (int i = 0; i < privileges.length; i++) {
        privileges[i] = parseStringWithQuoto(privilegesNode.getChild(i).getText());
      }
      Path nodePath = parsePath(astNode.getChild(2));
      if (astNode.getChild(0).getType() == TSParser.TOK_USER) {
        // revoke user
        authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE,
            AuthorOperator.AuthorType.REVOKE_USER);
        authorOperator.setUserName(astNode.getChild(0).getChild(0).getText());
        authorOperator.setPrivilegeList(privileges);
        authorOperator.setNodeNameList(nodePath);
      } else if (astNode.getChild(0).getType() == TSParser.TOK_ROLE) {
        // revoke role
        authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_REVOKE,
            AuthorOperator.AuthorType.REVOKE_ROLE);
        authorOperator.setRoleName(astNode.getChild(0).getChild(0).getText());
        authorOperator.setPrivilegeList(privileges);
        authorOperator.setNodeNameList(nodePath);
      } else {
        throw new IllegalASTFormatException(
            "illegal ast tree in grant author command, please check you SQL statement");
      }
    } else {
      throw new IllegalASTFormatException(
          "illegal ast tree in grant author command, please check you SQL statement");
    }
    initializedOperator = authorOperator;
  }

  private void checkMetadataArgs(String dataType, String encoding)
      throws MetadataArgsErrorException {
    final String rle = "RLE";
    final String plain = "PLAIN";
    final String ts2Diff = "TS_2DIFF";
    final String bitmap = "BITMAP";
    final String gorilla = "GORILLA";
    TSDataType tsDataType;
    if (dataType == null) {
      throw new MetadataArgsErrorException("data type cannot be null");
    }

    try {
      tsDataType = TSDataType.valueOf(dataType);
    } catch (Exception e) {
      throw new MetadataArgsErrorException(String.format("data type %s not support", dataType));
    }

    if (encoding == null) {
      throw new MetadataArgsErrorException("encoding type cannot be null");
    }

    if (!encoding.equals(rle) && !encoding.equals(plain) && !encoding.equals(ts2Diff) && !encoding
        .equals(bitmap)
        && !encoding.equals(gorilla)) {
      throw new MetadataArgsErrorException(String.format("encoding %s is not support", encoding));
    }
    switch (tsDataType) {
      case BOOLEAN:
        if (!encoding.equals(plain) && !encoding.equals(rle)) {
          throw new MetadataArgsErrorException(
              String.format("encoding %s does not support %s", encoding, dataType));
        }
        break;
      case INT32:
        if ((!encoding.equals(plain) && !encoding.equals(rle) && !encoding.equals(ts2Diff))) {
          throw new MetadataArgsErrorException(
              String.format("encoding %s does not support %s", encoding, dataType));
        }
        break;
      case INT64:
        if ((!encoding.equals(plain) && !encoding.equals(rle) && !encoding.equals(ts2Diff))) {
          throw new MetadataArgsErrorException(
              String.format("encoding %s does not support %s", encoding, dataType));
        }
        break;
      case FLOAT:
        if ((!encoding.equals(plain) && !encoding.equals(rle) && !encoding.equals(ts2Diff)
            && !encoding.equals(gorilla))) {
          throw new MetadataArgsErrorException(
              String.format("encoding %s does not support %s", encoding, dataType));
        }
        break;
      case DOUBLE:
        if ((!encoding.equals(plain) && !encoding.equals(rle) && !encoding.equals(ts2Diff)
            && !encoding.equals(gorilla))) {
          throw new MetadataArgsErrorException(
              String.format("encoding %s does not support %s", encoding, dataType));
        }
        break;
      case TEXT:
        if (!encoding.equals(plain)) {
          throw new MetadataArgsErrorException(
              String.format("encoding %s does not support %s", encoding, dataType));
        }
        break;
      default:
        throw new MetadataArgsErrorException(
            String.format("data type %s is not supported", dataType));
    }
  }

}
