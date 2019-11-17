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
package org.apache.iotdb.db.qp.strategy;

import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_AND;
import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_OR;
import static org.apache.iotdb.db.qp.constant.SQLConstant.LESSTHAN;
import static org.apache.iotdb.db.qp.constant.SQLConstant.LESSTHANOREQUALTO;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.qp.logical.crud.UpdateOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.logical.sys.CreateTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.DataAuthOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.SetTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTTLOperator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.LinearFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.db.sql.parse.SqlBaseBaseListener;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.AddLabelContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.AlterUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.AndExpressionContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.AttributeClausesContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ConstantContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.CreatePropertyContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.CreateRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.CreateTimeseriesContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.CreateUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DateExpressionContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DeleteLabelContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DeleteStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DeleteStorageGroupContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DeleteTimeseriesContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DropRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.DropUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.FillClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.FromClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.FunctionCallContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.FunctionElementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.GrantRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.GrantRoleToUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.GrantUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.GrantWatermarkEmbeddingContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.GroupByClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.GroupByDeviceClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.InsertColumnSpecContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.InsertStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.InsertValuesSpecContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.LimitClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.LinkPathContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListAllRoleOfUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListAllUserOfRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListPrivilegesRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListPrivilegesUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListRolePrivilegesContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ListUserPrivilegesContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.LoadConfigurationStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.LoadStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.NodeNameContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.NodeNameWithoutStarContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.OffsetClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.OrExpressionContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.PredicateContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.PrefixPathContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.PrivilegesContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.PropertyContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.RevokeRoleContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.RevokeRoleFromUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.RevokeUserContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.RevokeWatermarkEmbeddingContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.RootOrIdContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SelectElementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SelectStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SetColContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SetStorageGroupContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SetTTLStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ShowAllTTLStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.ShowTTLStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SlimitClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SoffsetClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.SuffixPathContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.TimeIntervalContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.TimeseriesPathContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.TypeClauseContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.UnlinkPathContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.UnsetTTLStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.UpdateStatementContext;
import org.apache.iotdb.db.sql.parse.SqlBaseParser.WhereClauseContext;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * This class is a listener and you can get an operator which is a logical plan.
 */
public class LogicalGenerator extends SqlBaseBaseListener {

  private RootOperator initializedOperator = null;
  private ZoneId zoneId;
  private int operatorNumber;
  private CreateTimeSeriesOperator createTimeSeriesOperator;
  private InsertOperator insertOp;
  private SelectOperator selectOp;
  private UpdateOperator updateOp;
  private QueryOperator queryOp;
  private boolean isAndWhereClause = false;
  private boolean isOrWhereClause = false;
  private boolean isNotWhereClause = false;
  private DeleteDataOperator deleteDataOp;

  LogicalGenerator(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  RootOperator getLogicalPlan() {
    return initializedOperator;
  }

  @Override
  public void enterLoadConfigurationStatement(LoadConfigurationStatementContext ctx) {
    super.enterLoadConfigurationStatement(ctx);
    initializedOperator = new LoadConfigurationOperator();
  }

  @Override
  public void enterCreateTimeseries(CreateTimeseriesContext ctx) {
    super.enterCreateTimeseries(ctx);
    createTimeSeriesOperator = new CreateTimeSeriesOperator(SQLConstant.TOK_METADATA_CREATE);
    operatorNumber = SQLConstant.TOK_METADATA_CREATE;
    createTimeSeriesOperator.setPath(parseTimeseriesPath(ctx.timeseriesPath()));
  }

  @Override
  public void enterCreateProperty(CreatePropertyContext ctx) {
    super.enterCreateProperty(ctx);
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_CREATE,
        PropertyOperator.PropertyType.ADD_TREE);
    propertyOperator.setPropertyPath(new Path(ctx.ID().getText()));
    initializedOperator = propertyOperator;
    operatorNumber = SQLConstant.TOK_PROPERTY_CREATE;
  }

  @Override
  public void enterAddLabel(AddLabelContext ctx) {
    super.enterAddLabel(ctx);
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_ADD_LABEL,
        PropertyOperator.PropertyType.ADD_PROPERTY_LABEL);
    propertyOperator.setPropertyPath(new Path(new String[]{ctx.ID(1).getText(), ctx.ID(0).getText()}));
    initializedOperator = propertyOperator;
    operatorNumber = SQLConstant.TOK_PROPERTY_ADD_LABEL;
  }

  @Override
  public void enterDeleteLabel(DeleteLabelContext ctx) {
    super.enterDeleteLabel(ctx);
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_DELETE_LABEL,
        PropertyOperator.PropertyType.DELETE_PROPERTY_LABEL);
    propertyOperator.setPropertyPath(new Path(new String[]{ctx.ID(1).getText(), ctx.ID(0).getText()}));
    initializedOperator = propertyOperator;
    operatorNumber = SQLConstant.TOK_PROPERTY_DELETE_LABEL;
  }

  @Override
  public void enterLinkPath(LinkPathContext ctx) {
    super.enterLinkPath(ctx);
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_LINK,
        PropertyOperator.PropertyType.ADD_PROPERTY_TO_METADATA);
    Path metaPath = parsePrefixPath(ctx.prefixPath());
    propertyOperator.setMetadataPath(metaPath);
    propertyOperator.setPropertyPath(new Path(new String[]{ctx.propertyLabelPair().ID(0).getText()
        , ctx.propertyLabelPair().ID(1).getText()}));
    initializedOperator = propertyOperator;
    operatorNumber = SQLConstant.TOK_PROPERTY_LINK;
  }

  @Override
  public void enterUnlinkPath(UnlinkPathContext ctx) {
    super.enterUnlinkPath(ctx);
    PropertyOperator propertyOperator = new PropertyOperator(SQLConstant.TOK_PROPERTY_UNLINK,
        PropertyOperator.PropertyType.DEL_PROPERTY_FROM_METADATA);
    Path metaPath = parsePrefixPath(ctx.prefixPath());
    propertyOperator.setMetadataPath(metaPath);
    propertyOperator.setPropertyPath(new Path(new String[]{ctx.propertyLabelPair().ID(0).getText()
        , ctx.propertyLabelPair().ID(1).getText()}));
    initializedOperator = propertyOperator;
    operatorNumber = SQLConstant.TOK_PROPERTY_UNLINK;
  }

  @Override
  public void enterCreateUser(CreateUserContext ctx) {
    super.enterCreateUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE,
        AuthorOperator.AuthorType.CREATE_USER);
    authorOperator.setUserName(ctx.userName.getText());
    authorOperator.setPassWord(removeStringQuote(ctx.password.getText()));
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_CREATE;
  }

  @Override
  public void enterCreateRole(CreateRoleContext ctx) {
    super.enterCreateRole(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE,
        AuthorOperator.AuthorType.CREATE_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_CREATE;
  }

  @Override
  public void enterAlterUser(AlterUserContext ctx) {
    super.enterAlterUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_UPDATE_USER,
        AuthorOperator.AuthorType.UPDATE_USER);
    authorOperator.setUserName(ctx.userName.getText());
    authorOperator.setNewPassword(removeStringQuote(ctx.password.getText()));
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_UPDATE_USER;
  }

  @Override
  public void enterDropUser(DropUserContext ctx) {
    super.enterDropUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP,
        AuthorOperator.AuthorType.DROP_USER);
    authorOperator.setUserName(ctx.ID().getText());
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_DROP;
  }

  @Override
  public void enterDropRole(DropRoleContext ctx) {
    super.enterDropRole(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP,
        AuthorOperator.AuthorType.DROP_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_DROP;
  }

  @Override
  public void enterGrantUser(GrantUserContext ctx) {
    super.enterGrantUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
        AuthorOperator.AuthorType.GRANT_USER);
    authorOperator.setUserName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_GRANT;
  }

  @Override
  public void enterGrantRole(GrantRoleContext ctx) {
    super.enterGrantRole(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
        AuthorType.GRANT_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_GRANT;
  }

  @Override
  public void enterRevokeUser(RevokeUserContext ctx) {
    super.enterRevokeUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
        AuthorType.REVOKE_USER);
    authorOperator.setUserName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_GRANT;
  }

  @Override
  public void enterRevokeRole(RevokeRoleContext ctx) {
    super.enterRevokeRole(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
        AuthorType.REVOKE_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_GRANT;
  }

  @Override
  public void enterGrantRoleToUser(GrantRoleToUserContext ctx) {
    super.enterGrantRoleToUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
        AuthorOperator.AuthorType.GRANT_ROLE_TO_USER);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setUserName(ctx.userName.getText());
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_GRANT;
  }

  @Override
  public void enterRevokeRoleFromUser(RevokeRoleFromUserContext ctx) {
    super.enterRevokeRoleFromUser(ctx);
    AuthorOperator authorOperator = new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT,
        AuthorType.REVOKE_ROLE_FROM_USER);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setUserName(ctx.userName.getText());
    initializedOperator = authorOperator;
    operatorNumber = SQLConstant.TOK_AUTHOR_GRANT;
  }

  @Override
  public void enterLoadStatement(LoadStatementContext ctx) {
    super.enterLoadStatement(ctx);
    if (ctx.prefixPath().nodeName().size() < 3 ) {
      throw new SQLParserException("data load command: child count < 3\n");
    }

    String csvPath = ctx.STRING_LITERAL().getText();
    StringContainer sc = new StringContainer(TsFileConstant.PATH_SEPARATOR);
    List<NodeNameContext> nodeNames = ctx.prefixPath().nodeName();
    sc.addTail(ctx.prefixPath().ROOT().getText());
    for(NodeNameContext nodeName : nodeNames) {
      sc.addTail(nodeName.getText());
    }
    initializedOperator = new LoadDataOperator(SQLConstant.TOK_DATALOAD,
        removeStringQuote(csvPath),
        sc.toString());
    operatorNumber = SQLConstant.TOK_DATALOAD;
  }

  @Override
  public void enterGrantWatermarkEmbedding(GrantWatermarkEmbeddingContext ctx) {
    super.enterGrantWatermarkEmbedding(ctx);
    List<RootOrIdContext> rootOrIdList = ctx.rootOrId();
    List<String> users = new ArrayList<>();
    for(RootOrIdContext rootOrId : rootOrIdList) {
      users.add(rootOrId.getText());
    }
    initializedOperator = new DataAuthOperator(SQLConstant.TOK_GRANT_WATERMARK_EMBEDDING, users);
  }

  @Override
  public void enterRevokeWatermarkEmbedding(RevokeWatermarkEmbeddingContext ctx) {
    super.enterRevokeWatermarkEmbedding(ctx);
    List<RootOrIdContext> rootOrIdList = ctx.rootOrId();
    List<String> users = new ArrayList<>();
    for(RootOrIdContext rootOrId : rootOrIdList) {
      users.add(rootOrId.getText());
    }
    initializedOperator = new DataAuthOperator(SQLConstant.TOK_REVOKE_WATERMARK_EMBEDDING, users);
    operatorNumber = SQLConstant.TOK_REVOKE_WATERMARK_EMBEDDING;
  }

  @Override
  public void enterListUser(ListUserContext ctx) {
    super.enterListUser(ctx);
    initializedOperator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_USER);
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListRole(ListRoleContext ctx) {
    super.enterListRole(ctx);
    initializedOperator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_ROLE);
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListPrivilegesUser(ListPrivilegesUserContext ctx) {
    super.enterListPrivilegesUser(ctx);
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    operator.setUserName(ctx.ID().getText());
    operator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    initializedOperator = operator;
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListPrivilegesRole(ListPrivilegesRoleContext ctx) {
    super.enterListPrivilegesRole(ctx);
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
    operator.setRoleName((ctx.ID().getText()));
    operator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    initializedOperator = operator;
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListUserPrivileges(ListUserPrivilegesContext ctx) {
    super.enterListUserPrivileges(ctx);
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    operator.setUserName(ctx.ID().getText());
    initializedOperator = operator;
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListRolePrivileges(ListRolePrivilegesContext ctx) {
    super.enterListRolePrivileges(ctx);
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
    operator.setRoleName(ctx.ID().getText());
    initializedOperator = operator;
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListAllRoleOfUser(ListAllRoleOfUserContext ctx) {
    super.enterListAllRoleOfUser(ctx);
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_USER_ROLES);
    initializedOperator = operator;
    operator.setUserName(ctx.ID().getText());
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterListAllUserOfRole(ListAllUserOfRoleContext ctx) {
    super.enterListAllUserOfRole(ctx);
    AuthorOperator operator = new AuthorOperator(SQLConstant.TOK_LIST,
        AuthorOperator.AuthorType.LIST_ROLE_USERS);
    initializedOperator = operator;
    operator.setRoleName((ctx.ID().getText()));
    operatorNumber = SQLConstant.TOK_LIST;
  }

  @Override
  public void enterSetTTLStatement(SetTTLStatementContext ctx) {
    super.enterSetTTLStatement(ctx);
    SetTTLOperator operator = new SetTTLOperator(SQLConstant.TOK_SET);
    operator.setStorageGroup(parsePrefixPath(ctx.prefixPath()).getFullPath());
    operator.setDataTTL(Long.parseLong(ctx.INT().getText()));
    initializedOperator = operator;
    operatorNumber = SQLConstant.TOK_SET;
  }

  @Override
  public void enterUnsetTTLStatement(UnsetTTLStatementContext ctx) {
    super.enterUnsetTTLStatement(ctx);
    SetTTLOperator operator = new SetTTLOperator(SQLConstant.TOK_UNSET);
    operator.setStorageGroup(parsePrefixPath(ctx.prefixPath()).getFullPath());
    initializedOperator = operator;
    operatorNumber = SQLConstant.TOK_UNSET;
  }

  @Override
  public void enterShowTTLStatement(ShowTTLStatementContext ctx) {
    super.enterShowTTLStatement(ctx);
    List<String> storageGroups = new ArrayList<>();
    List<PrefixPathContext> prefixPathList = ctx.prefixPath();
    for(PrefixPathContext prefixPath : prefixPathList) {
      storageGroups.add(parsePrefixPath(prefixPath).getFullPath());
    }
    initializedOperator = new ShowTTLOperator(storageGroups);
  }

  @Override
  public void enterShowAllTTLStatement(ShowAllTTLStatementContext ctx) {
    super.enterShowAllTTLStatement(ctx);
    List<String> storageGroups = new ArrayList<>();
    initializedOperator = new ShowTTLOperator(storageGroups);
  }

  private String[] parsePrivilege(PrivilegesContext ctx) {
    List<TerminalNode> privilegeList  = ctx.STRING_LITERAL();
    List<String> privileges = new ArrayList<>();
    for(TerminalNode privilege : privilegeList) {
      privileges.add(removeStringQuote(privilege.getText()));
    }
    return privileges.toArray(new String[0]);
  }

  private String removeStringQuote(String src) {
    if(src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'') {
      return src.substring(1, src.length() - 1);
    } else if(src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
      return src.substring(1, src.length() - 1);
    } else {
      throw new SQLParserException("error format for string with quote:" + src);
    }
  }

  @Override
  public void enterDeleteTimeseries(DeleteTimeseriesContext ctx) {
    super.enterDeleteTimeseries(ctx);
    List<Path> deletePaths = new ArrayList<>();
    List<PrefixPathContext> prefixPaths = ctx.prefixPath();
    for(PrefixPathContext prefixPath : prefixPaths) {
      deletePaths.add(parsePrefixPath(prefixPath));
    }
    DeleteTimeSeriesOperator deleteTimeSeriesOperator = new DeleteTimeSeriesOperator(
        SQLConstant.TOK_METADATA_DELETE);
    deleteTimeSeriesOperator.setDeletePathList(deletePaths);
    initializedOperator = deleteTimeSeriesOperator;
    operatorNumber = SQLConstant.TOK_METADATA_DELETE;
  }

  @Override
  public void enterSetStorageGroup(SetStorageGroupContext ctx) {
    super.enterSetStorageGroup(ctx);
    SetStorageGroupOperator setStorageGroupOperator = new SetStorageGroupOperator(
        SQLConstant.TOK_METADATA_SET_FILE_LEVEL);
    Path path = parsePrefixPath(ctx.prefixPath());
    setStorageGroupOperator.setPath(path);
    initializedOperator = setStorageGroupOperator;
    operatorNumber = SQLConstant.TOK_METADATA_SET_FILE_LEVEL;
  }

  @Override
  public void enterDeleteStorageGroup(DeleteStorageGroupContext ctx) {
    super.enterDeleteStorageGroup(ctx);
    List<Path> deletePaths = new ArrayList<>();
    List<PrefixPathContext> prefixPaths = ctx.prefixPath();
    for(PrefixPathContext prefixPath : prefixPaths) {
      deletePaths.add(parsePrefixPath(prefixPath));
    }
    DeleteStorageGroupOperator deleteStorageGroupOperator = new DeleteStorageGroupOperator(
        SQLConstant.TOK_METADATA_DELETE_FILE_LEVEL);
    deleteStorageGroupOperator.setDeletePathList(deletePaths);
    initializedOperator = deleteStorageGroupOperator;
    operatorNumber = SQLConstant.TOK_METADATA_DELETE_FILE_LEVEL;
  }

  @Override
  public void enterDeleteStatement(DeleteStatementContext ctx) {
    super.enterDeleteStatement(ctx);
    operatorNumber = SQLConstant.TOK_DELETE;
    deleteDataOp = new DeleteDataOperator(SQLConstant.TOK_DELETE);
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    List<PrefixPathContext> prefixPaths = ctx.prefixPath();
    for(PrefixPathContext prefixPath : prefixPaths) {
      Path path = parsePrefixPath(prefixPath);
      selectOp.addSelectPath(path);
    }
    deleteDataOp.setSelectOperator(selectOp);
    initializedOperator = deleteDataOp;
  }

  @Override
  public void enterGroupByClause(GroupByClauseContext ctx) {
    super.enterGroupByClause(ctx);
    queryOp.setGroupBy(true);

    // parse timeUnit
    queryOp.setUnit(parseDuration(ctx.DURATION().getText()));

    // parse show intervals
    List<Pair<Long, Long>> intervals = new ArrayList<>();
    long startTime;
    long endTime;
    List<TimeIntervalContext> timeIntervals = ctx.timeInterval();
    for(TimeIntervalContext timeInterval : timeIntervals) {
      if(timeInterval.timeValue(0).INT() != null) {
        startTime = Long.parseLong(timeInterval.timeValue(0).INT().getText());
      } else {
        startTime = parseTimeFormat(timeInterval.timeValue(0).dateFormat().getText());
      }
      if(timeInterval.timeValue(1).INT() != null) {
        endTime = Long.parseLong(timeInterval.timeValue(1).INT().getText());
      } else {
        endTime = parseTimeFormat(timeInterval.timeValue(1).dateFormat().getText());
      }
      intervals.add(new Pair<>(startTime,endTime));
    }
    queryOp.setIntervals(intervals);

    //
    if(ctx.timeValue() != null) {
      if(ctx.timeValue().INT() != null) {
        queryOp.setOrigin(Long.parseLong(ctx.timeValue().INT().getText()));
      } else {
        queryOp.setOrigin(parseTimeFormat(ctx.timeValue().dateFormat().getText()));
      }
    } else {
      queryOp.setOrigin(parseTimeFormat(SQLConstant.START_TIME_STR));
    }
  }

  @Override
  public void enterFillClause(FillClauseContext ctx) {
    super.enterFillClause(ctx);
    FilterOperator filterOperator = queryOp.getFilterOperator();
    if (!filterOperator.isLeaf() || filterOperator.getTokenIntType() != SQLConstant.EQUAL) {
      throw new SQLParserException("Only \"=\" can be used in fill function");
    }
    List<TypeClauseContext> list = ctx.typeClause();
    Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
    for(TypeClauseContext typeClause : list) {
      parseTypeClause(typeClause, fillTypes);
    }
    queryOp.setFill(true);
    queryOp.setFillTypes(fillTypes);
  }

  private void parseTypeClause(TypeClauseContext ctx, Map<TSDataType, IFill> fillTypes) {
    TSDataType dataType = parseType(ctx.dataType().getText());
    if(ctx.linearClause() != null && dataType == TSDataType.TEXT) {
      throw new SQLParserException(String.format("type %s cannot use %s fill function"
          , dataType, ctx.linearClause().LINEAR().getText()));
    }

    if(ctx.linearClause() != null) {
      if(ctx.linearClause().DURATION(0) != null) {
        long beforeRange = parseDuration(ctx.linearClause().DURATION(0).getText());
        long afterRange = parseDuration(ctx.linearClause().DURATION(1).getText());
        fillTypes.put(dataType, new LinearFill(beforeRange, afterRange));
      } else {
        fillTypes.put(dataType, new LinearFill(-1, -1));
      }
    } else {
      if(ctx.previousClause().DURATION() != null) {
        long preRange = parseDuration(ctx.previousClause().DURATION().getText());
        fillTypes.put(dataType, new PreviousFill(preRange));
      } else {
        fillTypes.put(dataType, new PreviousFill(-1));
      }
    }
  }

  @Override
  public void enterGroupByDeviceClause(GroupByDeviceClauseContext ctx) {
    super.enterGroupByDeviceClause(ctx);
    queryOp.setGroupByDevice(true);
  }

  /**
   * parse datatype node.
   */
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
        throw new SQLParserException("not a valid fill type : " + type);
    }
  }

  @Override
  public void enterLimitClause(LimitClauseContext ctx) {
    super.enterLimitClause(ctx);
    int limit = Integer.parseInt(ctx.INT().getText());
    if(limit <= 0) {
      throw new SQLParserException("LIMIT <N>: N must be a positive integer and can not be zero.");
    }
  }

  @Override
  public void enterSlimitClause(SlimitClauseContext ctx) {
    super.enterSlimitClause(ctx);
    int slimit = Integer.parseInt(ctx.INT().getText());
    if(slimit <= 0) {
      throw new SQLParserException("SLIMIT <SN>: SN must be a positive integer and can not be zero.");
    }
    queryOp.setSeriesLimit(slimit);
  }

  @Override
  public void enterOffsetClause(OffsetClauseContext ctx) {
    super.enterOffsetClause(ctx);
  }

  @Override
  public void enterSoffsetClause(SoffsetClauseContext ctx) {
    super.enterSoffsetClause(ctx);
    queryOp.setSeriesOffset(Integer.parseInt(ctx.INT().getText()));
  }

  @Override
  public void enterInsertColumnSpec(InsertColumnSpecContext ctx) {
    super.enterInsertColumnSpec(ctx);
    List<NodeNameWithoutStarContext> nodeNamesWithoutStar = ctx.nodeNameWithoutStar();
    List<String> measurementList = new ArrayList<>();
    for(NodeNameWithoutStarContext nodeNameWithoutStar: nodeNamesWithoutStar) {
      String measurement = nodeNameWithoutStar.getText();
      if(measurement.contains("\"") || measurement.contains("\'")){
        measurement = measurement.substring(1, measurement.length()-1);
      }
      measurementList.add(measurement);
    }
    insertOp.setMeasurementList(measurementList.toArray(new String[0]));
  }

  @Override
  public void enterInsertValuesSpec(InsertValuesSpecContext ctx) {
    super.enterInsertValuesSpec(ctx);
    long timestamp;
    if(ctx.dateFormat() != null) {
      timestamp = parseTimeFormat(ctx.dateFormat().getText());
    } else {
      timestamp = Long.parseLong(ctx.INT().getText());
    }
    insertOp.setTime(timestamp);
    List<String> valueList = new ArrayList<>();
    List<ConstantContext> values = ctx.constant();
    for(ConstantContext value : values) {
      valueList.add(value.getText());
    }
    insertOp.setValueList(valueList.toArray(new String[0]));
    initializedOperator  = insertOp;
  }

  private Path parseTimeseriesPath(TimeseriesPathContext ctx) {
    List<NodeNameWithoutStarContext> nodeNamesWithoutStar = ctx.nodeNameWithoutStar();
    List<String> path = new ArrayList<>();
    if(ctx.ROOT() != null) {
      path.add(ctx.ROOT().getText());
    }
    for(NodeNameWithoutStarContext nodeNameWithoutStar: nodeNamesWithoutStar) {
      path.add(nodeNameWithoutStar.getText());
    }
    return new Path(new StringContainer(path.toArray(new String[0]), TsFileConstant.PATH_SEPARATOR));
  }

  @Override
  public void enterAttributeClauses(AttributeClausesContext ctx) {
    super.enterAttributeClauses(ctx);
    String dataType = ctx.dataType().getChild(0).getText().toUpperCase();
    String encoding = ctx.encoding().getChild(0).getText().toUpperCase();
    createTimeSeriesOperator.setDataType(TSDataType.valueOf(dataType));
    createTimeSeriesOperator.setEncoding(TSEncoding.valueOf(encoding));
    String compressor;
    List<PropertyContext> properties = ctx.property();
    Map<String, String> props = new HashMap<>(properties.size(), 1);
    if(ctx.propertyValue() != null) {
      compressor = ctx.propertyValue().getText().toUpperCase();
    } else {
      compressor = TSFileDescriptor.getInstance().getConfig().getCompressor().toUpperCase();
    }
    checkMetadataArgs(dataType, encoding, compressor);
    if(ctx.property(0) != null) {
      for (PropertyContext property : properties) {
        props.put(property.ID().getText().toLowerCase(), property.propertyValue().getText().toLowerCase());
      }
    }
    createTimeSeriesOperator.setCompressor(CompressionType.valueOf(compressor));
    createTimeSeriesOperator.setProps(props);
    initializedOperator = createTimeSeriesOperator;
  }

  @Override
  public void enterInsertStatement(InsertStatementContext ctx) {
    super.enterInsertStatement(ctx);
    insertOp = new InsertOperator(SQLConstant.TOK_INSERT);
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    operatorNumber = SQLConstant.TOK_INSERT;
    selectOp.addSelectPath(parseTimeseriesPath(ctx.timeseriesPath()));
    insertOp.setSelectOperator(selectOp);
  }

  @Override
  public void enterUpdateStatement(UpdateStatementContext ctx) {
    super.enterUpdateStatement(ctx);
    updateOp = new UpdateOperator(SQLConstant.TOK_UPDATE);
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    fromOp.addPrefixTablePath(parsePrefixPath(ctx.prefixPath()));
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    operatorNumber = SQLConstant.TOK_UPDATE;
    initializedOperator = updateOp;
  }

  @Override
  public void enterSelectStatement(SelectStatementContext ctx) {
    super.enterSelectStatement(ctx);
    operatorNumber = SQLConstant.TOK_QUERY;
    queryOp = new QueryOperator(SQLConstant.TOK_QUERY);
    initializedOperator = queryOp;
  }

  @Override
  public void enterFromClause(FromClauseContext ctx) {
    super.enterFromClause(ctx);
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    List<PrefixPathContext> prefixFromPaths = ctx.prefixPath();
    for(PrefixPathContext prefixFromPath : prefixFromPaths) {
      Path path = parsePrefixPath(prefixFromPath);
      fromOp.addPrefixTablePath(path);
    }
    queryOp.setFromOperator(fromOp);
  }

  @Override
  public void enterFunctionElement(FunctionElementContext ctx) {
    super.enterFunctionElement(ctx);
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    List<FunctionCallContext> functionCallContextList = ctx.functionCall();
    for(FunctionCallContext functionCallContext : functionCallContextList) {
      Path path = parseSuffixPath(functionCallContext.suffixPath());
      selectOp.addClusterPath(path, functionCallContext.ID().getText());
    }
    queryOp.setSelectOperator(selectOp);
  }

  @Override
  public void enterSelectElement(SelectElementContext ctx) {
    super.enterSelectElement(ctx);
    selectOp = new SelectOperator(SQLConstant.TOK_SELECT);
    List<SuffixPathContext> suffixPaths = ctx.suffixPath();
    for(SuffixPathContext suffixPath : suffixPaths) {
      Path path = parseSuffixPath(suffixPath);
      selectOp.addSelectPath(path);
    }
    queryOp.setSelectOperator(selectOp);
  }

  @Override
  public void enterSetCol(SetColContext ctx) {
    super.enterSetCol(ctx);
    selectOp.addSelectPath(parseSuffixPath(ctx.suffixPath()));
    updateOp.setSelectOperator(selectOp);
    updateOp.setValue(ctx.constant().getText());
  }


  private Path parsePrefixPath(PrefixPathContext ctx) {
    List<NodeNameContext> nodeNames = ctx.nodeName();
    List<String> path = new ArrayList<>();
    path.add(ctx.ROOT().getText());
    for(NodeNameContext nodeName : nodeNames) {
      path.add(nodeName.getText());
    }
    return new Path(new StringContainer(path.toArray(new String[0]), TsFileConstant.PATH_SEPARATOR));
  }

  /**
   * parse duration to time value.
   * @param durationStr represent duration string like: 12d8m9ns, 1y1mo, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  private Long parseDuration(String durationStr) {
    String timestampPrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();

    long total = 0;
    long tmp = 0;
    for (int i = 0; i < durationStr.length(); i++) {
      char ch = durationStr.charAt(i);
      if (Character.isDigit(ch)) {
        tmp *= 10;
        tmp += (ch - '0');
      } else {
        String unit = durationStr.charAt(i) + "";
        // This is to identify units with two letters.
        if (i + 1 < durationStr.length() && !Character.isDigit(durationStr.charAt(i + 1))) {
          i++;
          unit += durationStr.charAt(i);
        }
        total += DatetimeUtils
            .convertDurationStrToLong(tmp, unit.toLowerCase(), timestampPrecision);
        tmp = 0;
      }
    }
    if(total <= 0) {
      throw new SQLParserException("Interval must more than 0.");
    }
    return total;
  }

  @Override
  public void enterWhereClause(WhereClauseContext ctx) {
    super.enterWhereClause(ctx);
    FilterOperator whereOp = new FilterOperator(SQLConstant.TOK_WHERE);
    whereOp.addChildOperator(parseOrExpression(ctx.orExpression()));
    switch (operatorNumber) {
      case SQLConstant.TOK_DELETE :
        deleteDataOp.setFilterOperator(whereOp.getChildren().get(0));
        long deleteTime = parseDeleteTimeFilter(deleteDataOp);
        deleteDataOp.setTime(deleteTime);
        break;
      case SQLConstant.TOK_QUERY:
        queryOp.setFilterOperator(whereOp.getChildren().get(0));
        break;
      case SQLConstant.TOK_UPDATE:
        updateOp.setFilterOperator(whereOp.getChildren().get(0));
        break;
    }
  }


  private FilterOperator parseOrExpression(OrExpressionContext ctx) {
    if(ctx.andExpression().size() == 1) {
      isOrWhereClause = false;
      return parseAndExpression(ctx.andExpression(0));
    }
    isOrWhereClause = true;
    FilterOperator binaryOp = new FilterOperator(KW_OR);
    if(ctx.andExpression().size() > 2) {
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(0)));
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(1)));
      for (int i = 2; i < ctx.andExpression().size(); i++) {
        FilterOperator op = new FilterOperator(KW_OR);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parseAndExpression(ctx.andExpression(i)));
        binaryOp = op;
      }
    } else {
      for(AndExpressionContext andExpressionContext : ctx.andExpression())
        binaryOp.addChildOperator(parseAndExpression(andExpressionContext));
    }
    return binaryOp;
  }

  private FilterOperator parseAndExpression(AndExpressionContext ctx) {
    if (ctx.predicate().size() == 1) {
      isAndWhereClause = false;
      return parsePredicate(ctx.predicate(0));
    }
    isAndWhereClause = true;
    FilterOperator binaryOp = new FilterOperator(KW_AND);
    int size = ctx.predicate().size();
    if (size > 2) {
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(0)));
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(1)));
      for (int i = 2; i < size; i++) {
        FilterOperator op = new FilterOperator(KW_AND);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parsePredicate(ctx.predicate(i)));
        binaryOp = op;
      }
    } else {
      for (PredicateContext predicateContext : ctx.predicate()) {
        binaryOp.addChildOperator(parsePredicate(predicateContext));
      }
    }
    return binaryOp;
  }

  private FilterOperator parsePredicate(PredicateContext ctx) {
    if (ctx.OPERATOR_NOT() != null) {
      isNotWhereClause = true;
      FilterOperator notOp = new FilterOperator(SQLConstant.KW_NOT);
      notOp.addChildOperator(parseOrExpression(ctx.orExpression()));
      return notOp;
    } else if (ctx.LR_BRACKET() != null && ctx.OPERATOR_NOT() == null) {
      return parseOrExpression(ctx.orExpression());
    } else {
      Path path = null;
      BasicFunctionOperator basic = null;
      if (ctx.prefixPath() != null) {
        path = parsePrefixPath(ctx.prefixPath());
      }
      if (ctx.suffixPath() != null) {
        path = parseSuffixPath(ctx.suffixPath());
      }
      if (ctx.constant().dateExpression() != null) {
        if(!path.equals(SQLConstant.RESERVED_TIME)) {
          throw new SQLParserException(path.toString(), "Date can only be used to time");
        }
        basic = new BasicFunctionOperator(ctx.comparisonOperator().type.getType(), path, Long.toString(parseDateExpression(ctx.constant().dateExpression())));
      } else {
        basic = new BasicFunctionOperator(ctx.comparisonOperator().type.getType(), path, ctx.constant().getText());
      }
      if (!isNotWhereClause && !isAndWhereClause && !isOrWhereClause) {
        return basic;
      }
      return basic;
    }
  }

  private Path parseSuffixPath(SuffixPathContext ctx) {
    List<NodeNameContext> nodeNames = ctx.nodeName();
    List<String> path = new ArrayList<>();
    for(NodeNameContext nodeName : nodeNames) {
      path.add(nodeName.getText());
    }
    return new Path(new StringContainer(path.toArray(new String[0]), TsFileConstant.PATH_SEPARATOR));
  }


  void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  /**
   * parse time expression, which is addition and subtraction expression of duration time, now() or
   * DataTimeFormat time.
   * <p>
   * eg. now() + 1d - 2h
   * </p>
   */
  private Long parseDateExpression(DateExpressionContext ctx){
    long time;
    time = parseTimeFormat(ctx.getChild(0).getText());
    for(int i = 1; i < ctx.getChildCount(); i = i+2) {
      if(ctx.getChild(i).getText().equals("+")) {
        time+= parseDuration(ctx.getChild(i+1).getText());
      } else {
        time-= parseDuration(ctx.getChild(i+1).getText());
      }
    }
    return time;
  }

  /**
   * function for parsing time format.
   */
  long parseTimeFormat(String timestampStr) throws SQLParserException {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new SQLParserException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    try {
      return DatetimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SQLParserException(String
          .format("Input time format %s error. "
              + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
              + "refer to user document for more info.", timestampStr));
    }
  }

  /**
   * for delete command, time should only have an end time.
   *
   * @param operator delete logical plan
   */
  private long parseDeleteTimeFilter(DeleteDataOperator operator)  {
    FilterOperator filterOperator = operator.getFilterOperator();
    if (filterOperator.getTokenIntType() != LESSTHAN
        && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
      throw new SQLParserException(
          "For delete command, where clause must be like : time < XXX or time <= XXX");
    }
    long time = Long.parseLong(((BasicFunctionOperator) filterOperator).getValue());
    if (filterOperator.getTokenIntType() == LESSTHAN) {
      time = time - 1;
    }
    return time;
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
      throw new SQLParserException("data type cannot be null");
    }

    try {
      tsDataType = TSDataType.valueOf(dataType);
    } catch (Exception e) {
      throw new SQLParserException(String.format("data type %s not support", dataType));
    }

    if (encoding == null) {
      throw new SQLParserException("encoding type cannot be null");
    }

    try {
      tsEncoding = TSEncoding.valueOf(encoding);
    } catch (Exception e) {
      throw new SQLParserException(String.format("encoding %s is not support", encoding));
    }

    try {
      CompressionType.valueOf(compressor);
    } catch (Exception e) {
      throw new SQLParserException(String.format("compressor %s is not support", compressor));
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
      throw new SQLParserException(
          String.format("encoding %s does not support %s", tsEncoding, tsDataType));
    }
  }
}
