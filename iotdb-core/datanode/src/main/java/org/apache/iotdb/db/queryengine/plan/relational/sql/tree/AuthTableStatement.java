package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class AuthTableStatement extends Statement implements IConfigStatement {

  private final AuthDDLType statementType;

  private String tableName;
  private String database;
  private String username;
  private String rolename;

  private String password;

  private PrivilegeType privilegeType;

  private boolean grantOption;

  public AuthTableStatement(
      AuthDDLType statementType,
      String database,
      String table,
      PrivilegeType type,
      String username,
      String rolename,
      boolean grantOption) {
    super(null);
    this.statementType = statementType;
    this.database = database;
    this.tableName = table;
    this.privilegeType = type;
    this.rolename = rolename;
    this.username = username;
    this.grantOption = grantOption;
  }

  public AuthTableStatement(AuthDDLType statementType) {
    super(null);
    this.statementType = statementType;
  }

  public AuthTableStatement(
      AuthDDLType statementType,
      PrivilegeType type,
      String username,
      String rolename,
      boolean grantOption) {
    super(null);
    this.statementType = statementType;
    this.privilegeType = type;
    this.username = username;
    this.rolename = rolename;
    this.grantOption = grantOption;
    this.tableName = null;
    this.database = null;
  }

  public AuthDDLType getStatementType() {
    return statementType;
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabase() {
    return this.database;
  }

  public boolean isToUser() {
    return username != null;
  }

  public String getUsername() {
    return username;
  }

  public String getRolename() {
    return rolename;
  }

  public String getPassword() {
    return this.password;
  }

  public boolean hasGrantOption() {
    return grantOption;
  }

  public PrivilegeType getPrivilegeType() {
    return privilegeType;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setUserName(String name) {
    this.username = username;
  }

  public void setRoleName(String name) {
    this.username = name;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AuthTableStatement that = (AuthTableStatement) o;
    return grantOption == that.grantOption
        && statementType == that.statementType
        && Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(username, that.username)
        && Objects.equals(rolename, that.rolename);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableAuthorPlan(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        statementType, database, tableName, privilegeType, username, rolename, grantOption);
  }

  @Override
  public QueryType getQueryType() {
    switch (this.statementType) {
      case GRANT_USER:
      case GRANT_ROLE:
      case CREATE_ROLE:
      case CREATE_USER:
      case REVOKE_ROLE:
      case REVOKE_USER:
      case REVOKE_USER_ROLE:
        return QueryType.WRITE;
      case LIST_ROLE:
      case LIST_USER:
        return QueryType.READ;
      default:
        throw new IllegalArgumentException("Unknow authorType:" + this.statementType);
    }
  }

  @Override
  public String toString() {
    return "auth statement: "
        + statementType
        + "to Database"
        + database
        + " "
        + tableName
        + "user name:"
        + username
        + "role name"
        + rolename
        + "privileges:"
        + privilegeType
        + "grantoption:"
        + grantOption;
  }
}
