package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class AuthTableStatement extends Statement implements IConfigStatement {

  private final AuthDDLType statementType;

  private final String tableName;
  private String database;
  private final boolean toUser;
  private final String username;

  private final PrivilegeType privilegeType;

  private final boolean grantOption;

  public AuthTableStatement(
      AuthDDLType statementType,
      String database,
      String table,
      PrivilegeType type,
      boolean toUser,
      String username,
      boolean grantOption) {
    super(null);
    this.statementType = statementType;
    this.database = database;
    this.tableName = table;
    this.privilegeType = type;
    this.toUser = toUser;
    this.username = username;
    this.grantOption = grantOption;
  }

  public AuthTableStatement(
      AuthDDLType statementType,
      PrivilegeType type,
      boolean toUser,
      String username,
      boolean grantOption) {
    super(null);
    this.statementType = statementType;
    this.privilegeType = type;
    this.toUser = toUser;
    this.username = username;
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
    return toUser;
  }

  public String getUsername() {
    return username;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AuthTableStatement that = (AuthTableStatement) o;
    return toUser == that.toUser
        && grantOption == that.grantOption
        && statementType == that.statementType
        && Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(username, that.username);
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
        statementType, database, tableName, privilegeType, toUser, username, grantOption);
  }

  @Override
  public QueryType getQueryType() {
    switch (this.statementType) {
      case GRANT:
      case REVOKE:
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
        + (toUser ? "user" : "role")
        + "name:"
        + username
        + "privileges:"
        + privilegeType
        + "grantoption:"
        + grantOption;
  }
}
