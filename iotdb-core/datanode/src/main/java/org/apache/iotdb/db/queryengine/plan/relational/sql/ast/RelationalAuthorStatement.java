package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class RelationalAuthorStatement extends Statement {

  private final AuthorRType authorType;

  private String tableName;
  private String database;
  private String userName;
  private String roleName;

  private String password;

  private PrivilegeType privilegeType;

  private boolean grantOption;

  public RelationalAuthorStatement(
      AuthorRType authorType,
      String database,
      String table,
      PrivilegeType type,
      String username,
      String rolename,
      boolean grantOption) {
    super(null);
    this.authorType = authorType;
    this.database = database;
    this.tableName = table;
    this.privilegeType = type;
    this.roleName = rolename;
    this.userName = username;
    this.grantOption = grantOption;
  }

  public RelationalAuthorStatement(AuthorRType statementType) {
    super(null);
    this.authorType = statementType;
  }

  public RelationalAuthorStatement(
      AuthorRType statementType,
      PrivilegeType type,
      String username,
      String roleName,
      boolean grantOption) {
    super(null);
    this.authorType = statementType;
    this.privilegeType = type;
    this.userName = username;
    this.roleName = roleName;
    this.grantOption = grantOption;
    this.tableName = null;
    this.database = null;
  }

  public AuthorRType getAuthorType() {
    return authorType;
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabase() {
    return this.database;
  }

  public String getUserName() {
    return userName;
  }

  public String getRoleName() {
    return roleName;
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

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RelationalAuthorStatement that = (RelationalAuthorStatement) o;
    return grantOption == that.grantOption
        && authorType == that.authorType
        && Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(privilegeType, that.privilegeType)
        && Objects.equals(password, that.password);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRelationalAuthorPlan(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        authorType, database, tableName, privilegeType, userName, roleName, grantOption, password);
  }

  public QueryType getQueryType() {
    switch (this.authorType) {
      case CREATE_ROLE:
      case CREATE_USER:
      case DROP_ROLE:
      case DROP_USER:
      case UPDATE_USER:
      case GRANT_ROLE_ANY:
      case GRANT_USER_ANY:
      case GRANT_ROLE_DB:
      case GRANT_USER_DB:
      case GRANT_ROLE_TB:
      case GRANT_USER_TB:
      case GRANT_USER_ROLE:
      case GRANT_USER_SYS:
      case GRANT_ROLE_SYS:
      case REVOKE_ROLE_DB:
      case REVOKE_USER_DB:
      case REVOKE_ROLE_TB:
      case REVOKE_USER_TB:
      case REVOKE_ROLE_ANY:
      case REVOKE_USER_ANY:
      case REVOKE_ROLE_SYS:
      case REVOKE_USER_SYS:
      case REVOKE_USER_ROLE:
        return QueryType.WRITE;
      case LIST_ROLE:
      case LIST_USER:
        return QueryType.READ;
      default:
        throw new IllegalArgumentException("Unknow authorType:" + this.authorType);
    }
  }

  @Override
  public String toString() {
    return "auth statement: "
        + authorType
        + "to Database: "
        + database
        + "."
        + tableName
        + ", user name: "
        + userName
        + ", role name: "
        + roleName
        + ", privileges:"
        + privilegeType
        + ", grantoption:"
        + grantOption;
  }
}
