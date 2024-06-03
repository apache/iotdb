package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class AuthDDLStatement extends Statement {

  private final AuthDDLType statementType;

  private final AuthObjectType objectType;
  private final String objectName;
  private final boolean toUser;
  private final String username;

  private final List<PrivilegeType> privilegeTypeList;

  private final boolean grantOption;

  public AuthDDLStatement(
      AuthDDLType statementType,
      AuthObjectType objectType,
      String objectName,
      List<PrivilegeType> typeList,
      boolean toUser,
      String username,
      boolean grantOption) {
    super(null);
    this.statementType = statementType;
    this.objectType = objectType;
    this.objectName = objectName;
    this.privilegeTypeList = typeList;
    this.toUser = toUser;
    this.username = username;
    this.grantOption = grantOption;
  }

  public AuthDDLStatement(
      AuthDDLType statementType,
      List<PrivilegeType> typeList,
      boolean toUser,
      String username,
      boolean grantOption) {
    super(null);
    this.statementType = statementType;
    this.privilegeTypeList = typeList;
    this.toUser = toUser;
    this.username = username;
    this.grantOption = grantOption;
    this.objectName = null;
    this.objectType = AuthObjectType.INVALIDATE;
  }

  public AuthDDLType getStatementType() {
    return statementType;
  }

  public AuthObjectType getObjectType() {
    return objectType;
  }

  public String getObjectName() {
    return objectName;
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

  public List<PrivilegeType> getPrivilegeType() {
    return privilegeTypeList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AuthDDLStatement that = (AuthDDLStatement) o;
    return toUser == that.toUser
        && grantOption == that.grantOption
        && statementType == that.statementType
        && objectType == that.objectType
        && Objects.equals(objectName, that.objectName)
        && Objects.equals(username, that.username);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        statementType, objectType, objectName, privilegeTypeList, toUser, username, grantOption);
  }

  @Override
  public String toString() {
    return "auth statement: "
        + statementType
        + "to "
        + objectType
        + " "
        + objectName
        + (toUser ? "user" : "role")
        + "name:"
        + username
        + "privileges:"
        + privilegeTypeList
        + "grantoption:"
        + grantOption;
  }
}
