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
package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RelationalAuthorStatement extends Statement {

  private final AuthorRType authorType;

  private String tableName;
  private String database;
  private String userName;
  private String roleName;

  private String password;

  private Set<PrivilegeType> privilegeType;

  private boolean grantOption;

  public RelationalAuthorStatement(
      AuthorRType authorType,
      String userName,
      String roleName,
      String database,
      String table,
      Set<PrivilegeType> type,
      boolean grantOption,
      String password) {
    super(null);
    this.authorType = authorType;
    this.database = database;
    this.tableName = table;
    this.privilegeType = type;
    this.roleName = roleName;
    this.userName = userName;
    this.grantOption = grantOption;
    this.password = password;
  }

  public RelationalAuthorStatement(AuthorRType statementType) {
    super(null);
    this.authorType = statementType;
  }

  public RelationalAuthorStatement(
      AuthorRType statementType,
      Set<PrivilegeType> type,
      String userName,
      String roleName,
      boolean grantOption) {
    super(null);
    this.authorType = statementType;
    this.privilegeType = type;
    this.userName = userName;
    this.roleName = roleName;
    this.grantOption = grantOption;
    this.tableName = null;
    this.database = null;
    this.password = null;
  }

  public RelationalAuthorStatement(
      AuthorRType statementType, String userName, String roleName, boolean grantOption) {
    super(null);
    this.authorType = statementType;
    this.userName = userName;
    this.roleName = roleName;
    this.grantOption = grantOption;
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

  public boolean isGrantOption() {
    return grantOption;
  }

  public Set<PrivilegeType> getPrivilegeTypes() {
    return privilegeType;
  }

  public String getPrivilegesString() {
    return privilegeType.stream().map(Enum::name).collect(Collectors.joining(","));
  }

  public Set<Integer> getPrivilegeIds() {
    Set<Integer> privilegeIds = new HashSet<>();
    for (PrivilegeType privilegeType : privilegeType) {
      privilegeIds.add(privilegeType.ordinal());
    }
    return privilegeIds;
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
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
      case GRANT_ROLE_ALL:
      case GRANT_USER_ALL:
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
      case REVOKE_ROLE_ALL:
      case REVOKE_USER_ALL:
      case REVOKE_ROLE_SYS:
      case REVOKE_USER_SYS:
      case REVOKE_USER_ROLE:
        return QueryType.WRITE;
      case LIST_ROLE:
      case LIST_USER:
      case LIST_ROLE_PRIV:
      case LIST_USER_PRIV:
        return QueryType.READ;
      default:
        throw new IllegalArgumentException("Unknown authorType:" + this.authorType);
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
        + ", grantOption:"
        + grantOption;
  }
}
