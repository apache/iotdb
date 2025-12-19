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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.schema.table.Audit;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.utils.DataNodeAuthUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RelationalAuthorStatement extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RelationalAuthorStatement.class);

  private final AuthorRType authorType;

  private String tableName;
  private String database;
  private String userName;
  private String roleName;

  private String password;
  private String oldPassword;

  private Set<PrivilegeType> privilegeType;

  private boolean grantOption;
  private long executedByUserId;
  private String newUsername = "";
  private String loginAddr;

  // the id of userName
  private long associatedUserId = -1;

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

  public long getExecutedByUserId() {
    return executedByUserId;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setUserName(String userName) {
    this.userName = userName;
    if (authorType != AuthorRType.CREATE_USER) {
      this.associatedUserId = AuthorityChecker.getUserId(userName).orElse(-1L);
    }
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setExecutedByUserId(long executedByUserId) {
    this.executedByUserId = executedByUserId;
  }

  public String getNewUsername() {
    return newUsername;
  }

  public void setNewUsername(String newUsername) {
    this.newUsername = newUsername;
  }

  public String getLoginAddr() {
    return loginAddr;
  }

  public void setLoginAddr(String loginAddr) {
    this.loginAddr = loginAddr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RelationalAuthorStatement that = (RelationalAuthorStatement) o;
    return grantOption == that.grantOption
        && authorType == that.authorType
        && Objects.equals(loginAddr, that.loginAddr)
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
      case RENAME_USER:
      case ACCOUNT_UNLOCK:
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

  /**
   * Post-process when the statement is successfully executed.
   *
   * @return null if the post-process succeeds, a status otherwise.
   */
  public TSStatus onSuccess() {
    if (authorType == AuthorRType.CREATE_USER) {
      return onCreateUserSuccess();
    } else if (authorType == AuthorRType.UPDATE_USER) {
      return onUpdateUserSuccess();
    } else if (authorType == AuthorRType.DROP_USER) {
      return onDropUserSuccess();
    }
    return null;
  }

  private TSStatus onCreateUserSuccess() {
    associatedUserId = AuthorityChecker.getUserId(userName).orElse(-1L);
    // the old password is expected to be encrypted during updates, so we also encrypt it here to
    // keep consistency
    TSStatus tsStatus =
        DataNodeAuthUtils.recordPasswordHistory(
            associatedUserId,
            password,
            AuthUtils.encryptPassword(password),
            CommonDateTimeUtils.currentTime());
    try {
      RpcUtils.verifySuccess(tsStatus);
    } catch (StatementExecutionException e) {
      return new TSStatus(e.getStatusCode()).setMessage(e.getMessage());
    }
    return null;
  }

  private TSStatus onUpdateUserSuccess() {
    TSStatus tsStatus =
        DataNodeAuthUtils.recordPasswordHistory(
            associatedUserId, password, oldPassword, CommonDateTimeUtils.currentTime());
    try {
      RpcUtils.verifySuccess(tsStatus);
    } catch (StatementExecutionException e) {
      return new TSStatus(e.getStatusCode()).setMessage(e.getMessage());
    }
    return null;
  }

  private TSStatus onDropUserSuccess() {
    TSStatus tsStatus = DataNodeAuthUtils.deletePasswordHistory(associatedUserId);
    try {
      RpcUtils.verifySuccess(tsStatus);
    } catch (StatementExecutionException e) {
      return new TSStatus(e.getStatusCode()).setMessage(e.getMessage());
    }
    return null;
  }

  public String getOldPassword() {
    return oldPassword;
  }

  public void setOldPassword(String oldPassword) {
    this.oldPassword = oldPassword;
  }

  public TSStatus checkStatementIsValid(String currentUser) {
    switch (authorType) {
      case ACCOUNT_UNLOCK:
        break;
      case CREATE_USER:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot create user has same name with admin user");
        }
        break;
      case CREATE_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(roleName)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot create role has same name with admin user");
        }
        break;
      case DROP_USER:
        if (AuthorityChecker.SUPER_USER.equals(userName) || userName.equals(currentUser)) {
          return AuthorityChecker.getTSStatus(false, "Cannot drop admin user or yourself");
        }
        break;
      case DROP_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(false, "Cannot drop role with admin name");
        }
        break;
      case GRANT_ROLE_ANY:
      case GRANT_USER_ANY:
      case REVOKE_ROLE_ANY:
      case REVOKE_USER_ANY:
      case GRANT_USER_SYS:
      case GRANT_ROLE_SYS:
      case REVOKE_USER_SYS:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant/revoke privileges of admin user");
        }
        break;
      case GRANT_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(false, "Cannot grant role to admin");
        }
        break;
      case REVOKE_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(false, "Cannot revoke role from admin");
        }
        break;
      case GRANT_ROLE_ALL:
      case REVOKE_ROLE_ALL:
      case GRANT_USER_ALL:
      case REVOKE_USER_ALL:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant/revoke all privileges of admin user");
        }
        break;
      case GRANT_USER_DB:
      case GRANT_ROLE_DB:
      case REVOKE_USER_DB:
      case REVOKE_ROLE_DB:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant/revoke privileges of admin user");
        }
        if (InformationSchema.INFORMATION_DATABASE.equals(database)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant or revoke any privileges to information_schema");
        }
        if (Audit.TABLE_MODEL_AUDIT_DATABASE.equals(database)) {
          return AuthorityChecker.getTSStatus(
              false,
              "Cannot grant or revoke any privileges to " + Audit.TABLE_MODEL_AUDIT_DATABASE);
        }
        break;
      case GRANT_USER_TB:
      case GRANT_ROLE_TB:
      case REVOKE_USER_TB:
      case REVOKE_ROLE_TB:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant/revoke privileges of admin user");
        }
        if (InformationSchema.INFORMATION_DATABASE.equals(database)) {
          return AuthorityChecker.getTSStatus(
              false, "Cannot grant or revoke any privileges to information_schema");
        }
        if (Audit.TABLE_MODEL_AUDIT_DATABASE.equals(database)) {
          return AuthorityChecker.getTSStatus(
              false,
              "Cannot grant or revoke any privileges to " + Audit.TABLE_MODEL_AUDIT_DATABASE);
        }
        break;
      default:
        break;
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  public long getAssociatedUserId() {
    return associatedUserId;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(tableName);
    size += RamUsageEstimator.sizeOf(database);
    size += RamUsageEstimator.sizeOf(userName);
    size += RamUsageEstimator.sizeOf(roleName);
    size += RamUsageEstimator.sizeOf(password);
    size += RamUsageEstimator.sizeOf(oldPassword);
    size += RamUsageEstimator.sizeOf(newUsername);
    size += RamUsageEstimator.sizeOf(loginAddr);
    size += RamUsageEstimator.sizeOfCollection(privilegeType);
    return size;
  }
}
