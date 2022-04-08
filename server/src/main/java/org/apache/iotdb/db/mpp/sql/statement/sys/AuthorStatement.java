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
package org.apache.iotdb.db.mpp.sql.statement.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;

public class AuthorStatement extends Statement {

  private final AuthorOperator.AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private String[] privilegeList;
  private PartialPath nodeName;

  /**
   * AuthorOperator Constructor with AuthorType.
   *
   * @param type author type
   */
  public AuthorStatement(AuthorOperator.AuthorType type) {
    super();
    authorType = type;
    statementType = StatementType.AUTHOR;
  }

  /**
   * AuthorOperator Constructor with OperatorType.
   *
   * @param type statement type
   */
  public AuthorStatement(StatementType type) {
    super();
    authorType = null;
    statementType = type;
  }

  public AuthorOperator.AuthorType getAuthorType() {
    return authorType;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public String getPassWord() {
    return password;
  }

  public void setPassWord(String password) {
    this.password = password;
  }

  public String getNewPassword() {
    return newPassword;
  }

  public void setNewPassword(String newPassword) {
    this.newPassword = newPassword;
  }

  public String[] getPrivilegeList() {
    return privilegeList;
  }

  public void setPrivilegeList(String[] authorizationList) {
    this.privilegeList = authorizationList;
  }

  public PartialPath getNodeName() {
    return nodeName;
  }

  public void setNodeNameList(PartialPath nodePath) {
    this.nodeName = nodePath;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    switch (this.authorType) {
      case CREATE_USER:
        return visitor.visitCreateUser(this, context);
      case CREATE_ROLE:
        return visitor.visitCreateRole(this, context);
      case DROP_USER:
        return visitor.visitDropUser(this, context);
      case DROP_ROLE:
        return visitor.visitDropRole(this, context);
      case GRANT_ROLE:
        return visitor.visitGrantRole(this, context);
      case GRANT_USER:
        return visitor.visitGrantUser(this, context);
      case GRANT_ROLE_TO_USER:
        return visitor.visitGrantRoleToUser(this, context);
      case REVOKE_USER:
        return visitor.visitRevokeUser(this, context);
      case REVOKE_ROLE:
        return visitor.visitRevokeRole(this, context);
      case REVOKE_ROLE_FROM_USER:
        return visitor.visitRevokeRoleFromUser(this, context);
      case UPDATE_USER:
        return visitor.visitAlterUser(this, context);
      case LIST_USER:
        return visitor.visitListUser(this, context);
      case LIST_ROLE:
        return visitor.visitListRole(this, context);
      case LIST_USER_PRIVILEGE:
        return visitor.visitListUserPrivileges(this, context);
      case LIST_ROLE_PRIVILEGE:
        return visitor.visitListRolePrivileges(this, context);
      case LIST_USER_ROLES:
        return visitor.visitListAllUserOfRole(this, context);
      case LIST_ROLE_USERS:
        return visitor.visitListAllRoleOfUser(this, context);
      default:
        return null;
    }
  }
}
