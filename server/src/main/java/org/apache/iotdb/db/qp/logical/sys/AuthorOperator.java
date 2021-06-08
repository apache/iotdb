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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

/**
 * this class maintains information in Author statement, including CREATE, DROP, GRANT and REVOKE.
 */
public class AuthorOperator extends Operator {

  private final AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private String[] privilegeList;
  private PartialPath nodeName;

  /**
   * AuthorOperator Constructor with AuthorType.
   *
   * @param tokenIntType token in Int type
   * @param type author type
   */
  public AuthorOperator(int tokenIntType, AuthorType type) {
    super(tokenIntType);
    authorType = type;
    operatorType = OperatorType.AUTHOR;
  }

  /**
   * AuthorOperator Constructor with OperatorType.
   *
   * @param tokenIntType token in Int type
   * @param type operator type
   */
  public AuthorOperator(int tokenIntType, OperatorType type) {
    super(tokenIntType);
    authorType = null;
    operatorType = type;
  }

  public AuthorType getAuthorType() {
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
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    try {
      return new AuthorPlan(
          authorType, userName, roleName, password, newPassword, privilegeList, nodeName);
    } catch (AuthException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  public enum AuthorType {
    CREATE_USER,
    CREATE_ROLE,
    DROP_USER,
    DROP_ROLE,
    GRANT_ROLE,
    GRANT_USER,
    GRANT_ROLE_TO_USER,
    REVOKE_USER,
    REVOKE_ROLE,
    REVOKE_ROLE_FROM_USER,
    UPDATE_USER,
    LIST_USER,
    LIST_ROLE,
    LIST_USER_PRIVILEGE,
    LIST_ROLE_PRIVILEGE,
    LIST_USER_ROLES,
    LIST_ROLE_USERS;

    /**
     * deserialize short number.
     *
     * @param i short number
     * @return NamespaceType
     */
    public static AuthorType deserialize(short i) {
      switch (i) {
        case 0:
          return CREATE_USER;
        case 1:
          return CREATE_ROLE;
        case 2:
          return DROP_USER;
        case 3:
          return DROP_ROLE;
        case 4:
          return GRANT_ROLE;
        case 5:
          return GRANT_USER;
        case 6:
          return GRANT_ROLE_TO_USER;
        case 7:
          return REVOKE_USER;
        case 8:
          return REVOKE_ROLE;
        case 9:
          return REVOKE_ROLE_FROM_USER;
        case 10:
          return UPDATE_USER;
        case 11:
          return LIST_USER;
        case 12:
          return LIST_ROLE;
        case 13:
          return LIST_USER_PRIVILEGE;
        case 14:
          return LIST_ROLE_PRIVILEGE;
        case 15:
          return LIST_USER_ROLES;
        case 16:
          return LIST_ROLE_USERS;
        default:
          return null;
      }
    }

    /**
     * serialize.
     *
     * @return short number
     */
    public short serialize() {
      switch (this) {
        case CREATE_USER:
          return 0;
        case CREATE_ROLE:
          return 1;
        case DROP_USER:
          return 2;
        case DROP_ROLE:
          return 3;
        case GRANT_ROLE:
          return 4;
        case GRANT_USER:
          return 5;
        case GRANT_ROLE_TO_USER:
          return 6;
        case REVOKE_USER:
          return 7;
        case REVOKE_ROLE:
          return 8;
        case REVOKE_ROLE_FROM_USER:
          return 9;
        case UPDATE_USER:
          return 10;
        case LIST_USER:
          return 11;
        case LIST_ROLE:
          return 12;
        case LIST_USER_PRIVILEGE:
          return 13;
        case LIST_ROLE_PRIVILEGE:
          return 14;
        case LIST_USER_ROLES:
          return 15;
        case LIST_ROLE_USERS:
          return 16;
        default:
          return -1;
      }
    }
  }
}
