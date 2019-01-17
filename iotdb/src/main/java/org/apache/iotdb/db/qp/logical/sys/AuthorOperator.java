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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * this class maintains information in Author statement, including CREATE, DROP, GRANT and REVOKE.
 */
public class AuthorOperator extends RootOperator {

  private final AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private String[] privilegeList;
  private Path nodeName;

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

  public Path getNodeName() {
    return nodeName;
  }

  public void setNodeNameList(Path nodePath) {
    this.nodeName = nodePath;
  }

  public enum AuthorType {
    CREATE_USER, CREATE_ROLE, DROP_USER, DROP_ROLE, GRANT_ROLE, GRANT_USER, GRANT_ROLE_TO_USER,
    REVOKE_USER, REVOKE_ROLE, REVOKE_ROLE_FROM_USER, UPDATE_USER, LIST_USER, LIST_ROLE,
    LIST_USER_PRIVILEGE, LIST_ROLE_PRIVILEGE, LIST_USER_ROLES, LIST_ROLE_USERS
  }
}
