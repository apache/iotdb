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
package org.apache.iotdb.confignode.physical.sys;

import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class AuthorPlan extends PhysicalPlan {

  private PhysicalPlanType authorType;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private String nodeName;
  private String userName;

  public AuthorPlan(PhysicalPlanType type) {
    super(type);
  }

  /**
   * AuthorPlan Constructor.
   *
   * @param authorType author type
   * @param userName user name
   * @param roleName role name
   * @param password password
   * @param newPassword new password
   * @param permissions permissions
   * @param nodeName node name in Path structure
   * @throws AuthException Authentication Exception
   */
  public AuthorPlan(
      PhysicalPlanType authorType,
      String userName,
      String roleName,
      String password,
      String newPassword,
      Set<Integer> permissions,
      String nodeName)
      throws AuthException {
    this(authorType);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.permissions = permissions;
    this.nodeName = nodeName;
  }

  public PhysicalPlanType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(PhysicalPlanType authorType) {
    this.authorType = authorType;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getNewPassword() {
    return newPassword;
  }

  public void setNewPassword(String newPassword) {
    this.newPassword = newPassword;
  }

  public Set<Integer> getPermissions() {
    return permissions;
  }

  public void setPermissions(Set<Integer> permissions) {
    this.permissions = permissions;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  private Set<Integer> strToPermissions(String[] authorizationList) throws AuthException {
    Set<Integer> result = new HashSet<>();
    if (authorizationList == null) {
      return result;
    }
    for (String s : authorizationList) {
      PrivilegeType[] types = PrivilegeType.values();
      boolean legal = false;
      for (PrivilegeType privilegeType : types) {
        if (s.equalsIgnoreCase(privilegeType.name())) {
          result.add(privilegeType.ordinal());
          legal = true;
          break;
        }
      }
      if (!legal) {
        throw new AuthException("No such privilege " + s);
      }
    }
    return result;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getPlanTypeOrdinal(authorType));
    buffer.putInt(userName.length());
    buffer.put(userName.getBytes());
    buffer.putInt(roleName.length());
    buffer.put(roleName.getBytes());
    buffer.putInt(password.length());
    buffer.put(password.getBytes());
    buffer.putInt(newPassword.length());
    buffer.put(newPassword.getBytes());
    if (permissions == null && permissions.size() == 0) {
      buffer.put("false".getBytes());
    } else {
      buffer.put("true".getBytes());
      buffer.putInt(permissions.size());
      for (Integer permission : permissions) {
        buffer.putInt(permission);
      }
    }
    if (nodeName == null && nodeName.equals("")) {
      buffer.put("false".getBytes());
    } else {
      buffer.put("true".getBytes());
      buffer.putInt(nodeName.length());
      buffer.put(nodeName.getBytes());
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    int type = buffer.getInt();
    userName = getAuthorInfo(buffer);
    roleName = getAuthorInfo(buffer);
    password = getAuthorInfo(buffer);
    newPassword = getAuthorInfo(buffer);
    String permissionIsNull = getAuthorInfo(buffer);
    if ("true".equals(permissionIsNull)) {
      int permissionSize = buffer.getInt();
      for (int i = 0; i < permissionSize; i++) {
        permissions.add(buffer.getInt());
      }
    }
    String nodeNameIsNull = getAuthorInfo(buffer);
    if ("true".equals(nodeNameIsNull)) {
      nodeName = getAuthorInfo(buffer);
    }
  }

  private String getAuthorInfo(ByteBuffer buffer) {
    int infoSize = buffer.getInt();
    byte[] byteInfo = new byte[infoSize];
    buffer.get(byteInfo, 0, infoSize);
    return new String(byteInfo, 0, infoSize);
  }

  private int getPlanTypeOrdinal(PhysicalPlanType physicalPlanType) {
    int type;
    switch (physicalPlanType) {
      case CREATE_USER:
        type = PhysicalPlanType.CREATE_USER.ordinal();
        break;
      case CREATE_ROLE:
        type = PhysicalPlanType.CREATE_ROLE.ordinal();
        break;
      case DROP_USER:
        type = PhysicalPlanType.DROP_USER.ordinal();
        break;
      case DROP_ROLE:
        type = PhysicalPlanType.DROP_ROLE.ordinal();
        break;
      case GRANT_ROLE:
        type = PhysicalPlanType.GRANT_ROLE.ordinal();
        break;
      case GRANT_USER:
        type = PhysicalPlanType.GRANT_USER.ordinal();
        break;
      case GRANT_ROLE_TO_USER:
        type = PhysicalPlanType.GRANT_ROLE_TO_USER.ordinal();
        break;
      case REVOKE_USER:
        type = PhysicalPlanType.REVOKE_USER.ordinal();
        break;
      case REVOKE_ROLE:
        type = PhysicalPlanType.REVOKE_ROLE.ordinal();
        break;
      case REVOKE_ROLE_FROM_USER:
        type = PhysicalPlanType.REVOKE_ROLE_FROM_USER.ordinal();
        break;
      case UPDATE_USER:
        type = PhysicalPlanType.UPDATE_USER.ordinal();
        break;
      case LIST_USER:
        type = PhysicalPlanType.LIST_USER.ordinal();
        break;
      case LIST_ROLE:
        type = PhysicalPlanType.LIST_ROLE.ordinal();
        break;
      case LIST_USER_PRIVILEGE:
        type = PhysicalPlanType.LIST_USER_PRIVILEGE.ordinal();
        break;
      case LIST_ROLE_PRIVILEGE:
        type = PhysicalPlanType.LIST_ROLE_PRIVILEGE.ordinal();
        break;
      case LIST_USER_ROLES:
        type = PhysicalPlanType.LIST_USER_ROLES.ordinal();
        break;
      case LIST_ROLE_USERS:
        type = PhysicalPlanType.LIST_ROLE_USERS.ordinal();
        break;
      default:
        throw new IllegalArgumentException("Unknown operator: " + physicalPlanType);
    }
    return type;
  }
}
