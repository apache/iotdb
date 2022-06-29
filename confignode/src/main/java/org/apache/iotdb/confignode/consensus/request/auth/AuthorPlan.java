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
package org.apache.iotdb.confignode.consensus.request.auth;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class AuthorPlan extends ConfigPhysicalPlan {

  private ConfigPhysicalPlanType authorType;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private String nodeName;
  private String userName;

  public AuthorPlan(ConfigPhysicalPlanType type) {
    super(type);
    authorType = type;
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
      ConfigPhysicalPlanType authorType,
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

  public ConfigPhysicalPlanType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(ConfigPhysicalPlanType authorType) {
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

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    BasicStructureSerDeUtil.write(getPlanTypeOrdinal(authorType), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    BasicStructureSerDeUtil.write(newPassword, stream);
    if (permissions == null) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      stream.writeInt(permissions.size());
      for (int permission : permissions) {
        stream.writeInt(permission);
      }
    }
    BasicStructureSerDeUtil.write(nodeName, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    userName = BasicStructureSerDeUtil.readString(buffer);
    roleName = BasicStructureSerDeUtil.readString(buffer);
    password = BasicStructureSerDeUtil.readString(buffer);
    newPassword = BasicStructureSerDeUtil.readString(buffer);
    byte hasPermissions = buffer.get();
    if (hasPermissions == (byte) 0) {
      this.permissions = null;
    } else {
      int permissionsSize = buffer.getInt();
      this.permissions = new HashSet<>();
      for (int i = 0; i < permissionsSize; i++) {
        permissions.add(buffer.getInt());
      }
    }
    nodeName = BasicStructureSerDeUtil.readString(buffer);
  }

  private int getPlanTypeOrdinal(ConfigPhysicalPlanType configPhysicalPlanType) {
    int type;
    switch (configPhysicalPlanType) {
      case CreateUser:
        type = ConfigPhysicalPlanType.CreateUser.ordinal();
        break;
      case CreateRole:
        type = ConfigPhysicalPlanType.CreateRole.ordinal();
        break;
      case DropUser:
        type = ConfigPhysicalPlanType.DropUser.ordinal();
        break;
      case DropRole:
        type = ConfigPhysicalPlanType.DropRole.ordinal();
        break;
      case GrantRole:
        type = ConfigPhysicalPlanType.GrantRole.ordinal();
        break;
      case GrantUser:
        type = ConfigPhysicalPlanType.GrantUser.ordinal();
        break;
      case GrantRoleToUser:
        type = ConfigPhysicalPlanType.GrantRoleToUser.ordinal();
        break;
      case RevokeUser:
        type = ConfigPhysicalPlanType.RevokeUser.ordinal();
        break;
      case RevokeRole:
        type = ConfigPhysicalPlanType.RevokeRole.ordinal();
        break;
      case RevokeRoleFromUser:
        type = ConfigPhysicalPlanType.RevokeRoleFromUser.ordinal();
        break;
      case UpdateUser:
        type = ConfigPhysicalPlanType.UpdateUser.ordinal();
        break;
      case ListUser:
        type = ConfigPhysicalPlanType.ListUser.ordinal();
        break;
      case ListRole:
        type = ConfigPhysicalPlanType.ListRole.ordinal();
        break;
      case ListUserPrivilege:
        type = ConfigPhysicalPlanType.ListUserPrivilege.ordinal();
        break;
      case ListRolePrivilege:
        type = ConfigPhysicalPlanType.ListRolePrivilege.ordinal();
        break;
      case ListUserRoles:
        type = ConfigPhysicalPlanType.ListUserRoles.ordinal();
        break;
      case ListRoleUsers:
        type = ConfigPhysicalPlanType.ListRoleUsers.ordinal();
        break;
      default:
        throw new IllegalArgumentException("Unknown operator: " + configPhysicalPlanType);
    }
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthorPlan that = (AuthorPlan) o;
    return Objects.equals(authorType, that.authorType)
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(password, that.password)
        && Objects.equals(newPassword, that.newPassword)
        && Objects.equals(permissions, that.permissions)
        && Objects.equals(nodeName, that.nodeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        authorType, userName, roleName, password, newPassword, permissions, nodeName);
  }
}
