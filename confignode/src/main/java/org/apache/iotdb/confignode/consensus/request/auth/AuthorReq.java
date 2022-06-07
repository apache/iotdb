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
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class AuthorReq extends ConfigRequest {

  private ConfigRequestType authorType;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private String nodeName;
  private String userName;

  public AuthorReq(ConfigRequestType type) {
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
  public AuthorReq(
      ConfigRequestType authorType,
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

  public ConfigRequestType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(ConfigRequestType authorType) {
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
  protected void serializeImpl(ByteBuffer buffer) {
    BasicStructureSerDeUtil.write(getPlanTypeOrdinal(authorType), buffer);
    BasicStructureSerDeUtil.write(userName, buffer);
    BasicStructureSerDeUtil.write(roleName, buffer);
    BasicStructureSerDeUtil.write(password, buffer);
    BasicStructureSerDeUtil.write(newPassword, buffer);
    if (permissions == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      buffer.putInt(permissions.size());
      for (int permission : permissions) {
        buffer.putInt(permission);
      }
    }
    BasicStructureSerDeUtil.write(nodeName, buffer);
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

  private int getPlanTypeOrdinal(ConfigRequestType configRequestType) {
    int type;
    switch (configRequestType) {
      case CreateUser:
        type = ConfigRequestType.CreateUser.ordinal();
        break;
      case CreateRole:
        type = ConfigRequestType.CreateRole.ordinal();
        break;
      case DropUser:
        type = ConfigRequestType.DropUser.ordinal();
        break;
      case DropRole:
        type = ConfigRequestType.DropRole.ordinal();
        break;
      case GrantRole:
        type = ConfigRequestType.GrantRole.ordinal();
        break;
      case GrantUser:
        type = ConfigRequestType.GrantUser.ordinal();
        break;
      case GrantRoleToUser:
        type = ConfigRequestType.GrantRoleToUser.ordinal();
        break;
      case RevokeUser:
        type = ConfigRequestType.RevokeUser.ordinal();
        break;
      case RevokeRole:
        type = ConfigRequestType.RevokeRole.ordinal();
        break;
      case RevokeRoleFromUser:
        type = ConfigRequestType.RevokeRoleFromUser.ordinal();
        break;
      case UpdateUser:
        type = ConfigRequestType.UpdateUser.ordinal();
        break;
      case ListUser:
        type = ConfigRequestType.ListUser.ordinal();
        break;
      case ListRole:
        type = ConfigRequestType.ListRole.ordinal();
        break;
      case ListUserPrivilege:
        type = ConfigRequestType.ListUserPrivilege.ordinal();
        break;
      case ListRolePrivilege:
        type = ConfigRequestType.ListRolePrivilege.ordinal();
        break;
      case ListUserRoles:
        type = ConfigRequestType.ListUserRoles.ordinal();
        break;
      case ListRoleUsers:
        type = ConfigRequestType.ListRoleUsers.ordinal();
        break;
      default:
        throw new IllegalArgumentException("Unknown operator: " + configRequestType);
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
    AuthorReq that = (AuthorReq) o;
    return Objects.equals(authorType, that.authorType)
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(password, that.password)
        && Objects.equals(newPassword, that.newPassword)
        && Objects.equals(permissions, that.permissions)
        && Objects.equals(nodeName, that.nodeName);
  }
}
