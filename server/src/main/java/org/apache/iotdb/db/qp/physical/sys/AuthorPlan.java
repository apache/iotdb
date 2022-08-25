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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AuthorPlan extends PhysicalPlan {

  private AuthorOperator.AuthorType authorType;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private List<PartialPath> nodeNameList;
  private String userName;

  /**
   * AuthorPlan Constructor.
   *
   * @param authorType author type
   * @param userName user name
   * @param roleName role name
   * @param password password
   * @param newPassword new password
   * @param authorizationList authorization list in String[] structure
   * @param nodeNameList node name in Path structure
   * @throws AuthException Authentication Exception
   */
  public AuthorPlan(
      AuthorOperator.AuthorType authorType,
      String userName,
      String roleName,
      String password,
      String newPassword,
      String[] authorizationList,
      List<PartialPath> nodeNameList)
      throws AuthException {
    super(Operator.OperatorType.AUTHOR);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.permissions = AuthUtils.strToPermissions(authorizationList);
    this.nodeNameList = nodeNameList;
    switch (authorType) {
      case DROP_ROLE:
        this.setOperatorType(Operator.OperatorType.DELETE_ROLE);
        break;
      case DROP_USER:
        this.setOperatorType(Operator.OperatorType.DELETE_USER);
        break;
      case GRANT_ROLE:
        this.setOperatorType(Operator.OperatorType.GRANT_ROLE_PRIVILEGE);
        break;
      case GRANT_USER:
        this.setOperatorType(Operator.OperatorType.GRANT_USER_PRIVILEGE);
        break;
      case CREATE_ROLE:
        this.setOperatorType(Operator.OperatorType.CREATE_ROLE);
        break;
      case CREATE_USER:
        this.setOperatorType(Operator.OperatorType.CREATE_USER);
        break;
      case REVOKE_ROLE:
        this.setOperatorType(Operator.OperatorType.REVOKE_ROLE_PRIVILEGE);
        break;
      case REVOKE_USER:
        this.setOperatorType(Operator.OperatorType.REVOKE_USER_PRIVILEGE);
        break;
      case UPDATE_USER:
        this.setOperatorType(Operator.OperatorType.MODIFY_PASSWORD);
        break;
      case GRANT_USER_ROLE:
        this.setOperatorType(Operator.OperatorType.GRANT_USER_ROLE);
        break;
      case REVOKE_USER_ROLE:
        this.setOperatorType(Operator.OperatorType.REVOKE_USER_ROLE);
        break;
      case LIST_USER_PRIVILEGE:
        this.setQuery(true);
        this.setOperatorType(Operator.OperatorType.LIST_USER_PRIVILEGE);
        break;
      case LIST_ROLE_PRIVILEGE:
        this.setQuery(true);
        this.setOperatorType(Operator.OperatorType.LIST_ROLE_PRIVILEGE);
        break;
      case LIST_USER:
        this.setQuery(true);
        this.setOperatorType(Operator.OperatorType.LIST_USER);
        break;
      case LIST_ROLE:
        this.setQuery(true);
        this.setOperatorType(Operator.OperatorType.LIST_ROLE);
        break;
      default:
    }
  }

  public AuthorPlan(OperatorType operatorType) throws IOException {
    super(operatorType);
    setAuthorType(transformOperatorTypeToAuthorType(operatorType));
  }

  private AuthorType transformOperatorTypeToAuthorType(OperatorType operatorType)
      throws IOException {
    AuthorType type;
    switch (operatorType) {
      case CREATE_ROLE:
        type = AuthorType.CREATE_ROLE;
        break;
      case DELETE_ROLE:
        type = AuthorType.DROP_ROLE;
        break;
      case CREATE_USER:
        type = AuthorType.CREATE_USER;
        break;
      case REVOKE_USER_ROLE:
        type = AuthorType.REVOKE_USER_ROLE;
        break;
      case REVOKE_ROLE_PRIVILEGE:
        type = AuthorType.REVOKE_ROLE;
        break;
      case REVOKE_USER_PRIVILEGE:
        type = AuthorType.REVOKE_USER;
        break;
      case GRANT_ROLE_PRIVILEGE:
        type = AuthorType.GRANT_ROLE;
        break;
      case GRANT_USER_PRIVILEGE:
        type = AuthorType.GRANT_USER;
        break;
      case GRANT_USER_ROLE:
        type = AuthorType.GRANT_USER_ROLE;
        break;
      case MODIFY_PASSWORD:
        type = AuthorType.UPDATE_USER;
        break;
      case DELETE_USER:
        type = AuthorType.DROP_USER;
        break;
      default:
        throw new IOException("unrecognized author type " + operatorType.name());
    }
    return type;
  }

  public void setAuthorType(AuthorOperator.AuthorType type) {
    this.authorType = type;
  }

  public AuthorOperator.AuthorType getAuthorType() {
    return authorType;
  }

  public String getRoleName() {
    return roleName;
  }

  public String getPassword() {
    return password;
  }

  public String getNewPassword() {
    return newPassword;
  }

  public Set<Integer> getPermissions() {
    return permissions;
  }

  public void setPermissions(Set<Integer> permissions) {
    this.permissions = permissions;
  }

  public List<PartialPath> getNodeNameList() {
    return nodeNameList != null ? nodeNameList : Collections.emptyList();
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public String toString() {
    return "userName: "
        + userName
        + "\nroleName: "
        + roleName
        + "\npassword: "
        + password
        + "\nnewPassword: "
        + newPassword
        + "\npermissions: "
        + permissions
        + "\nnodeName: "
        + nodeNameList
        + "\nauthorType: "
        + authorType;
  }

  @Override
  public List<PartialPath> getPaths() {
    return nodeNameList != null ? nodeNameList : Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuthorPlan)) {
      return false;
    }
    AuthorPlan that = (AuthorPlan) o;
    return getAuthorType() == that.getAuthorType()
        && Objects.equals(getUserName(), that.getUserName())
        && Objects.equals(getRoleName(), that.getRoleName())
        && Objects.equals(getPassword(), that.getPassword())
        && Objects.equals(getNewPassword(), that.getNewPassword())
        && Objects.equals(getPermissions(), that.getPermissions())
        && Objects.equals(getNodeNameList(), that.getNodeNameList());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getAuthorType(),
        getUserName(),
        getRoleName(),
        getPassword(),
        getNewPassword(),
        getPermissions(),
        getNodeNameList());
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = this.getPlanType(super.getOperatorType());
    stream.writeByte((byte) type);
    stream.writeInt(authorType.ordinal());
    putString(stream, userName);
    putString(stream, roleName);
    putString(stream, password);
    putString(stream, newPassword);
    if (permissions == null) {
      stream.writeBoolean(false);
    } else {
      stream.writeBoolean(true);
      stream.writeInt(permissions.size());
      for (int permission : permissions) {
        stream.writeInt(permission);
      }
    }
    if (nodeNameList == null) {
      ReadWriteIOUtils.writeStringList(Collections.emptyList(), stream);
    } else {
      ReadWriteIOUtils.writeStringList(
          nodeNameList.stream().map(PartialPath::getFullPath).collect(Collectors.toList()), stream);
    }

    stream.writeLong(index);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    int type = this.getPlanType(super.getOperatorType());
    buffer.put((byte) type);
    buffer.putInt(authorType.ordinal());
    putString(buffer, userName);
    putString(buffer, roleName);
    putString(buffer, password);
    putString(buffer, newPassword);
    if (permissions == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      buffer.putInt(permissions.size());
      for (int permission : permissions) {
        buffer.putInt(permission);
      }
    }
    if (nodeNameList == null) {
      ReadWriteIOUtils.writeStringList(Collections.emptyList(), buffer);
    } else {
      ReadWriteIOUtils.writeStringList(
          nodeNameList.stream().map(PartialPath::getFullPath).collect(Collectors.toList()), buffer);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.authorType = AuthorType.values()[buffer.getInt()];
    this.userName = readString(buffer);
    this.roleName = readString(buffer);
    this.password = readString(buffer);
    this.newPassword = readString(buffer);
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
    List<String> nodeNameList = ReadWriteIOUtils.readStringList(buffer);
    if (nodeNameList.size() == 0) {
      this.nodeNameList = null;
    } else {
      this.nodeNameList =
          nodeNameList.stream()
              .map(
                  path -> {
                    try {
                      return new PartialPath(path);
                    } catch (IllegalPathException e) {
                      throw new IllegalArgumentException("Deserialize node paths failed.");
                    }
                  })
              .collect(Collectors.toList());
    }

    this.index = buffer.getLong();
  }

  private int getPlanType(OperatorType operatorType) {
    int type;
    switch (operatorType) {
      case CREATE_ROLE:
        type = PhysicalPlanType.CREATE_ROLE.ordinal();
        break;
      case DELETE_ROLE:
        type = PhysicalPlanType.DELETE_ROLE.ordinal();
        break;
      case CREATE_USER:
        type = PhysicalPlanType.CREATE_USER.ordinal();
        break;
      case REVOKE_USER_ROLE:
        type = PhysicalPlanType.REVOKE_USER_ROLE.ordinal();
        break;
      case REVOKE_ROLE_PRIVILEGE:
        type = PhysicalPlanType.REVOKE_ROLE_PRIVILEGE.ordinal();
        break;
      case REVOKE_USER_PRIVILEGE:
        type = PhysicalPlanType.REVOKE_USER_PRIVILEGE.ordinal();
        break;
      case GRANT_ROLE_PRIVILEGE:
        type = PhysicalPlanType.GRANT_ROLE_PRIVILEGE.ordinal();
        break;
      case GRANT_USER_PRIVILEGE:
        type = PhysicalPlanType.GRANT_USER_PRIVILEGE.ordinal();
        break;
      case GRANT_USER_ROLE:
        type = PhysicalPlanType.GRANT_USER_ROLE.ordinal();
        break;
      case MODIFY_PASSWORD:
        type = PhysicalPlanType.MODIFY_PASSWORD.ordinal();
        break;
      case DELETE_USER:
        type = PhysicalPlanType.DELETE_USER.ordinal();
        break;
      default:
        throw new IllegalArgumentException("Unknown operator: " + operatorType);
    }
    return type;
  }
}
