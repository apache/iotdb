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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AuthorNode extends PlanNode {

  private AuthorOperator.AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private PartialPath nodeName;

  public AuthorNode(
      PlanNodeId id,
      AuthorOperator.AuthorType authorType,
      String userName,
      String roleName,
      String password,
      String newPassword,
      String[] privilegeList,
      PartialPath nodeName)
      throws AuthException {
    super(id);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.permissions = strToPermissions(privilegeList);
    this.nodeName = nodeName;
  }

  public AuthorNode(
      PlanNodeId id,
      AuthorOperator.AuthorType authorType,
      String userName,
      String roleName,
      String password,
      String newPassword,
      Set<Integer> permissions,
      PartialPath nodeName)
      throws AuthException {
    super(id);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.permissions = permissions;
    this.nodeName = nodeName;
  }

  public AuthorOperator.AuthorType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(AuthorOperator.AuthorType authorType) {
    this.authorType = authorType;
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

  public PartialPath getNodeName() {
    return nodeName;
  }

  public void setNodeName(PartialPath nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of AuthorNode is not implemented");
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return null;
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    byteBuffer.putShort((short) PlanNodeType.AUTHOR.ordinal());
    ReadWriteIOUtils.write(this.getPlanNodeId().getId(), byteBuffer);
    ReadWriteIOUtils.write(getPlanType(authorType), byteBuffer);
    ReadWriteIOUtils.write(userName, byteBuffer);
    ReadWriteIOUtils.write(roleName, byteBuffer);
    ReadWriteIOUtils.write(password, byteBuffer);
    ReadWriteIOUtils.write(newPassword, byteBuffer);
    if (permissions == null) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      byteBuffer.putInt(permissions.size());
      for (int permission : permissions) {
        byteBuffer.putInt(permission);
      }
    }
    if (nodeName == null) {
      byteBuffer.put((byte) 0);
    } else {
      byteBuffer.put((byte) 1);
      ReadWriteIOUtils.write(nodeName.getFullPath(), byteBuffer);
    }
    // no children node, need to set 0
    byteBuffer.putInt(0);
  }

  public static AuthorNode deserialize(ByteBuffer byteBuffer) {
    String id;
    AuthorOperator.AuthorType authorType;
    String userName;
    String roleName;
    String password;
    String newPassword;
    Set<Integer> permissions;
    PartialPath nodeName;

    id = ReadWriteIOUtils.readString(byteBuffer);
    authorType = AuthorOperator.AuthorType.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    userName = ReadWriteIOUtils.readString(byteBuffer);
    roleName = ReadWriteIOUtils.readString(byteBuffer);
    password = ReadWriteIOUtils.readString(byteBuffer);
    newPassword = ReadWriteIOUtils.readString(byteBuffer);
    byte hasPermissions = byteBuffer.get();
    if (hasPermissions == (byte) 0) {
      permissions = null;
    } else {
      int permissionsSize = byteBuffer.getInt();
      permissions = new HashSet<>();
      for (int i = 0; i < permissionsSize; i++) {
        permissions.add(ReadWriteIOUtils.readInt(byteBuffer));
      }
    }
    byte hasNodeName = byteBuffer.get();
    if (hasNodeName == (byte) 0) {
      nodeName = null;
    } else {
      try {
        nodeName = new PartialPath(ReadWriteIOUtils.readString(byteBuffer));
      } catch (IllegalPathException e) {
        throw new IllegalArgumentException("Can not deserialize AuthorNode", e);
      }
    }
    try {
      return new AuthorNode(
          new PlanNodeId(id),
          authorType,
          userName,
          roleName,
          password,
          newPassword,
          permissions,
          nodeName);
    } catch (AuthException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new NotImplementedException("serializeAttributes of AuthorNode is not implemented");
  }

  public Set<Integer> strToPermissions(String[] privilegeList) throws AuthException {
    Set<Integer> result = new HashSet<>();
    if (privilegeList == null) {
      return result;
    }
    for (String s : privilegeList) {
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

  private int getPlanType(AuthorOperator.AuthorType authorType) {
    int type;
    switch (authorType) {
      case CREATE_USER:
        type = AuthorOperator.AuthorType.CREATE_USER.ordinal();
        break;
      case CREATE_ROLE:
        type = AuthorOperator.AuthorType.CREATE_ROLE.ordinal();
        break;
      case DROP_USER:
        type = AuthorOperator.AuthorType.DROP_USER.ordinal();
        break;
      case DROP_ROLE:
        type = AuthorOperator.AuthorType.DROP_ROLE.ordinal();
        break;
      case GRANT_ROLE:
        type = AuthorOperator.AuthorType.GRANT_ROLE.ordinal();
        break;
      case GRANT_USER:
        type = AuthorOperator.AuthorType.GRANT_USER.ordinal();
        break;
      case GRANT_ROLE_TO_USER:
        type = AuthorOperator.AuthorType.GRANT_ROLE_TO_USER.ordinal();
        break;
      case REVOKE_USER:
        type = AuthorOperator.AuthorType.REVOKE_USER.ordinal();
        break;
      case REVOKE_ROLE:
        type = AuthorOperator.AuthorType.REVOKE_ROLE.ordinal();
        break;
      case REVOKE_ROLE_FROM_USER:
        type = AuthorOperator.AuthorType.REVOKE_ROLE_FROM_USER.ordinal();
        break;
      case UPDATE_USER:
        type = AuthorOperator.AuthorType.UPDATE_USER.ordinal();
        break;
      case LIST_USER:
        type = AuthorOperator.AuthorType.LIST_USER.ordinal();
        break;
      case LIST_ROLE:
        type = AuthorOperator.AuthorType.LIST_ROLE.ordinal();
        break;
      case LIST_USER_PRIVILEGE:
        type = AuthorOperator.AuthorType.LIST_USER_PRIVILEGE.ordinal();
        break;
      case LIST_ROLE_PRIVILEGE:
        type = AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE.ordinal();
        break;
      case LIST_USER_ROLES:
        type = AuthorOperator.AuthorType.LIST_USER_ROLES.ordinal();
        break;
      case LIST_ROLE_USERS:
        type = AuthorOperator.AuthorType.LIST_ROLE_USERS.ordinal();
        break;
      default:
        throw new IllegalArgumentException("Unknown operator: " + authorType);
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
    AuthorNode that = (AuthorNode) o;
    return this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(authorType, that.authorType)
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(password, that.password)
        && Objects.equals(newPassword, that.newPassword)
        && Objects.equals(permissions, that.permissions)
        && Objects.equals(nodeName, that.nodeName);
  }
}
