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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
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
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(getPlanType(authorType));
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
      buffer.putInt(nodeName.getFullPath().length());
      buffer.put(nodeName.getFullPath().getBytes());
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new NotImplementedException();
  }

  public static AuthorNode deserialize(ByteBuffer buffer) {
    return null;
  }

  public Set<Integer> strToPermissions(String[] authorizationList) throws AuthException {
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

  private static String getAuthorInfo(ByteBuffer buffer) {
    int infoSize = buffer.getInt();
    byte[] byteInfo = new byte[infoSize];
    buffer.get(byteInfo, 0, infoSize);
    return new String(byteInfo, 0, infoSize);
  }

  private int getPlanType(AuthorOperator.AuthorType physicalPlanType) {
    int type;
    switch (physicalPlanType) {
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
        throw new IllegalArgumentException("Unknown operator: " + physicalPlanType);
    }
    return type;
  }
}
