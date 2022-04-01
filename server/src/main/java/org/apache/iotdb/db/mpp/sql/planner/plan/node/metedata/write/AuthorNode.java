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
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorNode extends PlanNode {

  private AuthorStatement.AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private PartialPath nodeName;

  public AuthorNode(
      PlanNodeId id,
      AuthorStatement.AuthorType authorType,
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

  public AuthorStatement.AuthorType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(AuthorStatement.AuthorType authorType) {
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

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
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
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}
}
