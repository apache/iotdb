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
    super(authorType);
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
  protected void serializeImpl(ByteBuffer buffer) {}

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {}
}
