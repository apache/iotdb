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

package org.apache.iotdb.confignode.consensus.request.read.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AuthorReadPlan extends ConfigPhysicalReadPlan {

  private final ConfigPhysicalPlanType authorType;
  private final String roleName;
  private String password;
  private final String newPassword;
  private Set<Integer> permissions;
  private final List<PartialPath> nodeNameList;
  private String userName;
  private final boolean grantOpt;

  /**
   * {@link AuthorReadPlan} Constructor.
   *
   * @param authorType author type
   * @param userName user name
   * @param roleName role name
   * @param password password
   * @param newPassword new password
   * @param permissions permissions
   * @param grantOpt with grant option, only grant statement can set grantOpt = true
   * @param nodeNameList node name in Path structure
   */
  public AuthorReadPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String password,
      final String newPassword,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList) {
    super(authorType);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.permissions = permissions;
    this.grantOpt = grantOpt;
    this.nodeNameList = nodeNameList;
  }

  public String getRoleName() {
    return roleName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(final String password) {
    this.password = password;
  }

  public Set<Integer> getPermissions() {
    return permissions;
  }

  public void setPermissions(final Set<Integer> permissions) {
    this.permissions = permissions;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(final String userName) {
    this.userName = userName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AuthorReadPlan that = (AuthorReadPlan) o;
    return Objects.equals(authorType, that.authorType)
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(password, that.password)
        && Objects.equals(newPassword, that.newPassword)
        && Objects.equals(permissions, that.permissions)
        && grantOpt == that.grantOpt
        && Objects.equals(nodeNameList, that.nodeNameList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        authorType, userName, roleName, password, newPassword, permissions, nodeNameList, grantOpt);
  }

  @Override
  public String toString() {
    return "[type:"
        + authorType
        + ", username:"
        + userName
        + ", rolename:"
        + roleName
        + ", permissions:"
        + PrivilegeType.toPriType(permissions)
        + ", grant option:"
        + grantOpt
        + ", paths:"
        + nodeNameList
        + "]";
  }
}
