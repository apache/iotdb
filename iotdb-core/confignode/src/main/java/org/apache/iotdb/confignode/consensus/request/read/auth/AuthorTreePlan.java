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

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AuthorTreePlan extends AuthorPlan {
  protected Set<Integer> permissions;
  protected List<PartialPath> nodeNameList;

  public AuthorTreePlan(final ConfigPhysicalPlanType type) {
    super(type);
  }

  /**
   * {@link AuthorTreePlan} Constructor.
   *
   * @param authorType author type
   * @param userName user name
   * @param roleName role name
   * @param password password
   * @param permissions permissions
   * @param grantOpt with grant option, only grant statement can set grantOpt = true
   * @param nodeNameList node name in Path structure
   */
  public AuthorTreePlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String password,
      final String newPassword,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList) {
    super(authorType, userName, roleName, password, newPassword, grantOpt);
    this.permissions = permissions;
    this.nodeNameList = nodeNameList;
  }

  public Set<Integer> getPermissions() {
    return permissions;
  }

  public void setPermissions(Set<Integer> permissions) {
    this.permissions = permissions;
  }

  public List<PartialPath> getNodeNameList() {
    return nodeNameList;
  }

  public void setNodeNameList(List<PartialPath> nodeNameList) {
    this.nodeNameList = nodeNameList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof AuthorTreePlan) {
      AuthorTreePlan that = (AuthorTreePlan) o;
      return super.equals(that)
          && Objects.equals(permissions, that.permissions)
          && Objects.equals(nodeNameList, that.nodeNameList);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), permissions, nodeNameList);
  }

  @Override
  public String toString() {
    return "[type:"
        + super.getType()
        + ", username:"
        + super.getUserName()
        + ", rolename:"
        + super.getRoleName()
        + ", permissions:"
        + PrivilegeType.toPriType(permissions)
        + ", grant option:"
        + super.getGrantOpt()
        + ", paths:"
        + nodeNameList
        + "]";
  }
}
