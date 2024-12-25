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
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class AuthorRelationalPlan extends AuthorPlan {
  protected int permission;
  protected String databaseName;
  protected String tableName;

  public AuthorRelationalPlan(final ConfigPhysicalPlanType authorType) {
    super(authorType);
  }

  public AuthorRelationalPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final int permission,
      final boolean grantOpt,
      final String password) {
    super(authorType);
    this.userName = userName;
    this.roleName = roleName;
    this.grantOpt = grantOpt;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.permission = permission;
    this.password = password;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public Set<Integer> getPermissions() {
    return Collections.singleton(permission);
  }

  public int getPermission() {
    return permission;
  }

  public void setPermission(int permission) {
    this.permission = permission;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof AuthorRelationalPlan) {
      AuthorRelationalPlan that = (AuthorRelationalPlan) o;
      return super.equals(o)
          && Objects.equals(this.databaseName, that.databaseName)
          && Objects.equals(this.tableName, that.tableName)
          && Objects.equals(this.permission, that.permission);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseName, tableName, permission);
  }

  @Override
  public String toString() {
    return "[type:"
        + super.getType()
        + ", name:"
        + userName
        + ", role:"
        + roleName
        + ", permissions"
        + (permission == -1 ? "-1" : PrivilegeType.values()[permission])
        + ", grant option:"
        + grantOpt
        + ", DB:"
        + databaseName
        + ", TABLE:"
        + tableName
        + "]";
  }
}
