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
package org.apache.iotdb.confignode.consensus.request.write.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class AuthorRelationalPlan extends AuthorPlan {
  protected Set<Integer> permissions;
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
      final Set<Integer> permissions,
      final boolean grantOpt,
      final String password,
      final int maxSessionPerUser,
      final int minSessionPerUser) {
    super(
        authorType,
        userName,
        roleName,
        password,
        "",
        grantOpt,
        maxSessionPerUser,
        minSessionPerUser);

    this.databaseName = databaseName;
    this.tableName = tableName;
    this.permissions = permissions;
  }

  public AuthorRelationalPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final String password) {
    this(
        authorType,
        userName,
        roleName,
        databaseName,
        tableName,
        permissions,
        grantOpt,
        password,
        0,
        "");
  }

  public AuthorRelationalPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final String password,
      final long executedByUserId,
      final String newUsername) {
    super(authorType, userName, roleName, password, "", grantOpt, -1, -1);

    this.databaseName = databaseName;
    this.tableName = tableName;
    this.permissions = permissions;
    this.executedByUserId = executedByUserId;
    this.newUsername = newUsername;
  }

  public AuthorRelationalPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final int permission,
      final boolean grantOpt) {
    super(authorType, userName, roleName, "", "", grantOpt, -1, -1);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.permissions = Collections.singleton(permission);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Set<Integer> getPermissions() {
    return permissions;
  }

  public void setPermissions(Set<Integer> permissions) {
    this.permissions = permissions;
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
          && Objects.equals(this.permissions, that.permissions);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseName, tableName, permissions);
  }

  @Override
  public String toString() {
    return "[type:"
        + super.getType()
        + ", name:"
        + userName
        + ", role:"
        + roleName
        + ", permissions:"
        + PrivilegeType.toPriType(permissions)
        + ", grant option:"
        + grantOpt
        + ", DB:"
        + databaseName
        + ", TABLE:"
        + tableName
        + ", newUsername:"
        + newUsername
        + "]";
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    ConfigPhysicalPlanType authorType = getAuthorType();
    if (authorType == ConfigPhysicalPlanType.UpdateUserMaxSession
        || authorType == ConfigPhysicalPlanType.UpdateUserMinSession) {
      BasicStructureSerDeUtil.write(maxSessionPerUser, stream);
      BasicStructureSerDeUtil.write(minSessionPerUser, stream);
    }
    if (authorType == ConfigPhysicalPlanType.RRenameUser) {
      BasicStructureSerDeUtil.write(newUsername, stream);
    }
    if (authorType == ConfigPhysicalPlanType.RDropUserV2
        || authorType == ConfigPhysicalPlanType.RUpdateUserV2) {
      BasicStructureSerDeUtil.write(executedByUserId, stream);
    }
    BasicStructureSerDeUtil.write(databaseName, stream);
    BasicStructureSerDeUtil.write(tableName, stream);
    stream.writeInt(permissions.size());
    for (Integer permission : permissions) {
      stream.writeInt(permission);
    }
    stream.write(grantOpt ? (byte) 1 : (byte) 0);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    newPassword = "";
    userName = BasicStructureSerDeUtil.readString(buffer);
    roleName = BasicStructureSerDeUtil.readString(buffer);
    password = BasicStructureSerDeUtil.readString(buffer);
    ConfigPhysicalPlanType authorType = getAuthorType();
    if (authorType == ConfigPhysicalPlanType.UpdateUserMaxSession
        || authorType == ConfigPhysicalPlanType.UpdateUserMinSession) {
      maxSessionPerUser = buffer.getInt();
      minSessionPerUser = buffer.getInt();
    }
    if (authorType == ConfigPhysicalPlanType.RRenameUser) {
      newUsername = BasicStructureSerDeUtil.readString(buffer);
    }
    if (authorType == ConfigPhysicalPlanType.RDropUserV2
        || authorType == ConfigPhysicalPlanType.RUpdateUserV2) {
      executedByUserId = buffer.getLong();
    }
    databaseName = BasicStructureSerDeUtil.readString(buffer);
    tableName = BasicStructureSerDeUtil.readString(buffer);
    permissions = new HashSet<>();
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      permissions.add(buffer.getInt());
    }
    grantOpt = buffer.get() == (byte) 1;
  }
}
