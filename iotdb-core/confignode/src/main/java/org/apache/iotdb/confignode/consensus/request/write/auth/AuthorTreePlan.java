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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
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
   * @param maxSessioPerUser maxSessionPerUser of user
   * @param minSessionPerUser minSessionPerUser of user
   */
  public AuthorTreePlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String password,
      final String newPassword,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList,
      final Integer maxSessioPerUser,
      final Integer minSessionPerUser) {
    super(
        authorType,
        userName,
        roleName,
        password,
        newPassword,
        grantOpt,
        maxSessioPerUser,
        minSessionPerUser);
    this.permissions = permissions;
    this.nodeNameList = nodeNameList;
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
    this(
        authorType,
        userName,
        roleName,
        password,
        newPassword,
        permissions,
        grantOpt,
        nodeNameList,
        0,
        "");
  }

  public AuthorTreePlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String password,
      final String newPassword,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList,
      final long executedByUserId,
      final String newUsername) {
    super(authorType, userName, roleName, password, newPassword, grantOpt, -1, -1);
    this.permissions = permissions;
    this.nodeNameList = nodeNameList;
    this.executedByUserId = executedByUserId;
    this.newUsername = newUsername;
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
    BasicStructureSerDeUtil.write(newPassword, stream);
    ConfigPhysicalPlanType authorType = getAuthorType();
    if (authorType == ConfigPhysicalPlanType.UpdateUserMaxSession
        || authorType == ConfigPhysicalPlanType.UpdateUserMinSession) {
      BasicStructureSerDeUtil.write(maxSessionPerUser, stream);
      BasicStructureSerDeUtil.write(minSessionPerUser, stream);
    }
    if (authorType == ConfigPhysicalPlanType.RenameUser) {
      BasicStructureSerDeUtil.write(newUsername, stream);
    }
    if (authorType == ConfigPhysicalPlanType.DropUserV2
        || authorType == ConfigPhysicalPlanType.UpdateUserV2) {
      BasicStructureSerDeUtil.write(executedByUserId, stream);
    }
    if (permissions == null) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      stream.writeInt(permissions.size());
      for (int permission : permissions) {
        stream.writeInt(permission);
      }
    }
    BasicStructureSerDeUtil.write(nodeNameList.size(), stream);
    for (PartialPath partialPath : nodeNameList) {
      BasicStructureSerDeUtil.write(partialPath.getFullPath(), stream);
    }
    BasicStructureSerDeUtil.write(super.getGrantOpt() ? 1 : 0, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    userName = BasicStructureSerDeUtil.readString(buffer);
    roleName = BasicStructureSerDeUtil.readString(buffer);
    password = BasicStructureSerDeUtil.readString(buffer);
    newPassword = BasicStructureSerDeUtil.readString(buffer);
    ConfigPhysicalPlanType authorType = getAuthorType();
    if (authorType == ConfigPhysicalPlanType.UpdateUserMaxSession
        || authorType == ConfigPhysicalPlanType.UpdateUserMinSession) {
      maxSessionPerUser = buffer.getInt();
      minSessionPerUser = buffer.getInt();
    }
    if (authorType == ConfigPhysicalPlanType.RenameUser) {
      newUsername = BasicStructureSerDeUtil.readString(buffer);
    }
    if (authorType == ConfigPhysicalPlanType.DropUserV2
        || authorType == ConfigPhysicalPlanType.UpdateUserV2) {
      executedByUserId = buffer.getLong();
    }
    if (buffer.get() == (byte) 0) {
      this.permissions = null;
    } else {
      int permissionsSize = buffer.getInt();
      this.permissions = new HashSet<>();
      for (int i = 0; i < permissionsSize; i++) {
        permissions.add(buffer.getInt());
      }
    }

    int nodeNameListSize = BasicStructureSerDeUtil.readInt(buffer);
    nodeNameList = new ArrayList<>(nodeNameListSize);
    try {
      for (int i = 0; i < nodeNameListSize; i++) {
        nodeNameList.add(new PartialPath(BasicStructureSerDeUtil.readString(buffer)));
      }
    } catch (MetadataException e) {
      // do nothing
    }
    if (super.getAuthorType().ordinal() >= ConfigPhysicalPlanType.CreateUser.ordinal()) {
      super.setGrantOpt(BasicStructureSerDeUtil.readInt(buffer) > 0);
    }
  }
}
