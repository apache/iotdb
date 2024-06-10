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

package org.apache.iotdb.confignode.consensus.request.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AuthorTreePlan extends AuthorPlan {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AuthorTreePlan.class);
  private Set<Integer> permissions;
  private List<PartialPath> nodeNameList;

  public AuthorTreePlan(final ConfigPhysicalPlanType type) {
    super(type, true);
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
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList) {
    this(authorType);
    super.setUserName(userName);
    super.setRoleName(roleName);
    super.setGrantOpt(grantOpt);
    super.setPassword(password);
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
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getPlanType(super.getType()), stream);
    BasicStructureSerDeUtil.write(super.getUserName(), stream);
    BasicStructureSerDeUtil.write(super.getRoleName(), stream);
    BasicStructureSerDeUtil.write(super.getPassword(), stream);
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
    super.setUserName(BasicStructureSerDeUtil.readString(buffer));
    super.setRoleName(BasicStructureSerDeUtil.readString(buffer));
    super.setPassword(BasicStructureSerDeUtil.readString(buffer));
    final byte hasPermissions = buffer.get();
    if (hasPermissions == (byte) 0) {
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
      logger.error("Invalid path when deserialize authPlan: {}", nodeNameList, e);
    }
    if (super.getAuthorType().ordinal() >= ConfigPhysicalPlanType.CreateUser.ordinal()) {
      super.setGrantOpt(BasicStructureSerDeUtil.readInt(buffer) > 0);
    }
  }

  private short getPlanType(ConfigPhysicalPlanType configPhysicalPlanType) {
    short type;
    switch (configPhysicalPlanType) {
      case CreateUser:
        type = ConfigPhysicalPlanType.CreateUser.getPlanType();
        break;
      case CreateRole:
        type = ConfigPhysicalPlanType.CreateRole.getPlanType();
        break;
      case DropUser:
        type = ConfigPhysicalPlanType.DropUser.getPlanType();
        break;
      case DropRole:
        type = ConfigPhysicalPlanType.DropRole.getPlanType();
        break;
      case GrantRole:
        type = ConfigPhysicalPlanType.GrantRole.getPlanType();
        break;
      case GrantUser:
        type = ConfigPhysicalPlanType.GrantUser.getPlanType();
        break;
      case GrantRoleToUser:
        type = ConfigPhysicalPlanType.GrantRoleToUser.getPlanType();
        break;
      case RevokeUser:
        type = ConfigPhysicalPlanType.RevokeUser.getPlanType();
        break;
      case RevokeRole:
        type = ConfigPhysicalPlanType.RevokeRole.getPlanType();
        break;
      case RevokeRoleFromUser:
        type = ConfigPhysicalPlanType.RevokeRoleFromUser.getPlanType();
        break;
      case UpdateUser:
        type = ConfigPhysicalPlanType.UpdateUser.getPlanType();
        break;
      case ListUser:
        type = ConfigPhysicalPlanType.ListUser.getPlanType();
        break;
      case ListRole:
        type = ConfigPhysicalPlanType.ListRole.getPlanType();
        break;
      case ListUserPrivilege:
        type = ConfigPhysicalPlanType.ListUserPrivilege.getPlanType();
        break;
      case ListRolePrivilege:
        type = ConfigPhysicalPlanType.ListRolePrivilege.getPlanType();
        break;
      case ListUserRoles:
        type = ConfigPhysicalPlanType.ListUserRoles.getPlanType();
        break;
      case ListRoleUsers:
        type = ConfigPhysicalPlanType.ListRoleUsers.getPlanType();
        break;
      case CreateUserWithRawPassword:
        type = ConfigPhysicalPlanType.CreateUserWithRawPassword.getPlanType();
        break;
      default:
        throw new IllegalArgumentException("Unknown operator: " + configPhysicalPlanType);
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
    AuthorTreePlan that = (AuthorTreePlan) o;
    return super.equals(that)
        && Objects.equals(permissions, that.permissions)
        && Objects.equals(nodeNameList, that.nodeNameList);
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
