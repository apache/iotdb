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
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
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

public class AuthorPlan extends ConfigPhysicalPlan {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AuthorPlan.class);

  private ConfigPhysicalPlanType authorType;
  private String roleName;
  private String password;
  private String newPassword;
  private Set<Integer> permissions;
  private List<PartialPath> nodeNameList;
  private String userName;
  private boolean grantOpt;

  public AuthorPlan(final ConfigPhysicalPlanType type) {
    super(type);
    authorType = type;
  }

  /**
   * {@link AuthorPlan} Constructor.
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
  public AuthorPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String password,
      final String newPassword,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList) {
    this(authorType);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.permissions = permissions;
    this.grantOpt = grantOpt;
    this.nodeNameList = nodeNameList;
  }

  public ConfigPhysicalPlanType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(final ConfigPhysicalPlanType authorType) {
    this.authorType = authorType;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(final String roleName) {
    this.roleName = roleName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(final String password) {
    this.password = password;
  }

  public String getNewPassword() {
    return newPassword;
  }

  public Set<Integer> getPermissions() {
    return permissions;
  }

  public void setPermissions(final Set<Integer> permissions) {
    this.permissions = permissions;
  }

  public boolean getGrantOpt() {
    return this.grantOpt;
  }

  public void setGrantOpt(final boolean grantOpt) {
    this.grantOpt = grantOpt;
  }

  public List<PartialPath> getNodeNameList() {
    return nodeNameList;
  }

  public void setNodeNameList(final List<PartialPath> nodeNameList) {
    this.nodeNameList = nodeNameList;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(final String userName) {
    this.userName = userName;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(authorType.getPlanType(), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    BasicStructureSerDeUtil.write(newPassword, stream);
    if (permissions == null) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      stream.writeInt(permissions.size());
      for (final int permission : permissions) {
        stream.writeInt(permission);
      }
    }
    BasicStructureSerDeUtil.write(nodeNameList.size(), stream);
    for (final PartialPath partialPath : nodeNameList) {
      BasicStructureSerDeUtil.write(partialPath.getFullPath(), stream);
    }
    BasicStructureSerDeUtil.write(grantOpt ? 1 : 0, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    userName = BasicStructureSerDeUtil.readString(buffer);
    roleName = BasicStructureSerDeUtil.readString(buffer);
    password = BasicStructureSerDeUtil.readString(buffer);
    newPassword = BasicStructureSerDeUtil.readString(buffer);
    final byte hasPermissions = buffer.get();
    if (hasPermissions == (byte) 0) {
      this.permissions = null;
    } else {
      final int permissionsSize = buffer.getInt();
      this.permissions = new HashSet<>();
      for (int i = 0; i < permissionsSize; i++) {
        permissions.add(buffer.getInt());
      }
    }

    final int nodeNameListSize = BasicStructureSerDeUtil.readInt(buffer);
    nodeNameList = new ArrayList<>(nodeNameListSize);
    try {
      for (int i = 0; i < nodeNameListSize; i++) {
        nodeNameList.add(new PartialPath(BasicStructureSerDeUtil.readString(buffer)));
      }
    } catch (MetadataException e) {
      logger.error("Invalid path when deserialize authPlan: {}", nodeNameList, e);
    }
    grantOpt = false;
    if (this.authorType.ordinal() >= ConfigPhysicalPlanType.CreateUser.ordinal()) {
      grantOpt = BasicStructureSerDeUtil.readInt(buffer) > 0;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AuthorPlan that = (AuthorPlan) o;
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
