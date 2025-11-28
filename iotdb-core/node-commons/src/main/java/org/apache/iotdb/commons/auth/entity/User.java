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
package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.utils.SerializeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TListUserInfo;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** This class contains all information of a User. */
public class User extends Role {

  public static final long INTERNAL_USER_END_ID = 9999;

  public static final long INTERNAL_SYSTEM_ADMIN = 1;
  public static final long INTERNAL_SECURITY_ADMIN = 2;
  public static final long INTERNAL_AUDIT_ADMIN = 3;

  private long userId = -1;

  private String password;

  private Set<String> roleSet;

  private boolean isOpenIdUser = false; // default NO openIdUser

  public User() {
    // empty constructor
  }

  @TestOnly
  public User(String name, String password) {
    super(name);
    this.password = password;
    this.roleSet = new HashSet<>();
  }

  /**
   * construct function for User.
   *
   * @param name -user name
   * @param password -user password
   * @param userId -user index
   */
  public User(String name, String password, long userId) {
    super(name);
    this.password = password;
    this.userId = userId;
    this.roleSet = new HashSet<>();
  }

  /** ---------- set func ---------------* */
  public void setPassword(String password) {
    this.password = password;
  }

  public void setOpenIdUser(boolean openIdUser) {
    isOpenIdUser = openIdUser;
  }

  public void setRoleSet(Set<String> roles) {
    roleSet = roles;
  }

  public void addRole(String roleName) {
    roleSet.add(roleName);
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  /** ------------ get func ----------------* */
  public long getUserId() {
    return userId;
  }

  public String getPassword() {
    return password;
  }

  public boolean isOpenIdUser() {
    return isOpenIdUser;
  }

  public boolean hasRole(String role) {
    return roleSet.contains(role);
  }

  public Set<String> getRoleSet() {
    return roleSet;
  }

  public TUserResp getUserInfo(ModelType modelType) {
    TUserResp resp = new TUserResp();
    resp.setPermissionInfo(getRoleInfo(modelType));
    resp.setPassword(password);
    resp.setIsOpenIdUser(isOpenIdUser);
    resp.setRoleSet(roleSet);
    resp.setUserId(userId);
    return resp;
  }

  public TListUserInfo convertToListUserInfo() {
    TListUserInfo userInfo = new TListUserInfo();
    userInfo.setUserId(userId);
    userInfo.setUsername(name);
    userInfo.setMaxSessionPerUser(maxSessionPerUser);
    userInfo.setMinSessionPerUser(minSessionPerUser);
    return userInfo;
  }

  /** -------------- misc ----------------* */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    User user = (User) o;

    return contentEquals(user);
  }

  private boolean contentEquals(User user) {
    return super.equals((Role) user)
        && Objects.equals(roleSet, user.roleSet)
        && Objects.equals(password, user.password)
        && Objects.equals(isOpenIdUser, user.isOpenIdUser);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.getName(),
        password,
        super.getPathPrivilegeList(),
        super.getSysPrivilege(),
        roleSet,
        isOpenIdUser);
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serialize(super.getName(), dataOutputStream);
    SerializeUtils.serialize(password, dataOutputStream);

    try {
      dataOutputStream.writeInt(super.getSysPrivilege().size());
      for (PrivilegeType item : super.getSysPrivilege()) {
        dataOutputStream.writeInt(item.ordinal());
      }
      dataOutputStream.writeInt(super.getSysPriGrantOpt().size());
      for (PrivilegeType item : super.getSysPriGrantOpt()) {
        dataOutputStream.writeInt(item.ordinal());
      }
      dataOutputStream.writeInt(super.getPathPrivilegeList().size());
      for (PathPrivilege pathPrivilege : super.getPathPrivilegeList()) {
        dataOutputStream.write(pathPrivilege.serialize().array());
      }
    } catch (IOException e) {
      // unreachable
    }
    SerializeUtils.serializeStringList(new ArrayList<>(roleSet), dataOutputStream);

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    super.setName(SerializeUtils.deserializeString(buffer));
    password = SerializeUtils.deserializeString(buffer);
    int systemPriSize = buffer.getInt();
    Set<PrivilegeType> sysPri = new HashSet<>();
    for (int i = 0; i < systemPriSize; i++) {
      sysPri.add(PrivilegeType.values()[buffer.getInt()]);
    }
    super.setSysPrivilegeSet(sysPri);
    int sysPriGrantOptSize = buffer.getInt();
    Set<PrivilegeType> grantOpt = new HashSet<>();
    for (int i = 0; i < sysPriGrantOptSize; i++) {
      grantOpt.add(PrivilegeType.values()[buffer.getInt()]);
    }
    super.setSysPriGrantOpt(grantOpt);

    int privilegeListSize = buffer.getInt();
    List<PathPrivilege> privilegeList = new ArrayList<>(privilegeListSize);
    for (int i = 0; i < privilegeListSize; i++) {
      PathPrivilege pathPrivilege = new PathPrivilege();
      pathPrivilege.deserialize(buffer);
      privilegeList.add(pathPrivilege);
    }
    super.setPrivilegeList(privilegeList);
    roleSet = new HashSet<>(SerializeUtils.deserializeStringList(buffer));
  }

  /**
   * TestOnly, get the string representation of the user.
   *
   * @return string representation of the user
   */
  @Override
  public String toString() {
    return "User{"
        + "id="
        + userId
        + ", name='"
        + super.getName()
        + '\''
        + ", pathPrivilegeList="
        + pathPrivilegeList
        + ", sysPrivilegeSet="
        + priSetToString(sysPrivilegeSet, sysPriGrantOpt)
        + ", AnyScopePrivilegeMap="
        + priSetToString(anyScopePrivilegeSet, anyScopePrivilegeGrantOptSet)
        + ", objectPrivilegeMap="
        + objectPrivilegeMap
        + ", roleList="
        + roleSet
        + ", isOpenIdUser="
        + isOpenIdUser
        + '}';
  }
}
