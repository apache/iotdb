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

  private String password;

  private List<String> roleList;

  private boolean isOpenIdUser = false; // default NO openIdUser

  private boolean useWaterMark = false; // default NO watermark

  public User() {
    // empty constructor
  }

  /**
   * construct function for User.
   *
   * @param name -user name
   * @param password -user password
   */
  public User(String name, String password) {
    super(name);
    this.password = password;
    this.roleList = new ArrayList<>();
  }

  /** ---------- set func ---------------* */
  public void setPassword(String password) {
    this.password = password;
  }

  public void setUseWaterMark(boolean useWaterMark) {
    this.useWaterMark = useWaterMark;
  }

  public void setOpenIdUser(boolean openIdUser) {
    isOpenIdUser = openIdUser;
  }

  public void setRoleList(List<String> roles) {
    roleList = roles;
  }

  /** ------------ get func ----------------* */
  public String getPassword() {
    return password;
  }

  public boolean isUseWaterMark() {
    return useWaterMark;
  }

  public boolean isOpenIdUser() {
    return isOpenIdUser;
  }

  public boolean hasRole(String role) {
    return roleList.contains(role);
  }

  public List<String> getRoleList() {
    return roleList;
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
    return Objects.equals(super.getName(), user.getName())
        && Objects.equals(password, user.password)
        && Objects.equals(super.getPathPrivilegeList(), user.getPathPrivilegeList())
        && Objects.equals(super.getSysPrivilege(), user.getSysPrivilege())
        && Objects.equals(roleList, user.roleList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.getName(),
        password,
        super.getPathPrivilegeList(),
        super.getSysPrivilege(),
        roleList,
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
      for (Integer item : super.getSysPrivilege()) {
        dataOutputStream.writeInt(item);
      }
      dataOutputStream.writeInt(super.getSysPriGrantOpt().size());
      for (Integer item : super.getSysPriGrantOpt()) {
        dataOutputStream.writeInt(item);
      }
      dataOutputStream.writeInt(super.getPathPrivilegeList().size());
      for (PathPrivilege pathPrivilege : super.getPathPrivilegeList()) {
        dataOutputStream.write(pathPrivilege.serialize().array());
      }
      dataOutputStream.writeBoolean(useWaterMark);
    } catch (IOException e) {
      // unreachable
    }
    SerializeUtils.serializeStringList(roleList, dataOutputStream);

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    super.setName(SerializeUtils.deserializeString(buffer));
    password = SerializeUtils.deserializeString(buffer);
    int systemPriSize = buffer.getInt();
    Set<Integer> sysPri = new HashSet<>();
    for (int i = 0; i < systemPriSize; i++) {
      sysPri.add(buffer.getInt());
    }
    super.setSysPrivilegeSet(sysPri);
    int sysPriGrantOptSize = buffer.getInt();
    Set<Integer> grantOpt = new HashSet<>();
    for (int i = 0; i < sysPriGrantOptSize; i++) {
      grantOpt.add(buffer.getInt());
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
    useWaterMark = buffer.get() == 1;
    roleList = SerializeUtils.deserializeStringList(buffer);
  }

  @Override
  public String toString() {
    return "User{"
        + "name='"
        + super.getName()
        + '\''
        + ", password='"
        + password
        + '\''
        + ", pathPrivilegeList="
        + super.getPathPrivilegeList()
        + ", sysPrivilegeSet="
        + super.getSysPrivilege()
        + ", roleList="
        + roleList
        + ", isOpenIdUser="
        + isOpenIdUser
        + ", useWaterMark="
        + useWaterMark
        + '}';
  }
}
