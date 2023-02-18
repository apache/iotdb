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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** This class contains all information of a role. */
public class Role {

  private String name;
  private List<PathPrivilege> privilegeList;

  public Role() {
    // empty constructor
  }

  public Role(String name) {
    this.name = name;
    this.privilegeList = new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<PathPrivilege> getPrivilegeList() {
    return privilegeList;
  }

  public void setPrivilegeList(List<PathPrivilege> privilegeList) {
    this.privilegeList = privilegeList;
  }

  public boolean hasPrivilege(String path, int privilegeId) {
    return AuthUtils.hasPrivilege(path, privilegeId, privilegeList);
  }

  public void addPrivilege(String path, int privilegeId) {
    AuthUtils.addPrivilege(path, privilegeId, privilegeList);
  }

  public void removePrivilege(String path, int privilegeId) {
    AuthUtils.removePrivilege(path, privilegeId, privilegeList);
  }

  /** set privileges of path. */
  public void setPrivileges(String path, Set<Integer> privileges) {
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        pathPrivilege.setPrivileges(privileges);
      }
    }
  }

  public Set<Integer> getPrivileges(String path) throws AuthException {
    return AuthUtils.getPrivileges(path, privilegeList);
  }

  public boolean checkPrivilege(String path, int privilegeId) throws AuthException {
    return AuthUtils.checkPrivilege(path, privilegeId, privilegeList);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Role role = (Role) o;
    return Objects.equals(name, role.name) && Objects.equals(privilegeList, role.privilegeList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, privilegeList);
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serialize(name, dataOutputStream);

    try {
      dataOutputStream.writeInt(privilegeList.size());
      for (PathPrivilege pathPrivilege : privilegeList) {
        dataOutputStream.write(pathPrivilege.serialize().array());
      }
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    name = SerializeUtils.deserializeString(buffer);
    int privilegeListSize = buffer.getInt();
    privilegeList = new ArrayList<>(privilegeListSize);
    for (int i = 0; i < privilegeListSize; i++) {
      PathPrivilege pathPrivilege = new PathPrivilege();
      pathPrivilege.deserialize(buffer);
      privilegeList.add(pathPrivilege);
    }
  }

  @Override
  public String toString() {
    return "Role{" + "name='" + name + '\'' + ", privilegeList=" + privilegeList + '}';
  }
}
