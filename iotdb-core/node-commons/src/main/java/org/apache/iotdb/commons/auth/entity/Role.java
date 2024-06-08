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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** This class contains all information of a role. */
public class Role {

  private String name;
  private List<PathPrivilege> pathPrivilegeList;

  private Set<Integer> sysPrivilegeSet;

  private Set<Integer> sysPriGrantOpt;

  private static final int SYS_PRI_SIZE = PrivilegeType.getSysPriCount();

  private Map<String, ObjectPrivilege> objectPrivileges;

  private boolean serviceReady = true;

  public Role() {
    // empty constructor
  }

  public Role(String name) {
    this.name = name;
    this.pathPrivilegeList = new ArrayList<>();
    this.sysPrivilegeSet = new HashSet<>();
    this.sysPriGrantOpt = new HashSet<>();
    this.objectPrivileges = new HashMap<>();
  }

  /** ------------- get func -----------------* */
  public String getName() {
    return name;
  }

  public List<PathPrivilege> getPathPrivilegeList() {
    return pathPrivilegeList;
  }

  public Map<String, ObjectPrivilege> getObjectPrivileges() {
    return objectPrivileges;
  }

  public Set<Integer> getSysPrivilege() {
    return sysPrivilegeSet;
  }

  public Set<Integer> getPathPrivileges(PartialPath path) {
    return AuthUtils.getPrivileges(path, pathPrivilegeList);
  }

  public Set<Integer> getSysPriGrantOpt() {
    return sysPriGrantOpt;
  }

  public int getAllSysPrivileges() {
    int privs = 0;
    for (Integer sysPri : sysPrivilegeSet) {
      privs |= 1 << AuthUtils.sysPriTopos(sysPri);
    }
    for (Integer sysGrantOpt : sysPriGrantOpt) {
      privs |= 1 << (AuthUtils.sysPriTopos(sysGrantOpt) + 16);
    }
    return privs;
  }

  public boolean getServiceReady() {
    return serviceReady;
  }

  /** -------------- set func ----------------* */
  public void setName(String name) {
    this.name = name;
  }

  public void setPrivilegeList(List<PathPrivilege> privilegeList) {
    this.pathPrivilegeList = privilegeList;
  }

  public void setObjectPrivileges(Map<String, ObjectPrivilege> objectPrivilegeMap) {
    this.objectPrivileges = objectPrivilegeMap;
  }

  public void setPathPrivileges(PartialPath path, Set<Integer> privileges) {
    for (PathPrivilege pathPrivilege : pathPrivilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        pathPrivilege.setPrivileges(privileges);
      }
    }
  }

  public void addPathPrivilege(PartialPath path, int privilegeId, boolean grantOpt) {
    AuthUtils.addPrivilege(path, privilegeId, pathPrivilegeList, grantOpt);
  }

  public void removePathPrivilege(PartialPath path, int privilegeId) {
    AuthUtils.removePrivilege(path, privilegeId, pathPrivilegeList);
  }

  public void setSysPrivilegeSet(Set<Integer> privilegeSet) {
    this.sysPrivilegeSet = privilegeSet;
  }

  public void setSysPriGrantOpt(Set<Integer> grantOpt) {
    this.sysPriGrantOpt = grantOpt;
  }

  public void setSysPrivilegeSet(int privilegeMask) {
    if (sysPrivilegeSet == null) {
      sysPrivilegeSet = new HashSet<>();
    }
    if (sysPriGrantOpt == null) {
      sysPriGrantOpt = new HashSet<>();
    }
    for (int i = 0; i < SYS_PRI_SIZE; i++) {
      if ((privilegeMask & (1 << i)) != 0) {
        sysPrivilegeSet.add(AuthUtils.posToSysPri(i));
      }
      if ((privilegeMask & (1 << (i + 16))) != 0) {
        sysPriGrantOpt.add(AuthUtils.posToSysPri(i));
      }
    }
  }

  public void addSysPrivilege(int privilegeId) {
    sysPrivilegeSet.add(privilegeId);
  }

  public void removeSysPrivilege(int privilegeId) {
    sysPrivilegeSet.remove(privilegeId);
  }

  public void setServiceReady(boolean ready) {
    serviceReady = ready;
  }

  /** ------------ check func ---------------* */
  public boolean hasPrivilegeToRevoke(PartialPath path, int privilegeId) {
    if (path == null) {
      return sysPrivilegeSet.contains(privilegeId);
    } else {
      return AuthUtils.hasPrivilegeToReovke(path, privilegeId, pathPrivilegeList);
    }
  }

  public boolean hasObjectPrivilegeToRevoke(
      String databaseName, String tableName, PrivilegeType type) {
    if (!objectPrivileges.containsKey(databaseName)) {
      return false;
    }
    ObjectPrivilege objectPrivilege = this.objectPrivileges.get(databaseName);
    if (tableName == null) {
      return objectPrivilege.checkDBPrivilege(type);
    } else {
      return objectPrivilege.checkTablePrivilege(tableName, type);
    }
  }

  public boolean checkPathPrivilege(PartialPath path, int privilegeId) {
    return AuthUtils.checkPathPrivilege(path, privilegeId, pathPrivilegeList);
  }

  public boolean checkObjectPrivilege(String databaseName, String tableName, PrivilegeType type) {
    if (!this.objectPrivileges.containsKey(databaseName)) {
      return false;
    }
    ObjectPrivilege objectPrivilege = this.objectPrivileges.get(databaseName);
    if (tableName == null) {
      return objectPrivilege.checkDBPrivilege(type);
    } else {
      return objectPrivilege.checkTablePrivilege(tableName, type);
    }
  }

  public boolean checkObjectPrivilegeGrantOpt(
      String databaseName, String tableName, PrivilegeType type) {
    if (!this.objectPrivileges.containsKey(databaseName)) {
      return false;
    }
    ObjectPrivilege objectPrivileges = this.objectPrivileges.get(databaseName);
    if (tableName == null) {
      return objectPrivileges.checkDBGrantOption(type);
    } else {
      return objectPrivileges.checkTableGrantOption(tableName, type);
    }
  }

  public boolean checkPathPrivilegeGrantOpt(PartialPath path, int privilegeId) {
    return AuthUtils.checkPathPrivilegeGrantOpt(path, privilegeId, pathPrivilegeList);
  }

  public boolean checkSysPrivilege(int privilegeId) {
    return sysPrivilegeSet.contains(privilegeId);
  }

  public boolean checkSysPriGrantOpt(int privilegeId) {
    return sysPrivilegeSet.contains(privilegeId) && sysPriGrantOpt.contains(privilegeId);
  }

  /** ----------- misc --------------------* */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Role role = (Role) o;
    return Objects.equals(name, role.name)
        && Objects.equals(pathPrivilegeList, role.pathPrivilegeList)
        && Objects.equals(sysPrivilegeSet, role.sysPrivilegeSet)
        && Objects.equals(sysPriGrantOpt, role.sysPriGrantOpt)
        && Objects.equals(this.objectPrivileges, role.objectPrivileges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, pathPrivilegeList, sysPrivilegeSet, objectPrivileges);
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serialize(name, dataOutputStream);

    try {
      dataOutputStream.writeInt(sysPrivilegeSet.size());
      for (Integer item : sysPrivilegeSet) {
        dataOutputStream.writeInt(item);
      }
      dataOutputStream.writeInt(sysPriGrantOpt.size());
      for (Integer item : sysPriGrantOpt) {
        dataOutputStream.writeInt(item);
      }
      dataOutputStream.writeInt(pathPrivilegeList.size());
      for (PathPrivilege pathPrivilege : pathPrivilegeList) {
        dataOutputStream.write(pathPrivilege.serialize().array());
      }
      dataOutputStream.writeInt(objectPrivileges.size());
      for (Map.Entry<String, ObjectPrivilege> objectPrivilegeEntry : objectPrivileges.entrySet()) {
        dataOutputStream.write(objectPrivilegeEntry.getValue().serialize().array());
      }
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    name = SerializeUtils.deserializeString(buffer);
    int sysPrivilegeSize = buffer.getInt();
    sysPrivilegeSet = new HashSet<>();
    for (int i = 0; i < sysPrivilegeSize; i++) {
      sysPrivilegeSet.add(buffer.getInt());
    }
    int sysPriGrantOptSize = buffer.getInt();
    sysPriGrantOpt = new HashSet<>();
    for (int i = 0; i < sysPriGrantOptSize; i++) {
      sysPriGrantOpt.add(buffer.getInt());
    }
    int privilegeListSize = buffer.getInt();
    pathPrivilegeList = new ArrayList<>(privilegeListSize);
    for (int i = 0; i < privilegeListSize; i++) {
      PathPrivilege pathPrivilege = new PathPrivilege();
      pathPrivilege.deserialize(buffer);
      pathPrivilegeList.add(pathPrivilege);
    }
    int objectSize = buffer.getInt();
    objectPrivileges = new HashMap<>();
    for (int i = 0; i < objectSize; i++) {
      ObjectPrivilege objectPrivilege = new ObjectPrivilege();
      objectPrivilege.deserialize(buffer);
      this.objectPrivileges.put(objectPrivilege.getDatabaseName(), objectPrivilege);
    }
  }

  @Override
  public String toString() {
    return "Role{"
        + "name='"
        + name
        + '\''
        + ", pathPrivilegeList="
        + pathPrivilegeList
        + ", systemPrivilegeSet="
        + sysPriToString()
        + ", objectPrivilegesMap"
        + this.objectPrivileges
        + '}';
  }

  private Set<String> sysPriToString() {
    Set<String> priSet = new HashSet<>();
    for (Integer pri : sysPrivilegeSet) {
      StringBuilder str = new StringBuilder(String.valueOf(PrivilegeType.values()[pri].toString()));
      if (sysPriGrantOpt.contains(pri)) {
        str.append("_with_grant_option ");
      }
      priSet.add(str.toString());
    }
    return priSet;
  }
}
