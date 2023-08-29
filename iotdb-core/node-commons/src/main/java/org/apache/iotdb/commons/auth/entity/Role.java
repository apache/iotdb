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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
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

/** This class contains all information of a role. */
public class Role {

  private String name;
  private List<PathPrivilege> pathPrivilegeList;

  private Set<Integer> sysPrivilegeSet;

  private Set<Integer> sysPriGrantOpt;

  private static final int SYS_PRI_SIZE = PrivilegeType.getSysPriCount();

  public Role() {
    // empty constructor
  }

  public Role(String name) {
    this.name = name;
    this.pathPrivilegeList = new ArrayList<>();
    this.sysPrivilegeSet = new HashSet<>();
    this.sysPriGrantOpt = new HashSet<>();
  }

  /** ------------- get func -----------------* */
  public String getName() {
    return name;
  }

  public List<PathPrivilege> getPathPrivilegeList() {
    return pathPrivilegeList;
  }

  public Set<Integer> getSysPrivilege() {
    return sysPrivilegeSet;
  }

  public Set<Integer> getPathPrivileges(PartialPath path) throws AuthException {
    return AuthUtils.getPrivileges(path, pathPrivilegeList);
  }

  public Set<Integer> getSysPriGrantOpt() {
    return sysPriGrantOpt;
  }

  public int getAllSysPrivileges() {
    int privs = 0;
    for (Integer sysPri : sysPrivilegeSet) {
      privs |= (0b1 << sysPriTopos(sysPri));
    }
    for (Integer sysPriGrantOpt : sysPriGrantOpt) {
      privs |= 0b1 << (sysPriTopos(sysPriGrantOpt) + 16);
    }
    return privs;
  }

  /** -------------- set func ----------------* */
  public void setName(String name) {
    this.name = name;
  }

  public void setPrivilegeList(List<PathPrivilege> privilegeList) {
    this.pathPrivilegeList = privilegeList;
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

  private int posToSysPri(int pos) {
    switch (pos) {
      case 0:
        return PrivilegeType.MANAGE_DATABASE.ordinal();
      case 1:
        return PrivilegeType.MANAGE_USER.ordinal();
      case 2:
        return PrivilegeType.MANAGE_ROLE.ordinal();
      case 3:
        return PrivilegeType.USE_TRIGGER.ordinal();
      case 4:
        return PrivilegeType.USE_UDF.ordinal();
      case 5:
        return PrivilegeType.USE_CQ.ordinal();
      case 6:
        return PrivilegeType.USE_PIPE.ordinal();
      case 7:
        return PrivilegeType.EXTEND_TEMPLATE.ordinal();
      case 8:
        return PrivilegeType.AUDIT.ordinal();
      case 9:
        return PrivilegeType.MAINTAIN.ordinal();
      default:
        return -1;
    }
  }

  private int sysPriTopos(int privilegeId) {
    PrivilegeType type = PrivilegeType.values()[privilegeId];
    switch (type) {
      case MANAGE_DATABASE:
        return 0;
      case MANAGE_USER:
        return 1;
      case MANAGE_ROLE:
        return 2;
      case USE_TRIGGER:
        return 3;
      case USE_UDF:
        return 4;
      case USE_CQ:
        return 5;
      case USE_PIPE:
        return 6;
      case EXTEND_TEMPLATE:
        return 7;
      case AUDIT:
        return 8;
      case MAINTAIN:
        return 9;
      default:
        return -1;
    }
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
        sysPrivilegeSet.add(posToSysPri(i));
      }
      if ((privilegeMask & (1 << (i + 16))) != 0) {
        sysPriGrantOpt.add(posToSysPri(i));
      }
    }
  }

  public void addSysPrivilege(int privilegeId) {
    sysPrivilegeSet.add(privilegeId);
  }

  public void removeSysPrivilege(int privilegeId) {
    sysPrivilegeSet.remove(privilegeId);
  }

  /** ------------ check func ---------------* */
  public boolean hasPrivilege(PartialPath path, int privilegeId) {
    if (path == null) {
      return sysPrivilegeSet.contains(privilegeId);
    } else {
      return AuthUtils.hasPrivilege(path, privilegeId, pathPrivilegeList);
    }
  }

  public boolean checkPathPrivilege(PartialPath path, int privilegeId) {
    return AuthUtils.checkPathPrivilege(path, privilegeId, pathPrivilegeList);
  }

  public boolean checkPathPrivilegeGrantOpt(PartialPath path, int privilegeId) {
    return AuthUtils.checkPathPrivilegeGrantOpt(path, privilegeId, pathPrivilegeList);
  }

  public boolean checkSysPrivilege(int privilegeId) {
    return sysPrivilegeSet.contains(privilegeId);
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
        && Objects.equals(sysPriGrantOpt, role.sysPriGrantOpt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, pathPrivilegeList, sysPrivilegeSet);
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
        + sysPrivilegeSet
        + '}';
  }
}
