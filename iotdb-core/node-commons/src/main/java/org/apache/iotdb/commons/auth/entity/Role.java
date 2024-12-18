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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TDBPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TTablePrivilege;

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

public class Role {

  protected String name;
  protected List<PathPrivilege> pathPrivilegeList;

  protected Map<String, DatabasePrivilege> objectPrivilegeMap;

  protected Set<PrivilegeType> sysPrivilegeSet;

  protected Set<PrivilegeType> sysPriGrantOpt;

  protected Set<PrivilegeType> anyScopePrivilegeSet;

  protected Set<PrivilegeType> anyScopePrivileGrantOptSet;

  @TestOnly
  public Role() {
    this.pathPrivilegeList = new ArrayList<>();
    this.sysPrivilegeSet = new HashSet<>();
    this.sysPriGrantOpt = new HashSet<>();
    this.objectPrivilegeMap = new HashMap<>();
    this.anyScopePrivileGrantOptSet = new HashSet<>();
    this.anyScopePrivilegeSet = new HashSet<>();
  }

  public Role(String name) {
    this.name = name;
    this.pathPrivilegeList = new ArrayList<>();
    this.sysPrivilegeSet = new HashSet<>();
    this.sysPriGrantOpt = new HashSet<>();
    this.objectPrivilegeMap = new HashMap<>();
    this.anyScopePrivileGrantOptSet = new HashSet<>();
    this.anyScopePrivilegeSet = new HashSet<>();
  }

  /** ------------- get func -----------------* */
  public String getName() {
    return name;
  }

  public List<PathPrivilege> getPathPrivilegeList() {
    return pathPrivilegeList;
  }

  public Set<PrivilegeType> getSysPrivilege() {
    return sysPrivilegeSet;
  }

  private Set<Integer> getPrivilegeIntSet(Set<PrivilegeType> privs) {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType priv : privs) {
      res.add(priv.ordinal());
    }
    return res;
  }

  public Set<PrivilegeType> getPathPrivileges(PartialPath path) {
    return AuthUtils.getPrivileges(path, pathPrivilegeList);
  }

  public Set<PrivilegeType> getSysPriGrantOpt() {
    return sysPriGrantOpt;
  }

  public Set<PrivilegeType> getAnyScopePrivileGrantOpt() {
    return anyScopePrivileGrantOptSet;
  }

  public Set<PrivilegeType> getAnyScopePrivilegeSet() {
    return anyScopePrivilegeSet;
  }

  public Map<String, DatabasePrivilege> getObjectPrivilegeMap() {
    return objectPrivilegeMap;
  }

  public boolean checkObjectPrivilege(String objectName) {
    return this.objectPrivilegeMap.containsKey(objectName);
  }

  public DatabasePrivilege getObjectPrivilege(String objectName) {
    return this.objectPrivilegeMap.computeIfAbsent(
        objectName, k -> new DatabasePrivilege(objectName));
  }

  public List<TPathPrivilege> getPathPrivilegeInfo() {
    List<TPathPrivilege> privilegeList = new ArrayList<>();
    for (PathPrivilege pathPrivilege : pathPrivilegeList) {
      TPathPrivilege tpathPriv = new TPathPrivilege();
      tpathPriv.setPath(pathPrivilege.getPath().getFullPath());
      tpathPriv.setPriSet(pathPrivilege.getPrivilegeIntSet());
      tpathPriv.setPriGrantOpt(pathPrivilege.getGrantOptIntSet());
      privilegeList.add(tpathPriv);
    }
    return privilegeList;
  }

  public void loadPathPrivilegeInfo(List<TPathPrivilege> pathinfo) throws MetadataException {
    for (TPathPrivilege tPathPrivilege : pathinfo) {
      PathPrivilege pathPri = new PathPrivilege();
      pathPri.setPath(new PartialPath(tPathPrivilege.getPath()));
      pathPri.setPrivilegesInt(tPathPrivilege.getPriSet());
      pathPri.setGrantOptInt(tPathPrivilege.getPriGrantOpt());
      pathPrivilegeList.add(pathPri);
    }
  }

  public Set<TDBPrivilege> getRelationalPrivilegeInfo() {
    Set<TDBPrivilege> privileges = new HashSet<>();
    for (DatabasePrivilege databasePrivilege : objectPrivilegeMap.values()) {
      TDBPrivilege tdbPrivilege = new TDBPrivilege();
      tdbPrivilege.setDatabasename(databasePrivilege.getDatabaseName());
      tdbPrivilege.setPrivileges(databasePrivilege.getPrivilegeSet());
      tdbPrivilege.setGrantOpt(databasePrivilege.getPrivilegeGrantOptSet());
      if (!databasePrivilege.getTablePrivilegeMap().isEmpty()) {
        for (TablePrivilege tablePrivilege : databasePrivilege.getTablePrivilegeMap().values()) {
          TTablePrivilege tTablePrivilege = new TTablePrivilege();
          tTablePrivilege.setTablename(tablePrivilege.getTableName());
          tTablePrivilege.setPrivileges(tablePrivilege.getPrivilegesIntSet());
          tTablePrivilege.setGrantOption(tablePrivilege.getGrantOptionIntSet());
          tdbPrivilege.putToTableinfo(tablePrivilege.getTableName(), tTablePrivilege);
        }
      } else {
        tdbPrivilege.setTableinfo(new HashMap<>());
      }
      privileges.add(tdbPrivilege);
    }
    return privileges;
  }

  public void loadRelationalPrivileInfo(Map<String, TDBPrivilege> info) {
    for (TDBPrivilege tdbPrivilege : info.values()) {
      DatabasePrivilege databasePrivilege = new DatabasePrivilege(tdbPrivilege.getDatabasename());
      for (Integer privId : tdbPrivilege.getPrivileges()) {
        databasePrivilege.grantDBObjectPrivilege(PrivilegeType.values()[privId]);
      }
      for (Integer privId : tdbPrivilege.getGrantOpt()) {
        databasePrivilege.grantDBGrantOption(PrivilegeType.values()[privId]);
      }
      if (tdbPrivilege.getTableinfoSize() != 0) {
        for (TTablePrivilege tablePrivilege : tdbPrivilege.getTableinfo().values()) {
          for (Integer privId : tablePrivilege.getPrivileges()) {
            databasePrivilege.grantTableObjectPrivilege(
                tablePrivilege.getTablename(), PrivilegeType.values()[privId]);
          }
          for (Integer privId : tablePrivilege.getGrantOption()) {
            databasePrivilege.grantTableObejctGrantOption(
                tablePrivilege.getTablename(), PrivilegeType.values()[privId]);
          }
        }
      }
      this.objectPrivilegeMap.put(tdbPrivilege.getDatabasename(), databasePrivilege);
    }
  }

  public TRoleResp getRoleInfo(ModelType modelType) {
    TRoleResp roleResp = new TRoleResp();
    roleResp.setName(name);
    switch (modelType) {
      case RELATIONAL:
        Set<Integer> privs = new HashSet<>();
        for (PrivilegeType priv : sysPrivilegeSet) {
          if (priv.forRelationalSys()) {
            privs.add(priv.ordinal());
          }
        }
        roleResp.setSysPriSet(privs);
        privs.clear();
        for (PrivilegeType priv : sysPriGrantOpt) {
          if (priv.forRelationalSys()) {
            privs.add(priv.ordinal());
          }
        }
        roleResp.setSysPriSetGrantOpt(privs);
        roleResp.setAnyScopeSet(getPrivilegeIntSet(anyScopePrivilegeSet));
        roleResp.setAnyScopeGrantSet(getPrivilegeIntSet(anyScopePrivileGrantOptSet));
        roleResp.setPrivilegeList(new ArrayList<>());
        Set<TDBPrivilege> tdbPrivileges = getRelationalPrivilegeInfo();
        roleResp.setDbPrivilegeMap(new HashMap<>());
        for (TDBPrivilege tdbPrivilege : tdbPrivileges) {
          roleResp.putToDbPrivilegeMap(tdbPrivilege.getDatabasename(), tdbPrivilege);
        }
        break;
      case TREE:
        roleResp.setSysPriSet(getPrivilegeIntSet(sysPrivilegeSet));
        roleResp.setSysPriSetGrantOpt(getPrivilegeIntSet(sysPriGrantOpt));
        roleResp.setAnyScopeSet(new HashSet<>());
        roleResp.setAnyScopeGrantSet(new HashSet<>());
        roleResp.setPrivilegeList(getPathPrivilegeInfo());
        roleResp.setDbPrivilegeMap(new HashMap<>());
        break;
      case ALL:
        roleResp.setSysPriSet(getPrivilegeIntSet(sysPrivilegeSet));
        roleResp.setSysPriSetGrantOpt(getPrivilegeIntSet(sysPriGrantOpt));
        roleResp.setAnyScopeSet(getPrivilegeIntSet(anyScopePrivilegeSet));
        roleResp.setAnyScopeGrantSet(getPrivilegeIntSet(anyScopePrivileGrantOptSet));
        roleResp.setPrivilegeList(getPathPrivilegeInfo());
        Set<TDBPrivilege> tdbPrivileges1 = getRelationalPrivilegeInfo();
        roleResp.setDbPrivilegeMap(new HashMap<>());
        for (TDBPrivilege tdbPrivilege : tdbPrivileges1) {
          roleResp.putToDbPrivilegeMap(tdbPrivilege.getDatabasename(), tdbPrivilege);
        }
        break;
    }
    return roleResp;
  }

  /** -------------- set func ----------------* */
  public void setName(String name) {
    this.name = name;
  }

  public void setPrivilegeList(List<PathPrivilege> privilegeList) {
    this.pathPrivilegeList = privilegeList;
  }

  public void setPathPrivileges(PartialPath path, Set<PrivilegeType> privileges) {
    for (PathPrivilege pathPrivilege : pathPrivilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        pathPrivilege.setPrivileges(privileges);
      }
    }
  }

  public void setObjectPrivilegeMap(Map<String, DatabasePrivilege> privilegeMap) {
    this.objectPrivilegeMap = privilegeMap;
  }

  public void grantPathPrivilege(PartialPath path, PrivilegeType priv, boolean grantOpt) {
    AuthUtils.addPrivilege(path, priv, pathPrivilegeList, grantOpt);
  }

  public void revokePathPrivilege(PartialPath path, PrivilegeType priv) {
    AuthUtils.removePrivilege(path, priv, pathPrivilegeList);
  }

  public void setSysPrivilegeSet(Set<PrivilegeType> privilegeSet) {
    this.sysPrivilegeSet = privilegeSet;
  }

  public void setSysPrivilegeSetInt(Set<Integer> privilegeSet) {
    for (Integer priv : privilegeSet) {
      this.sysPrivilegeSet.add(PrivilegeType.values()[priv]);
    }
  }

  public void setSysPrivilegesWithMask(int privMask) {
    final int SYS_PRI_SIZE = PrivilegeType.getPrivilegeCount(PrivilegeModelType.SYSTEM);
    for (int i = 0; i < SYS_PRI_SIZE; i++) {
      if ((privMask & (1 << i)) != 0) {
        sysPrivilegeSet.add(AuthUtils.posToSysPri(i));
        if ((privMask & (1 << (i + 16))) != 0) {
          sysPriGrantOpt.add(AuthUtils.posToSysPri(i));
        }
      }
    }
  }

  public void setAnyScopePrivilegeSetWithMask(int privMask) {
    final int PRI_COUNT = PrivilegeType.getPrivilegeCount(PrivilegeModelType.RELATIONAL);
    for (int i = 0; i < PRI_COUNT; i++) {
      if ((privMask & (1 << i)) != 0) {
        anyScopePrivilegeSet.add(AuthUtils.posToObjPri(i));
        if ((privMask & (1 << (i + 16))) != 0) {
          anyScopePrivileGrantOptSet.add(AuthUtils.posToObjPri(i));
        }
      }
    }
  }

  public void setSysPriGrantOpt(Set<PrivilegeType> grantOpt) {
    this.sysPriGrantOpt = grantOpt;
  }

  public void setSysPriGrantOptInt(Set<Integer> grantOptInt) {
    for (Integer priv : grantOptInt) {
      this.sysPriGrantOpt.add(PrivilegeType.values()[priv]);
    }
  }

  public void grantSysPrivilege(PrivilegeType priv, boolean grantOpt) {
    sysPrivilegeSet.add(priv);
    if (grantOpt) {
      sysPriGrantOpt.add(priv);
    }
  }

  private DatabasePrivilege getObjectPrivilegeInternal(String dbName) {
    return objectPrivilegeMap.computeIfAbsent(dbName, DatabasePrivilege::new);
  }

  public void grantAnyScopePrivilege(PrivilegeType priv, boolean grantOpt) {
    anyScopePrivilegeSet.add(priv);
    if (grantOpt) {
      anyScopePrivileGrantOptSet.add(priv);
    }
  }

  public void revokeAnyScopePrivilege(PrivilegeType priv) {
    anyScopePrivilegeSet.remove(priv);
    anyScopePrivileGrantOptSet.remove(priv);
  }

  public void grantDBPrivilege(String dbName, PrivilegeType priv, boolean grantOption) {
    DatabasePrivilege databasePrivilege = getObjectPrivilege(dbName);
    databasePrivilege.grantDBObjectPrivilege(priv);
    if (grantOption) {
      databasePrivilege.grantDBGrantOption(priv);
    }
  }

  public void grantTBPrivilege(
      String dbName, String tbName, PrivilegeType priv, boolean grantOption) {
    DatabasePrivilege databasePrivilege = getObjectPrivilege(dbName);
    databasePrivilege.grantTableObjectPrivilege(tbName, priv);
    if (grantOption) {
      databasePrivilege.grantTableObejctGrantOption(tbName, priv);
    }
  }

  public void revokeDBPrivilege(String dbName, PrivilegeType priv) {
    DatabasePrivilege databasePrivilege = getObjectPrivilegeInternal(dbName);
    databasePrivilege.revokeDBObjectPrivilege(priv);
    databasePrivilege.revokeGrantOptionFromDB(priv);
  }

  public void revokeTBPrivilege(String dbName, String tbName, PrivilegeType priv) {
    DatabasePrivilege databasePrivilege = getObjectPrivilegeInternal(dbName);
    databasePrivilege.revokeTableObjectGrantOption(tbName, priv);
    databasePrivilege.revokeTableObjectPrivilege(tbName, priv);
    if (databasePrivilege.getTablePrivilegeMap().isEmpty()) {
      this.objectPrivilegeMap.remove(dbName);
    }
  }

  public void grantSysPrivilegeGrantOption(PrivilegeType priv) {
    sysPriGrantOpt.add(priv);
  }

  public void revokeSysPrivilege(PrivilegeType priv) {
    sysPrivilegeSet.remove(priv);
  }

  /** ------------ check func ---------------* */
  public boolean hasPrivilegeToRevoke(PartialPath path, PrivilegeType priv) {
    return AuthUtils.hasPrivilegeToRevoke(path, priv, pathPrivilegeList);
  }

  public boolean hasPrivilegeToRevoke(PrivilegeType priv) {
    return this.sysPrivilegeSet.contains(priv);
  }

  public boolean hasPrivilegeToRevoke(String dbname, PrivilegeType priv) {
    return checkDatabasePrivilege(dbname, priv);
  }

  public boolean hasPrivilegeToRevoke(String dbname, String tbname, PrivilegeType priv) {
    return this.objectPrivilegeMap.containsKey(dbname)
        && this.objectPrivilegeMap.get(dbname).checkTablePrivilege(tbname, priv);
  }

  public boolean checkDatabasePrivilege(String dbname, PrivilegeType priv) {
    return this.objectPrivilegeMap.containsKey(dbname)
        && this.objectPrivilegeMap.get(dbname).checkDBPrivilege(priv);
  }

  public boolean checkTablePrivilege(String dbname, String table, PrivilegeType priv) {
    return this.objectPrivilegeMap.containsKey(dbname)
        && (this.objectPrivilegeMap.get(dbname).checkDBPrivilege(priv)
            || this.objectPrivilegeMap.get(dbname).checkTablePrivilege(table, priv));
  }

  public boolean checkObjectPrivilegeGrantOpt(String dbname, String table, PrivilegeType priv) {
    DatabasePrivilege privilege = this.objectPrivilegeMap.get(dbname);
    if (privilege == null) {
      return false;
    }

    if (table.isEmpty()) {
      return privilege.checkDBGrantOption(priv);
    } else {
      return privilege.checkDBGrantOption(priv) || privilege.checkTableGrantOption(table, priv);
    }
  }

  public boolean checkPathPrivilege(PartialPath path, PrivilegeType priv) {
    return AuthUtils.checkPathPrivilege(path, priv, pathPrivilegeList);
  }

  public boolean checkPathPrivilegeGrantOpt(PartialPath path, PrivilegeType priv) {
    return AuthUtils.checkPathPrivilegeGrantOpt(path, priv, pathPrivilegeList);
  }

  public boolean checkSysPrivilege(PrivilegeType priv) {
    return sysPrivilegeSet.contains(priv);
  }

  public boolean checkSysPriGrantOpt(PrivilegeType priv) {
    return sysPrivilegeSet.contains(priv) && sysPriGrantOpt.contains(priv);
  }

  public boolean checkDBVisible(String database) {
    return !anyScopePrivilegeSet.isEmpty() || objectPrivilegeMap.containsKey(database);
  }

  public boolean checkTBVisible(String database, String tbName) {
    return !anyScopePrivilegeSet.isEmpty()
        || objectPrivilegeMap.containsKey(database)
            && objectPrivilegeMap.get(database).getTablePrivilegeMap().containsKey(tbName);
  }

  public int getAllSysPrivileges() {
    int privs = 0;
    for (PrivilegeType sysPri : sysPrivilegeSet) {
      privs |= 1 << AuthUtils.sysPriTopos(sysPri);
    }
    for (PrivilegeType sysGrantOpt : sysPriGrantOpt) {
      privs |= 1 << (AuthUtils.sysPriTopos(sysGrantOpt) + 16);
    }
    return privs;
  }

  public int getAnyScopePrivileges() {
    int privs = 0;
    for (PrivilegeType anyScope : anyScopePrivilegeSet) {
      privs |= 1 << AuthUtils.objPriToPos(anyScope);
    }
    for (PrivilegeType anyScopeGrantOpt : anyScopePrivileGrantOptSet) {
      privs |= 1 << (AuthUtils.objPriToPos(anyScopeGrantOpt) + 16);
    }
    return privs;
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
        && Objects.equals(objectPrivilegeMap, role.objectPrivilegeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, pathPrivilegeList, sysPrivilegeSet);
  }

  private void serializePrivileSet(DataOutputStream outputStream, Set<PrivilegeType> set)
      throws IOException {
    outputStream.writeInt(set.size());
    for (PrivilegeType priv : set) {
      outputStream.writeInt(priv.ordinal());
    }
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serialize(name, dataOutputStream);

    try {
      serializePrivileSet(dataOutputStream, sysPrivilegeSet);
      serializePrivileSet(dataOutputStream, sysPriGrantOpt);
      dataOutputStream.writeInt(pathPrivilegeList.size());
      for (PathPrivilege pathPrivilege : pathPrivilegeList) {
        dataOutputStream.write(pathPrivilege.serialize().array());
      }
      serializePrivileSet(dataOutputStream, anyScopePrivilegeSet);
      serializePrivileSet(dataOutputStream, anyScopePrivileGrantOptSet);
      dataOutputStream.writeInt(objectPrivilegeMap.size());
      for (Map.Entry<String, DatabasePrivilege> item : objectPrivilegeMap.entrySet()) {
        SerializeUtils.serialize(item.getKey(), dataOutputStream);
        dataOutputStream.write(item.getValue().serialize().array());
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
      sysPrivilegeSet.add(PrivilegeType.values()[buffer.getInt()]);
    }
    int sysPriGrantOptSize = buffer.getInt();
    sysPriGrantOpt = new HashSet<>();
    for (int i = 0; i < sysPriGrantOptSize; i++) {
      sysPriGrantOpt.add(PrivilegeType.values()[buffer.getInt()]);
    }
    int privilegeListSize = buffer.getInt();
    pathPrivilegeList = new ArrayList<>(privilegeListSize);
    for (int i = 0; i < privilegeListSize; i++) {
      PathPrivilege pathPrivilege = new PathPrivilege();
      pathPrivilege.deserialize(buffer);
      pathPrivilegeList.add(pathPrivilege);
    }

    int objectPrivilegesSize = buffer.getInt();
    for (int i = 0; i < objectPrivilegesSize; i++) {
      DatabasePrivilege databasePrivilege = new DatabasePrivilege();
      String objectName = SerializeUtils.deserializeString(buffer);
      databasePrivilege.deserialize(buffer);
      this.objectPrivilegeMap.put(objectName, databasePrivilege);
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
        + ", objectPrivilegeSet="
        + objectPrivilegeMap
        + '}';
  }

  private Set<String> sysPriToString() {
    Set<String> priSet = new HashSet<>();
    for (PrivilegeType priv : sysPrivilegeSet) {
      StringBuilder str = new StringBuilder(String.valueOf(priv));
      if (sysPriGrantOpt.contains(priv)) {
        str.append("_with_grant_option ");
      }
      priSet.add(str.toString());
    }
    return priSet;
  }
}
