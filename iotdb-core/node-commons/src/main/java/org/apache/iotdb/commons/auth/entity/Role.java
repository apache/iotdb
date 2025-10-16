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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Role {

  protected String name;
  protected int maxSessionPerUser = -1;
  protected int minSessionPerUser = -1;
  protected List<PathPrivilege> pathPrivilegeList;

  protected Map<String, DatabasePrivilege> objectPrivilegeMap;

  protected Set<PrivilegeType> sysPrivilegeSet;

  protected Set<PrivilegeType> sysPriGrantOpt;

  protected Set<PrivilegeType> anyScopePrivilegeSet;

  protected Set<PrivilegeType> anyScopePrivilegeGrantOptSet;

  @TestOnly
  public Role() {
    this.pathPrivilegeList = new ArrayList<>();
    this.sysPrivilegeSet = new HashSet<>();
    this.sysPriGrantOpt = new HashSet<>();
    this.objectPrivilegeMap = new HashMap<>();
    this.anyScopePrivilegeGrantOptSet = new HashSet<>();
    this.anyScopePrivilegeSet = new HashSet<>();
  }

  public Role(String name) {
    this.name = name;
    this.pathPrivilegeList = new ArrayList<>();
    this.sysPrivilegeSet = new HashSet<>();
    this.sysPriGrantOpt = new HashSet<>();
    this.objectPrivilegeMap = new HashMap<>();
    this.anyScopePrivilegeGrantOptSet = new HashSet<>();
    this.anyScopePrivilegeSet = new HashSet<>();
  }

  /** ------------- get func -----------------* */
  public String getName() {
    return name;
  }

  public int getMaxSessionPerUser() {
    return maxSessionPerUser;
  }

  public int getMinSessionPerUser() {
    return minSessionPerUser;
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

  public Set<PrivilegeType> getAnyScopePrivilegeGrantOpt() {
    return anyScopePrivilegeGrantOptSet;
  }

  public Set<PrivilegeType> getAnyScopePrivilegeSet() {
    return anyScopePrivilegeSet;
  }

  public Map<String, DatabasePrivilege> getDBScopePrivilegeMap() {
    return objectPrivilegeMap;
  }

  private DatabasePrivilege getObjectPrivilege(String objectName) {
    return this.objectPrivilegeMap.computeIfAbsent(
        objectName, k -> new DatabasePrivilege(objectName));
  }

  public List<TPathPrivilege> getTreePrivilegeInfo() {
    List<TPathPrivilege> privilegeList = new ArrayList<>();
    for (PathPrivilege pathPrivilege : pathPrivilegeList) {
      TPathPrivilege pathPriv = new TPathPrivilege();
      pathPriv.setPath(pathPrivilege.getPath().getFullPath());
      pathPriv.setPriSet(pathPrivilege.getPrivilegeIntSet());
      pathPriv.setPriGrantOpt(pathPrivilege.getGrantOptIntSet());
      privilegeList.add(pathPriv);
    }
    return privilegeList;
  }

  public void loadTreePrivilegeInfo(List<TPathPrivilege> pathPrivilegeInfo)
      throws MetadataException {
    for (TPathPrivilege tPathPrivilege : pathPrivilegeInfo) {
      PathPrivilege pathPri = new PathPrivilege();
      pathPri.setPath(new PartialPath(tPathPrivilege.getPath()));
      pathPri.setPrivilegesInt(tPathPrivilege.getPriSet());
      pathPri.setGrantOptInt(tPathPrivilege.getPriGrantOpt());
      pathPrivilegeList.add(pathPri);
    }
  }

  public Set<TDBPrivilege> getDatabaseAndTablePrivilegeInfo() {
    Set<TDBPrivilege> privileges = new HashSet<>();
    for (DatabasePrivilege databasePrivilege : objectPrivilegeMap.values()) {
      TDBPrivilege tdbPrivilege = new TDBPrivilege();
      tdbPrivilege.setDatabaseName(databasePrivilege.getDatabaseName());
      tdbPrivilege.setPrivileges(databasePrivilege.getPrivilegeSet());
      tdbPrivilege.setGrantOpt(databasePrivilege.getPrivilegeGrantOptSet());
      if (!databasePrivilege.getTablePrivilegeMap().isEmpty()) {
        for (TablePrivilege tablePrivilege : databasePrivilege.getTablePrivilegeMap().values()) {
          TTablePrivilege tTablePrivilege = new TTablePrivilege();
          tTablePrivilege.setTableName(tablePrivilege.getTableName());
          tTablePrivilege.setPrivileges(tablePrivilege.getPrivilegesIntSet());
          tTablePrivilege.setGrantOption(tablePrivilege.getGrantOptionIntSet());
          tdbPrivilege.putToTablePrivilegeMap(tablePrivilege.getTableName(), tTablePrivilege);
        }
      } else {
        tdbPrivilege.setTablePrivilegeMap(new HashMap<>());
      }
      privileges.add(tdbPrivilege);
    }
    return privileges;
  }

  public void loadDatabaseAndTablePrivilegeInfo(Map<String, TDBPrivilege> info) {
    for (TDBPrivilege tdbPrivilege : info.values()) {
      DatabasePrivilege databasePrivilege = new DatabasePrivilege(tdbPrivilege.getDatabaseName());
      for (Integer privId : tdbPrivilege.getPrivileges()) {
        databasePrivilege.grantDBPrivilege(PrivilegeType.values()[privId]);
      }
      for (Integer privId : tdbPrivilege.getGrantOpt()) {
        databasePrivilege.grantDBGrantOption(PrivilegeType.values()[privId]);
      }
      if (tdbPrivilege.getTablePrivilegeMapSize() != 0) {
        for (TTablePrivilege tablePrivilege : tdbPrivilege.getTablePrivilegeMap().values()) {
          for (Integer privId : tablePrivilege.getPrivileges()) {
            databasePrivilege.grantTablePrivilege(
                tablePrivilege.getTableName(), PrivilegeType.values()[privId]);
          }
          for (Integer privId : tablePrivilege.getGrantOption()) {
            databasePrivilege.grantTableGrantOption(
                tablePrivilege.getTableName(), PrivilegeType.values()[privId]);
          }
        }
      }
      this.objectPrivilegeMap.put(tdbPrivilege.getDatabaseName(), databasePrivilege);
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
        Set<Integer> privGrantOpt = new HashSet<>();
        for (PrivilegeType priv : sysPriGrantOpt) {
          if (priv.forRelationalSys()) {
            privGrantOpt.add(priv.ordinal());
          }
        }
        roleResp.setSysPriSetGrantOpt(privGrantOpt);
        roleResp.setAnyScopeSet(getPrivilegeIntSet(anyScopePrivilegeSet));
        roleResp.setAnyScopeGrantSet(getPrivilegeIntSet(anyScopePrivilegeGrantOptSet));
        roleResp.setPrivilegeList(new ArrayList<>());
        Set<TDBPrivilege> tdbPrivileges = getDatabaseAndTablePrivilegeInfo();
        roleResp.setDbPrivilegeMap(new HashMap<>());
        for (TDBPrivilege tdbPrivilege : tdbPrivileges) {
          roleResp.putToDbPrivilegeMap(tdbPrivilege.getDatabaseName(), tdbPrivilege);
        }
        break;
      case TREE:
        roleResp.setSysPriSet(getPrivilegeIntSet(sysPrivilegeSet));
        roleResp.setSysPriSetGrantOpt(getPrivilegeIntSet(sysPriGrantOpt));
        roleResp.setAnyScopeSet(new HashSet<>());
        roleResp.setAnyScopeGrantSet(new HashSet<>());
        roleResp.setPrivilegeList(getTreePrivilegeInfo());
        roleResp.setDbPrivilegeMap(new HashMap<>());
        break;
      case ALL:
        roleResp.setSysPriSet(getPrivilegeIntSet(sysPrivilegeSet));
        roleResp.setSysPriSetGrantOpt(getPrivilegeIntSet(sysPriGrantOpt));
        roleResp.setAnyScopeSet(getPrivilegeIntSet(anyScopePrivilegeSet));
        roleResp.setAnyScopeGrantSet(getPrivilegeIntSet(anyScopePrivilegeGrantOptSet));
        roleResp.setPrivilegeList(getTreePrivilegeInfo());
        Set<TDBPrivilege> tdbPrivileges1 = getDatabaseAndTablePrivilegeInfo();
        roleResp.setDbPrivilegeMap(new HashMap<>());
        for (TDBPrivilege tdbPrivilege : tdbPrivileges1) {
          roleResp.putToDbPrivilegeMap(tdbPrivilege.getDatabaseName(), tdbPrivilege);
        }
        break;
    }
    return roleResp;
  }

  /** -------------- set func ----------------* */
  public void setName(String name) {
    this.name = name;
  }

  public void setMaxSessionPerUser(int maxSessionPerUser) {
    this.maxSessionPerUser = maxSessionPerUser;
  }

  public void setMinSessionPerUser(int minSessionPerUser) {
    this.minSessionPerUser = minSessionPerUser;
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

  public void revokePathPrivilegeGrantOption(PartialPath path, PrivilegeType priv) {
    AuthUtils.removePrivilegeGrantOption(path, priv, pathPrivilegeList);
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
          anyScopePrivilegeGrantOptSet.add(AuthUtils.posToObjPri(i));
        }
      }
    }
  }

  public void setAnyScopePrivilegeSet(Set<PrivilegeType> privilegeSet) {
    this.anyScopePrivilegeSet = privilegeSet;
  }

  public void setAnyScopePrivilegeSetInt(Set<Integer> privilegeSet) {
    for (Integer priv : privilegeSet) {
      this.anyScopePrivilegeSet.add(PrivilegeType.values()[priv]);
    }
  }

  public void setAnyScopePrivilegeGrantOptSet(Set<PrivilegeType> grantOpt) {
    this.anyScopePrivilegeGrantOptSet = grantOpt;
  }

  public void setAnyScopePrivilegeGrantOptSetInt(Set<Integer> privilegeSet) {
    for (Integer priv : privilegeSet) {
      this.anyScopePrivilegeGrantOptSet.add(PrivilegeType.values()[priv]);
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

  public void grantAnyScopePrivilege(PrivilegeType priv, boolean grantOpt) {
    anyScopePrivilegeSet.add(priv);
    if (grantOpt) {
      anyScopePrivilegeGrantOptSet.add(priv);
    }
  }

  public void revokeAnyScopePrivilege(PrivilegeType priv) {
    anyScopePrivilegeSet.remove(priv);
    anyScopePrivilegeGrantOptSet.remove(priv);
  }

  public void revokeAnyScopePrivilegeGrantOption(PrivilegeType priv) {
    anyScopePrivilegeGrantOptSet.remove(priv);
  }

  public void revokeAllRelationalPrivileges() {
    objectPrivilegeMap = new HashMap<>();
    sysPrivilegeSet.removeIf(PrivilegeType::forRelationalSys);
    sysPriGrantOpt.removeIf(PrivilegeType::forRelationalSys);
    anyScopePrivilegeSet = new HashSet<>();
    anyScopePrivilegeGrantOptSet = new HashSet<>();
  }

  public void grantDBPrivilege(String dbName, PrivilegeType priv, boolean grantOption) {
    DatabasePrivilege databasePrivilege = getObjectPrivilege(dbName);
    databasePrivilege.grantDBPrivilege(priv);
    if (grantOption) {
      databasePrivilege.grantDBGrantOption(priv);
    }
  }

  public void grantTBPrivilege(
      String dbName, String tbName, PrivilegeType priv, boolean grantOption) {
    DatabasePrivilege databasePrivilege = getObjectPrivilege(dbName);
    databasePrivilege.grantTablePrivilege(tbName, priv);
    if (grantOption) {
      databasePrivilege.grantTableGrantOption(tbName, priv);
    }
  }

  public void revokeDBPrivilege(String dbName, PrivilegeType priv) {
    if (!objectPrivilegeMap.containsKey(dbName)) {
      return;
    }
    DatabasePrivilege databasePrivilege = objectPrivilegeMap.get(dbName);
    databasePrivilege.revokeDBPrivilege(priv);
    if (databasePrivilege.getTablePrivilegeMap().isEmpty()
        && databasePrivilege.getPrivilegeSet().isEmpty()) {
      objectPrivilegeMap.remove(dbName);
    }
  }

  public void revokeDBPrivilegeGrantOption(String dbName, PrivilegeType priv) {
    if (!objectPrivilegeMap.containsKey(dbName)) {
      return;
    }
    DatabasePrivilege databasePrivilege = objectPrivilegeMap.get(dbName);
    databasePrivilege.revokeDBGrantOption(priv);
  }

  public void revokeTBPrivilege(String dbName, String tbName, PrivilegeType priv) {
    if (!objectPrivilegeMap.containsKey(dbName)) {
      return;
    }
    DatabasePrivilege databasePrivilege = objectPrivilegeMap.get(dbName);
    databasePrivilege.revokeTablePrivilege(tbName, priv);
    if (databasePrivilege.getTablePrivilegeMap().isEmpty()
        && databasePrivilege.getPrivilegeSet().isEmpty()) {
      this.objectPrivilegeMap.remove(dbName);
    }
  }

  public void revokeTBPrivilegeGrantOption(String dbName, String tbName, PrivilegeType priv) {
    if (!objectPrivilegeMap.containsKey(dbName)) {
      return;
    }
    DatabasePrivilege databasePrivilege = objectPrivilegeMap.get(dbName);
    if (!databasePrivilege.getTablePrivilegeMap().containsKey(tbName)) {
      return;
    }
    TablePrivilege tablePrivilege = databasePrivilege.getTablePrivilegeMap().get(tbName);
    tablePrivilege.revokeGrantOption(priv);
  }

  public void grantSysPrivilegeGrantOption(PrivilegeType priv) {
    sysPriGrantOpt.add(priv);
  }

  public void revokeSysPrivilege(PrivilegeType priv) {
    sysPrivilegeSet.remove(priv);
  }

  public void revokeSysPrivilegeGrantOption(PrivilegeType priv) {
    sysPriGrantOpt.remove(priv);
  }

  /** ------------ check func ---------------* */
  public boolean hasPrivilegeToRevoke(PartialPath path, PrivilegeType priv) {
    return AuthUtils.hasPrivilegeToRevoke(path, priv, pathPrivilegeList);
  }

  public boolean hasPrivilegeToRevoke(PrivilegeType priv) {
    return this.sysPrivilegeSet.contains(priv);
  }

  public boolean hasPrivilegeToRevoke(String dbname, PrivilegeType priv) {
    return this.objectPrivilegeMap.containsKey(dbname)
        && this.objectPrivilegeMap.get(dbname).checkDBPrivilege(priv);
  }

  public boolean hasPrivilegeToRevoke(String dbname, String tbName, PrivilegeType priv) {
    return this.objectPrivilegeMap.containsKey(dbname)
        && this.objectPrivilegeMap.get(dbname).checkTablePrivilege(tbName, priv);
  }

  public boolean checkAnyScopePrivilege(PrivilegeType priv) {
    return anyScopePrivilegeSet.contains(priv);
  }

  public boolean checkDatabasePrivilege(String dbname, PrivilegeType priv) {
    return checkAnyScopePrivilege(priv)
        || (this.objectPrivilegeMap.containsKey(dbname)
            && this.objectPrivilegeMap.get(dbname).checkDBPrivilege(priv));
  }

  public boolean checkTablePrivilege(String dbname, String table, PrivilegeType priv) {
    return checkAnyScopePrivilege(priv)
        || (this.objectPrivilegeMap.containsKey(dbname)
            && (this.objectPrivilegeMap.get(dbname).checkDBPrivilege(priv)
                || this.objectPrivilegeMap.get(dbname).checkTablePrivilege(table, priv)));
  }

  public boolean checkDatabasePrivilegeGrantOption(String dbname, PrivilegeType priv) {
    if (checkAnyScopePrivilegeGrantOption(priv)) {
      return true;
    }
    return this.objectPrivilegeMap.containsKey(dbname)
        && this.objectPrivilegeMap.get(dbname).checkDBGrantOption(priv);
  }

  public boolean checkTablePrivilegeGrantOption(String dbname, String table, PrivilegeType priv) {
    if (checkAnyScopePrivilegeGrantOption(priv)) {
      return true;
    }
    if (checkDatabasePrivilegeGrantOption(dbname, priv)) {
      return true;
    }
    return this.objectPrivilegeMap.containsKey(dbname)
        && this.objectPrivilegeMap.get(dbname).checkTableGrantOption(table, priv);
  }

  public boolean checkAnyScopePrivilegeGrantOption(PrivilegeType priv) {
    return this.anyScopePrivilegeGrantOptSet.contains(priv)
        && this.anyScopePrivilegeSet.contains(priv);
  }

  public boolean checkPathPrivilege(PartialPath path, PrivilegeType priv) {
    return AuthUtils.checkPathPrivilege(path, priv, pathPrivilegeList);
  }

  public boolean checkPathPrivilegeGrantOpt(PartialPath path, PrivilegeType priv) {
    return AuthUtils.checkPathPrivilegeGrantOpt(path, priv, pathPrivilegeList);
  }

  public boolean checkSysPrivilege(PrivilegeType priv) {
    return priv.getAllPrivilegesContainingCurrentPrivilege().stream()
        .anyMatch(sysPrivilegeSet::contains);
  }

  public boolean checkSysPriGrantOpt(PrivilegeType priv) {
    return sysPrivilegeSet.contains(priv) && sysPriGrantOpt.contains(priv);
  }

  public boolean checkAnyVisible() {
    return !anyScopePrivilegeSet.isEmpty();
  }

  public boolean checkDBVisible(String database) {
    return !anyScopePrivilegeSet.isEmpty() || objectPrivilegeMap.containsKey(database);
  }

  public boolean checkTBVisible(String database, String tbName) {
    // Has any scope privileges
    if (!anyScopePrivilegeSet.isEmpty()) {
      return true;
    }

    // Fail back early
    if (!objectPrivilegeMap.containsKey(database)) {
      return false;
    }

    // Has db scope privileges
    if (!objectPrivilegeMap.get(database).getPrivilegeSet().isEmpty()) {
      return true;
    }

    return objectPrivilegeMap.get(database).getTablePrivilegeMap().containsKey(tbName);
  }

  public int getAllSysPrivileges() {
    int privs = 0;
    for (PrivilegeType sysPri : sysPrivilegeSet) {
      privs |= 1 << AuthUtils.sysPriToPos(sysPri);
    }
    for (PrivilegeType sysGrantOpt : sysPriGrantOpt) {
      privs |= 1 << (AuthUtils.sysPriToPos(sysGrantOpt) + 16);
    }
    return privs;
  }

  public int getAnyScopePrivileges() {
    int privs = 0;
    for (PrivilegeType anyScope : anyScopePrivilegeSet) {
      privs |= 1 << AuthUtils.objPriToPos(anyScope);
    }
    for (PrivilegeType anyScopeGrantOpt : anyScopePrivilegeGrantOptSet) {
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
        && Objects.equals(objectPrivilegeMap, role.objectPrivilegeMap)
        && Objects.equals(anyScopePrivilegeSet, role.anyScopePrivilegeSet)
        && Objects.equals(anyScopePrivilegeGrantOptSet, role.anyScopePrivilegeGrantOptSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        pathPrivilegeList,
        sysPrivilegeSet,
        sysPriGrantOpt,
        anyScopePrivilegeSet,
        anyScopePrivilegeGrantOptSet,
        objectPrivilegeMap);
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serialize(name, dataOutputStream);

    try {
      SerializeUtils.serializePrivilegeTypeSet(sysPrivilegeSet, dataOutputStream);
      SerializeUtils.serializePrivilegeTypeSet(sysPriGrantOpt, dataOutputStream);
      dataOutputStream.writeInt(pathPrivilegeList.size());
      for (PathPrivilege pathPrivilege : pathPrivilegeList) {
        dataOutputStream.write(pathPrivilege.serialize().array());
      }
      SerializeUtils.serializePrivilegeTypeSet(anyScopePrivilegeSet, dataOutputStream);
      SerializeUtils.serializePrivilegeTypeSet(anyScopePrivilegeGrantOptSet, dataOutputStream);
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

    SerializeUtils.deserializePrivilegeTypeSet(anyScopePrivilegeSet, buffer);
    SerializeUtils.deserializePrivilegeTypeSet(anyScopePrivilegeGrantOptSet, buffer);

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
        + priSetToString(sysPrivilegeSet, sysPriGrantOpt)
        + ", AnyScopePrivilegeMap="
        + priSetToString(anyScopePrivilegeSet, anyScopePrivilegeGrantOptSet)
        + ", objectPrivilegeSet="
        + objectPrivilegeMap
        + '}';
  }

  public Set<String> priSetToString(Set<PrivilegeType> privs, Set<PrivilegeType> grantOpt) {
    Set<String> priSet = new HashSet<>();
    ArrayList<PrivilegeType> privBak = new ArrayList<>(privs);
    Collections.sort(privBak);

    for (PrivilegeType priv : privBak) {
      StringBuilder str = new StringBuilder(String.valueOf(priv));
      if (grantOpt.contains(priv)) {
        str.append("_with_grant_option");
      }
      priSet.add(str.toString());
    }
    return priSet;
  }
}
