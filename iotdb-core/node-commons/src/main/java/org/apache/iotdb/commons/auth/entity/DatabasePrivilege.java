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

import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;

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

public class DatabasePrivilege {
  private String databaseName;

  private Map<String, TablePrivilege> tablePrivilegeMap;
  private Set<PrivilegeType> privilegeSet;
  private Set<PrivilegeType> grantOptionSet;

  public DatabasePrivilege(String databaseName) {
    this.databaseName = databaseName;
    this.tablePrivilegeMap = new HashMap<>();
    this.privilegeSet = new HashSet<>();
    this.grantOptionSet = new HashSet<>();
  }

  public DatabasePrivilege() {
    // This construction is used for deserializing from thrift.
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  public Map<String, TablePrivilege> getTablePrivilegeMap() {
    return this.tablePrivilegeMap;
  }

  public void setPrivileges(int mask) {
    final int PRI_SIZE = PrivilegeType.getPrivilegeCount(PrivilegeModelType.RELATIONAL);
    for (int i = 0; i < PRI_SIZE; i++) {
      if (((1 << i) & mask) != 0) {
        this.privilegeSet.add(AuthUtils.posToObjPri(i));
        if (((1 << (i + 16)) & mask) != 0) {
          this.grantOptionSet.add(AuthUtils.posToObjPri(i));
        }
      }
    }
  }

  public int getAllPrivileges() {
    int privilege = 0;
    for (PrivilegeType pri : privilegeSet) {
      privilege |= 1 << AuthUtils.objPriToPos(pri);
    }
    for (PrivilegeType pri : grantOptionSet) {
      privilege |= 1 << (AuthUtils.objPriToPos(pri) + 16);
    }
    return privilege;
  }

  public Set<Integer> getPrivilegeSet() {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType priv : this.privilegeSet) {
      res.add(priv.ordinal());
    }
    return res;
  }

  public Set<Integer> getPrivilegeGrantOptSet() {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType priv : this.grantOptionSet) {
      res.add(priv.ordinal());
    }
    return res;
  }

  public void grantDBPrivilege(PrivilegeType privilegeType) {
    this.privilegeSet.add(privilegeType);
  }

  public void revokeDBPrivilege(PrivilegeType privilegeType) {
    this.privilegeSet.remove(privilegeType);
    revokeDBGrantOption(privilegeType);
  }

  public void grantDBGrantOption(PrivilegeType privilegeType) {
    this.grantOptionSet.add(privilegeType);
  }

  public void revokeDBGrantOption(PrivilegeType privilegeType) {
    this.grantOptionSet.remove(privilegeType);
  }

  public void grantTablePrivilege(String tableName, PrivilegeType privilegeType) {
    if (!this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = new TablePrivilege(tableName);
      tablePrivilege.grantPrivilege(privilegeType);
      this.tablePrivilegeMap.put(tableName, tablePrivilege);
    } else {
      tablePrivilegeMap.get(tableName).grantPrivilege(privilegeType);
    }
  }

  public void revokeTablePrivilege(String tableName, PrivilegeType privilegeType) {
    if (this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
      tablePrivilege.revokePrivilege(privilegeType);
      tablePrivilege.revokeGrantOption(privilegeType);
      if (tablePrivilege.getPrivileges().isEmpty()) {
        this.tablePrivilegeMap.remove(tableName);
      }
    }
  }

  public void grantTableGrantOption(String tableName, PrivilegeType privilegeType) {
    if (this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
      tablePrivilege.grantOption(privilegeType);
    }
  }

  public void revokeTableGrantOption(String tableName, PrivilegeType privilegeType) {
    if (this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
      tablePrivilege.revokeGrantOption(privilegeType);
    }
  }

  public boolean checkDBPrivilege(PrivilegeType privilegeType) {
    return this.privilegeSet.contains(privilegeType);
  }

  public boolean checkTablePrivilege(String tableName, PrivilegeType privilegeType) {
    TablePrivilege privileges = tablePrivilegeMap.get(tableName);
    if (privileges == null) {
      return false;
    }
    return privileges.getPrivileges().contains(privilegeType);
  }

  public boolean checkDBGrantOption(PrivilegeType type) {
    return this.privilegeSet.contains(type) && this.grantOptionSet.contains(type);
  }

  public boolean checkTableGrantOption(String tableName, PrivilegeType type) {
    TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
    if (tablePrivilege == null) {
      return false;
    }
    return tablePrivilege.getPrivileges().contains(type)
        && tablePrivilege.getGrantOption().contains(type);
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DatabasePrivilege that = (DatabasePrivilege) o;
    return databaseName.equals(that.databaseName)
        && Objects.equals(privilegeSet, that.privilegeSet)
        && Objects.equals(grantOptionSet, that.grantOptionSet)
        && Objects.equals(tablePrivilegeMap, that.tablePrivilegeMap);
  }

  public int hashCode() {
    return Objects.hash(databaseName, tablePrivilegeMap, privilegeSet, grantOptionSet);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Database(").append(databaseName).append("):{");
    List<PrivilegeType> list = new ArrayList<>(this.privilegeSet);
    Collections.sort(list);
    for (PrivilegeType type : list) {
      builder.append(type);
      if (grantOptionSet.contains(type)) {
        builder.append("_with_grant_option");
      }
      builder.append(",");
    }
    builder.append("; Tables: [");

    for (Map.Entry<String, TablePrivilege> tablePriv : this.tablePrivilegeMap.entrySet()) {
      builder.append(" ").append(tablePriv.getValue().toString());
    }
    builder.append("]}");
    return builder.toString();
  }

  public ByteBuffer serialize() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    SerializeUtils.serialize(this.databaseName, dataOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.privilegeSet, dataOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.grantOptionSet, dataOutputStream);
    ReadWriteIOUtils.write(this.tablePrivilegeMap.size(), dataOutputStream);
    for (Map.Entry<String, TablePrivilege> tablePrivilegeEntry :
        this.tablePrivilegeMap.entrySet()) {
      tablePrivilegeEntry.getValue().serialize(dataOutputStream);
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    this.privilegeSet = new HashSet<>();
    this.grantOptionSet = new HashSet<>();
    this.tablePrivilegeMap = new HashMap<>();
    this.databaseName = SerializeUtils.deserializeString(buffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.privilegeSet, buffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.grantOptionSet, buffer);
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      TablePrivilege tablePrivilege = new TablePrivilege();
      tablePrivilege.deserialize(buffer);
      this.tablePrivilegeMap.put(tablePrivilege.getTableName(), tablePrivilege);
    }
  }
}
