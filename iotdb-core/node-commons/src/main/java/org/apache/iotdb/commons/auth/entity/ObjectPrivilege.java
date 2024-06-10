package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class ObjectPrivilege {
  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectPrivilege.class);
  private String databaseName;

  private static final int PATH_PRI_SIZE = PrivilegeType.getPathPriCount();
  private Map<String, TablePrivilege> tablePrivilegeMap;
  private Set<PrivilegeType> privilegeTypeSet;
  private Set<PrivilegeType> grantOptionSet;

  public ObjectPrivilege(String databaseName) {
    this.databaseName = databaseName;
    this.tablePrivilegeMap = new HashMap<>();
    this.privilegeTypeSet = new HashSet<>();
    this.grantOptionSet = new HashSet<>();
  }

  public ObjectPrivilege() {
    //
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  public Map<String, TablePrivilege> getTablePrivilegeMap() {
    return this.tablePrivilegeMap;
  }

  public Set<Integer> getPrivileges() {
    Set<Integer> ret = new HashSet<>();
    for (PrivilegeType type : privilegeTypeSet) {
      ret.add(type.ordinal());
    }
    return ret;
  }

  public Set<Integer> getGrantOptions() {
    Set<Integer> ret = new HashSet<>();
    for (PrivilegeType type : grantOptionSet) {
      ret.add(type.ordinal());
    }
    return ret;
  }

  public int getAllPrivileges() {
    return AuthUtils.getAllPrivileges(this.privilegeTypeSet, this.grantOptionSet);
  }

  public void setPrivileges(int privs) {
    for (int i = 0; i < PATH_PRI_SIZE; i++) {
      if (((1 << i) & privs) != 0) {
        privilegeTypeSet.add(PrivilegeType.values()[AuthUtils.pathPosToPri(i)]);
      }
      if ((1 << (i + 16) & privs) != 0) {
        grantOptionSet.add(PrivilegeType.values()[AuthUtils.pathPosToPri(i)]);
      }
    }
  }

  public void grantDBObjectPrivilege(PrivilegeType privilegeType) {
    this.privilegeTypeSet.add(privilegeType);
  }

  public void revokeDBObjectPrivilege(PrivilegeType privilegeType) {
    this.privilegeTypeSet.remove(privilegeType);
    revokeGrantOptionFromDB(privilegeType);
  }

  public void grantGrantoptionToDB(PrivilegeType privilegeType) {
    this.grantOptionSet.add(privilegeType);
  }

  public void revokeGrantOptionFromDB(PrivilegeType privilegeType) {
    this.grantOptionSet.remove(privilegeType);
  }

  public void grantTableObjectPrivilege(String tableName, PrivilegeType privilegeType) {
    if (!this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = new TablePrivilege(tableName);
      tablePrivilege.grantPrivilege(privilegeType);
      this.tablePrivilegeMap.put(tableName, tablePrivilege);
    } else {
      tablePrivilegeMap.get(tableName).grantPrivilege(privilegeType);
    }
  }

  public void revokeTableObjectPrivilege(String tableName, PrivilegeType privilegeType) {
    if (this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
      tablePrivilege.revokePrivilege(privilegeType);
      tablePrivilege.revokeGrantOption(privilegeType);
      if (tablePrivilege.getPrivileges().isEmpty()) {
        this.tablePrivilegeMap.remove(tableName);
      }
    }
  }

  public boolean grantTableObejctGrantOption(String tableName, PrivilegeType privilegeType) {
    if (this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
      tablePrivilege.grantOption(privilegeType);
      return true;
    }
    return false;
  }

  public boolean revokeTableObjectGrantOption(String tableName, PrivilegeType privilegeType) {
    if (this.tablePrivilegeMap.containsKey(tableName)) {
      TablePrivilege tablePrivilege = this.tablePrivilegeMap.get(tableName);
      tablePrivilege.revokeGrantOption(privilegeType);
      return true;
    }
    return false;
  }

  public boolean checkDBPrivilege(PrivilegeType privilegeType) {
    return this.privilegeTypeSet.contains(privilegeType);
  }

  public boolean checkTablePrivilege(String tableName, PrivilegeType privilegeType) {
    if (!this.tablePrivilegeMap.containsKey(tableName)) {
      return false;
    }
    return tablePrivilegeMap.get(tableName).getPrivileges().contains(privilegeType);
  }

  public boolean checkDBGrantOption(PrivilegeType type) {
    return this.privilegeTypeSet.contains(type) && this.grantOptionSet.contains(type);
  }

  public boolean checkTableGrantOption(String tableName, PrivilegeType type) {
    if (!this.tablePrivilegeMap.containsKey(tableName)) {
      return false;
    }
    return this.tablePrivilegeMap.get(tableName).getPrivileges().contains(type)
        && this.tablePrivilegeMap.get(tableName).getGrantOption().contains(type);
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ObjectPrivilege that = (ObjectPrivilege) o;
    return databaseName.equals(that.databaseName)
        && Objects.equals(privilegeTypeSet, that.privilegeTypeSet)
        && Objects.equals(grantOptionSet, that.grantOptionSet)
        && tablePrivilegeMap.equals(that.tablePrivilegeMap);
  }

  public int hashCode() {
    return Objects.hash(databaseName, tablePrivilegeMap, privilegeTypeSet, grantOptionSet);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Database: ").append(databaseName);
    for (PrivilegeType type : privilegeTypeSet) {
      builder.append(" ").append(type);
      if (grantOptionSet.contains(type)) {
        builder.append("_with_grant_option");
      }
    }
    if (!this.tablePrivilegeMap.isEmpty()) {
      builder.append("Tables: ");
    }

    for (Map.Entry<String, TablePrivilege> tablePriv : this.tablePrivilegeMap.entrySet()) {
      builder.append(tablePriv.getValue().toString());
    }
    return builder.toString();
  }

  public ByteBuffer serialize() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.privilegeTypeSet, dataOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.grantOptionSet, dataOutputStream);
    ReadWriteIOUtils.write(this.tablePrivilegeMap.size(), dataOutputStream);
    for (Map.Entry<String, TablePrivilege> tablePrivilegeEntry :
        this.tablePrivilegeMap.entrySet()) {
      tablePrivilegeEntry.getValue().serialize(dataOutputStream);
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    this.privilegeTypeSet = new HashSet<>();
    this.grantOptionSet = new HashSet<>();
    this.tablePrivilegeMap = new HashMap<>();
    this.databaseName = SerializeUtils.deserializeString(buffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.privilegeTypeSet, buffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.grantOptionSet, buffer);
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      TablePrivilege tablePrivilege = new TablePrivilege();
      tablePrivilege.deserialize(buffer);
      this.tablePrivilegeMap.put(tablePrivilege.getTableName(), tablePrivilege);
    }
  }
}
