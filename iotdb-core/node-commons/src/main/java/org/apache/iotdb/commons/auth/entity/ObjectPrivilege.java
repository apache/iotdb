package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.utils.SerializeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class ObjectPrivilege {
  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectPrivilege.class);
  private final String databaseName;
  private static final int Object_PRI_SIZE = PrivilegeType.getPathPriCount();
  private Map<String, TablePrivilege> tablePrivilegeMap;
  Set<PrivilegeType> privilegeTypeSet;
  Set<PrivilegeType> grantOptionSet;

  public ObjectPrivilege(String databaseName) {
    this.databaseName = databaseName;
    this.tablePrivilegeMap = new HashMap<>();
    this.privilegeTypeSet = new HashSet<>();
    this.grantOptionSet = new HashSet<>();
  }

  public void grantDBObjectPrivilege(PrivilegeType privilegeType) {
    this.privilegeTypeSet.add(privilegeType);
  }

  public void revokeDBObjectPrivilege(PrivilegeType privilegeType) {
    this.privilegeTypeSet.remove(privilegeType);
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

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.privilegeTypeSet, dataOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.grantOptionSet, dataOutputStream);

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }
}
