package org.apache.iotdb.commons.auth.entity;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

public class TablePrivilege {
  private final String tableName;
  private final Set<PrivilegeType> privileges;
  private final Set<PrivilegeType> grantOption;

  public TablePrivilege(String tableName) {
    this.tableName = tableName;
    this.privileges = new HashSet<>();
    this.grantOption = new HashSet<>();
  }

  public String getTableName() {
    return this.tableName;
  }

  public Set<PrivilegeType> getPrivileges() {
    return this.privileges;
  }

  public Set<PrivilegeType> getGrantOption() {
    return this.grantOption;
  }

  public void grantPrivilege(PrivilegeType priv) {
    this.privileges.add(priv);
  }

  public void revokePrivilege(PrivilegeType priv) {
    this.privileges.remove(priv);
  }

  public void grantOption(PrivilegeType priv) {
    this.grantOption.add(priv);
  }

  public void revokeGrantOption(PrivilegeType priv) {
    this.grantOption.remove(priv);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.tableName).append(" ");
    for (PrivilegeType type : privileges) {
      builder.append(type);
      if (grantOption.contains(type)) {
        builder.append("_with_grant_option ");
      }
    }
    return builder.toString();
  }

  public void serialize(OutputStream outputStream) throws IOException {}
}
