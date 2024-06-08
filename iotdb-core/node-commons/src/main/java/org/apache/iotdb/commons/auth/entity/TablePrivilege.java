package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class TablePrivilege {
  private String tableName;
  private Set<PrivilegeType> privileges;
  private Set<PrivilegeType> grantOption;

  private static final int PATH_PRI_SIZE = PrivilegeType.getPathPriCount();

  public TablePrivilege(String tableName) {
    this.tableName = tableName;
    this.privileges = new HashSet<>();
    this.grantOption = new HashSet<>();
  }

  public TablePrivilege() {
    //
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

  public void setPrivileges(int privs) {
    for (int i = 0; i < PATH_PRI_SIZE; i++) {
      if (((1 << i) & privs) != 0) {
        privileges.add(PrivilegeType.values()[AuthUtils.pathPosToPri(i)]);
      }
      if ((1 << (i + 16) & privs) != 0) {
        grantOption.add(PrivilegeType.values()[AuthUtils.pathPosToPri(i)]);
      }
    }
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

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(this.tableName, outputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.privileges, (DataOutputStream) outputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.grantOption, (DataOutputStream) outputStream);
  }

  public void deserialize(ByteBuffer byteBuffer) {
    this.privileges = new HashSet<>();
    this.grantOption = new HashSet<>();
    this.tableName = SerializeUtils.deserializeString(byteBuffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.privileges, byteBuffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.grantOption, byteBuffer);
  }
}
