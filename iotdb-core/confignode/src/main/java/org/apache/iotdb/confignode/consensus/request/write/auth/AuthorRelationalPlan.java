package org.apache.iotdb.confignode.consensus.request.write.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class AuthorRelationalPlan extends AuthorPlan {
  private int permission;
  private String databaseName;
  private String tableName;

  public AuthorRelationalPlan(final ConfigPhysicalPlanType authorType) {
    super(authorType);
  }

  public AuthorRelationalPlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final boolean grantOpt,
      final int permission,
      final String password) {
    super(authorType);
    this.userName = userName;
    this.roleName = roleName;
    this.grantOpt = grantOpt;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.permission = permission;
    this.password = password;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public Set<Integer> getPermissions() {
    return Collections.singleton(permission);
  }

  public int getPermission() {
    return permission;
  }

  public void setPermission(int permission) {
    this.permission = permission;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(super.getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    if (this.databaseName == null) {
      BasicStructureSerDeUtil.write((byte) 0, stream);
    } else {
      BasicStructureSerDeUtil.write((byte) 1, stream);
      BasicStructureSerDeUtil.write(databaseName, stream);
    }

    if (this.tableName == null) {
      BasicStructureSerDeUtil.write((byte) 0, stream);
    } else {
      BasicStructureSerDeUtil.write((byte) 1, stream);
      BasicStructureSerDeUtil.write(tableName, stream);
    }
    BasicStructureSerDeUtil.write(this.permission, stream);
    BasicStructureSerDeUtil.write(grantOpt ? (byte) 1 : (byte) 0, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    userName = BasicStructureSerDeUtil.readString(buffer);
    roleName = BasicStructureSerDeUtil.readString(buffer);
    if (buffer.get() == (byte) 1) {
      this.databaseName = BasicStructureSerDeUtil.readString(buffer);
    }
    if (buffer.get() == (byte) 1) {
      this.tableName = BasicStructureSerDeUtil.readString(buffer);
    }
    this.permission = BasicStructureSerDeUtil.readInt(buffer);
    grantOpt = buffer.get() == (byte) 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthorRelationalPlan that = (AuthorRelationalPlan) o;
    return super.equals(o)
        && Objects.equals(this.databaseName, that.databaseName)
        && Objects.equals(this.tableName, that.tableName)
        && Objects.equals(this.permission, that.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), databaseName, tableName, permission);
  }

  @Override
  public String toString() {
    return "[type:"
        + super.getType()
        + ", name:"
        + userName
        + ", role:"
        + roleName
        + ", permissions"
        + (permission == -1 ? "-1" : PrivilegeType.values()[permission])
        + ", grant option:"
        + grantOpt
        + ", DB:"
        + databaseName
        + ", TABLE:"
        + tableName
        + "]";
  }
}
