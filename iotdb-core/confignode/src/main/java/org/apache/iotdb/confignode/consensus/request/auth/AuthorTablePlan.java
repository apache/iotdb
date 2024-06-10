package org.apache.iotdb.confignode.consensus.request.auth;

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

public class AuthorTablePlan extends AuthorPlan {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AuthorTablePlan.class);

  private int permission;
  private String databaseName;
  private String tableName;

  public AuthorTablePlan(final ConfigPhysicalPlanType type) {
    super(type, false);
  }

  public AuthorTablePlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final boolean grantOpt,
      final int permission,
      final String password) {
    this(authorType);
    super.setUserName(userName);
    super.setRoleName(roleName);
    super.setPassword(password);
    super.setGrantOpt(grantOpt);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.permission = permission;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
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
    ReadWriteIOUtils.write(super.getAuthorType().ordinal(), stream);
    BasicStructureSerDeUtil.write(super.getUserName(), stream);
    BasicStructureSerDeUtil.write(super.getRoleName(), stream);
    BasicStructureSerDeUtil.write(super.getPassword(), stream);
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
    BasicStructureSerDeUtil.write(super.getGrantOpt() ? (byte) 1 : (byte) 0, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    super.setUserName(BasicStructureSerDeUtil.readString(buffer));
    super.setRoleName(BasicStructureSerDeUtil.readString(buffer));
    if (buffer.get() == (byte) 1) {
      this.databaseName = BasicStructureSerDeUtil.readString(buffer);
    }
    if (buffer.get() == (byte) 1) {
      this.tableName = BasicStructureSerDeUtil.readString(buffer);
    }
    this.permission = BasicStructureSerDeUtil.readInt(buffer);
    super.setGrantOpt(buffer.get() == (byte) 1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthorTablePlan that = (AuthorTablePlan) o;
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
        + super.getAuthorType()
        + ", name:"
        + super.getUserName()
        + ", role:"
        + super.getRoleName()
        + ", permissions"
        + PrivilegeType.values()[permission]
        + ", grant option:"
        + super.getGrantOpt()
        + ", DB:"
        + databaseName
        + ", TABLE:"
        + tableName
        + "]";
  }
}
