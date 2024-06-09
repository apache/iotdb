package org.apache.iotdb.confignode.consensus.request.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class AuthorTablePlan extends ConfigPhysicalPlan {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AuthorTablePlan.class);

  private ConfigPhysicalPlanType authorType;

  private String name;
  private boolean isUser;
  private int permission;
  private String databaseName;
  private String tableName;
  private boolean grantOpt;

  public AuthorTablePlan(final ConfigPhysicalPlanType type) {
    super(type);
    authorType = type;
  }

  public AuthorTablePlan(
      final ConfigPhysicalPlanType authorType,
      final String name,
      final boolean isUser,
      final String databaseName,
      final String tableName,
      final boolean grantOpt,
      final int permission) {
    this(authorType);
    this.authorType = authorType;
    this.name = name;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.grantOpt = grantOpt;
    this.permission = permission;
  }

  public ConfigPhysicalPlanType getAuthorType() {
    return authorType;
  }

  public void setAuthorType(ConfigPhysicalPlanType authorType) {
    this.authorType = authorType;
  }

  public boolean getIsUser() {
    return this.isUser;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getName() {
    return name;
  }

  public int getPermission() {
    return permission;
  }

  public boolean isGrantOpt() {
    return this.grantOpt;
  }

  public void setUser(boolean isUser) {
    this.isUser = isUser;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setGrantOpt(boolean grantOpt) {
    this.grantOpt = grantOpt;
  }

  public void setPermission(int permission) {
    this.permission = permission;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(authorType.getPlanType(), stream);
    BasicStructureSerDeUtil.write(name, stream);
    BasicStructureSerDeUtil.write(isUser ? (byte) 1 : (byte) 0, stream);
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
    BasicStructureSerDeUtil.write(this.grantOpt ? (byte) 1 : (byte) 0, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    this.name = BasicStructureSerDeUtil.readString(buffer);
    this.isUser = buffer.get() == (byte) 1;
    if (buffer.get() == (byte) 1) {
      this.databaseName = BasicStructureSerDeUtil.readString(buffer);
    }
    if (buffer.get() == (byte) 1) {
      this.tableName = BasicStructureSerDeUtil.readString(buffer);
    }
    this.permission = BasicStructureSerDeUtil.readInt(buffer);
    this.grantOpt = buffer.get() == (byte) 1;
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
    return Objects.equals(this.authorType, that.authorType)
        && Objects.equals(this.isUser, that.isUser)
        && Objects.equals(this.name, that.name)
        && Objects.equals(this.databaseName, that.databaseName)
        && Objects.equals(this.tableName, that.tableName)
        && Objects.equals(this.permission, that.permission)
        && Objects.equals(this.grantOpt, that.grantOpt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authorType, name, isUser, databaseName, tableName, permission, grantOpt);
  }

  @Override
  public String toString() {
    return "[type:"
        + authorType
        + ", name:"
        + name
        + "isUser:"
        + isUser
        + ", permissions"
        + PrivilegeType.values()[permission]
        + ", grant option:"
        + grantOpt
        + ", DB:"
        + databaseName
        + ", TABLE:"
        + tableName
        + "]";
  }
}
