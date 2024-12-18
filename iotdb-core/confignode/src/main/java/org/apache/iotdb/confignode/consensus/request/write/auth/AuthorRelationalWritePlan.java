package org.apache.iotdb.confignode.consensus.request.write.auth;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorRelationalPlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AuthorRelationalWritePlan extends AuthorRelationalPlan {

  public AuthorRelationalWritePlan(final ConfigPhysicalPlanType authorType) {
    super(authorType);
  }

  public AuthorRelationalWritePlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String databaseName,
      final String tableName,
      final boolean grantOpt,
      final int permission,
      final String password) {
    super(authorType, userName, roleName, databaseName, tableName, grantOpt, permission, password);
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(super.getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    if (super.getDatabaseName() == null) {
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
    password = BasicStructureSerDeUtil.readString(buffer);
    if (buffer.get() == (byte) 1) {
      this.databaseName = BasicStructureSerDeUtil.readString(buffer);
    }
    if (buffer.get() == (byte) 1) {
      this.tableName = BasicStructureSerDeUtil.readString(buffer);
    }
    this.permission = BasicStructureSerDeUtil.readInt(buffer);
    grantOpt = buffer.get() == (byte) 1;
  }
}
