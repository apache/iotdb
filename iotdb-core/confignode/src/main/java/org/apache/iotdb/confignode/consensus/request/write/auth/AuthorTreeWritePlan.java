package org.apache.iotdb.confignode.consensus.request.write.auth;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.auth.AuthorTreePlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorTreeWritePlan extends AuthorTreePlan {

  public AuthorTreeWritePlan(final ConfigPhysicalPlanType type) {
    super(type);
  }

  public AuthorTreeWritePlan(
      final ConfigPhysicalPlanType authorType,
      final String userName,
      final String roleName,
      final String password,
      final String newPassword,
      final Set<Integer> permissions,
      final boolean grantOpt,
      final List<PartialPath> nodeNameList) {
    super(
        authorType, userName, roleName, password, newPassword, permissions, grantOpt, nodeNameList);
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    if (permissions == null) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      stream.writeInt(permissions.size());
      for (int permission : permissions) {
        stream.writeInt(permission);
      }
    }
    BasicStructureSerDeUtil.write(nodeNameList.size(), stream);
    for (PartialPath partialPath : nodeNameList) {
      BasicStructureSerDeUtil.write(partialPath.getFullPath(), stream);
    }
    BasicStructureSerDeUtil.write(super.getGrantOpt() ? 1 : 0, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    userName = BasicStructureSerDeUtil.readString(buffer);
    roleName = BasicStructureSerDeUtil.readString(buffer);
    password = BasicStructureSerDeUtil.readString(buffer);
    if (buffer.get() == (byte) 0) {
      this.permissions = null;
    } else {
      int permissionsSize = buffer.getInt();
      this.permissions = new HashSet<>();
      for (int i = 0; i < permissionsSize; i++) {
        permissions.add(buffer.getInt());
      }
    }

    int nodeNameListSize = BasicStructureSerDeUtil.readInt(buffer);
    nodeNameList = new ArrayList<>(nodeNameListSize);
    try {
      for (int i = 0; i < nodeNameListSize; i++) {
        nodeNameList.add(new PartialPath(BasicStructureSerDeUtil.readString(buffer)));
      }
    } catch (MetadataException e) {
      // do nothing
    }
    if (super.getAuthorType().ordinal() >= ConfigPhysicalPlanType.CreateUser.ordinal()) {
      super.setGrantOpt(BasicStructureSerDeUtil.readInt(buffer) > 0);
    }
  }
}
