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

  public AuthorRelationalWritePlan(AuthorRelationalPlan authorRelationalPlan) {
    super(
        authorRelationalPlan.getAuthorType(),
        authorRelationalPlan.getUserName(),
        authorRelationalPlan.getRoleName(),
        authorRelationalPlan.getDatabaseName(),
        authorRelationalPlan.getTableName(),
        authorRelationalPlan.getGrantOpt(),
        authorRelationalPlan.getPermission(),
        authorRelationalPlan.getPassword());
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(roleName, stream);
    BasicStructureSerDeUtil.write(password, stream);
    if (super.getDatabaseName() == null) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      BasicStructureSerDeUtil.write(databaseName, stream);
    }

    if (this.tableName == null) {
      stream.write((byte) 0);
    } else {
      stream.write((byte) 1);
      BasicStructureSerDeUtil.write(tableName, stream);
    }
    BasicStructureSerDeUtil.write(this.permission, stream);
    stream.write(grantOpt ? (byte) 1 : (byte) 0);
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
