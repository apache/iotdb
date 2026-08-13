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

package org.apache.iotdb.confignode.consensus.request.write.quota;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DeleteUserResourceQuotaPlan extends ConfigPhysicalPlan {

  /** Immutable unique key of the user; rename-safe. */
  private long userId;

  /** Current userName; used for legacy throttle cleanup and display only. */
  private String userName;

  public DeleteUserResourceQuotaPlan() {
    super(ConfigPhysicalPlanType.deleteUserResourceQuota);
  }

  public DeleteUserResourceQuotaPlan(long userId, String userName) {
    super(ConfigPhysicalPlanType.deleteUserResourceQuota);
    this.userId = userId;
    this.userName = userName;
  }

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeLong(userId);
    BasicStructureSerDeUtil.write(userName, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    userId = buffer.getLong();
    userName = BasicStructureSerDeUtil.readString(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DeleteUserResourceQuotaPlan that = (DeleteUserResourceQuotaPlan) o;
    return userId == that.userId && Objects.equals(userName, that.userName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), userId, userName);
  }
}
