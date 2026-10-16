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

package org.apache.iotdb.confignode.consensus.request.write.cq;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType.DROP_CQ;

public class DropCQPlan extends ConfigPhysicalPlan {

  private String cqId;

  // may be null in user call of drop CQ
  private String cqToken;

  public DropCQPlan() {
    super(DROP_CQ);
  }

  public DropCQPlan(String cqId) {
    super(DROP_CQ);
    Validate.notNull(cqId);
    this.cqId = cqId;
  }

  public DropCQPlan(String cqId, String cqToken) {
    super(DROP_CQ);
    Validate.notNull(cqId);
    Validate.notNull(cqToken);
    this.cqId = cqId;
    this.cqToken = cqToken;
  }

  public String getCqId() {
    return cqId;
  }

  public Optional<String> getCqToken() {
    return Optional.ofNullable(cqToken);
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(cqId, stream);
    ReadWriteIOUtils.write(cqToken, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    cqId = ReadWriteIOUtils.readString(buffer);
    cqToken = ReadWriteIOUtils.readString(buffer);
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
    DropCQPlan that = (DropCQPlan) o;
    return cqId.equals(that.cqId) && Objects.equals(cqToken, that.cqToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), cqId, cqToken);
  }
}
