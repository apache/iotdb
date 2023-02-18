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

import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType.ADD_CQ;

public class AddCQPlan extends ConfigPhysicalPlan {

  private TCreateCQReq req;

  private String md5;

  private long firstExecutionTime;

  public AddCQPlan() {
    super(ADD_CQ);
  }

  public AddCQPlan(TCreateCQReq req, String md5, long firstExecutionTime) {
    super(ADD_CQ);
    Validate.notNull(req);
    Validate.notNull(md5);
    this.req = req;
    this.md5 = md5;
    this.firstExecutionTime = firstExecutionTime;
  }

  public TCreateCQReq getReq() {
    return req;
  }

  public String getMd5() {
    return md5;
  }

  public long getFirstExecutionTime() {
    return firstExecutionTime;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTCreateCQReq(req, stream);
    ReadWriteIOUtils.write(md5, stream);
    ReadWriteIOUtils.write(firstExecutionTime, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    req = ThriftCommonsSerDeUtils.deserializeTCreateCQReq(buffer);
    md5 = ReadWriteIOUtils.readString(buffer);
    firstExecutionTime = ReadWriteIOUtils.readLong(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    AddCQPlan addCQPlan = (AddCQPlan) o;
    return firstExecutionTime == addCQPlan.firstExecutionTime
        && Objects.equals(req, addCQPlan.req)
        && Objects.equals(md5, addCQPlan.md5);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), req, md5, firstExecutionTime);
  }
}
