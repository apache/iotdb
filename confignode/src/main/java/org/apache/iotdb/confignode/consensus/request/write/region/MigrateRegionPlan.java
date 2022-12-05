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
package org.apache.iotdb.confignode.consensus.request.write.region;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MigrateRegionPlan extends ConfigPhysicalPlan {

  private int regionId;

  private int fromId;

  private int toId;

  public MigrateRegionPlan() {
    super(ConfigPhysicalPlanType.MigrateRegion);
  }

  public MigrateRegionPlan(int regionId, int fromId, int toId) {
    this();
    this.regionId = regionId;
    this.fromId = fromId;
    this.toId = toId;
  }

  public int getRegionId() {
    return this.regionId;
  }

  public int getFromId() {
    return this.fromId;
  }

  public int getToId() {
    return this.toId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(regionId);
    stream.writeInt(fromId);
    stream.writeInt(toId);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.regionId = buffer.getInt();
    this.fromId = buffer.getInt();
    this.toId = buffer.getInt();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MigrateRegionPlan that = (MigrateRegionPlan) o;
    return this.regionId == that.getRegionId()
        && this.fromId == that.getFromId()
        && this.toId == that.getToId();
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + regionId;
    hashcode = hashcode * 31 + fromId;
    hashcode = hashcode * 31 + toId;
    return hashcode;
  }
}
