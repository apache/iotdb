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

package org.apache.iotdb.confignode.consensus.request.write.database;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SetTTLPlan extends ConfigPhysicalPlan {

  private String[] databasePathPattern;

  private long TTL;

  public SetTTLPlan() {
    super(ConfigPhysicalPlanType.SetTTL);
  }

  public SetTTLPlan(List<String> databasePathPattern, long TTL) {
    this();
    this.databasePathPattern = databasePathPattern.toArray(new String[0]);
    this.TTL = TTL;
  }

  public String[] getDatabasePathPattern() {
    return databasePathPattern;
  }

  public long getTTL() {
    return TTL;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(databasePathPattern.length);
    for (String node : databasePathPattern) {
      BasicStructureSerDeUtil.write(node, stream);
    }
    stream.writeLong(TTL);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {

    int length = buffer.getInt();
    databasePathPattern = new String[length];
    for (int i = 0; i < length; i++) {
      databasePathPattern[i] = BasicStructureSerDeUtil.readString(buffer);
    }
    TTL = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SetTTLPlan setTTLPlan = (SetTTLPlan) o;
    return TTL == setTTLPlan.TTL
        && Arrays.equals(this.databasePathPattern, setTTLPlan.databasePathPattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(databasePathPattern), TTL);
  }
}
