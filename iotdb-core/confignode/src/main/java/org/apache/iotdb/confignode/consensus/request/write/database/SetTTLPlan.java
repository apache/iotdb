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

  private String[] pathPattern;

  boolean isDataBase = false;

  private long TTL;

  public SetTTLPlan() {
    super(ConfigPhysicalPlanType.SetTTL);
  }

  public SetTTLPlan(long TTL, String... pathPatterns) {
    super(ConfigPhysicalPlanType.SetTTL);
    this.pathPattern = pathPatterns;
    this.TTL = TTL;
  }

  public SetTTLPlan(List<String> pathPattern, long TTL) {
    this();
    this.pathPattern = pathPattern.toArray(new String[0]);
    this.TTL = TTL;
  }

  public SetTTLPlan(String[] pathPattern, long TTL) {
    this();
    this.pathPattern = pathPattern;
    this.TTL = TTL;
  }

  public String[] getPathPattern() {
    return pathPattern;
  }

  public long getTTL() {
    return TTL;
  }

  public void setPathPattern(String[] pathPattern) {
    this.pathPattern = pathPattern;
  }

  public void setTTL(long TTL) {
    this.TTL = TTL;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(pathPattern.length);
    for (String node : pathPattern) {
      BasicStructureSerDeUtil.write(node, stream);
    }
    stream.writeLong(TTL);
    stream.writeBoolean(isDataBase);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int length = buffer.getInt();
    pathPattern = new String[length];
    for (int i = 0; i < length; i++) {
      pathPattern[i] = BasicStructureSerDeUtil.readString(buffer);
    }
    TTL = buffer.getLong();
    isDataBase = buffer.get() != 0;
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
    return isDataBase == setTTLPlan.isDataBase
        && TTL == setTTLPlan.TTL
        && Arrays.equals(this.pathPattern, setTTLPlan.pathPattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(pathPattern), TTL, isDataBase);
  }

  public boolean isDataBase() {
    return isDataBase;
  }

  public void setDataBase(boolean dataBase) {
    isDataBase = dataBase;
  }
}
