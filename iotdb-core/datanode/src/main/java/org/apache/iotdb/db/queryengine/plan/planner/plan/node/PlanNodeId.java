/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class PlanNodeId implements Accountable {
  private final String id;

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PlanNodeId.class);

  public PlanNodeId(String id) {
    this.id = id;
  }

  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return this.id;
  }

  @Override
  public int hashCode() {
    return this.id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PlanNodeId) {
      return this.id.equals(((PlanNodeId) obj).getId());
    }
    return false;
  }

  public static PlanNodeId deserialize(ByteBuffer byteBuffer) {
    return new PlanNodeId(ReadWriteIOUtils.readString(byteBuffer));
  }

  public static PlanNodeId deserialize(InputStream stream) throws IOException {
    return new PlanNodeId(ReadWriteIOUtils.readString(stream));
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(id, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(id, stream);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + RamUsageEstimator.sizeOf(id);
  }
}
