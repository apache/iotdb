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

package org.apache.iotdb.cluster.log.meta;

import static org.apache.iotdb.cluster.log.Log.Types.PHYSICAL_PLAN;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class PhysicalPlanLog extends Log {

  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private PhysicalPlan plan;

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      dataOutputStream.writeByte((byte) PHYSICAL_PLAN.ordinal());

      dataOutputStream.writeLong(getPreviousLogIndex());
      dataOutputStream.writeLong(getPreviousLogTerm());
      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getPreviousLogTerm());

      plan.serializeTo(dataOutputStream);
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

    setPreviousLogIndex(buffer.getLong());
    setPreviousLogTerm(buffer.getLong());
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());

    plan.deserializeFrom(buffer);
  }

  public PhysicalPlan getPlan() {
    return plan;
  }
}
