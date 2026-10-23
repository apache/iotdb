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

package org.apache.iotdb.confignode.consensus.request.write.procedure;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A single consensus entry for a parent procedure and its newly submitted children. */
public class UpdateProceduresPlan extends ConfigPhysicalPlan {
  private List<Procedure<ConfigNodeProcedureEnv>> procedures;

  public UpdateProceduresPlan() {
    super(ConfigPhysicalPlanType.UpdateProcedures);
  }

  public UpdateProceduresPlan(List<Procedure<ConfigNodeProcedureEnv>> procedures) {
    this();
    this.procedures = new ArrayList<>(procedures);
  }

  public List<Procedure<ConfigNodeProcedureEnv>> getProcedures() {
    return procedures;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(procedures.size());
    for (Procedure<ConfigNodeProcedureEnv> procedure : procedures) {
      ByteBuffer buffer = new UpdateProcedurePlan(procedure).serializeToByteBuffer();
      stream.writeInt(buffer.remaining());
      stream.write(buffer.array(), buffer.position(), buffer.remaining());
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    procedures = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int length = buffer.getInt();
      // Some procedures read optional trailing fields. Isolate each record from the next one.
      ByteBuffer procedureBuffer = buffer.slice();
      procedureBuffer.limit(length);
      procedures.add(
          ((UpdateProcedurePlan) ConfigPhysicalPlan.Factory.create(procedureBuffer))
              .getProcedure());
      buffer.position(buffer.position() + length);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return procedures.equals(((UpdateProceduresPlan) obj).procedures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(procedures);
  }
}
