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
 *
 */
package org.apache.iotdb.db.newsync.pipedata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.receiver.load.ILoader;
import org.apache.iotdb.db.newsync.receiver.load.SchemaLoader;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SchemaPipeData extends PipeData {
  private static final int SERIALIZE_BUFFER_SIZE = 1024;

  private PhysicalPlan plan;

  public SchemaPipeData(PhysicalPlan plan, long serialNumber) {
    super(serialNumber);
    this.plan = plan;
  }

  @Override
  public Type getType() {
    return Type.PHYSICALPLAN;
  }

  @Override
  public long serialize(DataOutputStream stream) throws IOException {
    long serializeSize = super.serialize(stream);
    byte[] bytes = getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);
    serializeSize += (Integer.BYTES + bytes.length);
    return serializeSize;
  }

  private byte[] getBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(SERIALIZE_BUFFER_SIZE);
    plan.serialize(buffer);
    byte[] bytes = new byte[buffer.position()];
    buffer.flip();
    buffer.get(bytes);
    return bytes;
  }

  public static SchemaPipeData deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    long serialNumber = stream.readLong();
    byte[] bytes = new byte[stream.readInt()];
    stream.read(bytes);
    PhysicalPlan plan = PhysicalPlan.Factory.create(ByteBuffer.wrap(bytes));
    return new SchemaPipeData(plan, serialNumber);
  }

  @Override
  public ILoader createLoader() {
    return new SchemaLoader(plan);
  }

  @Override
  public void sendToTransport() {
    // senderTransport(getBytes(), this);
    // System.out.println(this);
  }

  @Override
  public String toString() {
    return "SchemaPipeData{" + "serialNumber=" + serialNumber + ", plan=" + plan + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaPipeData that = (SchemaPipeData) o;
    return Objects.equals(plan, that.plan) && Objects.equals(serialNumber, that.serialNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(plan, serialNumber);
  }
}
