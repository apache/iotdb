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
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SchemaPipeData extends PipeData {
  private static final int SERIALIZE_BUFFER_SIZE = 1024;

  private PhysicalPlan plan;

  public SchemaPipeData(long serialNumber) {
    super(serialNumber);
  }

  public SchemaPipeData(PhysicalPlan plan, long serialNumber) {
    super(serialNumber);
    this.plan = plan;
  }

  @Override
  public Type getType() {
    return Type.PHYSICALPLAN;
  }

  @Override
  public long serializeImpl(DataOutputStream stream) throws IOException {
    byte[] bytes = getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);
    return Integer.BYTES + bytes.length;
  }

  private byte[] getBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(SERIALIZE_BUFFER_SIZE);
    plan.serialize(buffer);
    byte[] bytes = new byte[buffer.position()];
    buffer.flip();
    buffer.get(bytes);
    return bytes;
  }

  @Override
  public void deserializeImpl(DataInputStream stream) throws IOException, IllegalPathException {
    byte[] bytes = new byte[stream.readInt()];
    stream.read(bytes);
    this.plan = PhysicalPlan.Factory.create(ByteBuffer.wrap(bytes));
  }

  @Override
  public void sendToTransport() {
    // senderTransport(getBytes(), this);
    System.out.println(this);
  }

  @Override
  public String toString() {
    return "SchemaPipeData{" + "serialNumber=" + serialNumber + ", plan=" + plan + '}';
  }
}
