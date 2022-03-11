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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class DummyPlan extends PhysicalPlan {

  public static final String GROUP_ID_SEPARATOR = "#";
  public static final String META_GROUP_ID = "meta";

  protected byte[] workload;
  protected boolean needForward;
  protected String groupIdentifier = META_GROUP_ID;

  public DummyPlan() {
    super(OperatorType.EMPTY);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.DUMMY.ordinal());
    stream.writeInt(workload == null ? 0 : workload.length);
    if (workload != null) {
      stream.write(workload);
    }
    stream.write(needForward ? 1 : 0);

    byte[] bytes = groupIdentifier.getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.DUMMY.ordinal());
    buffer.putInt(workload == null ? 0 : workload.length);
    if (workload != null) {
      buffer.put(workload);
    }
    buffer.put(needForward ? (byte) 1 : 0);

    byte[] bytes = groupIdentifier.getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int size = buffer.getInt();
    workload = new byte[size];
    buffer.get(workload);
    needForward = buffer.get() == 1;

    int groupIdSize = buffer.getInt();
    byte[] bytes = new byte[groupIdSize];
    buffer.get(bytes);
    groupIdentifier = new String(bytes);
  }

  public void setWorkload(byte[] workload) {
    this.workload = workload;
  }

  public boolean isNeedForward() {
    return needForward;
  }

  public void setNeedForward(boolean needForward) {
    this.needForward = needForward;
  }

  public byte[] getWorkload() {
    return workload;
  }

  public String getGroupIdentifier() {
    return groupIdentifier;
  }

  public void setGroupIdentifier(String groupIdentifier) {
    this.groupIdentifier = groupIdentifier;
  }

  @Override
  public String toString() {
    return "ExprPlan";
  }
}
