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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class SetSystemModePlan extends PhysicalPlan {

  private boolean isReadOnly;

  public SetSystemModePlan() {
    super(false, OperatorType.SET_SYSTEM_MODE);
  }

  public SetSystemModePlan(boolean isReadOnly) {
    super(false, OperatorType.SET_SYSTEM_MODE);
    this.isReadOnly = isReadOnly;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public boolean isReadOnly() {
    return isReadOnly;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.writeByte((byte) PhysicalPlanType.SET_SYSTEM_MODE.ordinal());

    outputStream.writeBoolean(isReadOnly);
    outputStream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.SET_SYSTEM_MODE.ordinal());

    ReadWriteIOUtils.write(isReadOnly, buffer);

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {

    isReadOnly = buffer.get() == 1;
    this.index = buffer.getLong();
  }
}
