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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class DropTriggerPlan extends PhysicalPlan {

  private String triggerName;

  public DropTriggerPlan() {
    super(false, OperatorType.DROP_TRIGGER);
    canBeSplit = false;
  }

  public DropTriggerPlan(String triggerName) {
    super(false, OperatorType.DROP_TRIGGER);
    this.triggerName = triggerName;
    canBeSplit = false;
  }

  public String getTriggerName() {
    return triggerName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.DROP_TRIGGER.ordinal());

    putString(stream, triggerName);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.DROP_TRIGGER.ordinal());

    putString(buffer, triggerName);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    triggerName = readString(buffer);
  }
}
