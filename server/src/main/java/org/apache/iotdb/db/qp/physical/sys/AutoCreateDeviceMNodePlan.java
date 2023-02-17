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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IAutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class AutoCreateDeviceMNodePlan extends PhysicalPlan implements IAutoCreateDeviceMNodePlan {

  private static final Logger logger = LoggerFactory.getLogger(AutoCreateDeviceMNodePlan.class);
  protected PartialPath path;

  public AutoCreateDeviceMNodePlan() {
    super(Operator.OperatorType.AUTO_CREATE_DEVICE_MNODE);
  }

  public AutoCreateDeviceMNodePlan(PartialPath path) {
    super(Operator.OperatorType.AUTO_CREATE_DEVICE_MNODE);
    this.path = path;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(path);
  }

  public PartialPath getPath() {
    return path;
  }

  @Override
  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.AUTO_CREATE_DEVICE_MNODE.ordinal());
    putString(buffer, path.getFullPath());
    buffer.putLong(index);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.AUTO_CREATE_DEVICE_MNODE.ordinal());
    putString(stream, path.getFullPath());
    stream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    String pathString = readString(buffer);
    try {
      path = new PartialPath(pathString);
    } catch (IllegalPathException e) {
      logger.error("Failed to deserialize device {} from buffer", pathString);
    }
    index = buffer.getLong();
  }
}
