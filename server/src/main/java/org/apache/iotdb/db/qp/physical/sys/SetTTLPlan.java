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

public class SetTTLPlan extends PhysicalPlan {

  private PartialPath storageGroup;
  private long dataTTL;

  public SetTTLPlan() {
    super(false, OperatorType.TTL);
  }

  public SetTTLPlan(PartialPath storageGroup, long dataTTL) {
    // set TTL
    super(false, OperatorType.TTL);
    this.storageGroup = storageGroup;
    this.dataTTL = dataTTL;
  }

  public SetTTLPlan(PartialPath storageGroup) {
    // unset TTL
    this(storageGroup, Long.MAX_VALUE);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.TTL.ordinal();
    stream.writeByte((byte) type);
    stream.writeLong(dataTTL);
    putString(stream, storageGroup.getFullPath());

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.TTL.ordinal();
    buffer.put((byte) type);
    buffer.putLong(dataTTL);
    putString(buffer, storageGroup.getFullPath());

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.dataTTL = buffer.getLong();
    this.storageGroup = new PartialPath(readString(buffer));

    this.index = buffer.getLong();
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }
}
