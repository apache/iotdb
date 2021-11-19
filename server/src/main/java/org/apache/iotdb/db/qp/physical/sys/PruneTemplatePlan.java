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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class PruneTemplatePlan extends PhysicalPlan {

  String name;
  String[] prunedMeasurements;

  public PruneTemplatePlan() {
    super(false, OperatorType.PRUNE_TEMPLATE);
  }

  public PruneTemplatePlan(String name, List<String> prunedMeasurements) {
    super(false, OperatorType.PRUNE_TEMPLATE);

    this.name = name;
    this.prunedMeasurements = prunedMeasurements.toArray(new String[0]);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getPrunedMeasurements() {
    return Arrays.asList(prunedMeasurements);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.PRUNE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, buffer);

    // measurements
    ReadWriteIOUtils.write(prunedMeasurements.length, buffer);
    for (String measurement : prunedMeasurements) {
      ReadWriteIOUtils.write(measurement, buffer);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    name = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    prunedMeasurements = new String[size];
    for (int i = 0; i < size; i++) {
      prunedMeasurements[i] = ReadWriteIOUtils.readString(buffer);
    }

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.PRUNE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(name, stream);

    ReadWriteIOUtils.write(prunedMeasurements.length, stream);
    for (String measurement : prunedMeasurements) {
      ReadWriteIOUtils.write(measurement, stream);
    }

    stream.writeLong(index);
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
