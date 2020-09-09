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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MeasurementNodePlan extends PhysicalPlan {
  private MeasurementSchema schema;
  private String alias;
  private long offset;
  private String name;
  protected int childSize;

  public MeasurementNodePlan() {
    super(false, Operator.OperatorType.MEASUREMENT_MNODE);
  }

  public MeasurementNodePlan(String name, String alias,
                             long offset, int childSize, MeasurementSchema schema) {
    super(false, Operator.OperatorType.MEASUREMENT_MNODE);
    this.name = name;
    this.alias = alias;
    this.offset = offset;
    this.childSize = childSize;
    this.schema = schema;
  }

  @Override
  public List<PartialPath> getPaths() {
    return new ArrayList<>();
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.MEASUREMENT_MNODE.ordinal());

    putString(buffer, name);
    putString(buffer, alias);
    buffer.putLong(offset);
    buffer.putInt(childSize);
    schema.serializeTo(buffer);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    name = readString(buffer);
    alias = readString(buffer);
    offset = buffer.getLong();
    childSize = buffer.getInt();
    schema = MeasurementSchema.deserializeFrom(buffer);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getChildSize() {
    return childSize;
  }

  public void setChildSize(int childSize) {
    this.childSize = childSize;
  }

  public MeasurementSchema getSchema() {
    return schema;
  }

  public void setSchema(MeasurementSchema schema) {
    this.schema = schema;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "MeasurementMNode{" + name + "," + alias+ "," +
      schema + "," + offset + "," + childSize + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MeasurementNodePlan that = (MeasurementNodePlan) o;
    return Objects.equals(name, that.name)
      && Objects.equals(alias, that.alias)
      && Objects.equals(schema, that.schema)
      && Objects.equals(offset, that.offset)
      && Objects.equals(childSize, that.childSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, alias, schema, offset, childSize);
  }
}
