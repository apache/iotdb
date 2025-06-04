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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrAnchor.Type.PARTITION_START;

public class IrAnchor extends IrRowPattern {
  public enum Type {
    PARTITION_START,
    PARTITION_END
  }

  private final Type type;

  public IrAnchor(Type type) {
    this.type = requireNonNull(type, "type is null");
  }

  public Type getType() {
    return type;
  }

  @Override
  public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context) {
    return visitor.visitIrAnchor(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    IrAnchor o = (IrAnchor) obj;
    return Objects.equals(type, o.type);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public String toString() {
    return type == PARTITION_START ? "^" : "$";
  }

  public static void serialize(IrAnchor pattern, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(pattern.type.ordinal(), byteBuffer);
  }

  public static void serialize(IrAnchor pattern, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(pattern.type.ordinal(), stream);
  }

  public static IrAnchor deserialize(ByteBuffer byteBuffer) {
    Type type = Type.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    return new IrAnchor(type);
  }
}
