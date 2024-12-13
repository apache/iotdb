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

public class IrLabel extends IrRowPattern {
  private final String name;

  /**
   * Create IrLabel with given name. The name has to be in the canonical form with respect to SQL
   * identifier semantics.
   */
  public IrLabel(String name) {
    this.name = requireNonNull(name, "name is null");
  }

  public String getName() {
    return name;
  }

  @Override
  public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context) {
    return visitor.visitIrLabel(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    IrLabel o = (IrLabel) obj;
    return Objects.equals(name, o.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name;
  }

  public static void serialize(IrLabel label, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(label.name, byteBuffer);
  }

  public static void serialize(IrLabel label, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(label.name, stream);
  }

  public static IrLabel deserialize(ByteBuffer byteBuffer) {
    String name = ReadWriteIOUtils.readString(byteBuffer);
    return new IrLabel(name);
  }
}
