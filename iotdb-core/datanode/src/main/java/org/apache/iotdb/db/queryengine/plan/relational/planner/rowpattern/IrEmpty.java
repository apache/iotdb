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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class IrEmpty extends IrRowPattern {
  public IrEmpty() {}

  @Override
  public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context) {
    return visitor.visitIrEmpty(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public String toString() {
    return "()";
  }

  public static void serialize(IrEmpty pattern, ByteBuffer byteBuffer) {}

  public static void serialize(IrEmpty pattern, DataOutputStream stream) throws IOException {}

  public static IrEmpty deserialize(ByteBuffer byteBuffer) {
    return new IrEmpty();
  }
}
