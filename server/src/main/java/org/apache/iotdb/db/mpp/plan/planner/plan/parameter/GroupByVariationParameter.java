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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GroupByVariationParameter extends GroupByParameter {

  double delta;

  boolean ignoringNull;

  public GroupByVariationParameter(boolean ignoringNull, double delta) {
    super(WindowType.VARIATION_WINDOW);
    this.delta = delta;
    this.ignoringNull = ignoringNull;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(ignoringNull, byteBuffer);
    ReadWriteIOUtils.write(delta, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(ignoringNull, stream);
    ReadWriteIOUtils.write(delta, stream);
  }

  public double getDelta() {
    return delta;
  }

  public boolean isIgnoringNull() {
    return ignoringNull;
  }

  public static GroupByParameter deserialize(ByteBuffer buffer) {
    boolean ignoringNull = ReadWriteIOUtils.readBool(buffer);
    double delta = ReadWriteIOUtils.readDouble(buffer);
    return new GroupByVariationParameter(ignoringNull, delta);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    return this.delta == ((GroupByVariationParameter) obj).getDelta()
        && this.ignoringNull == ((GroupByVariationParameter) obj).isIgnoringNull();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), delta, ignoringNull);
  }
}
