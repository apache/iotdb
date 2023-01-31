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

/** The parameter of `GROUP BY`. including: `GROUP BY VARIATION` */
public class GroupByParameter {

  WindowType windowType;

  boolean ignoringNull;

  // the parameter of `GROUP BY VARIATION`
  double delta;

  public GroupByParameter(WindowType windowType, boolean ignoringNull) {
    this.windowType = windowType;
    this.ignoringNull = ignoringNull;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public boolean isIgnoringNull() {
    return ignoringNull;
  }

  // parameters for `GROUP BY VARIATION`
  public void setDelta(double delta) {
    this.delta = delta;
  }

  public double getDelta() {
    return delta;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(windowType.getType(), buffer);
    ReadWriteIOUtils.write(ignoringNull, buffer);
    if (windowType == WindowType.EVENT_WINDOW) {
      ReadWriteIOUtils.write(delta, buffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(windowType.getType(), stream);
    ReadWriteIOUtils.write(ignoringNull, stream);
    if (windowType == WindowType.EVENT_WINDOW) {
      ReadWriteIOUtils.write(delta, stream);
    }
  }

  public static GroupByParameter deserialize(ByteBuffer buffer) {
    byte type = ReadWriteIOUtils.readByte(buffer);
    boolean ignoringNull = ReadWriteIOUtils.readBool(buffer);
    if (type == WindowType.EVENT_WINDOW.getType()) {
      double delta = ReadWriteIOUtils.readDouble(buffer);
      GroupByParameter groupByParameter =
          new GroupByParameter(WindowType.EVENT_WINDOW, ignoringNull);
      groupByParameter.setDelta(delta);
      return groupByParameter;
    } else {
      throw new IllegalArgumentException("Unsupported Type in group by.");
    }
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof GroupByParameter)) {
      return false;
    }
    GroupByParameter other = (GroupByParameter) obj;
    boolean commonPart =
        this.windowType == other.windowType && this.ignoringNull == other.ignoringNull;

    if (!commonPart) return false;
    switch (this.windowType) {
      case EVENT_WINDOW:
        return this.delta == other.delta;
      default:
        return true;
    }
  }

  public int hashCode() {
    return Objects.hash(windowType, ignoringNull, delta);
  }
}
