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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** The parameter of `GROUP BY` clause. */
public abstract class GroupByParameter {

  protected WindowType windowType;

  protected boolean ignoringNull;

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

  protected abstract void serializeAttributes(ByteBuffer byteBuffer);

  protected abstract void serializeAttributes(DataOutputStream stream) throws IOException;

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(windowType.getType(), buffer);
    ReadWriteIOUtils.write(ignoringNull, buffer);
    serializeAttributes(buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(windowType.getType(), stream);
    ReadWriteIOUtils.write(ignoringNull, stream);
    serializeAttributes(stream);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GroupByParameter)) {
      return false;
    }
    GroupByParameter other = (GroupByParameter) obj;
    return this.windowType == other.windowType && this.ignoringNull == other.ignoringNull;
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowType, ignoringNull);
  }

  public static GroupByParameter deserialize(ByteBuffer byteBuffer) {
    byte type = ReadWriteIOUtils.readByte(byteBuffer);
    if (type == WindowType.EVENT_WINDOW.getType()) {
      return GroupByVariationParameter.deserialize(byteBuffer);
    } else if (type == WindowType.SERIES_WINDOW.getType()) {
      return GroupBySeriesParameter.deserialize(byteBuffer);
    } else throw new SemanticException("Unsupported window type");
  }
}
