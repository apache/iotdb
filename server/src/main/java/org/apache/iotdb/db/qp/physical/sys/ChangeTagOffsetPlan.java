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
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ChangeTagOffsetPlan extends PhysicalPlan {
  private PartialPath path;
  private long offset;

  public ChangeTagOffsetPlan() {
    super(false, Operator.OperatorType.CHANGE_TAG_OFFSET);
  }

  public ChangeTagOffsetPlan(PartialPath partialPath, long offset) {
    super(false, Operator.OperatorType.CHANGE_TAG_OFFSET);
    path = partialPath;
    this.offset = offset;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    if (path != null) {
      ret.add(path);
    }
    return ret;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.CHANGE_TAG_OFFSET.ordinal();
    buffer.put((byte) type);
    putString(buffer, path.getFullPath());
    buffer.putLong(offset);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.CHANGE_TAG_OFFSET.ordinal());
    putString(stream, path.getFullPath());
    stream.writeLong(offset);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    path = new PartialPath(readString(buffer));
    offset = buffer.getLong();
  }

  @Override
  public String toString() {
    return "ChangeTagOffset{" + path + "," + offset + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChangeTagOffsetPlan that = (ChangeTagOffsetPlan) o;
    return Objects.equals(path, that.path) && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, offset);
  }
}
