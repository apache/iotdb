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
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DropIndexPlan extends PhysicalPlan {

  protected List<PartialPath> paths;
  private IndexType indexType;

  public DropIndexPlan() {
    super(false, Operator.OperatorType.DROP_INDEX);
  }

  public DropIndexPlan(List<PartialPath> paths, IndexType indexType) {
    super(false, OperatorType.DROP_INDEX);
    this.paths = paths;
    this.indexType = indexType;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.DROP_INDEX.ordinal());
    stream.write((byte) indexType.serialize());

    stream.writeInt(paths.size());
    for (PartialPath path : paths) {
      putString(stream, path.getFullPath());
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.DROP_INDEX.ordinal();
    buffer.put((byte) type);
    buffer.put((byte) indexType.serialize());

    buffer.putInt(paths.size());
    for (PartialPath path : paths) {
      putString(buffer, path.getFullPath());
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    indexType = IndexType.deserialize(buffer.get());

    int pathNum = buffer.getInt();
    paths = new ArrayList<>();
    for (int i = 0; i < pathNum; i++) {
      paths.add(new PartialPath(readString(buffer)));
    }

    this.index = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DropIndexPlan that = (DropIndexPlan) o;
    return Objects.equals(paths, that.paths) && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(paths, indexType);
  }

  @Override
  public String toString() {
    return String.format("paths: %s, index type: %s", paths, indexType);
  }
}
