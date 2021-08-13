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
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateIndexPlan extends PhysicalPlan {

  protected List<PartialPath> paths;
  private Map<String, String> props;
  private long time;
  private IndexType indexType;

  public CreateIndexPlan() {
    super(false, OperatorType.CREATE_INDEX);
    canBeSplit = false;
  }

  public CreateIndexPlan(
      List<PartialPath> paths, Map<String, String> props, long startTime, IndexType indexType) {
    super(false, OperatorType.CREATE_INDEX);
    this.paths = paths;
    this.props = props;
    time = startTime;
    this.indexType = indexType;
    canBeSplit = false;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
  }

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_INDEX.ordinal());

    stream.write((byte) indexType.serialize());
    stream.writeLong(time);
    stream.writeInt(paths.size());
    for (PartialPath path : paths) {
      putString(stream, path.getFullPath());
    }

    // props
    if (props != null && !props.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(props, stream);
    } else {
      stream.write(0);
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.CREATE_INDEX.ordinal();
    buffer.put((byte) type);
    buffer.put((byte) indexType.serialize());
    buffer.putLong(time);
    buffer.putInt(paths.size());
    for (PartialPath path : paths) {
      putString(buffer, path.getFullPath());
    }

    // props
    if (props != null && !props.isEmpty()) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(props, buffer);
    } else {
      buffer.put((byte) 0);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    indexType = IndexType.deserialize(buffer.get());
    time = buffer.getLong();

    int pathNum = buffer.getInt();
    paths = new ArrayList<>();
    for (int i = 0; i < pathNum; i++) {
      paths.add(new PartialPath(readString(buffer)));
    }

    // props
    if (buffer.get() == 1) {
      props = ReadWriteIOUtils.readMap(buffer);
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
    CreateIndexPlan that = (CreateIndexPlan) o;
    return Objects.equals(paths, that.paths)
        && Objects.equals(props, that.props)
        && time == that.time
        && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(paths, props, time, indexType);
  }

  @Override
  public String toString() {
    return String.format(
        "paths: %s, index type: %s, start time: %s, props: %s", paths, indexType, time, props);
  }
}
