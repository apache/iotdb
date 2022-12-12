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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RewriteTimeseriesPlan extends PhysicalPlan {

  private PartialPath path;

  public RewriteTimeseriesPlan() {
    super(Operator.OperatorType.REWRITE_TIMESERIES);
  }

  public RewriteTimeseriesPlan(PartialPath path) {
    super(Operator.OperatorType.REWRITE_TIMESERIES);
    this.path = path;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public List<PartialPath> getPaths() {
    return path != null ? Collections.singletonList(path) : Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.REWRITE_TIMESERIES.ordinal());
    putString(stream, path.getFullPath());
    stream.writeLong(index);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.REWRITE_TIMESERIES.ordinal());
    putString(buffer, path.getFullPath());
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    path = new PartialPath(readString(buffer));
    this.index = buffer.getLong();
  }

  @Override
  public String toString() {
    return "RewriteTimeseries{" + path + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RewriteTimeseriesPlan that = (RewriteTimeseriesPlan) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}
