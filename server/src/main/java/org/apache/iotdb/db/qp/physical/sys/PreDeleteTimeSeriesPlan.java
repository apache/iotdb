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
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class PreDeleteTimeSeriesPlan extends PhysicalPlan {

  private PartialPath path;

  public PreDeleteTimeSeriesPlan() {
    super(Operator.OperatorType.PRE_DELETE_TIMESERIES_IN_CLUSTER);
  }

  public PreDeleteTimeSeriesPlan(PartialPath path) {
    super(Operator.OperatorType.PRE_DELETE_TIMESERIES_IN_CLUSTER);
    this.path = path;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.singletonList(path);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.PRE_DELETE_TIMESERIES_IN_CLUSTER.ordinal());
    path.serialize(stream);
    stream.writeLong(index);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.PRE_DELETE_TIMESERIES_IN_CLUSTER.ordinal());
    path.serialize(buffer);
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException, IOException {
    path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
    index = buffer.getLong();
  }
}
