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
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class PreDeleteTimeSeriesPlan extends PhysicalPlan {

  private PathPatternTree patternTree;

  public PreDeleteTimeSeriesPlan() {
    super(Operator.OperatorType.PRE_DELETE_TIMESERIES_IN_CLUSTER);
  }

  public PreDeleteTimeSeriesPlan(PathPatternTree patternTree) {
    super(Operator.OperatorType.PRE_DELETE_TIMESERIES_IN_CLUSTER);
    this.patternTree = patternTree;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public void setPatternTree(PathPatternTree patternTree) {
    this.patternTree = patternTree;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return patternTree.getAllPathPatterns();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.PRE_DELETE_TIMESERIES_IN_CLUSTER.ordinal());
    patternTree.serialize(stream);
    stream.writeLong(index);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.PRE_DELETE_TIMESERIES_IN_CLUSTER.ordinal());
    patternTree.serialize(buffer);
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException, IOException {
    patternTree = PathPatternTree.deserialize(buffer);
    index = buffer.getLong();
  }
}
