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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SelectIntoPlan extends PhysicalPlan {

  private QueryPlan queryPlan;
  private PartialPath fromPath;
  private List<PartialPath> intoPaths;
  private boolean isIntoPathsAligned;

  public SelectIntoPlan() {
    super(OperatorType.SELECT_INTO);
  }

  public SelectIntoPlan(
      QueryPlan queryPlan,
      PartialPath fromPath,
      List<PartialPath> intoPaths,
      boolean isIntoPathsAligned) {
    super(OperatorType.SELECT_INTO);
    this.queryPlan = queryPlan;
    this.fromPath = fromPath;
    this.intoPaths = intoPaths;
    this.isIntoPathsAligned = isIntoPathsAligned;
  }

  @Override
  public boolean isSelectInto() {
    return true;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.writeByte((byte) PhysicalPlanType.SELECT_INTO.ordinal());

    queryPlan.serialize(outputStream);

    putString(outputStream, fromPath.getFullPath());

    outputStream.writeInt(intoPaths.size());
    for (PartialPath intoPath : intoPaths) {
      putString(outputStream, intoPath.getFullPath());
    }

    outputStream.writeByte(isIntoPathsAligned ? 1 : 0);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.SELECT_INTO.ordinal());

    queryPlan.serialize(buffer);

    putString(buffer, fromPath.getFullPath());

    buffer.putInt(intoPaths.size());
    for (PartialPath intoPath : intoPaths) {
      putString(buffer, intoPath.getFullPath());
    }

    buffer.put((byte) (isIntoPathsAligned ? 1 : 0));
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException, IOException {
    queryPlan = (QueryPlan) Factory.create(buffer);

    fromPath = new PartialPath(readString(buffer));

    int intoPathsSize = buffer.getInt();
    intoPaths = new ArrayList<>(intoPathsSize);
    for (int i = 0; i < intoPathsSize; ++i) {
      intoPaths.add(new PartialPath(readString(buffer)));
    }

    isIntoPathsAligned = buffer.get() == (byte) 1;
  }

  /** mainly for query auth. */
  @Override
  public List<MeasurementPath> getPaths() {
    return queryPlan.getPaths();
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  public PartialPath getFromPath() {
    return fromPath;
  }

  public List<PartialPath> getIntoPaths() {
    return intoPaths;
  }

  public boolean isIntoPathsAligned() {
    return isIntoPathsAligned;
  }
}
