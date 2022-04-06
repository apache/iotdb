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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.source;

import java.nio.ByteBuffer;
import org.apache.iotdb.commons.partition.DataRegionReplicaSet;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public abstract class SourceNode extends PlanNode implements AutoCloseable {

  public SourceNode(PlanNodeId id) {
    super(id);
  }

  public abstract void open() throws Exception;

  public abstract DataRegionReplicaSet getDataRegionReplicaSet();

  public abstract void setDataRegionReplicaSet(DataRegionReplicaSet dataRegionReplicaSet);

  public abstract String getDeviceName();

  protected abstract String getExpressionString();

  @Override
  public final int hashCode() {
    return getExpressionString().hashCode();
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SourceNode)) {
      return false;
    }

    return getExpressionString().equals(o.toString());
  }
}
