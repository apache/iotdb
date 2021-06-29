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

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DropContinuousQueryPlan extends PhysicalPlan {

  private String continuousQueryName;

  public DropContinuousQueryPlan() {
    super(false, Operator.OperatorType.DROP_CONTINUOUS_QUERY);
  }

  public DropContinuousQueryPlan(String continuousQueryName) {
    super(false, Operator.OperatorType.DROP_CONTINUOUS_QUERY);
    this.continuousQueryName = continuousQueryName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return new ArrayList<>();
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.DROP_CONTINUOUS_QUERY.ordinal());
    ReadWriteIOUtils.write(continuousQueryName, buffer);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
  }
}
