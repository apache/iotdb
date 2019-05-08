/**
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
package org.apache.iotdb.cluster.rpc.raft.request;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.transfer.PhysicalPlanLogTransfer;

public abstract class BasicNonQueryRequest extends BasicRequest{

  private static final long serialVersionUID = -3082772186451384202L;
  /**
   * Serialized physical plans
   */
  private List<byte[]> physicalPlanBytes;

  public BasicNonQueryRequest(String groupID) {
    super(groupID);
  }

  protected void init(List<PhysicalPlan> physicalPlanBytes) throws IOException {
    this.physicalPlanBytes = new ArrayList<>(physicalPlanBytes.size());
    for (PhysicalPlan plan : physicalPlanBytes) {
      this.physicalPlanBytes.add(PhysicalPlanLogTransfer.operatorToLog(plan));
    }
  }

  public List<byte[]> getPhysicalPlanBytes() {
    return physicalPlanBytes;
  }

}
