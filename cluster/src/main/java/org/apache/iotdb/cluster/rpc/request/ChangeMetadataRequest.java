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
package org.apache.iotdb.cluster.rpc.request;

import java.io.IOException;
import java.io.Serializable;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;

public class ChangeMetadataRequest extends BasicRequest implements Serializable {

  private byte[] physicalPlanBytes;
  private byte[] requestType;

  public ChangeMetadataRequest(String groupID, PhysicalPlan plan) throws IOException {
    super(groupID);
    this.physicalPlanBytes = PhysicalPlanLogTransfer.operatorToLog(plan);
    this.requestType = String.valueOf(plan.getOperatorType()).getBytes();
  }

  public byte[] getPhysicalPlanBytes() {
    return physicalPlanBytes;
  }

  public void setPhysicalPlanBytes(byte[] physicalPlanBytes) {
    this.physicalPlanBytes = physicalPlanBytes;
  }

  public byte[] getRequestType() {
    return requestType;
  }

  public void setRequestType(byte[] requestType) {
    this.requestType = requestType;
  }
}
