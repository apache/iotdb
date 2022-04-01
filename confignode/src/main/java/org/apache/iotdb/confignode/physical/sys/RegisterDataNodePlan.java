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
package org.apache.iotdb.confignode.physical.sys;

import org.apache.iotdb.commons.partition.Endpoint;
import org.apache.iotdb.commons.partition.DataNodeLocation;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;

import java.nio.ByteBuffer;

public class RegisterDataNodePlan extends PhysicalPlan {

  private DataNodeLocation info;

  public RegisterDataNodePlan() {
    super(PhysicalPlanType.RegisterDataNode);
  }

  public RegisterDataNodePlan(int dataNodeID, Endpoint endpoint) {
    this();
    this.info = new DataNodeLocation(dataNodeID, endpoint);
  }

  public DataNodeLocation getInfo() {
    return info;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.RegisterDataNode.ordinal());
    buffer.putInt(info.getDataNodeID());
    buffer.putInt(info.getEndPoint().getIp().length());
    buffer.put(info.getEndPoint().getIp().getBytes());
    buffer.putInt(info.getEndPoint().getPort());
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    int dataNodeID = buffer.getInt();
    int ipLength = buffer.getInt();
    byte[] byteIp = new byte[ipLength];
    buffer.get(byteIp, 0, ipLength);
    String ip = new String(byteIp, 0, ipLength);
    int port = buffer.getInt();

    this.info = new DataNodeLocation(dataNodeID, new Endpoint(ip, port));
  }
}
