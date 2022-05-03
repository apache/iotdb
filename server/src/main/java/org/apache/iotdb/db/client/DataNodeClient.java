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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataNodeClient extends RegionClient {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeClient.class);

  private InternalService.Iface client;

  protected DataNodeClient(
      List<TEndPoint> nodes, ConsensusGroupId regionId, ClientManager clientManager) {
    super(nodes, regionId, clientManager);
  }

  // public DataNodeClient(ConsensusGroupId regionID, List<TEndPoint> nodes) {
  //        super(nodes,regionID,null);
  //    }

  @Override
  protected void reconnect() throws IoTDBConnectionException {
    super.reconnect();
  }

  @Override
  public void close() throws Exception {}
}
