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
package org.apache.iotdb.cluster.entity.raft;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.entity.PeerId;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.metadata.MetadataHolder;
import org.apache.iotdb.db.auth.AuthException;

public class MetadataRaftHolder extends MetadataHolder {

  private MetadataStateManchine fsm;
  private PeerId serverId;

  public MetadataRaftHolder(PeerId[] peerIds, PeerId serverId, RpcServer rpcServer) throws AuthException {
    fsm = new MetadataStateManchine();
    this.serverId = serverId;
    service = new RaftService(ClusterConfig.METADATA_GROUP_ID, peerIds, serverId, rpcServer, fsm);
  }

  public MetadataStateManchine getFsm() {
    return fsm;
  }

  public void setFsm(MetadataStateManchine fsm) {
    this.fsm = fsm;
  }
}
