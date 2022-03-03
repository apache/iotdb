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

package org.apache.iotdb.cluster.expr;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.exception.StartUpCheckFailureException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogDispatcher;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;

import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;
import java.util.Arrays;

public class ExprServer extends MetaClusterServer {

  protected ExprServer() throws QueryProcessException {
    super();
  }

  @Override
  protected MetaGroupMember createMetaGroupMember() throws QueryProcessException {
    return new ExprMember(new Factory(), thisNode, coordinator);
  }

  @Override
  protected String getClientThreadPrefix() {
    return "FollowerClient";
  }

  @Override
  protected String getServerClientName() {
    return "FollowerServer";
  }

  @Override
  protected TServerTransport getServerSocket() throws TTransportException {
    return new TServerSocket(new InetSocketAddress("0.0.0.0", thisNode.getMetaPort()));
  }

  public static void main(String[] args)
      throws StartupException, TTransportException, QueryProcessException,
          ConfigInconsistentException, StartUpCheckFailureException {

    String[] nodeStrings = args[0].split(":");
    String ip = nodeStrings[0];
    int port = Integer.parseInt(nodeStrings[1]);
    String[] allNodeStr = args[1].split(",");

    int dispatcherThreadNum = Integer.parseInt(args[2]);
    boolean useIndirectDispatcher = Boolean.parseBoolean(args[3]);
    boolean bypassRaft = Boolean.parseBoolean(args[4]);
    boolean useSW = Boolean.parseBoolean(args[5]);
    boolean enableWeakAcceptance = Boolean.parseBoolean(args[6]);
    boolean enableCommitReturn = Boolean.parseBoolean(args[7]);
    int maxBatchSize = Integer.parseInt(args[8]);
    int defaultLogBufferSize = Integer.parseInt(args[9]);
    boolean useCRaft = Boolean.parseBoolean(args[10]);

    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(Arrays.asList(allNodeStr));
    ClusterDescriptor.getInstance().getConfig().setInternalMetaPort(port);
    ClusterDescriptor.getInstance().getConfig().setInternalIp(ip);
    ClusterDescriptor.getInstance().getConfig().setEnableRaftLogPersistence(false);
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(50000);
    // ClusterDescriptor.getInstance().getConfig().setUseBatchInLogCatchUp(false);
    RaftMember.USE_LOG_DISPATCHER = true;
    ClusterDescriptor.getInstance().getConfig().setUseIndirectBroadcasting(useIndirectDispatcher);
    LogDispatcher.bindingThreadNum = dispatcherThreadNum;
    LogDispatcher.maxBatchSize = maxBatchSize;
    ExprMember.bypassRaft = bypassRaft;
    ExprMember.useSlidingWindow = useSW;
    ExprMember.ENABLE_WEAK_ACCEPTANCE = enableWeakAcceptance;
    ExprMember.ENABLE_COMMIT_RETURN = enableCommitReturn;
    Log.DEFAULT_BUFFER_SIZE = defaultLogBufferSize * 1024 + 512;
    RaftMember.USE_CRAFT = useCRaft;

    ExprServer server = new ExprServer();
    server.start();
    server.buildCluster();
  }
}
