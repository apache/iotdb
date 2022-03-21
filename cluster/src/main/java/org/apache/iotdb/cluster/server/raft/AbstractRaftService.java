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

package org.apache.iotdb.cluster.server.raft;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.thrift.TBaseAsyncProcessor;

public abstract class AbstractRaftService extends ThriftService {

  public void initThriftServiceThread(
      String daemonThreadName, String clientThreadName, ThriftServiceThread.ServerType serverType)
      throws IllegalAccessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    try {
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        thriftServiceThread =
            new ThriftServiceThread(
                (TBaseAsyncProcessor) processor,
                getID().getName(),
                clientThreadName,
                getBindIP(),
                getBindPort(),
                config.getRpcMaxConcurrentClientNum(),
                config.getThriftServerAwaitTimeForStopService(),
                new RaftServiceHandler(),
                false,
                ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS(),
                config.getThriftMaxFrameSize(),
                serverType);
      } else {
        thriftServiceThread =
            new ThriftServiceThread(
                processor,
                getID().getName(),
                clientThreadName,
                getBindIP(),
                getBindPort(),
                config.getRpcMaxConcurrentClientNum(),
                config.getThriftServerAwaitTimeForStopService(),
                new RaftServiceHandler(),
                false);
      }
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(daemonThreadName);
  }

  @Override
  public String getBindIP() {
    return ClusterDescriptor.getInstance().getConfig().getInternalIp();
  }
}
