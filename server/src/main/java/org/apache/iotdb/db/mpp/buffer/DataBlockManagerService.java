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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.exception.runtime.RPCServiceException;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.service.thrift.ThriftService;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService.Processor;

public class DataBlockManagerService extends ThriftService {

  private DataBlockServiceImpl impl;

  @Override
  public ThriftService getImplementation() {
    return DataBlockManagerServiceHolder.INSTANCE;
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    impl = new DataBlockServiceImpl();
    processor = new Processor<>(impl);
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.DATA_BLOCK_MANAGER_CLIENT.getName(),
              getBindIP(),
              getBindPort(),
              // TODO: hard coded maxWorkerThreads & timeoutSecond
              32,
              60,
              new DataBlockManagerServiceThriftHandler(),
              // TODO: hard coded compress strategy
              true);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.DATA_BLOCK_MANAGER_SERVICE.getName());
  }

  @Override
  public String getBindIP() {
    // TODO: hard coded bind IP.
    return "0.0.0.0";
  }

  @Override
  public int getBindPort() {
    // TODO: hard coded bind port.
    return 7777;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.DATA_BLOCK_MANAGER_SERVICE;
  }

  private static class DataBlockManagerServiceHolder {
    private static final DataBlockManagerService INSTANCE = new DataBlockManagerService();

    private DataBlockManagerServiceHolder() {}
  }
}
