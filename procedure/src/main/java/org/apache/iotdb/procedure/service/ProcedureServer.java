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

package org.apache.iotdb.procedure.service;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.procedure.ProcedureExecutor;
import org.apache.iotdb.procedure.conf.ProcedureNodeConfig;
import org.apache.iotdb.procedure.conf.ProcedureNodeConfigDescriptor;
import org.apache.iotdb.procedure.env.ClusterProcedureEnvironment;
import org.apache.iotdb.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.procedure.store.IProcedureStore;
import org.apache.iotdb.procedure.store.ProcedureStore;
import org.apache.iotdb.service.rpc.thrift.ProcedureService;

public class ProcedureServer extends ThriftService implements ProcedureNodeMBean {

  private static final ProcedureNodeConfig conf =
      ProcedureNodeConfigDescriptor.getInstance().getConf();

  private ProcedureScheduler scheduler = new SimpleProcedureScheduler();
  private ClusterProcedureEnvironment env = new ClusterProcedureEnvironment();
  private IProcedureStore store = new ProcedureStore();
  private ProcedureExecutor executor;

  private ProcedureServerProcessor client;

  public ProcedureServer() {
    executor = new ProcedureExecutor(env, store, scheduler);
    client = new ProcedureServerProcessor(executor);
  }

  public void initExecutor() {
    executor.init(conf.getWorkerThreadsCoreSize());
    executor.startWorkers();
    store.setRunning(true);
  }

  public void stop() {
    store.cleanup();
    store.setRunning(false);
    executor.stop();
  }

  public static ProcedureServer getInstance() {
    return ProcedureServerHolder.INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PROCEDURE_SERVICE;
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    this.processor = new ProcedureService.Processor<>(client);
    super.initSyncedServiceImpl(this.client);
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.PROCEDURE_NODE_CLIENT.getName(),
              getBindIP(),
              getBindPort(),
              conf.getRpcMaxConcurrentClientNum(),
              conf.getThriftServerAwaitTimeForStopService(),
              new ProcedureServiceHanlder(client),
              conf.isRpcThriftCompressionEnabled());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.PROCEDURE_NODE_SERVER.getName());
  }

  @Override
  public String getBindIP() {
    return conf.getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return conf.getRpcPort();
  }

  private static class ProcedureServerHolder {
    public static final ProcedureServer INSTANCE = new ProcedureServer();

    private ProcedureServerHolder() {}
  }
}
