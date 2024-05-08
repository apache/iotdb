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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.thrift.handler.InfluxDBServiceThriftHandler;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.service.thrift.impl.IInfluxDBServiceWithHandler;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxDBService.Processor;

public class InfluxDBRPCService extends ThriftService implements InfluxDBRPCServiceMBean {
  private IInfluxDBServiceWithHandler impl;

  public static InfluxDBRPCService getInstance() {
    return InfluxDBServiceHolder.INSTANCE;
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getRpcImplClassName()
        .equals(ClientRPCServiceImpl.class.getName())) {
      impl =
          (IInfluxDBServiceWithHandler)
              Class.forName(NewInfluxDBServiceImpl.class.getName()).newInstance();
    } else {
      impl =
          (IInfluxDBServiceWithHandler)
              Class.forName(IoTDBDescriptor.getInstance().getConfig().getInfluxDBImplClassName())
                  .newInstance();
    }
    initSyncedServiceImpl(null);
    processor = new Processor<>(impl);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.INFLUXDB_RPC_PROCESSOR.getName(),
              config.getRpcAddress(),
              config.getInfluxDBRpcPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new InfluxDBServiceThriftHandler(impl),
              IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.INFLUXDB_RPC_SERVICE.getName());
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getInfluxDBRpcPort();
  }

  @Override
  public int getRPCPort() {
    return getBindPort();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INFLUX_SERVICE;
  }

  private static class InfluxDBServiceHolder {

    private static final InfluxDBRPCService INSTANCE = new InfluxDBRPCService();

    private InfluxDBServiceHolder() {}
  }
}
