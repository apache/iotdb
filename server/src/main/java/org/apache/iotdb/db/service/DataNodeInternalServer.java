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
import org.apache.iotdb.db.service.thrift.DataNodeInternalServerMBean;
import org.apache.iotdb.db.service.thrift.ProcessorWithMetrics;
import org.apache.iotdb.db.service.thrift.handler.DataNodeInternalServiceHandler;
import org.apache.iotdb.db.service.thrift.handler.RPCServiceThriftHandler;
import org.apache.iotdb.db.service.thrift.impl.DataNodeInternalServiceImpl;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.service.rpc.thrift.ManagementIService;
import org.apache.iotdb.service.rpc.thrift.TSIService;

public class DataNodeInternalServer extends ThriftService implements DataNodeInternalServerMBean {

    private DataNodeInternalServiceImpl impl;
    @Override
    public ServiceType getID() {
        return ServiceType.DATA_NODE_INTERNAL_SERVICE;
    }

    @Override
    public ThriftService getImplementation() {
        return DataNodeInternalServerHolder.INSTANCE;
    }

    @Override
    public void initSyncedServiceImpl(Object serviceImpl) {
        impl = (DataNodeInternalServiceImpl) serviceImpl;
        super.initSyncedServiceImpl(serviceImpl);
    }

    @Override
    public void initTProcessor() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        processor = new ManagementIService.Processor<>(impl);
    }

    @Override
    public void initThriftServiceThread() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
        try {
            thriftServiceThread =
                    new ThriftServiceThread(
                            processor,
                            getID().getName(),
                            ThreadName.DATA_NODE_INTERNAL_CLIENT.getName(),
                            getBindIP(),
                            getBindPort(),
                            config.getRpcMaxConcurrentClientNum(),
                            config.getThriftServerAwaitTimeForStopService(),
                            new DataNodeInternalServiceHandler(impl),
                            IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
        } catch (RPCServiceException e) {
            throw new IllegalAccessException(e.getMessage());
        }
        thriftServiceThread.setName(ThreadName.RPC_SERVICE.getName());
    }

    @Override
    public String getBindIP() {
        return IoTDBDescriptor.getInstance().getConfig().getInternalIp();
    }

    @Override
    public int getBindPort() {
        return IoTDBDescriptor.getInstance().getConfig().getInternalPort();
    }

    public static DataNodeInternalServer getInstance() {
        return DataNodeInternalServerHolder.INSTANCE;
    }

    private static class DataNodeInternalServerHolder {

        private static final DataNodeInternalServer INSTANCE = new DataNodeInternalServer();

        private DataNodeInternalServerHolder() {}
    }
}
