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

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.CommonUtils;
import org.apache.iotdb.confignode.rpc.thrift.*;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.session.Config;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ConfigNodeClient {
    private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

    private static final int TIMEOUT_MS = 2000;

    private boolean enableRPCCompression;
    
    private ConfigIService.Iface client;

    private TTransport transport;

    private EndPoint configLeader;

    private List<Endpoint> configNodes;

    public ConfigNodeClient() throws BadNodeUrlException{
        //Read config nodes from configuration
        configNodes = CommonUtils.parseNodeUrls(IoTDBDescriptor.getInstance().getConfig().getConfigNodeUrls());
        Random random = new Random();
        Endpoint configNode = configNodes.get(random.nextInt(configNodes.size()));
        client = createClient(configNode);
    }

    public void connect(){

    }

    private boolean reconnect() {
        if(transport != null){
            transport.close();
        }

        try {
            init(endPoint);
        } catch (IoTDBConnectionException e) {
            logger.error("Can't connect with {}", endPoint);
            return false;
        }

        return true;
    }

    private void initTProtocol(TProtocol tProtocol){
        this.iprot_ = tProtocol;
        this.oprot_ = tProtocol;
    }

    private ConfigIService.Client createClient(Endpoint endpoint) throws IoTDBConnectionException {
        TTransport transport;
        try {
            transport =
                    RpcTransportFactory.INSTANCE.getTransport(
                            // as there is a try-catch already, we do not need to use TSocket.wrap
                            endpoint.getIp(), endpoint.getPort(), 2000);
            transport.open();
        } catch (TTransportException e) {
            throw new IoTDBConnectionException(e);
        }

        ConfigIService.Client client;
        if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
            client = new ConfigIService.Client(new TCompactProtocol(transport));
        } else {
            client = new ConfigIService.Client(new TBinaryProtocol(transport));
        }
        return client;
    }

    private void verifySuccess
    public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) {
        try {
            TDataNodeRegisterResp resp = client.registerDataNode(req);
        }catch (TException e){
            if(reconnect()){

            }
        }
        return resp;
    }

    public TDataNodeMessageResp getDataNodesMessage(int dataNodeID) throws TException {
        return super.getDataNodesMessage(dataNodeID);
    }

    public TSStatus setStorageGroup(TSetStorageGroupReq req) throws TException {
        return super.setStorageGroup(req);
    }

    public TSStatus deleteStorageGroup(TDeleteStorageGroupReq req) throws TException {
        return super.deleteStorageGroup(req);
    }

    public TStorageGroupMessageResp getStorageGroupsMessage() throws TException {
        return super.getStorageGroupsMessage();
    }

    public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req) throws TException {
        return super.getSchemaPartition(req);
    }

    public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req) throws TException {
        return super.getOrCreateSchemaPartition(req);
    }

    public TDataPartitionResp getDataPartition(TDataPartitionReq req) throws TException {
        return super.getDataPartition(req);
    }

    public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req) throws TException {
        return super.getOrCreateDataPartition(req);
    }

    public TSStatus operatePermission(TAuthorizerReq req) throws TException {
        return super.operatePermission(req);
    }

    public static class ConfigNodeFactory{
        private static TProtocolFactory protocolFactory = IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()
                ? new TCompactProtocol.Factory()
                : new TBinaryProtocol.Factory();

        public static ConfigNodeClient createClient(EndPoint endpoint) throws TTransportException {
                    return new ConfigNodeClient(protocolFactory.getProtocol(
                            RpcTransportFactory.INSTANCE.getTransport(
                                    new TSocket(
                                            TConfigurationConst.defaultTConfiguration,
                                            endpoint.getIp(),
                                            endpoint.getPort(),
                                            TIMEOUT_MS))),endpoint);
        }
    }
}
