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

import org.apache.iotdb.confignode.rpc.thrift.*;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.session.Config;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConfigNodeClient extends ConfigIService.Client{
    private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

    private static final int TIMEOUT_MS = 2000;

    private boolean enableRPCCompression;
    
    private ConfigIService.Iface client;

    private TTransport transport;

    private static EndPoint configLeader;
    // TestOnly
    public ConfigNodeClient() {}

    public ConfigNodeClient(Session session, EndPoint endPoint, ZoneId zoneId)
            throws IoTDBConnectionException {
//        this.session = session;
//        this.endPoint = endPoint;
//        endPointList.add(endPoint);
//        this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
//        init(endPoint);
    }

    public ConfigNodeClientn(Session session, ZoneId zoneId) throws IoTDBConnectionException {
//        this.session = session;
//        this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
//        this.endPointList = SessionUtils.parseSeedNodeUrls(session.nodeUrls);
//        initClusterConn();
    }

    private void init(EndPoint endPoint) throws IoTDBConnectionException {
        try {
            transport =
                    RpcTransportFactory.INSTANCE.getTransport(
                            // as there is a try-catch already, we do not need to use TSocket.wrap
                            endPoint.getIp(), endPoint.getPort(), TIMEOUT_MS);
            transport.open();
        } catch (TTransportException e) {
            throw new IoTDBConnectionException(e);
        }

        if (enableRPCCompression) {
            initTProtocol(new TCompactProtocol(transport));
        } else {
            initTProtocol(new TBinaryProtocol(transport));
        }
    }

    public void connect(){
        ConfigLeader = new EndPoint();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        ReentrantLock lock1 = new ReentrantLock();
        lock1.
    }

    private boolean reconnect(EndPoint endPoint) {
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

    @Override
    public DataNodeRegisterResp registerDataNode(DataNodeRegisterReq req) throws TException {
        return super.registerDataNode(req);
    }

    @Override
    public Map<Integer, DataNodeMessage> getDataNodesMessage(int dataNodeID) throws TException {
        return super.getDataNodesMessage(dataNodeID);
    }

    @Override
    public TSStatus setStorageGroup(SetStorageGroupReq req) throws TException {
        return super.setStorageGroup(req);
    }

    @Override
    public TSStatus deleteStorageGroup(DeleteStorageGroupReq req) throws TException {
        return super.deleteStorageGroup(req);
    }

    @Override
    public Map<String, StorageGroupMessage> getStorageGroupsMessage() throws TException {
        return super.getStorageGroupsMessage();
    }

    @Override
    public SchemaPartitionInfo getSchemaPartition(GetSchemaPartitionReq req) throws TException {
        return super.getSchemaPartition(req);
    }

    @Override
    public DataPartitionInfo getDataPartition(GetDataPartitionReq req) throws TException {
        return super.getDataPartition(req);
    }

    @Override
    public DeviceGroupHashInfo getDeviceGroupHashInfo() throws TException {
        return super.getDeviceGroupHashInfo();
    }

    @Override
    public DataPartitionInfo applyDataPartition(GetDataPartitionReq req) throws TException {
        return super.applyDataPartition(req);
    }

    @Override
    public SchemaPartitionInfo applySchemaPartition(GetSchemaPartitionReq req) throws TException {
        return super.applySchemaPartition(req);
    }

    @Override
    public DataPartitionInfoResp fetchDataPartitionInfo(FetchDataPartitionReq req) throws TException {
        return super.fetchDataPartitionInfo(req);
    }

    @Override
    public SchemaPartitionInfoResp fetchSchemaPartitionInfo(FetchSchemaPartitionReq req) throws TException {
        return super.fetchSchemaPartitionInfo(req);
    }

    @Override
    public PartitionInfoResp fetchPartitionInfo(FetchPartitionReq req) throws TException {
        return super.fetchPartitionInfo(req);
    }

    @Override
    public TSStatus operatePermission(AuthorizerReq req) throws TException {
        return super.operatePermission(req);
    }
}
