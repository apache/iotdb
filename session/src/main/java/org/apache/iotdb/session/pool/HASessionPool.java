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
package org.apache.iotdb.session.pool;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.ClusterStatus;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HASessionPool implements ISessionPool {

    private static final Logger logger = LoggerFactory.getLogger(HASessionPool.class);

    private int maxSize = 0;
    private final long waitToGetSessionTimeoutInMs;

    // parameters for Session constructor
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private int fetchSize;
    private ZoneId zoneId;
    private boolean enableRedirection;

    private Map<String, TEndPoint> deviceIdToEndpoint;

    private int thriftDefaultBufferSize;
    private int thriftMaxFrameSize;

    // The version number of the client which used for compatibility in the server
    private Version version;

    // parameters for Session#open()
    private final int connectionTimeoutInMs;
    private final boolean enableCompression;

    // Redirect-able SessionPool
    private final List<String> nodeUrls;
    private List<String> backupNodeUrls;
    private String backupUser;
    private String backupPassword;
    private SessionPool sessionPool;
    private SessionPool primarySessionPool = null;
    private SessionPool backupSessionPool = null;
    private Integer checkConnectPrimaryClusterS = 10;
    private ScheduledExecutorService checkPrimaryClusterExecutorService =
            Executors.newScheduledThreadPool(1);

    private ClusterStatus clusterStatus;

    private Integer downNodeNumLimit = null;

    public HASessionPool(String host, int port, String user, String password, int maxSize) {
        this(
                host,
                port,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                false,
                null,
                SessionConfig.DEFAULT_REDIRECTION_MODE,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(List<String> nodeUrls, String user, String password, int maxSize) {
        this(
                nodeUrls,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                false,
                null,
                SessionConfig.DEFAULT_REDIRECTION_MODE,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            String host, int port, String user, String password, int maxSize, boolean enableCompression) {
        this(
                host,
                port,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                enableCompression,
                null,
                SessionConfig.DEFAULT_REDIRECTION_MODE,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            List<String> nodeUrls, String user, String password, int maxSize, boolean enableCompression) {
        this(
                nodeUrls,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                enableCompression,
                null,
                SessionConfig.DEFAULT_REDIRECTION_MODE,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            String host,
            int port,
            String user,
            String password,
            int maxSize,
            boolean enableCompression,
            boolean enableRedirection) {
        this(
                host,
                port,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                enableCompression,
                null,
                enableRedirection,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            List<String> nodeUrls,
            String user,
            String password,
            int maxSize,
            boolean enableCompression,
            boolean enableRedirection) {
        this(
                nodeUrls,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                enableCompression,
                null,
                enableRedirection,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            String host, int port, String user, String password, int maxSize, ZoneId zoneId) {
        this(
                host,
                port,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                false,
                zoneId,
                SessionConfig.DEFAULT_REDIRECTION_MODE,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            List<String> nodeUrls, String user, String password, int maxSize, ZoneId zoneId) {
        this(
                nodeUrls,
                user,
                password,
                maxSize,
                SessionConfig.DEFAULT_FETCH_SIZE,
                60_000,
                false,
                zoneId,
                SessionConfig.DEFAULT_REDIRECTION_MODE,
                SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                SessionConfig.DEFAULT_VERSION,
                SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                SessionConfig.DEFAULT_MAX_FRAME_SIZE);
    }

    public HASessionPool(
            String host,
            int port,
            String user,
            String password,
            int maxSize,
            int fetchSize,
            long waitToGetSessionTimeoutInMs,
            boolean enableCompression,
            ZoneId zoneId,
            boolean enableRedirection,
            int connectionTimeoutInMs,
            Version version,
            int thriftDefaultBufferSize,
            int thriftMaxFrameSize) {
        this(
                host,
                port,
                user,
                password,
                null,
                null,
                null,
                maxSize,
                fetchSize,
                waitToGetSessionTimeoutInMs,
                enableCompression,
                zoneId,
                enableRedirection,
                connectionTimeoutInMs,
                version,
                thriftDefaultBufferSize,
                thriftMaxFrameSize);
    }

    public HASessionPool(
            List<String> nodeUrls,
            String user,
            String password,
            int maxSize,
            int fetchSize,
            long waitToGetSessionTimeoutInMs,
            boolean enableCompression,
            ZoneId zoneId,
            boolean enableRedirection,
            int connectionTimeoutInMs,
            Version version,
            int thriftDefaultBufferSize,
            int thriftMaxFrameSize) {
        this(
                nodeUrls,
                user,
                password,
                null,
                null,
                null,
                maxSize,
                fetchSize,
                waitToGetSessionTimeoutInMs,
                enableCompression,
                zoneId,
                enableRedirection,
                connectionTimeoutInMs,
                version,
                thriftDefaultBufferSize,
                thriftMaxFrameSize);
    }

    @SuppressWarnings("squid:S107")
    public HASessionPool(
            String host,
            int port,
            String user,
            String password,
            List<String> backupNodeUrls,
            String backupUser,
            String backupPassword,
            int maxSize,
            int fetchSize,
            long waitToGetSessionTimeoutInMs,
            boolean enableCompression,
            ZoneId zoneId,
            boolean enableRedirection,
            int connectionTimeoutInMs,
            Version version,
            int thriftDefaultBufferSize,
            int thriftMaxFrameSize) {
        this.maxSize = maxSize;
        this.host = host;
        this.port = port;
        this.nodeUrls = null;
        this.user = user;
        this.password = password;
        this.fetchSize = fetchSize;
        this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
        this.enableCompression = enableCompression;
        this.zoneId = zoneId;
        this.enableRedirection = enableRedirection;
        if (this.enableRedirection) {
            deviceIdToEndpoint = new ConcurrentHashMap<>();
        }
        this.connectionTimeoutInMs = connectionTimeoutInMs;
        this.version = version;
        this.thriftDefaultBufferSize = thriftDefaultBufferSize;
        this.thriftMaxFrameSize = thriftMaxFrameSize;
        this.backupNodeUrls = backupNodeUrls;
        this.backupUser = backupUser;
        this.backupPassword = backupPassword;
        usePrimarySessionPool();
        setDownNodeNumLimit();
    }

    public HASessionPool(
            List<String> nodeUrls,
            String user,
            String password,
            List<String> backupNodeUrls,
            String backupUser,
            String backupPassword,
            int maxSize,
            int fetchSize,
            long waitToGetSessionTimeoutInMs,
            boolean enableCompression,
            ZoneId zoneId,
            boolean enableRedirection,
            int connectionTimeoutInMs,
            Version version,
            int thriftDefaultBufferSize,
            int thriftMaxFrameSize) {
        this.maxSize = maxSize;
        this.host = null;
        this.port = -1;
        this.nodeUrls = nodeUrls;
        this.user = user;
        this.password = password;
        this.fetchSize = fetchSize;
        this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
        this.enableCompression = enableCompression;
        this.zoneId = zoneId;
        this.enableRedirection = enableRedirection;
        if (this.enableRedirection) {
            deviceIdToEndpoint = new ConcurrentHashMap<>();
        }
        this.connectionTimeoutInMs = connectionTimeoutInMs;
        this.version = version;
        this.thriftDefaultBufferSize = thriftDefaultBufferSize;
        this.thriftMaxFrameSize = thriftMaxFrameSize;
        this.backupNodeUrls = backupNodeUrls;
        this.backupUser = backupUser;
        this.backupPassword = backupPassword;
        usePrimarySessionPool();
        setDownNodeNumLimit();
    }

    private void usePrimarySessionPool() {
        if (null == primarySessionPool) {
            primarySessionPool =
                    new SessionPool(
                            nodeUrls,
                            user,
                            password,
                            maxSize,
                            fetchSize,
                            waitToGetSessionTimeoutInMs,
                            enableCompression,
                            zoneId,
                            enableRedirection,
                            connectionTimeoutInMs,
                            version,
                            thriftDefaultBufferSize,
                            thriftMaxFrameSize);
        }
        this.clusterStatus = ClusterStatus.PRIMARY_CLUSTER_UP;
        sessionPool = primarySessionPool;
    }

    private void useBackupSessionPool() {
        if (null == backupSessionPool) {
            backupSessionPool =
                    new SessionPool(
                            backupNodeUrls,
                            backupUser,
                            backupPassword,
                            maxSize,
                            fetchSize,
                            waitToGetSessionTimeoutInMs,
                            enableCompression,
                            zoneId,
                            enableRedirection,
                            connectionTimeoutInMs,
                            version,
                            thriftDefaultBufferSize,
                            thriftMaxFrameSize);
        }
        this.clusterStatus = ClusterStatus.BACKUP_CLUSTER_UP;
        sessionPool = backupSessionPool;
    }

    private void setDownNodeNumLimit() {
        String dataRegionConsensusProtocolClass = null;
        String schemaRegionConsensusProtocolClass = null;
        Integer dataReplicationFactor = null;
        Integer schemaReplicationFactor = null;
        int dataLimit = 1;
        int schemaLimit = 1;
        SessionDataSetWrapper wrapper = null;
        try {
            wrapper = primarySessionPool.executeQueryStatement("SHOW VARIABLES");
            while (wrapper.hasNext()) {
                RowRecord rowRecord = wrapper.next();
                if ("DataReplicationFactor".equals(rowRecord.getFields().get(0).getStringValue())) {
                    dataReplicationFactor = Integer.valueOf(rowRecord.getFields().get(1).getStringValue());
                    continue;
                }
                if ("SchemaReplicationFactor".equals(rowRecord.getFields().get(0).getStringValue())) {
                    schemaReplicationFactor = Integer.valueOf(rowRecord.getFields().get(1).getStringValue());
                    continue;
                }
                if ("DataRegionConsensusProtocolClass"
                        .equals(rowRecord.getFields().get(0).getStringValue())) {
                    dataRegionConsensusProtocolClass = rowRecord.getFields().get(1).getStringValue();
                    continue;
                }
                if ("SchemaRegionConsensusProtocolClass"
                        .equals(rowRecord.getFields().get(0).getStringValue())) {
                    schemaRegionConsensusProtocolClass = rowRecord.getFields().get(1).getStringValue();
                }
            }
        } catch (IoTDBConnectionException | StatementExecutionException ignored) {
        } finally {
            if (wrapper != null) {
                primarySessionPool.closeResultSet(wrapper);
            }
        }
        if (null == dataRegionConsensusProtocolClass
                || null == schemaRegionConsensusProtocolClass
                || null == dataReplicationFactor
                || null == schemaReplicationFactor) {
            return;
        }
        if ("org.apache.iotdb.consensus.iot.IoTConsensus".equals(dataRegionConsensusProtocolClass)) {
            dataLimit = dataReplicationFactor;
        } else if ("org.apache.iotdb.consensus.ratis.RatisConsensus"
                .equals(dataRegionConsensusProtocolClass)) {
            dataLimit = (dataReplicationFactor + 1) / 2;
        }

        if ("org.apache.iotdb.consensus.iot.IoTConsensus".equals(schemaRegionConsensusProtocolClass)) {
            schemaLimit = schemaReplicationFactor;
        } else if ("org.apache.iotdb.consensus.ratis.RatisConsensus"
                .equals(schemaRegionConsensusProtocolClass)) {
            schemaLimit = (schemaReplicationFactor + 1) / 2;
        }

        downNodeNumLimit = Math.max(dataLimit, schemaLimit);
    }

    private synchronized boolean changeBackup() {
        if (ClusterStatus.BACKUP_CLUSTER_UP.equals(clusterStatus)) {
            return true;
        }
        if (null == backupNodeUrls) {
            return false;
        }
        if (checkAvailableStatus()) {
            return false;
        }
        useBackupSessionPool();
        checkPrimaryClusterStatus();
        return true;
    }

    private boolean checkAvailableStatus() {
        if (null == downNodeNumLimit) {
            setDownNodeNumLimit();
            if (null == downNodeNumLimit) {
                return false;
            }
        }
        SessionDataSetWrapper wrapper = null;
        try {
            wrapper = primarySessionPool.executeQueryStatement("SHOW DATANODES");
            Integer statusNum = null;
            for (int k = 0; k < wrapper.getColumnNames().size(); k++) {
                if ("Status".equals(wrapper.getColumnNames().get(k))) {
                    statusNum = k;
                    break;
                }
            }
            if (null == statusNum) {
                return false;
            }
            int abnormalNum = 0;
            while (wrapper.hasNext()) {
                RowRecord rowRecord = wrapper.next();
                String status = rowRecord.getFields().get(statusNum).getStringValue();
                if (!"Running".equals(status)) {
                    abnormalNum++;
                }
            }
            wrapper.close();
            return abnormalNum < downNodeNumLimit;
        } catch (IoTDBConnectionException | StatementExecutionException ignored) {
        } finally {
            if (wrapper != null) {
                primarySessionPool.closeResultSet(wrapper);
            }
        }
        return false;
    }

    public void setCheckConnectPrimaryClusterS(Integer second) {
        checkConnectPrimaryClusterS = second;
    }

    @SuppressWarnings("unsafeThreadSchedule")
    private void checkPrimaryClusterStatus() {
        if (checkPrimaryClusterExecutorService.isShutdown()) {
            checkPrimaryClusterExecutorService = Executors.newScheduledThreadPool(1);
        }
        checkPrimaryClusterExecutorService.scheduleAtFixedRate(
                this::checkNode,
                checkConnectPrimaryClusterS,
                checkConnectPrimaryClusterS,
                TimeUnit.SECONDS);
    }

    private void checkNode() {
        if (!checkAvailableStatus()) {
            return;
        }
        usePrimarySessionPool();
        checkPrimaryClusterExecutorService.shutdown();
        logger.info("primary cluster is ready");
    }

    @Override
    public int currentAvailableSize() {
        return sessionPool.currentAvailableSize();
    }

    @Override
    public int currentOccupiedSize() {
        return sessionPool.currentOccupiedSize();
    }

    /** close all connections in the pool */
    @Override
    public synchronized void close() {
        primarySessionPool.close();
        if (null != backupSessionPool) {
            backupSessionPool.close();
        }
        if (null != checkPrimaryClusterExecutorService
                && !checkPrimaryClusterExecutorService.isShutdown()) {
            checkPrimaryClusterExecutorService.shutdown();
        }
    }

    @Override
    public void closeResultSet(SessionDataSetWrapper wrapper) {
        sessionPool.closeResultSet(wrapper);
    }

    /**
     * insert the data of a device. For each timestamp, the number of measurements is the same.
     *
     * @param tablet data batch
     */
    @Override
    public void insertTablet(Tablet tablet)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertTablet(tablet);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertTablet(tablet);
            }
        }
    }

    /**
     * insert the data of a device. For each timestamp, the number of measurements is the same.
     *
     * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
     *
     * @param tablet a tablet data of one device
     * @param sorted whether times in Tablet are in ascending order
     */
    @Override
    public void insertTablet(Tablet tablet, boolean sorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertTablet(tablet, sorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertTablet(tablet, sorted);
            }
        }
    }

    /**
     * insert the data of a device. For each timestamp, the number of measurements is the same.
     *
     * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
     *
     * @param tablet a tablet data of one device
     */
    @Override
    public void insertAlignedTablet(Tablet tablet)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedTablet(tablet);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedTablet(tablet);
            }
        }
    }

    /**
     * insert the data of a device. For each timestamp, the number of measurements is the same.
     *
     * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
     *
     * @param tablet a tablet data of one device
     * @param sorted whether times in Tablet are in ascending order
     */
    @Override
    public void insertAlignedTablet(Tablet tablet, boolean sorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedTablet(tablet, sorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedTablet(tablet, sorted);
            }
        }
    }

    /**
     * use batch interface to insert data
     *
     * @param tablets multiple batch
     */
    @Override
    public void insertTablets(Map<String, Tablet> tablets)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertTablets(tablets);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertTablets(tablets);
            }
        }
    }

    /**
     * use batch interface to insert data
     *
     * @param tablets multiple batch
     */
    @Override
    public void insertAlignedTablets(Map<String, Tablet> tablets)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedTablets(tablets);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedTablets(tablets);
            }
        }
    }

    /**
     * use batch interface to insert aligned data
     *
     * @param tablets multiple batch
     */
    @Override
    public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertTablets(tablets, sorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertTablets(tablets, sorted);
            }
        }
    }

    /**
     * use batch interface to insert aligned data
     *
     * @param tablets multiple batch
     */
    @Override
    public void insertAlignedTablets(Map<String, Tablet> tablets, boolean sorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedTablets(tablets, sorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedTablets(tablets, sorted);
            }
        }
    }

    /**
     * Insert data in batch format, which can reduce the overhead of network. This method is just like
     * jdbc batch insert, we pack some insert request in batch and send them to server If you want
     * improve your performance, please see insertTablet method
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertRecords(
            List<String> deviceIds,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
            }
        }
    }

    /**
     * Insert aligned data in batch format, which can reduce the overhead of network. This method is
     * just like jdbc batch insert, we pack some insert request in batch and send them to server If
     * you want improve your performance, please see insertTablet method.
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedRecords(
            List<String> multiSeriesIds,
            List<Long> times,
            List<List<String>> multiMeasurementComponentsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedRecords(
                    multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedRecords(
                        multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList);
            }
        }
    }

    /**
     * Insert data that belong to the same device in batch format, which can reduce the overhead of
     * network. This method is just like jdbc batch insert, we pack some insert request in batch and
     * send them to server If you want improve your performance, please see insertTablet method
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertRecordsOfOneDevice(
                    deviceId, times, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertRecordsOfOneDevice(
                        deviceId, times, measurementsList, typesList, valuesList);
            }
        }
    }

    /**
     * Insert data that belong to the same device in batch format, which can reduce the overhead of
     * network. This method is just like jdbc batch insert, we pack some insert request in batch and
     * send them to server If you want improve your performance, please see insertTablet method
     *
     * @see Session#insertTablet(Tablet)
     */
    @Deprecated
    @Override
    public void insertOneDeviceRecords(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertOneDeviceRecords(deviceId, times, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertOneDeviceRecords(
                        deviceId, times, measurementsList, typesList, valuesList);
            }
        }
    }

    /**
     * Insert String format data that belong to the same device in batch format, which can reduce the
     * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
     * batch and send them to server If you want improve your performance, please see insertTablet
     * method
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertStringRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<String>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList);
            }
        }
    }

    /**
     * Insert data that belong to the same device in batch format, which can reduce the overhead of
     * network. This method is just like jdbc batch insert, we pack some insert request in batch and
     * send them to server If you want improve your performance, please see insertTablet method
     *
     * @param haveSorted whether the times list has been ordered.
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList,
            boolean haveSorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertRecordsOfOneDevice(
                    deviceId, times, measurementsList, typesList, valuesList, haveSorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertRecordsOfOneDevice(
                        deviceId, times, measurementsList, typesList, valuesList, haveSorted);
            }
        }
    }

    /**
     * Insert data that belong to the same device in batch format, which can reduce the overhead of
     * network. This method is just like jdbc batch insert, we pack some insert request in batch and
     * send them to server If you want improve your performance, please see insertTablet method
     *
     * @param haveSorted whether the times list has been ordered.
     * @see Session#insertTablet(Tablet)
     */
    @Override
    @Deprecated
    public void insertOneDeviceRecords(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList,
            boolean haveSorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertOneDeviceRecords(
                    deviceId, times, measurementsList, typesList, valuesList, haveSorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertOneDeviceRecords(
                        deviceId, times, measurementsList, typesList, valuesList, haveSorted);
            }
        }
    }

    /**
     * Insert String format data that belong to the same device in batch format, which can reduce the
     * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
     * batch and send them to server If you want improve your performance, please see insertTablet
     * method
     *
     * @param haveSorted whether the times list has been ordered.
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertStringRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<String>> valuesList,
            boolean haveSorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertStringRecordsOfOneDevice(
                    deviceId, times, measurementsList, valuesList, haveSorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertStringRecordsOfOneDevice(
                        deviceId, times, measurementsList, valuesList, haveSorted);
            }
        }
    }

    /**
     * Insert aligned data that belong to the same device in batch format, which can reduce the
     * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
     * batch and send them to server If you want improve your performance, please see insertTablet
     * method.
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedRecordsOfOneDevice(
                    deviceId, times, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedRecordsOfOneDevice(
                        deviceId, times, measurementsList, typesList, valuesList);
            }
        }
    }

    /**
     * Insert aligned data as String format that belong to the same device in batch format, which can
     * reduce the overhead of network. This method is just like jdbc batch insert, we pack some insert
     * request in batch and send them to server If you want improve your performance, please see
     * insertTablet method.
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedStringRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<String>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedStringRecordsOfOneDevice(
                    deviceId, times, measurementsList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedStringRecordsOfOneDevice(
                        deviceId, times, measurementsList, valuesList);
            }
        }
    }

    /**
     * Insert aligned data that belong to the same device in batch format, which can reduce the
     * overhead of network. This method is just like jdbc batch insert, we pack some insert request in
     * batch and send them to server If you want improve your performance, please see insertTablet
     * method.
     *
     * @param haveSorted whether the times list has been ordered.
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList,
            boolean haveSorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedRecordsOfOneDevice(
                    deviceId, times, measurementsList, typesList, valuesList, haveSorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedRecordsOfOneDevice(
                        deviceId, times, measurementsList, typesList, valuesList, haveSorted);
            }
        }
    }

    /**
     * Insert aligned data as String format that belong to the same device in batch format, which can
     * reduce the overhead of network. This method is just like jdbc batch insert, we pack some insert
     * request in batch and send them to server If you want improve your performance, please see
     * insertTablet method.
     *
     * @param haveSorted whether the times list has been ordered.
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedStringRecordsOfOneDevice(
            String deviceId,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<String>> valuesList,
            boolean haveSorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedStringRecordsOfOneDevice(
                    deviceId, times, measurementsList, valuesList, haveSorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedStringRecordsOfOneDevice(
                        deviceId, times, measurementsList, valuesList, haveSorted);
            }
        }
    }

    /**
     * Insert data in batch format, which can reduce the overhead of network. This method is just like
     * jdbc batch insert, we pack some insert request in batch and send them to server If you want
     * improve your performance, please see insertTablet method
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertRecords(
            List<String> deviceIds,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<String>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertRecords(deviceIds, times, measurementsList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertRecords(deviceIds, times, measurementsList, valuesList);
            }
        }
    }

    /**
     * Insert aligned data in batch format, which can reduce the overhead of network. This method is
     * just like jdbc batch insert, we pack some insert request in batch and send them to server If
     * you want improve your performance, please see insertTablet method.
     *
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedRecords(
            List<String> multiSeriesIds,
            List<Long> times,
            List<List<String>> multiMeasurementComponentsList,
            List<List<String>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedRecords(
                    multiSeriesIds, times, multiMeasurementComponentsList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedRecords(
                        multiSeriesIds, times, multiMeasurementComponentsList, valuesList);
            }
        }
    }

    /**
     * insert data in one row, if you want improve your performance, please use insertRecords method
     * or insertTablet method
     *
     * @see Session#insertRecords(List, List, List, List, List)
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertRecord(
            String deviceId,
            long time,
            List<String> measurements,
            List<TSDataType> types,
            List<Object> values)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertRecord(deviceId, time, measurements, types, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertRecord(deviceId, time, measurements, types, values);
            }
        }
    }

    /**
     * insert aligned data in one row, if you want improve your performance, please use
     * insertAlignedRecords method or insertTablet method.
     *
     * @see Session#insertAlignedRecords(List, List, List, List, List)
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedRecord(
            String multiSeriesId,
            long time,
            List<String> multiMeasurementComponents,
            List<TSDataType> types,
            List<Object> values)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedRecord(
                    multiSeriesId, time, multiMeasurementComponents, types, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedRecord(
                        multiSeriesId, time, multiMeasurementComponents, types, values);
            }
        }
    }

    /**
     * insert data in one row, if you want improve your performance, please use insertRecords method
     * or insertTablet method
     *
     * @see Session#insertRecords(List, List, List, List, List)
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertRecord(
            String deviceId, long time, List<String> measurements, List<String> values)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertRecord(deviceId, time, measurements, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertRecord(deviceId, time, measurements, values);
            }
        }
    }

    /**
     * insert aligned data in one row, if you want improve your performance, please use
     * insertAlignedRecords method or insertTablet method.
     *
     * @see Session#insertAlignedRecords(List, List, List, List, List)
     * @see Session#insertTablet(Tablet)
     */
    @Override
    public void insertAlignedRecord(
            String multiSeriesId, long time, List<String> multiMeasurementComponents, List<String> values)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.insertAlignedRecord(multiSeriesId, time, multiMeasurementComponents, values);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertTablet(Tablet tablet)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertTablet(tablet);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertTablet(tablet);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertTablet(Tablet tablet, boolean sorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertTablet(tablet, sorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertTablet(tablet, sorted);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertTablets(Map<String, Tablet> tablets)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertTablets(tablets);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertTablets(tablets);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertTablets(tablets, sorted);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertTablets(tablets, sorted);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertRecords(
            List<String> deviceIds,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<String>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertRecords(deviceIds, times, measurementsList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertRecords(deviceIds, times, measurementsList, valuesList);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertRecords(
            List<String> deviceIds,
            List<Long> times,
            List<List<String>> measurementsList,
            List<List<TSDataType>> typesList,
            List<List<Object>> valuesList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertRecords(deviceIds, times, measurementsList, typesList, valuesList);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertRecord(
            String deviceId, long time, List<String> measurements, List<String> values)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertRecord(deviceId, time, measurements, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertRecord(deviceId, time, measurements, values);
            }
        }
    }

    /**
     * This method NOT insert data into database and the server just return after accept the request,
     * this method should be used to test other time cost in client
     */
    @Override
    public void testInsertRecord(
            String deviceId,
            long time,
            List<String> measurements,
            List<TSDataType> types,
            List<Object> values)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.testInsertRecord(deviceId, time, measurements, types, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.testInsertRecord(deviceId, time, measurements, types, values);
            }
        }
    }

    /**
     * delete a timeseries, including data and schema
     *
     * @param path timeseries to delete, should be a whole path
     */
    @Override
    public void deleteTimeseries(String path)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteTimeseries(path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteTimeseries(path);
            }
        }
    }

    /**
     * delete a timeseries, including data and schema
     *
     * @param paths timeseries to delete, should be a whole path
     */
    @Override
    public void deleteTimeseries(List<String> paths)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteTimeseries(paths);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteTimeseries(paths);
            }
        }
    }

    /**
     * delete data <= time in one timeseries
     *
     * @param path data in which time series to delete
     * @param time data with time stamp less than or equal to time will be deleted
     */
    @Override
    public void deleteData(String path, long time)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteData(path, time);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteData(path, time);
            }
        }
    }

    /**
     * delete data <= time in multiple timeseries
     *
     * @param paths data in which time series to delete
     * @param time data with time stamp less than or equal to time will be deleted
     */
    @Override
    public void deleteData(List<String> paths, long time)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteData(paths, time);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteData(paths, time);
            }
        }
    }

    /**
     * delete data >= startTime and data <= endTime in multiple timeseries
     *
     * @param paths data in which time series to delete
     * @param startTime delete range start time
     * @param endTime delete range end time
     */
    @Override
    public void deleteData(List<String> paths, long startTime, long endTime)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteData(paths, startTime, endTime);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteData(paths, startTime, endTime);
            }
        }
    }

    /** @deprecated Use {@link #createDatabase(String)} instead. */
    @Deprecated
    @Override
    public void setStorageGroup(String storageGroupId)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.setStorageGroup(storageGroupId);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.setStorageGroup(storageGroupId);
            }
        }
    }

    /** @deprecated Use {@link #deleteDatabase(String)} instead. */
    @Deprecated
    @Override
    public void deleteStorageGroup(String storageGroup)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteStorageGroup(storageGroup);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteStorageGroup(storageGroup);
            }
        }
    }

    /** @deprecated Use {@link #deleteDatabases(List)} instead. */
    @Deprecated
    @Override
    public void deleteStorageGroups(List<String> storageGroup)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteStorageGroups(storageGroup);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteStorageGroups(storageGroup);
            }
        }
    }

    @Override
    public void createDatabase(String database)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.createDatabase(database);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createDatabase(database);
            }
        }
    }

    @Override
    public void deleteDatabase(String database)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteDatabase(database);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteDatabase(database);
            }
        }
    }

    @Override
    public void deleteDatabases(List<String> databases)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteDatabases(databases);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteDatabases(databases);
            }
        }
    }

    @Override
    public void createTimeseries(
            String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.createTimeseries(path, dataType, encoding, compressor);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createTimeseries(path, dataType, encoding, compressor);
            }
        }
    }

    @Override
    public void createTimeseries(
            String path,
            TSDataType dataType,
            TSEncoding encoding,
            CompressionType compressor,
            Map<String, String> props,
            Map<String, String> tags,
            Map<String, String> attributes,
            String measurementAlias)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.createTimeseries(
                    path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createTimeseries(
                        path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
            }
        }
    }

    @Override
    public void createMultiTimeseries(
            List<String> paths,
            List<TSDataType> dataTypes,
            List<TSEncoding> encodings,
            List<CompressionType> compressors,
            List<Map<String, String>> propsList,
            List<Map<String, String>> tagsList,
            List<Map<String, String>> attributesList,
            List<String> measurementAliasList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.createMultiTimeseries(
                    paths,
                    dataTypes,
                    encodings,
                    compressors,
                    propsList,
                    tagsList,
                    attributesList,
                    measurementAliasList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createMultiTimeseries(
                        paths,
                        dataTypes,
                        encodings,
                        compressors,
                        propsList,
                        tagsList,
                        attributesList,
                        measurementAliasList);
            }
        }
    }

    @Override
    public boolean checkTimeseriesExists(String path)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.checkTimeseriesExists(path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.checkTimeseriesExists(path);
            }
        }
    }

    /**
     * Construct Template at session and create it at server.
     *
     * @see Template
     */
    @Override
    public void createSchemaTemplate(Template template)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        try {

            sessionPool.createSchemaTemplate(template);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createSchemaTemplate(template);
            }
        }
    }

    /**
     * Create a template with flat measurements, not tree structured. Need to specify datatype,
     * encoding and compressor of each measurement, and alignment of these measurements at once.
     *
     * @param measurements flat measurements of the template, cannot contain character dot
     * @param dataTypes datatype of each measurement in the template
     * @param encodings encodings of each measurement in the template
     * @param compressors compression type of each measurement in the template
     * @param isAligned specify whether these flat measurements are aligned
     * @oaram templateName name of template to create
     */
    @Override
    public void createSchemaTemplate(
            String templateName,
            List<String> measurements,
            List<TSDataType> dataTypes,
            List<TSEncoding> encodings,
            List<CompressionType> compressors,
            boolean isAligned)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        try {

            sessionPool.createSchemaTemplate(
                    templateName, measurements, dataTypes, encodings, compressors, isAligned);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createSchemaTemplate(
                        templateName, measurements, dataTypes, encodings, compressors, isAligned);
            }
        }
    }

    /**
     * Compatible for rel/0.12, this method will create an unaligned flat template as a result. Notice
     * that there is no aligned concept in 0.12, so only the first measurement in each nested list
     * matters.
     *
     * @param name name of the template
     * @param schemaNames it works as a virtual layer inside template in 0.12, and makes no difference
     *     after 0.13
     * @param measurements the first measurement in each nested list will constitute the final flat
     *     template
     * @param dataTypes the data type of each measurement, only the first one in each nested list
     *     matters as above
     * @param encodings the encoding of each measurement, only the first one in each nested list
     *     matters as above
     * @param compressors the compressor of each measurement
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    @Deprecated
    @Override
    public void createSchemaTemplate(
            String name,
            List<String> schemaNames,
            List<List<String>> measurements,
            List<List<TSDataType>> dataTypes,
            List<List<TSEncoding>> encodings,
            List<CompressionType> compressors)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.createSchemaTemplate(
                    name, schemaNames, measurements, dataTypes, encodings, compressors);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createSchemaTemplate(
                        name, schemaNames, measurements, dataTypes, encodings, compressors);
            }
        }
    }

    @Override
    public void addAlignedMeasurementsInTemplate(
            String templateName,
            List<String> measurementsPath,
            List<TSDataType> dataTypes,
            List<TSEncoding> encodings,
            List<CompressionType> compressors)
            throws IOException, IoTDBConnectionException, StatementExecutionException {

        try {
            sessionPool.addAlignedMeasurementsInTemplate(
                    templateName, measurementsPath, dataTypes, encodings, compressors);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.addAlignedMeasurementsInTemplate(
                        templateName, measurementsPath, dataTypes, encodings, compressors);
            }
        }
    }

    @Override
    public void addAlignedMeasurementInTemplate(
            String templateName,
            String measurementPath,
            TSDataType dataType,
            TSEncoding encoding,
            CompressionType compressor)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.addAlignedMeasurementInTemplate(
                    templateName, measurementPath, dataType, encoding, compressor);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.addAlignedMeasurementInTemplate(
                        templateName, measurementPath, dataType, encoding, compressor);
            }
        }
    }

    @Override
    public void addUnalignedMeasurementsInTemplate(
            String templateName,
            List<String> measurementsPath,
            List<TSDataType> dataTypes,
            List<TSEncoding> encodings,
            List<CompressionType> compressors)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.addUnalignedMeasurementsInTemplate(
                    templateName, measurementsPath, dataTypes, encodings, compressors);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.addUnalignedMeasurementsInTemplate(
                        templateName, measurementsPath, dataTypes, encodings, compressors);
            }
        }
    }

    @Override
    public void addUnalignedMeasurementInTemplate(
            String templateName,
            String measurementPath,
            TSDataType dataType,
            TSEncoding encoding,
            CompressionType compressor)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.addUnalignedMeasurementInTemplate(
                    templateName, measurementPath, dataType, encoding, compressor);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.addUnalignedMeasurementInTemplate(
                        templateName, measurementPath, dataType, encoding, compressor);
            }
        }
    }

    @Override
    public void deleteNodeInTemplate(String templateName, String path)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.deleteNodeInTemplate(templateName, path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.deleteNodeInTemplate(templateName, path);
            }
        }
    }

    @Override
    public int countMeasurementsInTemplate(String name)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.countMeasurementsInTemplate(name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.countMeasurementsInTemplate(name);
            }
        }
    }

    @Override
    public boolean isMeasurementInTemplate(String templateName, String path)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.isMeasurementInTemplate(templateName, path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.isMeasurementInTemplate(templateName, path);
            }
        }
    }

    @Override
    public boolean isPathExistInTemplate(String templateName, String path)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.isPathExistInTemplate(templateName, path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.isPathExistInTemplate(templateName, path);
            }
        }
    }

    @Override
    public List<String> showMeasurementsInTemplate(String templateName)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.showMeasurementsInTemplate(templateName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.showMeasurementsInTemplate(templateName);
            }
        }
    }

    @Override
    public List<String> showMeasurementsInTemplate(String templateName, String pattern)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.showMeasurementsInTemplate(templateName, pattern);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.showMeasurementsInTemplate(templateName, pattern);
            }
        }
    }

    @Override
    public List<String> showAllTemplates()
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.showAllTemplates();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.showAllTemplates();
            }
        }
    }

    @Override
    public List<String> showPathsTemplateSetOn(String templateName)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.showPathsTemplateSetOn(templateName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.showPathsTemplateSetOn(templateName);
            }
        }
    }

    @Override
    public List<String> showPathsTemplateUsingOn(String templateName)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.showPathsTemplateUsingOn(templateName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.showPathsTemplateUsingOn(templateName);
            }
        }
    }

    @Override
    public void setSchemaTemplate(String templateName, String prefixPath)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.setSchemaTemplate(templateName, prefixPath);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.setSchemaTemplate(templateName, prefixPath);
            }
        }
    }

    @Override
    public void unsetSchemaTemplate(String prefixPath, String templateName)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.unsetSchemaTemplate(prefixPath, templateName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.unsetSchemaTemplate(prefixPath, templateName);
            }
        }
    }

    @Override
    public void dropSchemaTemplate(String templateName)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.dropSchemaTemplate(templateName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.dropSchemaTemplate(templateName);
            }
        }
    }

    @Override
    public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.createTimeseriesUsingSchemaTemplate(devicePathList);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.createTimeseriesUsingSchemaTemplate(devicePathList);
            }
        }
    }

    /**
     * execure query sql users must call closeResultSet(SessionDataSetWrapper) if they do not use the
     * SessionDataSet any more. users do not need to call sessionDataSet.closeOpeationHandler() any
     * more.
     *
     * @param sql query statement
     * @return result set Notice that you must get the result instance. Otherwise a data leakage will
     *     happen
     */
    @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
    @Override
    public SessionDataSetWrapper executeQueryStatement(String sql)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeQueryStatement(sql);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeQueryStatement(sql);
            }
        }
    }

    /**
     * execure query sql users must call closeResultSet(SessionDataSetWrapper) if they do not use the
     * SessionDataSet any more. users do not need to call sessionDataSet.closeOpeationHandler() any
     * more.
     *
     * @param sql query statement
     * @param timeoutInMs the timeout of this query, in milliseconds
     * @return result set Notice that you must get the result instance. Otherwise a data leakage will
     *     happen
     */
    @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
    @Override
    public SessionDataSetWrapper executeQueryStatement(String sql, long timeoutInMs)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeQueryStatement(sql, timeoutInMs);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeQueryStatement(sql, timeoutInMs);
            }
        }
    }

    /**
     * execute non query statement
     *
     * @param sql non query statement
     */
    @Override
    public void executeNonQueryStatement(String sql)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            sessionPool.executeNonQueryStatement(sql);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                sessionPool.executeNonQueryStatement(sql);
            }
        }
    }

    @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
    @Override
    public SessionDataSetWrapper executeRawDataQuery(
            List<String> paths, long startTime, long endTime, long timeOut)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeRawDataQuery(paths, startTime, endTime, timeOut);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeRawDataQuery(paths, startTime, endTime, timeOut);
            }
        }
    }

    @Override
    public SessionDataSetWrapper executeLastDataQuery(List<String> paths, long lastTime, long timeOut)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeLastDataQuery(paths, lastTime, timeOut);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeLastDataQuery(paths, lastTime, timeOut);
            }
        }
    }

    @Override
    public SessionDataSetWrapper executeLastDataQuery(List<String> paths)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeLastDataQuery(paths);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeLastDataQuery(paths);
            }
        }
    }

    @Override
    public SessionDataSetWrapper executeAggregationQuery(
            List<String> paths, List<TAggregationType> aggregations)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeAggregationQuery(paths, aggregations);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeAggregationQuery(paths, aggregations);
            }
        }
    }

    @Override
    public SessionDataSetWrapper executeAggregationQuery(
            List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeAggregationQuery(paths, aggregations, startTime, endTime);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeAggregationQuery(paths, aggregations, startTime, endTime);
            }
        }
    }

    @Override
    public SessionDataSetWrapper executeAggregationQuery(
            List<String> paths,
            List<TAggregationType> aggregations,
            long startTime,
            long endTime,
            long interval)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeAggregationQuery(paths, aggregations, startTime, endTime, interval);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeAggregationQuery(
                        paths, aggregations, startTime, endTime, interval);
            }
        }
    }

    @Override
    public SessionDataSetWrapper executeAggregationQuery(
            List<String> paths,
            List<TAggregationType> aggregations,
            long startTime,
            long endTime,
            long interval,
            long slidingStep)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            return sessionPool.executeAggregationQuery(
                    paths, aggregations, startTime, endTime, interval, slidingStep);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (!changeBackup()) {
                throw e;
            } else {
                return sessionPool.executeAggregationQuery(
                        paths, aggregations, startTime, endTime, interval, slidingStep);
            }
        }
    }

    @Override
    public int getMaxSize() {
        return sessionPool.getMaxSize();
    }

    @Override
    public String getHost() {
        return sessionPool.getHost();
    }

    @Override
    public int getPort() {
        return sessionPool.getPort();
    }

    @Override
    public String getUser() {
        return sessionPool.getUser();
    }

    @Override
    public String getPassword() {
        return sessionPool.getPassword();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        sessionPool.setFetchSize(fetchSize);
    }

    @Override
    public int getFetchSize() {
        return sessionPool.getFetchSize();
    }

    @Override
    public void setTimeZone(String zoneId)
            throws IoTDBConnectionException, StatementExecutionException {
        sessionPool.setTimeZone(zoneId);
    }

    @Override
    public ZoneId getZoneId() {
        return sessionPool.getZoneId();
    }

    @Override
    public long getWaitToGetSessionTimeoutInMs() {
        return sessionPool.getWaitToGetSessionTimeoutInMs();
    }

    @Override
    public boolean isEnableCompression() {
        return sessionPool.isEnableCompression();
    }

    @Override
    public void setEnableRedirection(boolean enableRedirection) {
        sessionPool.setEnableRedirection(enableRedirection);
    }

    @Override
    public boolean isEnableRedirection() {
        return sessionPool.isEnableRedirection();
    }

    @Override
    public void setEnableQueryRedirection(boolean enableQueryRedirection) {
        sessionPool.setEnableQueryRedirection(enableQueryRedirection);
    }

    @Override
    public boolean isEnableQueryRedirection() {
        return sessionPool.isEnableQueryRedirection();
    }

    @Override
    public int getConnectionTimeoutInMs() {
        return sessionPool.getConnectionTimeoutInMs();
    }

    @Override
    public TSBackupConfigurationResp getBackupConfiguration()
            throws IoTDBConnectionException, StatementExecutionException {
        return sessionPool.getBackupConfiguration();
    }

    @Override
    public TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException {
        return sessionPool.fetchAllConnections();
    }

    @Override
    public void setVersion(Version version) {
        sessionPool.setVersion(version);
    }

    @Override
    public Version getVersion() {
        return sessionPool.getVersion();
    }

    @Override
    public void setQueryTimeout(long timeoutInMs) {
        sessionPool.setQueryTimeout(timeoutInMs);
    }

    @Override
    public long getQueryTimeout() {
        return sessionPool.getQueryTimeout();
    }

    public static class Builder {

        private String host = SessionConfig.DEFAULT_HOST;
        private int port = SessionConfig.DEFAULT_PORT;
        private List<String> nodeUrls = null;
        private int maxSize = SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE;
        private String user = SessionConfig.DEFAULT_USER;
        private String password = SessionConfig.DEFAULT_PASSWORD;
        private List<String> backupNodeUrls = null;
        private String backupUser = SessionConfig.DEFAULT_USER;
        private String backupPassword = SessionConfig.DEFAULT_PASSWORD;
        private int fetchSize = SessionConfig.DEFAULT_FETCH_SIZE;
        private long waitToGetSessionTimeoutInMs = 60_000;
        private int thriftDefaultBufferSize = SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY;
        private int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
        private boolean enableCompression = false;
        private ZoneId zoneId = null;
        private boolean enableRedirection = SessionConfig.DEFAULT_REDIRECTION_MODE;
        private int connectionTimeoutInMs = SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS;
        private Version version = SessionConfig.DEFAULT_VERSION;
        private long timeOut = SessionConfig.DEFAULT_QUERY_TIME_OUT;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder nodeUrls(List<String> nodeUrls) {
            this.nodeUrls = nodeUrls;
            return this;
        }

        public Builder maxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder backupNodeUrls(List<String> backupNodeUrls) {
            this.backupNodeUrls = backupNodeUrls;
            return this;
        }

        public Builder backupUser(String backupUser) {
            this.backupUser = backupUser;
            return this;
        }

        public Builder backupPassword(String backupPassword) {
            this.backupPassword = backupPassword;
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder zoneId(ZoneId zoneId) {
            this.zoneId = zoneId;
            return this;
        }

        public Builder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
            this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
            return this;
        }

        public Builder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
            this.thriftDefaultBufferSize = thriftDefaultBufferSize;
            return this;
        }

        public Builder thriftMaxFrameSize(int thriftMaxFrameSize) {
            this.thriftMaxFrameSize = thriftMaxFrameSize;
            return this;
        }

        public Builder enableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return this;
        }

        public Builder enableRedirection(boolean enableRedirection) {
            this.enableRedirection = enableRedirection;
            return this;
        }

        public Builder connectionTimeoutInMs(int connectionTimeoutInMs) {
            this.connectionTimeoutInMs = connectionTimeoutInMs;
            return this;
        }

        public Builder version(Version version) {
            this.version = version;
            return this;
        }

        public Builder timeOut(long timeOut) {
            this.timeOut = timeOut;
            return this;
        }

        public HASessionPool build() {
            if (nodeUrls == null) {
                return new HASessionPool(
                        host,
                        port,
                        user,
                        password,
                        backupNodeUrls,
                        backupUser,
                        backupPassword,
                        maxSize,
                        fetchSize,
                        waitToGetSessionTimeoutInMs,
                        enableCompression,
                        zoneId,
                        enableRedirection,
                        connectionTimeoutInMs,
                        version,
                        thriftDefaultBufferSize,
                        thriftMaxFrameSize);
            } else {
                return new HASessionPool(
                        nodeUrls,
                        user,
                        password,
                        backupNodeUrls,
                        backupUser,
                        backupPassword,
                        maxSize,
                        fetchSize,
                        waitToGetSessionTimeoutInMs,
                        enableCompression,
                        zoneId,
                        enableRedirection,
                        connectionTimeoutInMs,
                        version,
                        thriftDefaultBufferSize,
                        thriftMaxFrameSize);
            }
        }
    }
}
