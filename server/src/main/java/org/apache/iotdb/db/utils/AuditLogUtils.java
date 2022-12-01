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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandalonePartitionFetcher;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.ClientSession;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.service.basic.ServiceProvider.SESSION_MANAGER;
import static org.apache.iotdb.db.sync.pipedata.load.ILoader.SCHEMA_FETCHER;

public class AuditLogUtils {
  private static final Logger logger = LoggerFactory.getLogger(AuditLogUtils.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  public static final String LOG = "log";
  public static final String USERNAME = "username";
  public static final String ADDRESS = "address";
  public static final String AUDIT_LOG_DEVICE = "root.__system.audit.`user=%s`";
  public static final String LOG_LEVEL_IOTDB = "IOTDB";
  public static final String LOG_LEVEL_LOGGER = "LOGGER";
  public static final String LOG_LEVEL_NONE = "NONE";
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  public static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static void writeAuditLog(String log) {
    writeAuditLog(log, false);
  }

  public static void writeAuditLog(String log, boolean enableWrite) {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    String auditLogStorage = config.getAuditLogStorage();
    IClientSession currSession = SessionManager.getInstance().getCurrSession();
    if (currSession == null) {
      return;
    }
    ClientSession clientSession = (ClientSession) currSession;
    String clientAddress = clientSession.getClientAddress();
    int clientPort = ((ClientSession) currSession).getClientPort();
    String address = String.format("%s:%s", clientAddress, clientPort);
    String username = currSession.getUsername();
    if (clientSession.isEnableAudit() || enableWrite) {
      if (LOG_LEVEL_IOTDB.equals(auditLogStorage)) {
        try {
          COORDINATOR.execute(
              generateInsertStatement(log, username, address),
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              "",
              config.isClusterMode()
                  ? ClusterPartitionFetcher.getInstance()
                  : StandalonePartitionFetcher.getInstance(),
              SCHEMA_FETCHER);
        } catch (IllegalPathException | QueryProcessException | IoTDBConnectionException e) {
          logger.error("write audit log series error,", e);
        }
      } else if (LOG_LEVEL_LOGGER.equals(auditLogStorage)) {
        AUDIT_LOGGER.info("user:{},address:{},log:{}", username, address, log);
      }
    }
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      String log, String address, String username)
      throws IoTDBConnectionException, IllegalPathException, QueryProcessException {
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(new PartialPath(String.format(AUDIT_LOG_DEVICE, username)));
    insertStatement.setTime(DateTimeUtils.currentTime());
    insertStatement.setMeasurements(new String[] {LOG, USERNAME, ADDRESS});
    insertStatement.setAligned(false);

    List<TSDataType> types = new ArrayList<>();
    types.add(TSDataType.TEXT);
    types.add(TSDataType.TEXT);
    types.add(TSDataType.TEXT);
    List<Object> values = new ArrayList<>();
    values.add(log);
    values.add(username);
    values.add(address);
    insertStatement.fillValues(SessionUtils.getValueBuffer(types, values));
    return insertStatement;
  }
}
