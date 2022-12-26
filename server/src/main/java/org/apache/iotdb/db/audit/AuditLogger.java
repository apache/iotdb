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
package org.apache.iotdb.db.audit;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.ClientSession;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AuditLogger {
  private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  public static final String LOG = "log";
  public static final String USERNAME = "username";
  public static final String ADDRESS = "address";
  public static final String AUDIT_LOG_DEVICE = "root.__system.audit._%s";

  public static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static final List<AuditLogStorage> auditLogStorageList = config.getAuditLogStorage();

  public static final List<AuditLogOperation> auditLogOperationList = config.getAuditLogOperation();

  public static void log(String log, AuditLogOperation operation) {
    IClientSession currSession = SessionManager.getInstance().getCurrSession();
    String username = "";
    String address = "";
    if (currSession != null) {
      ClientSession clientSession = (ClientSession) currSession;
      String clientAddress = clientSession.getClientAddress();
      int clientPort = ((ClientSession) currSession).getClientPort();
      address = String.format("%s:%s", clientAddress, clientPort);
      username = currSession.getUsername();
    }

    if (auditLogOperationList.contains(operation)) {
      if (auditLogStorageList.contains(AuditLogStorage.IOTDB)) {
        try {
          InsertRowPlan insertRowPlan =
              new InsertRowPlan(
                  new PartialPath(String.format(AUDIT_LOG_DEVICE, username)),
                  DateTimeUtils.currentTime(),
                  new String[] {LOG, USERNAME, ADDRESS},
                  new String[] {log, username, address});
          if (IoTDB.serviceProvider == null) {
            return;
          }
          IoTDB.serviceProvider.getExecutor().insert(insertRowPlan);
        } catch (IllegalPathException | QueryProcessException e) {
          logger.error("write audit log series error,", e);
        }
      }
      if (auditLogStorageList.contains(AuditLogStorage.LOGGER)) {
        AUDIT_LOGGER.info("user:{},address:{},log:{}", username, address, log);
      }
    }
  }

  public static void log(String log, AuditLogOperation operation, boolean isNativeApi) {
    if (isNativeApi) {
      if (config.isEnableAuditLogForNativeInsertApi()) {
        log(log, operation);
      }
    } else {
      log(log, operation);
    }
  }
}
