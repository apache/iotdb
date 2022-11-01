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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogUtils {
  private static final Logger logger = LoggerFactory.getLogger(AuditLogUtils.class);
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  public static final String LOG = "log";
  public static final String TYPE = "type";
  public static final String USERNAME = "username";
  public static final String AUDIT_LOG_DEVICE = "root.__system.audit.'%s'";
  public static final String TYPE_QUERY = "QUERY";
  public static final String TYPE_LOGIN = "LOGIN";
  public static final String TYPE_LOGOUT = "LOGOUT";
  public static final String TYPE_INSERT = "INSERT";
  public static final String TYPE_DELETE = "DELETE";
  public static final String LOG_LEVEL_IOTDB = "IOTDB";
  public static final String LOG_LEVEL_LOGGER = "LOGGER";
  public static final String LOG_LEVEL_NONE = "NONE";

  public static void writeAuditLog(String type, String log) {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    String auditLogStorage = config.getAuditLogStorage();
    IClientSession currSession = SessionManager.getInstance().getCurrSession();
    if (currSession == null) {
      return;
    }
    String username = currSession.getUsername();
    if (LOG_LEVEL_IOTDB.equals(auditLogStorage)) {
      try {
        InsertRowPlan insertRowPlan =
            new InsertRowPlan(
                new PartialPath(String.format(AUDIT_LOG_DEVICE, username)),
                DateTimeUtils.currentTime(),
                new String[] {TYPE, LOG, USERNAME},
                new String[] {type, log, username});
        IoTDB.serviceProvider.getExecutor().insert(insertRowPlan);
      } catch (IllegalPathException | QueryProcessException e) {
        logger.error("write audit log series error,", e);
      }
    } else if (LOG_LEVEL_LOGGER.equals(auditLogStorage)) {
      if (type.contains(TYPE_INSERT)) {
        if (config.isEnableAuditLogWrite()) {
          AUDIT_LOGGER.debug("user:{},type:{},action:{}", username, type, log);
        }
      } else {
        AUDIT_LOGGER.info("user:{},type:{},action:{}", username, type, log);
      }
    }
  }
}
