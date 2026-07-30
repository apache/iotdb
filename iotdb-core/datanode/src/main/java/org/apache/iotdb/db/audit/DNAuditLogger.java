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

import org.apache.iotdb.commons.audit.AbstractAuditLogger;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DNAuditLogger extends AbstractAuditLogger {

  // This text matcher is only a fallback. Password-update semantic nodes must clear sql_string
  // before any audit entry is generated.
  private static final Pattern ALTER_USER_PASSWORD_PATTERN =
      Pattern.compile(
          "^(\\s*ALTER\\s+USER\\s+.+?\\s+SET\\s+PASSWORD\\s+)"
              + "(?:(?:U&)?'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\")"
              + "(\\s*;?\\s*)$",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern VALUES_PATTERN = Pattern.compile("(?i)(values)\\([^)]*\\)");

  private Coordinator coordinator;

  private DNAuditLogger() {
    // Empty constructor
  }

  public static DNAuditLogger getInstance() {
    return DNAuditLoggerHolder.INSTANCE;
  }

  public void setCoordinator(Coordinator coordinator) {
    DNAuditLoggerHolder.INSTANCE.coordinator = coordinator;
  }

  @NotNull
  private static InsertRowStatement generateInsertStatement(
      IAuditEntity auditLogFields, String log, PartialPath logDevice) {
    return generateInsertStatement(
        auditLogFields, log, logDevice, CommonDateTimeUtils.currentTime());
  }

  @NotNull
  static InsertRowStatement generateInsertStatement(
      IAuditEntity auditLogFields, String log, PartialPath logDevice, long logTimestamp) {
    String username = auditLogFields.getUsername();
    String address = auditLogFields.getCliHostname();
    AuditEventType type = auditLogFields.getAuditEventType();
    AuditLogOperation operation = auditLogFields.getAuditLogOperation();
    PrivilegeLevel privilegeLevel = PrivilegeLevel.GLOBAL;
    if (auditLogFields.getPrivilegeTypes() != null) {
      for (PrivilegeType privilegeType : auditLogFields.getPrivilegeTypes()) {
        privilegeLevel = judgePrivilegeLevel(privilegeType);
        if (privilegeLevel == PrivilegeLevel.GLOBAL) {
          break;
        }
      }
    }

    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(logDevice);
    insertStatement.setTime(logTimestamp);
    insertStatement.setMeasurements(
        new String[] {
          AUDIT_LOG_USERNAME,
          AUDIT_LOG_CLI_HOSTNAME,
          AUDIT_LOG_AUDIT_EVENT_TYPE,
          AUDIT_LOG_OPERATION_TYPE,
          AUDIT_LOG_PRIVILEGE_TYPE,
          AUDIT_LOG_PRIVILEGE_LEVEL,
          AUDIT_LOG_RESULT,
          AUDIT_LOG_DATABASE,
          AUDIT_LOG_SQL_STRING,
          AUDIT_LOG_LOG
        });
    insertStatement.setAligned(true);
    String sqlString = sanitizeAuditSql(auditLogFields.getSqlString());
    insertStatement.setValues(
        new Object[] {
          new Binary(username == null ? "null" : username, TSFileConfig.STRING_CHARSET),
          new Binary(address == null ? "null" : address, TSFileConfig.STRING_CHARSET),
          new Binary(type == null ? "null" : type.toString(), TSFileConfig.STRING_CHARSET),
          new Binary(
              operation == null ? "null" : operation.toString(), TSFileConfig.STRING_CHARSET),
          new Binary(
              auditLogFields.getPrivilegeTypes() == null
                  ? "null"
                  : auditLogFields.getPrivilegeTypeString(),
              TSFileConfig.STRING_CHARSET),
          new Binary(privilegeLevel.toString(), TSFileConfig.STRING_CHARSET),
          auditLogFields.getResult(),
          new Binary(
              auditLogFields.getDatabase() == null ? "null" : auditLogFields.getDatabase(),
              TSFileConfig.STRING_CHARSET),
          new Binary(sqlString == null ? "null" : sqlString, TSFileConfig.STRING_CHARSET),
          new Binary(log == null ? "null" : log, TSFileConfig.STRING_CHARSET)
        });
    insertStatement.setDataTypes(
        new TSDataType[] {
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.BOOLEAN,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
        });
    return insertStatement;
  }

  static String sanitizeAuditSql(String sqlString) {
    if (sqlString == null) {
      return null;
    }
    if (sqlString.regionMatches(true, 0, "CREATE USER", 0, "CREATE USER".length())) {
      sqlString = String.join(" ", Arrays.asList(sqlString.split(" ")).subList(0, 3)) + " ...";
    }
    Matcher alterUserMatcher = ALTER_USER_PASSWORD_PATTERN.matcher(sqlString);
    if (alterUserMatcher.matches()) {
      sqlString = alterUserMatcher.replaceFirst("$1...$2");
    }
    return VALUES_PATTERN.matcher(sqlString).replaceAll("$1(...)");
  }

  private static PrivilegeLevel judgePrivilegeLevel(PrivilegeType type) {
    if (type == null) {
      return PrivilegeLevel.GLOBAL;
    }
    switch (type) {
      case READ_DATA:
      case DROP:
      case ALTER:
      case CREATE:
      case DELETE:
      case INSERT:
      case SELECT:
      case MANAGE_DATABASE:
      case WRITE_DATA:
      case READ_SCHEMA:
      case WRITE_SCHEMA:
        return PrivilegeLevel.OBJECT;
      default:
        return PrivilegeLevel.GLOBAL;
    }
  }

  public void createViewIfNecessary() {}

  @Override
  public synchronized void log(IAuditEntity auditLogFields, Supplier<String> log) {}

  public void logFromCN(AuditLogFields auditLogFields, String log, int nodeId)
      throws IllegalPathException {}

  private static class DNAuditLoggerHolder {

    private static final DNAuditLogger INSTANCE = new DNAuditLogger();

    private DNAuditLoggerHolder() {}
  }
}
