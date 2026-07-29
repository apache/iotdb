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

package org.apache.iotdb.commons.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.utils.NodeUrlUtils;

import javax.net.ssl.SSLException;

import java.util.function.Supplier;

public abstract class AbstractAuditLogger {
  private static final long INTERNAL_AUDIT_LOG_USER_ID = 4;
  private static final ThreadLocal<Boolean> RECORDING_TRUSTED_CHANNEL_FAILURE =
      ThreadLocal.withInitial(() -> false);

  public static final String OBJECT_AUTHENTICATION_AUDIT_STR =
      "User %s (ID=%d) requests authority on object %s with result %s";
  public static final String AUDIT_LOG_NODE_ID = "node_id";
  public static final String AUDIT_LOG_USER_ID = "user_id";
  public static final String AUDIT_LOG_USERNAME = "username";
  public static final String AUDIT_LOG_CLI_HOSTNAME = "cli_hostname";
  public static final String AUDIT_LOG_AUDIT_EVENT_TYPE = "audit_event_type";
  public static final String AUDIT_LOG_OPERATION_TYPE = "operation_type";
  public static final String AUDIT_LOG_PRIVILEGE_TYPE = "privilege_type";
  public static final String AUDIT_LOG_PRIVILEGE_LEVEL = "privilege_level";
  public static final String AUDIT_LOG_RESULT = "result";
  public static final String AUDIT_LOG_DATABASE = "database";
  public static final String AUDIT_LOG_SQL_STRING = "sql_string";
  public static final String AUDIT_LOG_LOG = "log";

  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();
  protected static final boolean IS_AUDIT_LOG_ENABLED = CONFIG.isEnableAuditLog();

  public abstract void log(IAuditEntity auditLogFields, Supplier<String> log);

  public boolean noNeedInsertAuditLog(IAuditEntity auditLogFields) {
    return true;
  }

  public void recordObjectAuthenticationAuditLog(
      final IAuditEntity auditEntity, final Supplier<String> auditObject) {
    log(
        auditEntity.setAuditEventType(AuditEventType.OBJECT_AUTHENTICATION),
        () ->
            String.format(
                OBJECT_AUTHENTICATION_AUDIT_STR,
                auditEntity.getUsername(),
                auditEntity.getUserId(),
                auditObject.get(),
                auditEntity.getResult()));
  }

  /**
   * Records a failure of the trusted-channel function.
   *
   * <p>The caller determines the channel direction and supplies the actual initiator and target
   * identifiers. This keeps the audit hook independent of any concrete SSL/TLS implementation.
   */
  public void recordTrustedChannelFailureAuditLog(
      final IAuditEntity auditEntity,
      final Supplier<String> initiator,
      final Supplier<String> target) {
    log(
        auditEntity
            .setAuditEventType(AuditEventType.TRUSTED_CHANNEL_FUNCTION_FAILURE)
            .setAuditLogOperation(AuditLogOperation.CONTROL)
            .setResult(false),
        () ->
            String.format(
                CommonMessages
                    .LOG_TRUSTED_CHANNEL_FUNCTION_FAILED_INITIATOR_ARG_TARGET_ARG_E4C28443,
                initiator.get(),
                target.get()));
  }

  public static boolean isSslFailure(Throwable failure) {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof SSLException) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  public void recordTrustedChannelFailureAuditLogIfNecessary(
      Throwable failure, TEndPoint initiator, TEndPoint target) {
    if (RECORDING_TRUSTED_CHANNEL_FAILURE.get()
        || !isSslFailure(failure)
        || initiator == null
        || target == null) {
      return;
    }

    final String initiatorIdentifier = NodeUrlUtils.convertTEndPointUrl(initiator);
    final String targetIdentifier = NodeUrlUtils.convertTEndPointUrl(target);
    RECORDING_TRUSTED_CHANNEL_FAILURE.set(true);
    try {
      recordTrustedChannelFailureAuditLog(
          new UserEntity(
                  INTERNAL_AUDIT_LOG_USER_ID,
                  User.BUILTIN_INTERNAL_AUDIT_LOG_USERNAME,
                  initiatorIdentifier)
              .setPrivilegeType(PrivilegeType.AUDIT),
          () -> initiatorIdentifier,
          () -> targetIdentifier);
    } catch (RuntimeException auditFailure) {
      if (auditFailure != failure) {
        failure.addSuppressed(auditFailure);
      }
    } finally {
      RECORDING_TRUSTED_CHANNEL_FAILURE.remove();
    }
  }
}
