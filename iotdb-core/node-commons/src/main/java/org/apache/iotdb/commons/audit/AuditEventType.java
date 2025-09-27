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

public enum AuditEventType {
  CHANGE_AUDIT_OPTION,
  AUDIT_STORAGE_FULL,
  GENERATE_KEY,
  DESTROY_KEY,
  EXECUTE_ENCRYPT,
  OBJECT_AUTHENTICATION,
  LBAC_AUTHENTICATION,
  EXPORT_DATA_WITH_LABEL,
  IMPORT_DATA_WITH_LABEL,
  INTEGRITY_CHECK,
  LOGIN_FAIL_MAX_TIMES,
  MODIFY_PASSWD,
  LOGIN,
  LOGOUT,
  LOGIN_FINAL,
  MODIFY_SECURITY_OPTIONS,
  MODIFY_DEFAULT_SECURITY_VALUES,
  REVOKE_FAILED,
  GRANT_ROLE_FAILED,
  LOGIN_RESOURCE_RESTRICT,
  LOGIN_FAILED_TRIES,
  LOGIN_EXCEED_LIMIT,
  SESSION_TIME_EXCEEDED,
  LOGIN_REJECT_IP,
  SESSION_ENCRYPT_FAILED,
  SYSTEM_OPERATION,

  DN_SHUTDOWN;

  @Override
  public String toString() {
    return name();
  }
}
