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

package org.apache.iotdb.commons.lbac;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.i18n.LBACMessages;
import org.apache.iotdb.commons.lbac.operation.LabelAccessType;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.Serial;
import java.util.Set;

/** Thrown when an LBAC label comparison denies access to a protected object. */
@SuppressWarnings("java:S100")
public class LBACAccessDeniedException extends LBACRuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public LBACAccessDeniedException(
      final IAuditEntity auditEntity,
      final String policyName,
      final String subjectValue,
      final Set<String> objectComponentValues,
      final String objectLabelName,
      final LabelAccessType accessType) {
    super(
        String.format(
            LBACMessages
                .EXCEPTION_LBAC_CHECK_FAILED_FOR_USER_ARG_ARG_TO_LABEL_ARG_VALUE_ARG_UNDER_POLICY_ARG_USER_S_MERGED_GRANT_ARG_DOES_NOT_DOMINATE_REQUIRED_ARG_C18D4EA7,
            auditEntity.getUsername(),
            accessType,
            objectLabelName,
            objectComponentValues,
            policyName,
            subjectValue,
            objectComponentValues),
        TSStatusCode.LBAC_ACCESS_DENIED);
  }
}
