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
package org.apache.iotdb.db.queryengine.plan.statement;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AuthorityInformationStatement extends Statement {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorityInformationStatement.class);

  protected PathPatternTree authorityScope = SchemaConstant.ALL_MATCH_SCOPE;

  public PathPatternTree getAuthorityScope() {
    return authorityScope;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    LOGGER.info("=== AUTHORITY INFORMATION STATEMENT PERMISSION CHECK START ===");
    LOGGER.info("User: {}, Statement class: {}", userName, this.getClass().getSimpleName());

    try {
      if (!AuthorityChecker.SUPER_USER.equals(userName)) {
        LOGGER.info("User is not super user, getting authorized path tree for READ_SCHEMA");
        this.authorityScope =
            AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA);
        LOGGER.info("Authority scope set: {}", authorityScope);
      } else {
        LOGGER.info("User is super user, bypassing permission check");
      }
    } catch (AuthException e) {
      LOGGER.error("AuthException during permission check: {}", e.getMessage(), e);
      return new TSStatus(e.getCode().getStatusCode());
    }

    LOGGER.info("AuthorityInformationStatement permission check completed successfully");
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
