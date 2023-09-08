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

public abstract class AuthorityInformationStatement extends Statement {
  protected PathPatternTree authorityScope = SchemaConstant.ALL_MATCH_SCOPE;

  public PathPatternTree getAuthorityScope() {
    return authorityScope;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    try {
      if (!AuthorityChecker.SUPER_USER.equals(userName)) {
        this.authorityScope =
            AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA.ordinal());
      }
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
