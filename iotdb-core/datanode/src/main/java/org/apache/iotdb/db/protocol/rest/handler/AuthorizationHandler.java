/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class AuthorizationHandler {

  public Response checkAuthority(SecurityContext securityContext, Statement statement) {
    String userName = securityContext.getUserPrincipal().getName();
    long userId = AuthorityChecker.getUserId(userName).orElse(-1L);
    TSStatus status =
        AuthorityChecker.checkAuthority(statement, new UserEntity(userId, userName, ""));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return Response.ok()
          .entity(new ExecutionStatus().code(status.getCode()).message(status.getMessage()))
          .build();
    }
    return null;
  }
}
