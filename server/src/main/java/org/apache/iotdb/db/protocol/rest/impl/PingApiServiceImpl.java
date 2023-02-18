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

package org.apache.iotdb.db.protocol.rest.impl;

import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.db.protocol.rest.PingApiService;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.service.RPCService;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class PingApiServiceImpl extends PingApiService {

  private static final String UNAVAILABLE_SERVICE = "thrift service is unavailable";

  @Override
  public Response tryPing(SecurityContext securityContext) {
    if (RPCService.getInstance().getRPCServiceStatus().equals(ThriftService.STATUS_DOWN)) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(
              new ExecutionStatus()
                  .code(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                  .message(UNAVAILABLE_SERVICE))
          .build();
    }

    return Response.ok()
        .entity(
            new ExecutionStatus()
                .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                .message(TSStatusCode.SUCCESS_STATUS.name()))
        .build();
  }
}
