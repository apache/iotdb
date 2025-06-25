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

package org.apache.iotdb.db.protocol.rest.v2.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.protocol.rest.v2.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.v2.model.PrefixPathList;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;

import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;

public class FastLastHandler {

  public static TSLastDataQueryReq createTSLastDataQueryReq(
      IClientSession clientSession, PrefixPathList prefixPathList) {
    TSLastDataQueryReq req = new TSLastDataQueryReq();
    req.sessionId = clientSession.getId();
    req.paths =
        Collections.singletonList(String.join(".", prefixPathList.getPrefixPaths()) + ".**");
    req.time = Long.MIN_VALUE;
    req.setLegalPathNodes(true);
    return req;
  }

  public static Response buildErrorResponse(TSStatusCode statusCode) {
    return Response.ok()
        .entity(
            new org.apache.iotdb.db.protocol.rest.model.ExecutionStatus()
                .code(statusCode.getStatusCode())
                .message(statusCode.name()))
        .build();
  }

  public static Response buildExecutionStatusResponse(TSStatus status) {
    return Response.ok()
        .entity(new ExecutionStatus().code(status.getCode()).message(status.getMessage()))
        .build();
  }

  public static void setupTargetDataSet(
      org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet dataSet) {
    dataSet.addExpressionsItem("Timeseries");
    dataSet.addExpressionsItem("Value");
    dataSet.addExpressionsItem("DataType");
    dataSet.addDataTypesItem("TEXT");
    dataSet.addDataTypesItem("TEXT");
    dataSet.addDataTypesItem("TEXT");
    dataSet.setValues(new ArrayList<>());
    dataSet.setTimestamps(new ArrayList<>());
  }
}
