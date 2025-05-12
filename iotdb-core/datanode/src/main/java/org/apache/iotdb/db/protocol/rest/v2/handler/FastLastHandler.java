package org.apache.iotdb.db.protocol.rest.v2.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.protocol.rest.v2.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.v2.model.PrefixPathList;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;

import javax.ws.rs.core.Response;

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
  }
}
