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

package org.apache.iotdb.rest.protocol.v2.handler;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableId;
import org.apache.iotdb.rest.protocol.handler.QueryRowLimitUtils;
import org.apache.iotdb.rest.protocol.v2.model.ExecutionStatus;
import org.apache.iotdb.rest.protocol.v2.model.PrefixPathList;
import org.apache.iotdb.rest.protocol.v2.model.QueryDataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;

import jakarta.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            new org.apache.iotdb.rest.protocol.model.ExecutionStatus()
                .code(statusCode.getStatusCode())
                .message(statusCode.name()))
        .build();
  }

  public static Response buildExecutionStatusResponse(TSStatus status) {
    return Response.ok()
        .entity(new ExecutionStatus().code(status.getCode()).message(status.getMessage()))
        .build();
  }

  public static void setupTargetDataSet(QueryDataSet dataSet) {
    dataSet.addExpressionsItem("Timeseries");
    dataSet.addExpressionsItem("Value");
    dataSet.addExpressionsItem("DataType");
    dataSet.addDataTypesItem("TEXT");
    dataSet.addDataTypesItem("TEXT");
    dataSet.addDataTypesItem("TEXT");
    dataSet.setValues(new ArrayList<>());
    dataSet.setTimestamps(new ArrayList<>());
  }

  /**
   * Builds the fastLastQuery response directly from cached last values. The number of materialized
   * entries is capped by {@code actualRowSizeLimit}; once exceeded an "exceeded max row size"
   * response is returned instead of materializing the whole cache into heap.
   *
   * @param resultMap last values keyed by table/device/measurement, as filled by the schema cache
   * @param actualRowSizeLimit hard cap on the number of returned rows
   */
  public static Response fillLastValueDataSet(
      Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> resultMap,
      int actualRowSizeLimit) {
    QueryDataSet targetDataSet = new QueryDataSet();
    setupTargetDataSet(targetDataSet);
    List<Object> timeseries = new ArrayList<>();
    List<Object> valueList = new ArrayList<>();
    List<Object> dataTypeList = new ArrayList<>();
    int fetched = 0;

    for (final Map.Entry<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>>
        result : resultMap.entrySet()) {
      for (final Map.Entry<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>
          device2MeasurementLastEntry : result.getValue().entrySet()) {
        final String deviceWithSeparator =
            device2MeasurementLastEntry.getKey().toString() + TsFileConstant.PATH_SEPARATOR;
        for (final Map.Entry<String, Pair<TSDataType, TimeValuePair>> measurementLastEntry :
            device2MeasurementLastEntry.getValue().entrySet()) {
          final TimeValuePair tvPair = measurementLastEntry.getValue().getRight();
          if (tvPair == TableDeviceLastCache.PLACEHOLDER_EMPTY_COLUMN
              || tvPair.getValue() == null) {
            continue;
          }
          if (QueryRowLimitUtils.exceedsLimit(fetched, 1, actualRowSizeLimit)) {
            return QueryRowLimitUtils.buildRowSizeLimitExceededResponse(actualRowSizeLimit);
          }
          valueList.add(tvPair.getValue().getStringValue());
          dataTypeList.add(tvPair.getValue().getDataType().name());
          targetDataSet.addTimestampsItem(tvPair.getTimestamp());
          timeseries.add(deviceWithSeparator + measurementLastEntry.getKey());
          fetched++;
        }
      }
    }
    if (!timeseries.isEmpty()) {
      targetDataSet.addValuesItem(timeseries);
      targetDataSet.addValuesItem(valueList);
      targetDataSet.addValuesItem(dataTypeList);
    }
    return Response.ok().entity(targetDataSet).build();
  }
}
